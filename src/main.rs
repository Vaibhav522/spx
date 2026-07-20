use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use chrono::Utc;
use dotenvy::dotenv;
use mimalloc::MiMalloc;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, error, info, info_span, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use config::{Config, ConfigError};
use serde::Deserialize;

mod bucket_allocator;
mod db;
mod queue;
mod transcoder;

use bucket_allocator::BucketAllocator;
use db::DB;
use queue::{JobQueue, JobUpdate, SyncUpdate};
use transcoder::transcoder;

// ─── Allocator ────────────────────────────────────────────────────

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// ─── Config ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AppConfig {
    WORKER_COUNT: usize,
    WORKER_QUEUE_CAPACITY: usize,
    STORAGE_BUCKET_LIMIT: usize,
    MAX_JOB_FETCH_ATTEMPT: usize,
    OUTPUT_DIRECTORY_PATH: String,
    OUTPUT_FILE_EXTENSION: String,
    NATS_CONNECTION_URL: String,
    UPDATE_SYNC_THRESHOLD: usize,
}

fn load_configs() -> Result<AppConfig, ConfigError> {
    let config = Config::builder()
        .add_source(config::Environment::default())
        .build()?;
    config.try_deserialize()
}

// ─── main ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Bootstrap: env + tracing ──────────────────────────────
    dotenv().ok();

    // Rolling file: hourly rotation, keep 7 days, in ./logs/
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::HOURLY)
        .filename_prefix("spx")
        .filename_suffix("log")
        .max_log_files(24 * 7) // one week of hourly files
        .build("./logs")
        .expect("failed to create ./logs directory");

    // Non-blocking: disk writes happen on a background thread
    let (non_blocking, _log_guard) = tracing_appender::non_blocking(file_appender);

    // ── Layer stack ───────────────────────────────────────────
    // stderr:  human-readable, level from RUST_LOG (default: info)
    // file:    JSON, level from RUST_LOG (default: debug)
    // log:     bridge legacy `log::` calls (from deadpool, tokio, …)

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stderr_layer = fmt::layer()
        .with_target(false) // module path is noisy
        .with_writer(std::io::stderr);

    let file_layer = fmt::layer().json().with_writer(non_blocking);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stderr_layer)
        .with(file_layer)
        .with(tracing_log::LogTracer::default()) // ← bridge
        .init();

    // `_log_guard` MUST be held for the lifetime of the program.
    // Dropping it flushes + closes the background writer thread.
    // It lives in `main()`'s scope, so it outlasts all workers.

    // ── Config ────────────────────────────────────────────────
    let app_config = load_configs()?;
    info!(
        workers = app_config.WORKER_COUNT,
        queue_cap = app_config.WORKER_QUEUE_CAPACITY,
        bucket_limit = app_config.STORAGE_BUCKET_LIMIT,
        output_dir = %app_config.OUTPUT_DIRECTORY_PATH,
        output_ext = %app_config.OUTPUT_FILE_EXTENSION,
        "starting spx transcoder"
    );

    // ── DB ────────────────────────────────────────────────────
    info!("initializing database pool");
    let db = Arc::new(DB::initalize()?);

    // ── NATS ──────────────────────────────────────────────────
    info!(url = %app_config.NATS_CONNECTION_URL, "connecting to NATS");
    let nats_client = async_nats::connect(&app_config.NATS_CONNECTION_URL)
        .await
        .context("Failed to connect to NATS")?;
    let jetstream = async_nats::jetstream::new(nats_client);

    // ── Job queue ─────────────────────────────────────────────
    info!("initializing job queue (NATS-backed)");
    let job_queue = Arc::new(
        JobQueue::initalize(
            app_config.WORKER_QUEUE_CAPACITY,
            Arc::clone(&db),
            app_config.MAX_JOB_FETCH_ATTEMPT,
            jetstream.clone(),
        )
        .await?,
    );

    // ── Sync-update queue ─────────────────────────────────────
    info!(
        threshold = app_config.UPDATE_SYNC_THRESHOLD,
        "initializing sync-update queue"
    );
    let sync_update_queue = Arc::new(
        SyncUpdate::initalize(app_config.UPDATE_SYNC_THRESHOLD, Arc::clone(&db), jetstream).await?,
    );

    // ── Bucket allocator ──────────────────────────────────────
    info!(
        limit = app_config.STORAGE_BUCKET_LIMIT,
        dir = %app_config.OUTPUT_DIRECTORY_PATH,
        "initializing bucket allocator"
    );
    let mut bucket_allocator = BucketAllocator::initalize(
        app_config.STORAGE_BUCKET_LIMIT,
        app_config.OUTPUT_DIRECTORY_PATH,
    )?;

    bucket_allocator
        .scan_directory()
        .context("Failed to scan output directory")?;
    info!("output directory scan complete");

    let bucket_allocator = Arc::new(Mutex::new(bucket_allocator));

    // ── Core affinity ─────────────────────────────────────────
    let core_ids = core_affinity::get_core_ids().context("Failed to retrieve core IDs")?;

    if app_config.WORKER_COUNT > core_ids.len() {
        error!(
            workers = app_config.WORKER_COUNT,
            cores = core_ids.len(),
            "worker count exceeds available cores"
        );
        anyhow::bail!(
            "WORKER_COUNT ({}) exceeds available cores ({})",
            app_config.WORKER_COUNT,
            core_ids.len()
        );
    }
    info!(
        workers = app_config.WORKER_COUNT,
        cores = core_ids.len(),
        "core affinity plan"
    );

    // ── Initial job pull ──────────────────────────────────────
    if let Err(e) = job_queue.check_job_pull().await {
        warn!("initial job pull returned empty or failed: {e:#}");
    } else {
        info!("initial job pull complete");
    }

    let output_ext = app_config.OUTPUT_FILE_EXTENSION;

    // ── Spawn workers ─────────────────────────────────────────
    let mut handles = JoinSet::new();

    for i in 0..app_config.WORKER_COUNT {
        let db = Arc::clone(&db);
        let job_queue = Arc::clone(&job_queue);
        let sync_update_queue = Arc::clone(&sync_update_queue);
        let bucket_allocator = Arc::clone(&bucket_allocator);
        let output_ext = output_ext.clone();

        handles.spawn(async move {
            // ── Pin to core ───────────────────────────────
            match core_affinity::set_for_current(core_ids[i]) {
                Ok(_) => debug!(worker = i, core_id = core_ids[i].id, "pinned"),
                Err(e) => warn!(
                    worker = i,
                    core_id = core_ids[i].id,
                    "pin failed: {e:#}; running unpinned"
                ),
            }

            info!(worker = i, "worker started");

            loop {
                if job_queue.queue_completed.load(Ordering::Acquire) {
                    info!(worker = i, "queue_completed; shutting down");
                    break;
                }

                let start = std::time::Instant::now();

                // ── Get job ───────────────────────────────
                let worker_job = match job_queue.get_job().await {
                    Ok(j) => j,
                    Err(e) => {
                        warn!(worker = i, "get_job: {e:#}");
                        if job_queue.queue_completed.load(Ordering::Acquire) {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                };

                let file_sha = worker_job.get_file_sha();
                let input_path = worker_job.get_target_file_path();

                // ── Span: all logs for this job carry the
                //     same sha + worker fields ──────────────
                let job_span = info_span!(
                    "job",
                    worker = i,
                    sha = %file_sha,
                );

                async {
                    debug!(input = %input_path, "got job");

                    // ── Temp file ─────────────────────────
                    let mut tmpfile = match tempfile::NamedTempFile::new() {
                        Ok(f) => f,
                        Err(e) => {
                            error!("tempfile: {e}");
                            sync_update_queue
                                .enqueue_update(JobUpdate::new(
                                    file_sha.clone(),
                                    None,
                                    false,
                                    Utc::now(),
                                    None,
                                    start.elapsed(),
                                    Some(format!("tempfile: {e}")),
                                ))
                                .await
                                .ok();
                            return;
                        }
                    };

                    // ── Transcode ─────────────────────────
                    debug!("transcoding…");

                    let transcode_result = {
                        let input_path = input_path.clone();
                        tokio::task::spawn_blocking(move || transcoder(input_path, &mut tmpfile))
                            .await
                    };

                    let elapsed = start.elapsed();

                    match transcode_result {
                        Ok(Ok(metadata)) => {
                            debug!(
                                size = metadata.resampled_file_size,
                                elapsed_ms = elapsed.as_millis(),
                                "transcode ok"
                            );

                            // ── Allocate bucket ───────────
                            let mut allocator = bucket_allocator
                                .lock()
                                .expect("bucket_allocator lock poisoned");

                            let bucket = match allocator.allocate_bucket() {
                                Ok(b) => b,
                                Err(e) => {
                                    drop(allocator);
                                    error!("allocate_bucket: {e:#}");
                                    sync_update_queue
                                        .enqueue_update(JobUpdate::new(
                                            file_sha.clone(),
                                            None,
                                            false,
                                            Utc::now(),
                                            None,
                                            elapsed,
                                            Some(format!("allocate_bucket: {e}")),
                                        ))
                                        .await
                                        .ok();
                                    return;
                                }
                            };

                            let mut output_path = bucket.get_bucket_path();
                            output_path.push(&file_sha);
                            output_path.set_extension(&output_ext);

                            if let Err(e) = tmpfile.flush() {
                                warn!("flush: {e}");
                            }

                            match tmpfile.persist(&output_path) {
                                Ok(_) => {
                                    bucket.increment_elm();
                                    drop(allocator);

                                    let output_str = output_path
                                        .to_str()
                                        .map(String::from)
                                        .unwrap_or_else(|| output_path.display().to_string());

                                    info!(
                                        output = %output_str,
                                        size = metadata.resampled_file_size,
                                        elapsed_ms = elapsed.as_millis(),
                                        "job completed"
                                    );

                                    sync_update_queue
                                        .enqueue_update(JobUpdate::new(
                                            file_sha,
                                            Some(output_str),
                                            true,
                                            Utc::now(),
                                            Some(metadata.resampled_file_size as usize),
                                            elapsed,
                                            None,
                                        ))
                                        .await
                                        .ok();
                                }
                                Err(e) => {
                                    error!(path = %output_path.display(), "persist: {e:#}");
                                    sync_update_queue
                                        .enqueue_update(JobUpdate::new(
                                            file_sha,
                                            None,
                                            false,
                                            Utc::now(),
                                            None,
                                            elapsed,
                                            Some(format!("persist: {e}")),
                                        ))
                                        .await
                                        .ok();
                                }
                            }
                        }

                        Ok(Err(e)) => {
                            error!(elapsed_ms = elapsed.as_millis(), "transcode: {e:#}");
                            sync_update_queue
                                .enqueue_update(JobUpdate::new(
                                    file_sha,
                                    None,
                                    false,
                                    Utc::now(),
                                    None,
                                    elapsed,
                                    Some(format!("{e:#}")),
                                ))
                                .await
                                .ok();
                        }

                        Err(join_err) => {
                            error!("spawn_blocking panicked: {join_err:#}");
                            sync_update_queue
                                .enqueue_update(JobUpdate::new(
                                    file_sha,
                                    None,
                                    false,
                                    Utc::now(),
                                    None,
                                    elapsed,
                                    Some(format!("spawn_blocking: {join_err}")),
                                ))
                                .await
                                .ok();
                        }
                    }
                }
                .instrument(job_span)
                .await;
            }

            info!(worker = i, "worker finished");
            Ok::<_, anyhow::Error>(())
        });
    }

    info!(
        count = app_config.WORKER_COUNT,
        "all workers spawned; waiting…"
    );

    // ── Wait for all workers ────────────────────────────────────
    while let Some(result) = handles.join_next().await {
        match result {
            Ok(Ok(())) => debug!("worker joined cleanly"),
            Ok(Err(e)) => error!("worker exited with error: {e:#}"),
            Err(join_err) => error!("worker panicked: {join_err:#}"),
        }
    }

    info!("all workers finished; shutting down");
    Ok(())
}
