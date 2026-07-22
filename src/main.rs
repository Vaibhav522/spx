use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;

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
mod resampler;
mod temp_allocator;

use bucket_allocator::BucketAllocator;
use db::DB;
use queue::{JobQueue, JobUpdate, SyncUpdate};
use resampler::resampler;
use temp_allocator::TempHolder;

// ─── Allocator ────────────────────────────────────────────────────

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// ─── Config ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AppConfig {
    worker_count: usize,
    worker_queue_capacity: usize,
    storage_bucket_limit: usize,
    max_job_fetch_attempt: usize,
    output_directory_path: String,
    output_file_extension: String,
    nats_connection_url: String,
    update_sync_threshold: usize,
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

    tracing_log::LogTracer::init().expect("failed to initialize log tracer");

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stderr_layer)
        .with(file_layer)
        .init();

    // `_log_guard` MUST be held for the lifetime of the program.
    // Dropping it flushes + closes the background writer thread.
    // It lives in `main()`'s scope, so it outlasts all workers.

    // ── Config ────────────────────────────────────────────────
    let app_config = load_configs()?;
    info!(
        workers = app_config.worker_count,
        queue_cap = app_config.worker_queue_capacity,
        bucket_limit = app_config.storage_bucket_limit,
        output_dir = %app_config.output_directory_path,
        output_ext = %app_config.output_file_extension,
        "starting spx transcoder"
    );

    // ── DB ────────────────────────────────────────────────────
    info!("initializing database pool");
    let db = Arc::new(DB::initalize()?);

    // ── NATS ──────────────────────────────────────────────────
    info!(url = %app_config.nats_connection_url, "connecting to NATS");
    let nats_client: async_nats::Client = async_nats::connect(&app_config.nats_connection_url)
        .await
        .context("Failed to connect to NATS")?;
    let jetstream = async_nats::jetstream::new(nats_client);

    // ── Job queue ─────────────────────────────────────────────
    info!("initializing job queue (NATS-backed)");
    let job_queue = Arc::new(
        JobQueue::initalize(
            app_config.worker_queue_capacity,
            Arc::clone(&db),
            app_config.max_job_fetch_attempt,
            jetstream.clone(),
        )
        .await?,
    );

    // ── Sync-update queue ─────────────────────────────────────
    info!(
        threshold = app_config.update_sync_threshold,
        "initializing sync-update queue"
    );
    let sync_update_queue = Arc::new(
        SyncUpdate::initalize(app_config.update_sync_threshold, Arc::clone(&db), jetstream).await?,
    );

    // ── Bucket allocator ──────────────────────────────────────
    info!(
        limit = app_config.storage_bucket_limit,
        dir = %app_config.output_directory_path,
        "initializing bucket allocator"
    );
    let mut bucket_allocator = BucketAllocator::initalize(
        app_config.storage_bucket_limit,
        app_config.output_directory_path,
    )?;

    bucket_allocator
        .scan_directory()
        .context("Failed to scan output directory")?;
    info!("output directory scan complete");

    let bucket_allocator = Arc::new(Mutex::new(bucket_allocator));

    // ── Core affinity ─────────────────────────────────────────
    let core_ids = core_affinity::get_core_ids().context("Failed to retrieve core IDs")?;

    if app_config.worker_count > core_ids.len() {
        error!(
            workers = app_config.worker_count,
            cores = core_ids.len(),
            "worker count exceeds available cores"
        );
        anyhow::bail!(
            "WORKER_COUNT ({}) exceeds available cores ({})",
            app_config.worker_count,
            core_ids.len()
        );
    }
    info!(
        workers = app_config.worker_count,
        cores = core_ids.len(),
        "core affinity plan"
    );

    // ── Initial job pull ──────────────────────────────────────
    if let Err(e) = job_queue.check_job_pull().await {
        warn!("initial job pull returned empty or failed: {e:#}");
    } else {
        info!("initial job pull complete");
    }

    let output_ext = app_config.output_file_extension;

    // ── Spawn workers ─────────────────────────────────────────
    let mut handles = JoinSet::new();

    for i in 0..app_config.worker_count {
        let core_ids = core_ids.clone();
        let job_queue = Arc::clone(&job_queue);
        let sync_update_queue = Arc::clone(&sync_update_queue);
        let bucket_allocator_clone = Arc::clone(&bucket_allocator);
        let output_ext = output_ext.clone();

        handles.spawn(async move {
            // ── Pin to core ───────────────────────────────
            if core_affinity::set_for_current(core_ids[i]) {
                debug!(worker = i, core_id = core_ids[i].id, "pinned");
            } else {
                return;
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

                    let mut temp_holder =
                        match TempHolder::new(file_sha.clone(), output_ext.clone()) {
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

                    // take a cloned, owned File so we don't move a borrow out of temp_holder
                    let temp_file = match temp_holder.get_holder() {
                        Ok(f) => match f.try_clone() {
                            Ok(cloned) => cloned,
                            Err(e) => {
                                error!("clone tempfile: {e}");
                                sync_update_queue
                                    .enqueue_update(JobUpdate::new(
                                        file_sha.clone(),
                                        None,
                                        false,
                                        Utc::now(),
                                        None,
                                        start.elapsed(),
                                        Some(format!("tempfile clone: {e}")),
                                    ))
                                    .await
                                    .ok();
                                return;
                            }
                        },
                        Err(e) => {
                            error!("get_holder: {e}");
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

                    let transcode_result = {
                        tokio::task::spawn_blocking(move || {
                            let mut temp_file = temp_file;
                            resampler(input_path.clone(), &mut temp_file)
                        })
                        .await
                    };
                    let elapsed = start.elapsed();

                    match transcode_result {
                        Ok(Ok(metadata)) => match temp_holder
                            .persist(Arc::clone(&bucket_allocator_clone))
                            .await
                        {
                            Ok(output_path_str) => {
                                sync_update_queue
                                    .enqueue_update(JobUpdate::new(
                                        file_sha,
                                        Some(output_path_str),
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
                        },
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
                    };
                }
                .instrument(job_span)
                .await;
            }

            info!(worker = i, "worker finished");
            return;
        });
    }

    info!(
        count = app_config.worker_count,
        "all workers spawned; waiting…"
    );

    // ── Wait for all workers ────────────────────────────────────
    while let Some(result) = handles.join_next().await {
        match result {
            Ok(()) => debug!("worker joined cleanly"),
            Err(join_err) => error!("worker panicked: {join_err:#}"),
        }
    }

    info!("all workers finished; shutting down");
    Ok(())
}
