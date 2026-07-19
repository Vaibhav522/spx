extern crate core_affinity;

mod bucket_allocator;
mod db;
mod queue;
mod transcoder;

use bucket_allocator::BucketAllocator;
use chrono::Utc;
use db::DB;
use queue::{JobQueue, JobUpdate, SyncUpdate};
use transcoder::transcoder;

use dotenvy::dotenv;
use mimalloc::MiMalloc;

// Numa affinity implementation using microsoft MiMalloc
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/*
__config_params__

WORKER__COUNT = usize
WORKER_QUEUE_CAPACITY = usize
STORAGE__BUCKET__LIMIT = usize
MAX__JOB__FETCH__ATTEMPT = usize
OUTPUT__DIRECTORY__PATH = str
OUTPUT_FILE_EXTENSION = str
NATS_CONNECTION_URL: str,
UPDATE_SYNC_THRESHOLD: usize

// Database config
DB__PG__HOST = str
DB__PG__USER = str
DB__PG__PASSWORD = str
DB__PG__DBNAME = str
DB__PG__POOL__MAX_SIZE = usize
DB__PG__POOL__TIMEOUTS__WAIT__SECS = usize
DB__PG__POOL__TIMEOUTS__WAIT__NANOS = usize

*/

use config::{Config, ConfigError};
use serde::Deserialize;
use std::{
    fmt::Debug,
    sync::{Arc, atomic::Ordering},
};
use tokio;

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

#[tokio::main]
async fn main() {
    dotenv().ok();
    let app_config = load_configs()?;

    let mut db = Arc::new(DB::initalize()?);

    // initalizing and connecting to nats
    let nats_client = async_nats::connect(app_config.NATS_CONNECTION_URL).await?;
    // Create a JetStream context.
    let jetstream = async_nats::jetstream::new(nats_client);

    let mut job_queue = Arc::new(
        JobQueue::initalize(
            app_config.WORKER_QUEUE_CAPACITY,
            Arc::clone(&db),
            app_config.MAX_JOB_FETCH_ATTEMPT,
            jetstream,
        )
        .await?,
    );

    let mut sync_update_queue = Arc::new(
        SyncUpdate::initalize(app_config.UPDATE_SYNC_THRESHOLD, Arc::clone(&db), jetstream).await?,
    );

    let mut bucket_allocator = Arc::new(
        BucketAllocator::initalize(
            app_config.STORAGE_BUCKET_LIMIT,
            app_config.OUTPUT_DIRECTORY_PATH,
        )
        .expect("Failed"),
    );

    let core_ids = core_affinity::get_core_ids().expect("Failed to retrieve core IDs");

    if app_config.WORKER_COUNT > core_ids.len() {
        return;
    }

    let output_file_extension = app_config.OUTPUT_FILE_EXTENSION;

    job_queue.check_job_pull().await;
    bucket_allocator.scan_directory();

    for i in 0..app_config.WORKER_COUNT {
        let db_clone = Arc::clone(&db);
        let job_queue_clone = Arc::clone(&job_queue);
        let sync_update_queue_clone = Arc::clone(&sync_update_queue);
        let bucket_allocator_clone = Arc::new(&bucket_allocator);

        tokio::spawn(async move {
            // core affinity of workers
            if core_affinity::set_for_current(core_ids[i]) {
                loop {
                    if job_queue_clone.queue_completed.load(Ordering::Acquire) == true {
                        break;
                    }
                    let start_time = std::time::SystemTime::now();

                    let worker_job = job_queue_clone.get_job().await?;
                    let allocated_bucket = bucket_allocator_clone.allocate_bucket()?;

                    let allocated_path = allocated_bucket.get_bucket_path();
                    let file_sha = worker_job.get_file_sha();

                    allocated_path.push(file_sha);
                    allocated_path.set_extension(output_file_extension);

                    let mut tmpfile: tempfile::NamedTempFile = tempfile::NamedTempFile::new()
                        .map_err(|_: std::io::Error| "{e}".to_string())?;

                    if let Ok(transcoded) = transcoder(
                        worker_job.get_target_file_path(),
                        allocated_path,
                        &file_sha,
                        &mut tmpfile,
                    ) {
                        let elapsed: std::time::Duration = start_time.elapsed().unwrap();

                        tmpfile
                            .flush()
                            .map_err(|_| "Failed to flush temp file".to_string())?;
                        tmpfile
                            .persist(destination_path)
                            .map_err(|e| format!("Failed to persist output file: {e}"))?;

                        let output_path = allocated_path.to_str().map(String::from).unwrap();

                        allocated_bucket.increment_elm();
                        sync_update_queue
                            .enqueue_update(JobUpdate::new(
                                file_sha,
                                Some(output_path),
                                true,
                                Utc::now(),
                                transcoded.file_size,
                                elapsed,
                                None,
                            ))
                            .await;
                    } else {
                        let elapsed: std::time::Duration = start_time.elapsed().unwrap();

                        sync_update_queue
                            .enqueue_update(JobUpdate::new(
                                file_sha,
                                None,
                                false,
                                Utc::now(),
                                None,
                                elapsed,
                                Some(String::from("Error while_ transcoding")),
                            ))
                            .await;
                    }
                }
                todo!()
            } else {
                todo!()
            }
        });
    }
}

// emsure the worker count is always lesser then core_count.
//
