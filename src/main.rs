extern crate core_affinity;

mod db;
mod queue;
mod transcoder;
mod bucket_allocator;

use db::DB;
use queue::JobQueue;
use transcoder::transcoder;
use bucket_allocator::BucketAllocator;


use dotenvy::dotenv;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


/*
__config_params__

WORKER__COUNT = usize
QUEUE__CAPACITY = usize
STORAGE__BUCKET__LIMIT = usize
MAX__JOB__FETCH__ATTEMPT = usize
OUTPUT__DIRECTORY__PATH = str
OUTPUT_FILE_EXTENSION = str

// Database config
DB__PG__HOST = str
DB__PG__USER = str 
DB__PG__PASSWORD = str
DB__PG__DBNAME = str
DB__PG__POOL__MAX_SIZE = usize
DB__PG__POOL__TIMEOUTS__WAIT__SECS = usize
DB__PG__POOL__TIMEOUTS__WAIT__NANOS = usize

*/

use tokio;
use serde::{Deserialize};
use config::{Config, ConfigError};
use std::{fmt::Debug, path::PathBuf, sync::{Arc, atomic::Ordering}, thread};


#[derive(Debug, Deserialize)]
struct AppConfig {
    WORKER_COUNT: usize,
    QUEUE_CAPACITY: usize,
    STORAGE_BUCKET_LIMIT: usize,
    MAX_JOB_FETCH_ATTEMPT: usize,
    OUTPUT_DIRECTORY_PATH: String,
    OUTPUT_FILE_EXTENSION: String,
    NATS_CONNECTION_URL: String,
}


fn load_configs() -> Result<AppConfig, ConfigError> {
    let config = Config::builder().add_source(config::Environment::default()).build()?;
    config.try_deserialize()
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let app_config = load_configs()?;
    
    let mut db = Arc::new(
        DB::initalize()?
    );

    // initalizing and connecting to nats
    let nats_client = async_nats::connect(app_config.NATS_CONNECTION_URL).await?;
    // Create a JetStream context.
    let jetstream = async_nats::jetstream::new(nats_client);

    let mut queue = Arc::new(
        JobQueue::new(
            app_config.QUEUE_CAPACITY, 
            db, 
            app_config.MAX_JOB_FETCH_ATTEMPT,
            jetstream
        )
    );

    let mut bucket_allocator = Arc::new(
        BucketAllocator::new(app_config.STORAGE_BUCKET_LIMIT, app_config.OUTPUT_DIRECTORY_PATH).expect("Failed")
    );
    
    
    let core_ids = core_affinity::get_core_ids().expect("Failed to retrieve core IDs");
    
    if app_config.WORKER_COUNT > core_ids.len() {
        return 
    }

    let output_file_extension = app_config.OUTPUT_FILE_EXTENSION;

    queue.check_job_pull().await;
    bucket_allocator.scan_directory();

    for i in 0..app_config.WORKER_COUNT {
        let db_clone = Arc::clone(&db);
        let queue_clone = Arc::clone(&queue);
        //let nats_client_clone = Arc::clone(&nats_client);
        let bucket_allocator_clone = Arc::new(&bucket_allocator);

        thread::spawn(move || {
            if core_affinity::set_for_current(core_ids[i]) {
                    loop {
                        if queue_clone.queue_completed.load(Ordering::Acquire) == true {
                            break
                        }
                        let worker_job = queue_clone.get_job()?;
                        let allocated_bucket = bucket_allocator_clone.allocate_bucket()?;

                        let allocated_path = allocated_bucket.get_bucket_path();
                        let file_sha = worker_job.get_file_sha();

                        allocated_path.push(file_sha);
                        allocated_path.set_extension(output_file_extension);


                        if let Ok(transcoded) = transcoder(worker_job.get_target_file_path(), allocated_path, &file_sha) { 
                            allocated_bucket.increment_elm();
                            todo!();
                        } else {
                            continue
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


