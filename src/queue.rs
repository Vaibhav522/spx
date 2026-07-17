use chrono::{DateTime, Utc};
use crossbeam_queue::ArrayQueue;
use tokio::runtime::Runtime;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;


pub trait JobSource: {
    async fn fetch_job(&self, limit: usize) -> Result<Vec<Job>, Box<dyn Error>>;
}

pub struct Job {
    input_file_path: String,
    output_file_path: String,
    file_sha_hash: String,
    file_received_at: Option<DateTime<Utc>>,
}

impl Job {
    pub fn new(
        input_file_path: String,
        output_file_path: String,
        file_sha_hash: String,
        file_received_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            input_file_path: input_file_path,
            output_file_path: output_file_path,
            file_sha_hash: file_sha_hash,
            file_received_at: if file_received_at.is_some() {
                file_received_at
            } else {
                Some(Utc::now())
            },
        }
    }

    pub fn get_target_file_path(&self) -> String {
        return self.input_file_path.clone()
    }

    pub fn get_file_sha(&self) -> String {
        return self.file_sha_hash.clone()
    }
}

pub struct JobQueue<S: JobSource>  {
    queue_capacity: usize,
    queue: ArrayQueue<Job>,
    requested_job_pull: Arc<AtomicBool>,
    job_source: Arc<S>,
    max_job_fetch_attempt_count: usize,
    failed_attempt: Arc<AtomicUsize>,
    pub queue_completed: Arc<AtomicBool>,
    nats_context: async_nats::jetstream::context::Context
}

impl<S: JobSource>  JobQueue<S> {
    pub fn new(queue_capacity: usize, job_source: Arc<S>, max_job_fetch_attempt_count: usize, nats_context: async_nats::jetstream::context::Context) -> Self {
        Self {
            queue_capacity: queue_capacity,
            queue: ArrayQueue::new(queue_capacity),
            requested_job_pull: Arc::new(AtomicBool::new(false)),
            job_source: job_source,
            max_job_fetch_attempt_count: max_job_fetch_attempt_count,
            failed_attempt: Arc::new(AtomicUsize::new(0)),
            queue_completed: Arc::new(AtomicBool::new(false)),
            nats_context: nats_context
        }
    }

    pub fn add_jobs(&self, fetched_jobs: Vec<Job>)  {
        for fetched_job in fetched_jobs {
            let _ = self.queue.push(fetched_job);
        }
    }

    pub async fn check_job_pull(&self) -> Result<(), ()> {
        let fill_pct = (self.queue.len() as f64 / self.queue_capacity as f64) * 100.0;

        if fill_pct > 30.0 && self.requested_job_pull.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() && self.failed_attempt.load(Ordering::Relaxed) < self.max_job_fetch_attempt_count {
            let net_empty_fields = self.queue_capacity - self.queue.len();
                match self.job_source.fetch_job(net_empty_fields).await {
                    Ok(fetched_jobs) => {
                        if fetched_jobs.is_empty() {
                            self.queue_completed.swap(true, Ordering::Acquire);
                        } else {
                            self.add_jobs(fetched_jobs);
                            self.requested_job_pull.swap(false, Ordering::Acquire);
                        }
                    }
                    Err(_) => {
                        self.failed_attempt.fetch_add(1, Ordering::SeqCst);
                    }
                }
            return Ok(())
        } else {
            return Err(())
        }
    }


    pub fn get_job(&self) -> Result<Job, &'static str> {
        let fill_pct = (self.queue.len() as f64 / self.queue_capacity as f64) * 100.0;

        if fill_pct > 30.0 && self.requested_job_pull.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() && self.failed_attempt.load(Ordering::Relaxed) < self.max_job_fetch_attempt_count {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                self.check_job_pull().await
            });
        }
        if let Some(job) = self.queue.pop() {
            return Ok(job)
        } else {
            self.queue_completed.swap(true, Ordering::Acquire);
            return Err("No job's present!")
        }
    }
}


/* 
pub trait JobUpdate {
    async fn job_update(&self, ) -> Result<>;
}

pub struct JobUpdateStruct {

}

*/