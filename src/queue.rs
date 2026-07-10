use crossbeam_queue::ArrayQueue;
use chrono::{Utc};


struct Job {
    input_file_path: &str,
    output_file_path: &str,
    file_sha_hash: &str,
    file_received_at: Utc
}



struct JobQueue {
    queue_capacity: usize,
    queue: ArrayQueue,
}

impl JobQueue {
    pub fn new(queue_capacity: &usize) -> Self {
        Self {
            queue_capacity: *queue_capacity,
            queue: ArrayQueue::new(*queue_capacity)
        }
    }

    pub async fn add_jobs() {

    }

    pub async fn get_job(&mut self) -> Result<Job, String> {
        // calculate the number of jobs
        let job_space: usize = self.queue_capacity - self.queue.len();

        
    }
}
