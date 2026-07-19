use anyhow::Context;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::stream;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use serde::{Deserialize, Serialize};

pub trait JobSource {
    async fn fetch_job(&self, limit: usize) -> Result<Vec<Vec<u8>>, Box<dyn Error>>;
}

#[derive(Serialize, Deserialize)]
pub struct Job {
    input_file_path: String,
    file_sha_hash: String,
    file_received_at: Option<DateTime<Utc>>,
}

impl Job {
    pub fn new(
        input_file_path: String,
        file_sha_hash: String,
        file_received_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            input_file_path: input_file_path,
            file_sha_hash: file_sha_hash,
            file_received_at: if file_received_at.is_some() {
                file_received_at
            } else {
                Some(Utc::now())
            },
        }
    }

    pub fn get_target_file_path(&self) -> String {
        return self.input_file_path.clone();
    }

    pub fn get_file_sha(&self) -> String {
        return self.file_sha_hash.clone();
    }
}

pub struct JobQueue<S: JobSource> {
    designated_stream_name: String, // stream name which this worker look for messages
    job_source: Arc<S>,             // single source of truth, -- db in this case
    pending_queued_jobs: AtomicUsize, // total queued job's remaining
    queue_capacity: usize, // preset capacity for a queue, used for thresholding pending job and fetching new jobs.
    requested_job_pull: AtomicBool, // atomic condition if we have already made a request for job pull from db
    max_job_fetch_attempt_count: usize, // max job fetch failed attempts, queue makes
    failed_attempt: AtomicUsize,    // atomic use usize for net attempts we made so far.
    pub queue_completed: AtomicBool, // atomic bool for queue has been finished
    nats_context: async_nats::jetstream::context::Context, // nats context
    message_stream_context: async_nats::jetstream::consumer::pull::Stream, // message stream context to pull messages
}

impl<S: JobSource> JobQueue<S> {
    pub async fn initalize(
        queue_capacity: usize,
        job_source: Arc<S>,
        max_job_fetch_attempt_count: usize,
        nats_context: async_nats::jetstream::context::Context,
    ) -> anyhow::Result<Self> {
        // designated name of job stream
        let designated_stream_name = String::from("transcoding_job_streams");
        let designated_consumer_name = String::from("transcoding_job_worker");

        // stream context for job pulls
        let mut stream_context = nats_context
            .get_or_create_stream(stream::Config {
                name: designated_stream_name.clone(),
                subjects: vec![designated_stream_name.clone()],
                max_messages: queue_capacity as i64,
                ..Default::default()
            })
            .await
            .context("Error initalizing nats transcoding stream context")?;

        // extracting if any pending enqueued jobs still present
        let info = stream_context
            .info()
            .await
            .context("Error getting nats transcoding stream context info struct")?;
        let message_count = info.state.messages as usize; // casting i64 to usize

        // initalizing an consumer context for fetching jobs
        let consumer_context = stream_context
            .get_or_create_consumer(
                &designated_consumer_name,
                pull::Config {
                    ..Default::default()
                },
            )
            .await
            .context("Error initalizing nats transcoding consumer context")?;

        // message stream context
        let message_stream_context = consumer_context
            .stream()
            .max_messages_per_batch(queue_capacity)
            .messages()
            .await
            .context("Error initalizing nats transcoding message stream context")?;

        Ok(Self {
            designated_stream_name: designated_stream_name,
            queue_capacity: queue_capacity,
            requested_job_pull: AtomicBool::new(false),
            job_source: job_source,
            max_job_fetch_attempt_count: max_job_fetch_attempt_count,
            failed_attempt: AtomicUsize::new(0),
            queue_completed: AtomicBool::new(false),
            nats_context: nats_context,
            pending_queued_jobs: AtomicUsize::new(message_count),
            message_stream_context: message_stream_context,
        })
    }

    // inserting bytes stream, payload to nats
    pub async fn add_jobs(&self, fetched_jobs: Vec<Vec<u8>>) -> anyhow::Result<()> {
        for fetched_job in fetched_jobs {
            let _ = self
                .nats_context
                .publish(self.designated_stream_name.clone(), fetched_job.into())
                .await
                .context("Error publish transcoding job to nats")?;
        }

        return Ok(());
    }

    pub async fn check_job_pull(&self) -> anyhow::Result<()> {
        // percentage empty space in our queue
        let pending_queued_jobs = self.pending_queued_jobs.load(Ordering::Acquire);
        let fill_pct = (pending_queued_jobs as f64 / self.queue_capacity as f64) * 100.0;

        if fill_pct > 30.0
            && self
                .requested_job_pull
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            && self.failed_attempt.load(Ordering::Relaxed) < self.max_job_fetch_attempt_count
        {
            let net_empty_fields = self.queue_capacity - pending_queued_jobs;

            match self.job_source.fetch_job(net_empty_fields).await {
                Ok(fetched_jobs) => {
                    if fetched_jobs.is_empty() {
                        self.requested_job_pull.swap(false, Ordering::Acquire);
                        return Err(anyhow::anyhow!("Transcoding Job fetch returned empty!"));
                    } else {
                        self.add_jobs(fetched_jobs);
                        self.requested_job_pull.swap(false, Ordering::Acquire);
                    }
                }
                Err(e) => {
                    self.requested_job_pull.swap(false, Ordering::Acquire);
                    self.failed_attempt.fetch_add(1, Ordering::SeqCst);
                    return Err(anyhow::anyhow!(format!(
                        "Error faced when fetching for transcoding jobs! Error detail: {}",
                        e
                    )));
                }
            }
            return Ok(());
        } else {
            return Err(anyhow::anyhow!(
                "False Transcoding Job pull trigger, or some error!"
            ));
        }
    }

    pub async fn get_job(&mut self) -> anyhow::Result<Job> {
        let fill_pct = (self.pending_queued_jobs.load(Ordering::Acquire) as f64
            / self.queue_capacity as f64)
            * 100.0;

        if fill_pct > 30.0
            && !self.requested_job_pull.load(Ordering::Acquire)
            && self.failed_attempt.load(Ordering::Acquire) < self.max_job_fetch_attempt_count
        {
            self.check_job_pull().await?;
        }

        if let Some(Ok(job)) = self.message_stream_context.next().await {
            self.pending_queued_jobs.fetch_sub(1, Ordering::Acquire);
            job.ack().await;

            let deserialized_bytes = serde_json::from_slice::<Job>(&job.payload)?;
            return Ok(deserialized_bytes);
        } else {
            self.queue_completed.swap(true, Ordering::Acquire);
            return Err(anyhow::anyhow!("Transcoding Job fetch returned empty!"));
        }
    }
}

pub trait SyncSource {
    async fn sync_updates(&self, job_updates: Vec<JobUpdate>) -> Result<(), Box<dyn Error>>;
}

#[derive(Deserialize, Serialize)]
pub struct JobUpdate {
    pub file_sha: String,
    pub is_transcoded: bool, // true for success else false for error faced
    pub error_faced: Option<String>, // optional migh not be available when successfull processing
    pub processed_at: DateTime<Utc>,
    pub output_file_path: Option<String>, // optional, might not be available when facing error
    pub output_file_size: Option<usize>,  // optional, might not be available when error
    pub time_to_process: std::time::Duration,
}

impl JobUpdate {
    pub fn new(
        file_sha: String,
        output_file_path: Option<String>,
        is_transcoded: bool,
        processed_at: DateTime<Utc>,
        file_size: Option<usize>,
        time_to_process: std::time::Duration,
        error_faced: Option<String>,
    ) -> Self {
        Self {
            file_sha: file_sha,
            output_file_path: output_file_path,
            is_transcoded: is_transcoded,
            processed_at: processed_at,
            output_file_size: file_size,
            time_to_process: time_to_process,
            error_faced: error_faced,
        }
    }
}

pub struct SyncUpdate<S: SyncSource> {
    designated_stream_name: String, // stream name which this worker look for messages
    sync_source: Arc<S>,            // single source of truth, -- db in this case
    pending_queued_jobs: AtomicUsize, // total queued job's remaining
    sync_threshold: usize, // preset capacity for a queue, used for thresholding pending job and fetching new jobs.
    requested_db_sync: AtomicBool, // atomic condition if we have already made a request for job pull from db
    pub queue_completed: AtomicBool, // atomic bool for queue has been finished
    nats_context: async_nats::jetstream::context::Context, // nats context
    message_stream_context: async_nats::jetstream::consumer::pull::Sequence, // message stream context to pull messages
}

impl<S: SyncSource> SyncUpdate<S> {
    pub async fn initalize(
        sync_threshold: usize,
        sync_source: Arc<S>,
        nats_context: async_nats::jetstream::context::Context,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // designated name of job stream
        let designated_stream_name = String::from("db_sync_streams");
        let designated_consumer_name = String::from("db_sync_worker");

        // stream context for job pulls
        let mut stream_context = nats_context
            .get_or_create_stream(stream::Config {
                name: designated_stream_name.clone(),
                subjects: vec![designated_stream_name.clone()],
                ..Default::default()
            })
            .await?;

        // extracting if any pending enqueued jobs still present
        let info = stream_context.info().await?;
        let message_count = info.state.messages as usize; // casting i64 to usize

        // initalizing an consumer context for fetching jobs
        let consumer_context = stream_context
            .get_or_create_consumer(
                &designated_consumer_name,
                pull::Config {
                    ..Default::default()
                },
            )
            .await?;

        // message stream context
        let message_stream_context = consumer_context.sequence(sync_threshold)?;

        Ok(Self {
            designated_stream_name: designated_stream_name,
            sync_threshold: sync_threshold,
            requested_db_sync: AtomicBool::new(false),
            sync_source: sync_source,
            queue_completed: AtomicBool::new(false),
            nats_context: nats_context,
            pending_queued_jobs: AtomicUsize::new(message_count),
            message_stream_context: message_stream_context,
        })
    }

    pub async fn sync_updates(&mut self, finished_queue: Option<bool>) -> anyhow::Result<()> {
        if self.pending_queued_jobs.load(Ordering::Acquire) >= self.sync_threshold
            && !self.requested_db_sync.load(Ordering::Acquire)
            || finished_queue.unwrap_or(false)
        {
            let mut sync_jobs: Vec<JobUpdate> = vec![];
            let mut rows: Vec<async_nats::jetstream::message::Message> = vec![];

            if let Some(Ok(sync_message)) = self.message_stream_context.next().await.as_mut() {
                while let Some(Ok(message)) = sync_message.next().await {
                    rows.push(message.clone());
                    if let Ok(sync_job) = serde_json::from_slice::<JobUpdate>(&message.payload) {
                        sync_jobs.push(sync_job);
                    }
                }
            }

            if rows.is_empty() {
                return Err(anyhow::anyhow!("DB Sync Job fetch returned empty!"));
            }

            self.sync_source
                .sync_updates(sync_jobs)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;

            for row in rows {
                row.ack().await.map_err(|e| {
                    anyhow::anyhow!("Error acknowledging completed jobs messages!: {}", e)
                })?;
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn enqueue_update(
        &self,
        payload: JobUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let byte_payload = serde_json::to_vec::<JobUpdate>(&payload)?;
        self.nats_context
            .publish(self.designated_stream_name.clone(), byte_payload.into())
            .await;

        self.pending_queued_jobs.fetch_add(1, Ordering::Acquire);

        return Ok(());
    }
}
