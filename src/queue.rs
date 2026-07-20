use anyhow::Context;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::stream;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::Mutex; // ← add this

use serde::{Deserialize, Serialize};

pub trait JobSource {
    async fn fetch_job(&self, limit: usize) -> anyhow::Result<Vec<Vec<u8>>>;
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
            input_file_path,
            file_sha_hash,
            file_received_at: file_received_at.or_else(|| Some(Utc::now())),
        }
    }

    pub fn get_target_file_path(&self) -> String {
        self.input_file_path.clone()
    }

    pub fn get_file_sha(&self) -> String {
        self.file_sha_hash.clone()
    }
}

pub struct JobQueue<S: JobSource> {
    designated_stream_name: String,
    job_source: Arc<S>,
    pending_queued_jobs: AtomicUsize,
    queue_capacity: usize,
    requested_job_pull: AtomicBool,
    max_job_fetch_attempt_count: usize,
    failed_attempt: AtomicUsize,
    pub queue_completed: AtomicBool,
    nats_context: async_nats::jetstream::context::Context,
    // ← wrapped in Mutex so get_job() can take &self
    message_stream_context: Mutex<async_nats::jetstream::consumer::pull::Stream>,
}

impl<S: JobSource> JobQueue<S> {
    pub async fn initalize(
        queue_capacity: usize,
        job_source: Arc<S>,
        max_job_fetch_attempt_count: usize,
        nats_context: async_nats::jetstream::context::Context,
    ) -> anyhow::Result<Self> {
        let designated_stream_name = String::from("transcoding_job_streams");
        let designated_consumer_name = String::from("transcoding_job_worker");

        let mut stream_context = nats_context
            .get_or_create_stream(stream::Config {
                name: designated_stream_name.clone(),
                subjects: vec![designated_stream_name.clone()],
                max_messages: queue_capacity as i64,
                ..Default::default()
            })
            .await
            .context("Error initializing nats transcoding stream context")?;

        let info = stream_context
            .info()
            .await
            .context("Error getting nats transcoding stream context info struct")?;
        let message_count = info.state.messages as usize;

        let consumer_context = stream_context
            .get_or_create_consumer(
                &designated_consumer_name,
                pull::Config {
                    ..Default::default()
                },
            )
            .await
            .context("Error initializing nats transcoding consumer context")?;

        let message_stream_context = consumer_context
            .stream()
            .max_messages_per_batch(queue_capacity)
            .messages()
            .await
            .context("Error initializing nats transcoding message stream context")?;

        Ok(Self {
            designated_stream_name,
            queue_capacity,
            requested_job_pull: AtomicBool::new(false),
            job_source,
            max_job_fetch_attempt_count,
            failed_attempt: AtomicUsize::new(0),
            queue_completed: AtomicBool::new(false),
            nats_context,
            pending_queued_jobs: AtomicUsize::new(message_count),
            message_stream_context: Mutex::new(message_stream_context), // ← Mutex
        })
    }

    pub async fn add_jobs(&self, fetched_jobs: Vec<Vec<u8>>) -> anyhow::Result<()> {
        for fetched_job in fetched_jobs {
            self.nats_context
                .publish(self.designated_stream_name.clone(), fetched_job.into())
                .await
                .context("Error publishing transcoding job to nats")?;
        }
        Ok(())
    }

    pub async fn check_job_pull(&self) -> anyhow::Result<()> {
        let pending = self.pending_queued_jobs.load(Ordering::Acquire);
        let fill_pct = (pending as f64 / self.queue_capacity as f64) * 100.0;

        // FIXED: < 30.0 (refill when nearly empty, not when mostly full)
        if fill_pct < 30.0
            && self
                .requested_job_pull
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            && self.failed_attempt.load(Ordering::Relaxed) < self.max_job_fetch_attempt_count
        {
            let needed = self.queue_capacity - pending;

            match self.job_source.fetch_job(needed).await {
                Ok(fetched_jobs) => {
                    if fetched_jobs.is_empty() {
                        self.requested_job_pull.store(false, Ordering::Release);
                        return Err(anyhow::anyhow!("Transcoding Job fetch returned empty!"));
                    }
                    // FIXED: added .await
                    self.add_jobs(fetched_jobs).await?;
                    self.requested_job_pull.store(false, Ordering::Release);
                    Ok(())
                }
                Err(e) => {
                    self.requested_job_pull.store(false, Ordering::Release);
                    self.failed_attempt.fetch_add(1, Ordering::SeqCst);
                    Err(anyhow::anyhow!("Error fetching transcoding jobs: {e}"))
                }
            }
        } else {
            Ok(()) // no-op, not an error
        }
    }

    // CHANGED: &mut self → &self
    pub async fn get_job(&self) -> anyhow::Result<Job> {
        let pending = self.pending_queued_jobs.load(Ordering::Acquire);
        let fill_pct = (pending as f64 / self.queue_capacity as f64) * 100.0;

        // FIXED: < 30.0
        if fill_pct < 30.0
            && !self.requested_job_pull.load(Ordering::Acquire)
            && self.failed_attempt.load(Ordering::Acquire) < self.max_job_fetch_attempt_count
        {
            self.check_job_pull().await?;
        }

        // Lock only the stream, not the whole struct
        let mut stream = self.message_stream_context.lock().await;

        if let Some(Ok(job)) = stream.next().await {
            self.pending_queued_jobs.fetch_sub(1, Ordering::Acquire);
            job.ack()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to ack job"))?;

            let deserialized = serde_json::from_slice::<Job>(&job.payload)?;
            Ok(deserialized)
        } else {
            self.queue_completed.store(true, Ordering::Release);
            Err(anyhow::anyhow!("Job stream exhausted"))
        }
    }
}

// ─── SyncUpdate (same pattern) ────────────────────────────────────

pub trait SyncSource {
    async fn sync_updates(&self, job_updates: Vec<JobUpdate>) -> anyhow::Result<()>;
}

#[derive(Deserialize, Serialize)]
pub struct JobUpdate {
    pub file_sha: String,
    pub is_transcoded: bool,
    pub error_faced: Option<String>,
    pub processed_at: DateTime<Utc>,
    pub output_file_path: Option<String>,
    pub output_file_size: Option<usize>,
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
            file_sha,
            output_file_path,
            is_transcoded,
            processed_at,
            output_file_size: file_size,
            time_to_process,
            error_faced,
        }
    }
}

pub struct SyncUpdate<S: SyncSource> {
    designated_stream_name: String,
    sync_source: Arc<S>,
    pending_queued_jobs: AtomicUsize,
    sync_threshold: usize,
    requested_db_sync: AtomicBool,
    pub queue_completed: AtomicBool,
    nats_context: async_nats::jetstream::context::Context,
    // ← wrapped in Mutex
    message_stream_context: Mutex<async_nats::jetstream::consumer::pull::Sequence>,
}

impl<S: SyncSource> SyncUpdate<S> {
    pub async fn initalize(
        sync_threshold: usize,
        sync_source: Arc<S>,
        nats_context: async_nats::jetstream::context::Context,
    ) -> anyhow::Result<Self> {
        let designated_stream_name = String::from("db_sync_streams");
        let designated_consumer_name = String::from("db_sync_worker");

        let mut stream_context = nats_context
            .get_or_create_stream(stream::Config {
                name: designated_stream_name.clone(),
                subjects: vec![designated_stream_name.clone()],
                ..Default::default()
            })
            .await?;

        let info = stream_context.info().await?;
        let message_count = info.state.messages as usize;

        let consumer_context = stream_context
            .get_or_create_consumer(
                &designated_consumer_name,
                pull::Config {
                    ..Default::default()
                },
            )
            .await?;

        let message_stream_context = consumer_context.sequence(sync_threshold)?;

        Ok(Self {
            designated_stream_name,
            sync_threshold,
            requested_db_sync: AtomicBool::new(false),
            sync_source,
            queue_completed: AtomicBool::new(false),
            nats_context,
            pending_queued_jobs: AtomicUsize::new(message_count),
            message_stream_context: Mutex::new(message_stream_context),
        })
    }

    // CHANGED: &mut self → &self
    pub async fn sync_updates(&self, finished_queue: Option<bool>) -> anyhow::Result<()> {
        if (self.pending_queued_jobs.load(Ordering::Acquire) >= self.sync_threshold
            && !self.requested_db_sync.load(Ordering::Acquire))
            || finished_queue.unwrap_or(false)
        {
            let mut sync_jobs: Vec<JobUpdate> = vec![];
            let mut rows: Vec<async_nats::jetstream::message::Message> = vec![];

            let mut seq = self.message_stream_context.lock().await;

            if let Some(Ok(sync_message)) = seq.next().await.as_mut() {
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

            self.sync_source.sync_updates(sync_jobs).await?;

            for row in &rows {
                row.ack()
                    .await
                    .map_err(|_| anyhow::anyhow!("Problem while acknowledging messaging!"));
            }

            self.pending_queued_jobs
                .fetch_sub(rows.len(), Ordering::Acquire);

            Ok(())
        } else {
            Ok(())
        }
    }

    // CHANGED: &mut self → &self
    pub async fn enqueue_update(&self, payload: JobUpdate) -> anyhow::Result<()> {
        let byte_payload =
            serde_json::to_vec::<JobUpdate>(&payload).context("Error serializing the payload!")?;

        self.nats_context
            .publish(self.designated_stream_name.clone(), byte_payload.into())
            .await
            .context("Error publishing sync update to nats!")?;

        self.pending_queued_jobs.fetch_add(1, Ordering::Acquire);

        if self.pending_queued_jobs.load(Ordering::Acquire) >= self.sync_threshold
            && !self.requested_db_sync.load(Ordering::Acquire)
        {
            self.sync_updates(None).await?;
        }

        Ok(())
    }
}
