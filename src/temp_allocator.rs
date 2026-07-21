use anyhow::Context;
use tempfile;
use tokio::sync::Mutex;

use std::sync::Arc;

use crate::bucket_allocator::BucketAllocator;

pub struct TempHolder {
    file_sha: String,
    output_extension: String,
    holder: tempfile::NamedTempFile,
}

impl TempHolder {
    pub fn new(file_sha: String, output_extension: String) -> anyhow::Result<Self> {
        match tempfile::NamedTempFile::new() {
            Ok(f) => {
                return Ok(Self {
                    holder: f,
                    file_sha: file_sha,
                    output_extension: output_extension,
                });
            }
            Err(_) => return Err(anyhow::anyhow!("Problem with initializing a new tempfile!")),
        }
    }

    pub async fn persist(
        self,
        bucket_allocator: Arc<Mutex<BucketAllocator>>,
    ) -> anyhow::Result<String> {
        let mut allocator = bucket_allocator.lock().await;

        let allocated_bucket = allocator
            .allocate_bucket()
            .context("Failed to allocate bucket")?;
        let mut allocated_path = allocated_bucket.get_bucket_path();

        allocated_path.push(&self.file_sha);
        allocated_path.add_extension(&self.output_extension);

        self.holder
            .as_file()
            .sync_all()
            .context("Unable to sync tempfile")?;
        self.holder
            .persist(allocated_path.clone())
            .context("Failed to persist tempfile")?;

        let output_path = allocated_path
            .to_str()
            .map(String::from)
            .unwrap_or_else(|| allocated_path.display().to_string());

        Ok(output_path)
    }

    pub fn get_holder(&mut self) -> anyhow::Result<&mut std::fs::File> {
        Ok(self.holder.as_file_mut())
    }
}
