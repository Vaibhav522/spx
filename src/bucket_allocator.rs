use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

use anyhow::Context;

struct BucketEntry {
    bucket_id: String,
    bucket_path: PathBuf,
    bucket_elements: AtomicUsize,
}

impl BucketEntry {
    pub fn new(bucket_id: String, bucket_elements: usize, bucket_path: PathBuf) -> Self {
        Self {
            bucket_id: bucket_id,
            bucket_path: bucket_path.clone(),
            bucket_elements: AtomicUsize::new(bucket_elements),
        }
    }

    pub fn increment_elm(&mut self) {
        self.bucket_elements.fetch_add(1, Ordering::Acquire);
    }

    pub fn get_bucket_path(&self) -> PathBuf {
        return self.bucket_path.clone();
    }
}

pub struct BucketAllocator {
    bucket_limit: usize,
    output_directory: String,
    output_path: PathBuf,
    bucket_table: Vec<BucketEntry>,
}

impl BucketAllocator {
    pub fn initalize(bucket_limit: usize, output_directory: String) -> anyhow::Result<Self> {
        let output_path = PathBuf::from(output_directory.clone());

        return Ok(Self {
            bucket_limit: bucket_limit,
            output_directory: output_directory,
            output_path: output_path,
            bucket_table: vec![],
        });
    }

    pub fn scan_directory(&mut self) -> anyhow::Result<()> {
        let output_directory_contents = self.output_path.read_dir().with_context(|| {
            format!(
                "Failed to scan set output directory: {}",
                self.output_directory
            )
        })?;

        for entry in output_directory_contents {
            if let Ok(entry) = entry {
                if let Ok(metadata) = entry.metadata() {
                    if !metadata.is_dir() {
                        continue;
                    }
                }

                let entry_path: PathBuf = entry.path();

                if let Some(last_directory) = entry_path.components().last() {
                    if let Some(bucket_id) = last_directory.as_os_str().to_str() {
                        if let Ok(bucket_elements) = entry_path.read_dir() {
                            let bucket_elements_count: usize = bucket_elements.count();
                            if bucket_elements_count < self.bucket_limit {
                                self.bucket_table.push(BucketEntry::new(
                                    bucket_id.to_owned(),
                                    bucket_elements_count,
                                    entry_path,
                                ));
                            }
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    pub fn create_new_bucket(&mut self) -> anyhow::Result<&mut BucketEntry> {
        let bucket_uuid = Uuid::new_v4();
        let bucket_id: String = bucket_uuid.to_string();

        let bucket_path: PathBuf = self.output_path.join(&bucket_id);
        create_dir(&bucket_path).context("Faced error when creating directory!")?;

        let appending_bucket: BucketEntry =
            BucketEntry::new(bucket_id, 0 as usize, bucket_path.clone());

        self.bucket_table.push(appending_bucket);
        self.bucket_table
            .last_mut()
            .ok_or_else(|| anyhow::anyhow!("Error providing bucket: table is empty"))
    }

    pub fn allocate_bucket(&mut self) -> anyhow::Result<&mut BucketEntry> {
        for i in 0..self.bucket_table.len() {
            if self.bucket_table[i].bucket_elements.load(Ordering::Acquire) < self.bucket_limit {
                return Ok(&mut self.bucket_table[i]);
            }
        }
        self.create_new_bucket()
    }
}
