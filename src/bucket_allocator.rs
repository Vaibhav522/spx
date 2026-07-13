use uuid::Uuid;
use std::path::PathBuf;
use std::fs::create_dir;

struct BucketEntry {
    bucket_id: String,
    bucket_path: PathBuf,
    bucket_elements: usize,
}

impl BucketEntry {
    pub fn new(bucket_id: String, bucket_elements: usize, bucket_path: PathBuf) -> Self {
        Self {
            bucket_id: bucket_id,
            bucket_path: bucket_path.clone(),
            bucket_elements: bucket_elements,
        }
    }
    
    pub fn increment_elm(&mut self) {
        self.bucket_elements += 1;
    }
}


pub struct BucketAllocator {
    bucket_limit: usize,
    output_directory: String,
    output_path: PathBuf,
    bucket_table: Vec<BucketEntry>
}

impl BucketAllocator {
    pub fn new(bucket_limit: usize , output_directory: String) -> Result<Self, String> {
        let output_path: PathBuf = PathBuf::from(output_directory.clone());
        
        if !output_path.exists() {
            return Err("Output directory doesn't exists!".to_string());
        }

        return Ok(
            Self {
                bucket_limit: bucket_limit,
                output_directory: output_directory,
                output_path: output_path,
                bucket_table: vec![]
            }
        )
    }

    pub fn scan_directory(&mut self) -> Result<i8, String> {
        for entry in self.output_path.read_dir().expect("Failed") {
            if let Ok(entry) = entry {
                if let Ok(metadata) = entry.metadata() {
                    if !metadata.is_dir() {
                        continue;
                    }
                } 
                
                let entry_path: PathBuf = entry.path();

                //let last_directory: String = entry_path.components().last()?.as_os_str().to_str().map(|s| s.to_string());

                if let Some(last_directory) = entry_path.components().last() {
                    if let Some(bucket_id) = last_directory.as_os_str().to_str() {
                        if let Ok(bucket_elements) = entry_path.read_dir() {
                            let bucket_elements_count: usize = bucket_elements.count();
                            if bucket_elements_count < self.bucket_limit {
                                self.bucket_table.push(
                                    BucketEntry::new(
                                        bucket_id.to_owned(), 
                                        bucket_elements_count, 
                                        entry_path
                                    )
                                );
                            }
                        }
                    }
                }
            }
        }
        return Ok(1)
    }

    pub fn create_new_bucket(&mut self) -> Result<PathBuf, std::io::Error> {
        let bucket_uuid = Uuid::new_v4();
        let bucket_id: String = bucket_uuid.to_string();

        let bucket_path: PathBuf = self.output_path.join(&bucket_id);
        create_dir(&bucket_path)?;
        
        self.bucket_table.push(
            BucketEntry::new(
                bucket_id, 
                0 as usize,
                bucket_path.clone()
            )
        );
        return Ok(bucket_path.clone())
    }

    pub fn allocate_bucket(&mut self) -> Result<PathBuf, &'static str> {
        for bucket in &mut self.bucket_table {
            if bucket.bucket_elements < self.bucket_limit {
                bucket.increment_elm();
                return Ok(bucket.bucket_path.clone());
            }
        }

        self.create_new_bucket()
            .map_err(|_| "Error allocating bucket!")
    }

}
