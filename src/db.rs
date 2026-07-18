use chrono::Utc;
use deadpool_postgres::Runtime;
use deadpool_postgres::*;
use serde::Deserialize;
//use tokio_postgres::types::Date;
use std::{error::Error, time::SystemTime};

use crate::queue::{Job, JobSource, SyncSource, JobUpdate};

// Pulls loaded environment variables, and constructs and dead_pool config struct

#[derive(Deserialize)]
struct DBConfig {
    pg: deadpool_postgres::Config,
}
impl DBConfig {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("DB").separator("__"))
            .build()?;
        return Ok(cfg.try_deserialize()?);
    }
}

pub struct DB {
    pool: Pool,
}

fn serialize_job(row: &tokio_postgres::Row) -> Result<Vec<u8>, ()> {
    let job = Job::new(
        row.get("input_file_path"),
        row.get("file_sha"),
        Some(chrono::Utc::now()),
    );

    if let Ok(serialize_job) = serde_json::to_vec::<Job>(&job) {
        return Ok(serialize_job)
    } else {
        return Err(())
    }
}


impl DB {
    pub fn initalize() -> Result<Self, Box<dyn Error>> {
        let cfg: DBConfig = DBConfig::from_env()?;

        // tls configuration
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);

        let pool = cfg.pg.create_pool(Some(Runtime::Tokio1), tls)?;

        Ok(Self { pool: pool })
    }
}


impl JobSource for DB {
    async fn fetch_job(&self, limit: usize) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let sql_query = format!(
            "
                WITH jobs AS (
                    SELECT 
                        file_sha -- Assuming you have a primary key like id, uuid, or file_sha
                    FROM 
                        files 
                    WHERE 
                        -- Parentheses fix the OR/AND bug
                        (pre_processing_status = 'pending' OR pre_processing_status = 'failed')
                        AND lease_until < now() 
                        AND pre_processing_attempts < 3
                    ORDER BY
                        created_at ASC   -- Fixed ordering syntax
                    LIMIT {}
                    FOR UPDATE SKIP LOCKED -- Added SKIP LOCKED for parallel workers
                )
                UPDATE 
                    files f
                SET 
                    pre_processing_status = 'processing', 
                    lease_until = now() + interval '15 minutes', 
                    pre_processing_attempts = f.pre_processing_attempts + 1
                FROM 
                    jobs j
                WHERE 
                    f.file_sha = j.file_sha -- Using a Join-Update (much faster than IN clause)
                RETURNING 
                    f.file_name, f.file_sha; -- Returns data directly to your application              
        ", limit);

        let client = self.pool.get().await?;
        let stmt = client.prepare_cached(&sql_query).await?;
        let rows = client.query(&stmt, &[]).await?;


        let mut serialized_rows: Vec<Vec<u8>> = vec![];

        for row in rows {
            if let Ok(serialized_row) = serialize_job(&row) {
                serialized_rows.push(serialized_row);
            } else {
                continue
            }
        }
        return Ok(serialized_rows);
    }
}







impl SyncSource for DB {
    async fn sync_updates(&self, job_updates: Vec<JobUpdate>) -> Result<(), Box<dyn std::error::Error>> {
        let mut file_sha: Vec<String> = Vec::with_capacity(job_updates.len());
        let mut is_transcoded: Vec<bool> = Vec::with_capacity(job_updates.len());
        let mut error_faced: Vec<Option<String>> = Vec::with_capacity(job_updates.len());
        let mut processed_at: Vec<String> = Vec::with_capacity(job_updates.len());
        let mut output_file_path: Vec<Option<String>> = Vec::with_capacity(job_updates.len());
        let mut output_file_size: Vec<Option<i64>> = Vec::with_capacity(job_updates.len());
        let mut time_to_process: Vec<f64> = Vec::with_capacity(job_updates.len());

        for job_update in job_updates {
            file_sha.push(job_update.file_sha);
            output_file_path.push(job_update.output_file_path);
            is_transcoded.push(job_update.is_transcoded);
            error_faced.push(job_update.error_faced);
            processed_at.push(job_update.processed_at.to_rfc3339());
            time_to_process.push(job_update.time_to_process.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs_f64());

            if let Some(file_size) = job_update.output_file_size {
                output_file_size.push(Some(file_size as i64));
            } else {
                output_file_size.push(None);
            }
        }

        let sql_query = "
            UPDATE 
                files AS f
            SET 
                output_file_path = sync_updates.output_file_path,
                is_transcoded = sync_updates.is_transcoded,
                error_faced = sync_updates.error_faced,
                processed_at = sync_updates.processed_at,
                output_file_size = sync_updates.output_file_size,
                time_to_process = sync_updates.time_to_process
            FROM (
                SELECT * FROM UNNEST($1::text[], $2::text[], $3::boolean[], $4::text[], $5::timestamp[], $6::bigint[], $7::double precision[])
            ) AS sync_updates(
                    file_sha, 
                    output_file_path, 
                    is_transcoded, 
                    error_faced, 
                    processed_at, 
                    output_file_size, 
                    time_to_process
                )
            WHERE f.file_sha = sync_updates.file_sha;
        ";
    
        let client = self.pool.get().await?;
        let stmt = client.prepare(sql_query).await?;
        let update = client.execute(&stmt, &[&file_sha, &output_file_path, &is_transcoded, &error_faced, &processed_at, &output_file_size, &time_to_process]).await?;

        return Ok(())
    }
}
