use deadpool_postgres::Runtime;
use deadpool_postgres::*;
use serde::Deserialize;
//use tokio_postgres::types::Date;
use chrono::{DateTime, Utc};
use std::error::Error;

use crate::queue::{Job, JobSource};

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

fn deserialize_job(row: &tokio_postgres::Row) -> Job {
    Job::new(
        row.get("input_file_path"),
        row.get("pre_processed_path"),
        row.get("file_sha"),
        None,
    )
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

    pub async fn job_update(sha: &str) {
        let sql_query = format!(
            "
            {}
        ",
            sha
        );
    }
}


impl JobSource for DB {
    async fn fetch_job(&self, limit: usize) -> Result<Vec<Job>, Box<dyn Error>> {
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

        return Ok(rows.iter().map(|row| deserialize_job(row)).collect());
    }
}

