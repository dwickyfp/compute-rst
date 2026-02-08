use crate::destination_enum::{DestinationEnum, DestinationWithDlq};
use crate::crypto::decrypt_value;
use crate::dlq::store::DlqStore;
use crate::snowflake::SnowflakeDestination;
use crate::store::memory::CustomStore;
use anyhow::Result;
use etl::config::{
    BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use secrecy::ExposeSecret;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info};

pub struct PipelineManager {
    db_pool: Pool<Postgres>,
    pipelines: Arc<Mutex<HashMap<i32, JoinHandle<()>>>>,
    dlq_store: Arc<DlqStore>,
}

impl PipelineManager {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // Initialize DLQ store in current directory
        let dlq_path = PathBuf::from(".");
        let dlq_store = DlqStore::new(&dlq_path)?;

        Ok(Self {
            db_pool: pool,
            pipelines: Arc::new(Mutex::new(HashMap::new())),
            dlq_store: Arc::new(dlq_store),
        })
    }

    pub async fn run_migrations(&self) -> Result<()> {
        info!("Running database migrations...");
        let migrations = format!("{}", include_str!("../migrations/001_create_table.sql"));
        // Split by semicolon to execute separate statements
        for query in migrations.split(';') {
            let query = query.trim();
            if !query.is_empty() {
                sqlx::query(query).execute(&self.db_pool).await?;
            }
        }

        info!("Migrations completed successfully.");
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        self.run_migrations().await?;

        loop {
            if let Err(e) = self.sync_pipelines().await {
                error!("Error syncing pipelines: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn sync_pipelines(&self) -> Result<()> {
        // Fetch all pipelines from DB
        let rows = sqlx::query("SELECT id, name, status, source_id FROM pipelines")
            .fetch_all(&self.db_pool)
            .await?;

        let mut pipelines_lock = self.pipelines.lock().await;

        for row in rows {
            let id: i32 = row.get("id");
            let name: String = row.get("name");
            let status: String = row.get("status");
            let source_id: i32 = row.get("source_id");

            if status == "PAUSE" {
                if let Some(handle) = pipelines_lock.remove(&id) {
                    info!("Pausing pipeline {}: {}", id, name);
                    handle.abort();
                    self.update_metadata(id, "PAUSED", None).await?;
                }
            } else if status == "START" {
                if !pipelines_lock.contains_key(&id) {
                    info!("Starting pipeline {}: {}", id, name);
                    match self.start_pipeline(id, name.clone(), source_id).await {
                        Ok(handle) => {
                            pipelines_lock.insert(id, handle);
                            self.update_metadata(id, "RUNNING", None).await?;
                            self.update_last_start(id).await?;
                        }
                        Err(e) => {
                            error!("Failed to start pipeline {}: {}", id, e);
                            self.update_metadata(id, "ERROR", Some(&e.to_string()))
                                .await?;
                        }
                    }
                }
            } else if status == "REFRESH" {
                info!("Refreshing pipeline {}: {}", id, name);
                if let Some(handle) = pipelines_lock.remove(&id) {
                    handle.abort();
                }
                match self.start_pipeline(id, name.clone(), source_id).await {
                    Ok(handle) => {
                        pipelines_lock.insert(id, handle);
                        // SQL triggers or manual update to set status back to START might be needed
                        // For now, we update metadata.
                        // Note: Ideally we should update the pipeline status back to START in DB so we don't refresh loop
                        self.set_pipeline_status(id, "START").await?;
                        self.update_metadata(id, "RUNNING", None).await?;
                        self.update_last_start(id).await?;
                    }
                    Err(e) => {
                        error!("Failed to start pipeline {}: {}", id, e);
                        self.update_metadata(id, "ERROR", Some(&e.to_string()))
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn start_pipeline(
        &self,
        pipeline_id: i32,
        pipeline_name: String,
        source_id: i32,
    ) -> Result<JoinHandle<()>> {
        // Fetch Source Config
        let source_row = sqlx::query("SELECT * FROM sources WHERE id = $1")
            .bind(source_id)
            .fetch_one(&self.db_pool)
            .await?;

        // Construct Configs
        let pg_config = PgConnectionConfig {
            host: source_row.try_get("pg_host")?,
            port: source_row.try_get::<i32, _>("pg_port")? as u16,
            name: source_row.try_get("pg_database")?,
            username: source_row.try_get("pg_username")?,
            password: match source_row.try_get::<Option<String>, _>("pg_password")? {
                Some(p) => Some(decrypt_value(&p)?.into()),
                None => None,
            },
            tls: TlsConfig {
                enabled: false,
                trusted_root_certs: "".into(),
            },
            keepalive: None,
        };

        // Read config from environment with defaults
        let batch_max_size: usize = std::env::var("BATCH_MAX_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let batch_max_fill_ms: u64 = std::env::var("BATCH_MAX_FILL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let table_error_retry_delay_ms: u64 = std::env::var("TABLE_ERROR_RETRY_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10000);
        let table_error_retry_max_attempts: u32 = std::env::var("TABLE_ERROR_RETRY_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let max_table_sync_workers: u16 = std::env::var("MAX_TABLE_SYNC_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);

        let config = PipelineConfig {
            id: source_row.try_get::<i32, _>("replication_id")? as u64,
            publication_name: source_row.try_get("publication_name")?,
            pg_connection: pg_config.clone(),
            batch: BatchConfig {
                max_size: batch_max_size,
                max_fill_ms: batch_max_fill_ms,
            },
            table_error_retry_delay_ms,
            table_error_retry_max_attempts,
            max_table_sync_workers,
            table_sync_copy: TableSyncCopyConfig::SkipAllTables,
        };

        // Fetch all destinations for this pipeline from pipelines_destination
        let destinations_rows = sqlx::query(
            "SELECT pd.id as pd_id, d.* 
             FROM pipelines_destination pd
             JOIN destinations d ON pd.destination_id = d.id
             WHERE pd.pipeline_id = $1",
        )
        .bind(pipeline_id)
        .fetch_all(&self.db_pool)
        .await?;

        let mut destinations_with_dlq: Vec<Arc<DestinationWithDlq>> = Vec::new();

        for dest_row in destinations_rows {
            let dest_type: String = dest_row
                .try_get("type")
                .unwrap_or_else(|_| "SNOWFLAKE".to_string());
            let dest_config_json: serde_json::Value = dest_row.try_get("config")?;
            let pd_id: i32 = dest_row.try_get("pd_id")?;
            let dest_name: String = dest_row.try_get("name")?;

            let destination_enum = match dest_type.as_str() {
                "SNOWFLAKE" => {
                    let snowflake_config = crate::config::SnowflakeConfig {
                        account_id: dest_config_json["account"]
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                        user: dest_config_json["user"].as_str().unwrap_or("").to_string(),
                        database: dest_config_json["database"]
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                        schema: dest_config_json["schema"]
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                        table: pipeline_name.clone(),
                        role: dest_config_json["role"]
                            .as_str()
                            .unwrap_or("PUBLIC")
                            .to_string(),
                        private_key: dest_config_json["private_key"]
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                        private_key_passphrase: match dest_config_json["private_key_passphrase"].as_str() {
                            Some(p) if !p.is_empty() => Some(decrypt_value(p)?), 
                            _ => None,
                        },
                        landing_database: dest_config_json["landing_database"]
                            .as_str()
                            .map(|s| s.to_string())
                            .filter(|s| !s.is_empty()),
                        landing_schema: dest_config_json["landing_schema"]
                            .as_str()
                            .map(|s| s.to_string())
                            .filter(|s| !s.is_empty()),
                    };

                    let source_pool_url = format!(
                        "postgres://{}:{}@{}:{}/{}",
                        pg_config.username,
                        pg_config
                            .password
                            .as_ref()
                            .map(|p| p.expose_secret())
                            .unwrap_or(""),
                        pg_config.host,
                        pg_config.port,
                        pg_config.name
                    );

                    let source_pool = PgPoolOptions::new()
                        .max_connections(2)
                        .connect(&source_pool_url)
                        .await?;

                    DestinationEnum::Snowflake(SnowflakeDestination::new(
                        snowflake_config,
                        source_pool,
                        self.db_pool.clone(),
                        pipeline_id,
                        pd_id,
                        source_id,
                    )?)
                }
                "POSTGRES" | "POSTGRESQL" => {
                    // Reuse Source Config? Or Destination Config has connection info?
                    // Typically 'destinations' table config has the connection info for the target postgres.
                    let target_pg_config = PgConnectionConfig {
                        host: dest_config_json["host"].as_str().unwrap_or("").to_string(),
                        port: dest_config_json["port"].as_u64().unwrap_or(5432) as u16,
                        name: dest_config_json["database"]
                            .as_str()
                            .unwrap_or("postgres")
                            .to_string(),
                        username: dest_config_json["username"]
                            .as_str()
                            .unwrap_or("postgres")
                            .to_string(),
                        password: match dest_config_json["password"].as_str() {
                            Some(p) => Some(decrypt_value(p)?.into()),
                            None => None,
                        },
                        tls: TlsConfig {
                            enabled: false,
                            trusted_root_certs: "".into(),
                        },
                        keepalive: None,
                    };

                    let source_pool_url = format!(
                        "postgres://{}:{}@{}:{}/{}",
                        pg_config.username,
                        pg_config
                            .password
                            .as_ref()
                            .map(|p| p.expose_secret())
                            .unwrap_or(""),
                        pg_config.host,
                        pg_config.port,
                        pg_config.name
                    );

                    let source_pool = PgPoolOptions::new()
                        .max_connections(2)
                        .connect(&source_pool_url)
                        .await?;

                    DestinationEnum::Postgres(
                        crate::postgres::destination::PostgresDuckdbDestination::new(
                            dest_name,
                            target_pg_config,
                            self.db_pool.clone(),
                            source_pool,
                            pd_id,
                            pipeline_id,
                            source_id,
                        )?,
                    )
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported destination type: {}",
                        dest_type
                    ));
                }
            };

            // Wrap destination with DLQ support
            let dest_with_dlq = Arc::new(DestinationWithDlq::new(
                destination_enum,
                pd_id,
                self.db_pool.clone(),
                self.dlq_store.clone(),
            ));

            // Initialize from persistence (restores DLQ state from fjall)
            if let Err(e) = dest_with_dlq.clone().init_from_persistence().await {
                error!("Failed to initialize DLQ persistence for dest {}: {}", pd_id, e);
            }

            destinations_with_dlq.push(dest_with_dlq);
        }

        // Use MultiWithDlq for DLQ-aware multi-destination
        let destination = if destinations_with_dlq.len() == 1 {
            DestinationEnum::SingleWithDlq(
                destinations_with_dlq.into_iter().next().unwrap(),
            )
        } else {
            DestinationEnum::MultiWithDlq(Arc::new(destinations_with_dlq))
        };

        let store = CustomStore::new();

        let mut pipeline = Pipeline::new(config, store, destination);

        let db_pool = self.db_pool.clone();
        let handle = tokio::spawn(async move {
            match pipeline.start().await {
                Ok(_) => {
                    info!(
                        "Pipeline {} started. Waiting for completion...",
                        pipeline_id
                    );
                    if let Err(e) = pipeline.wait().await {
                        error!("Pipeline {} crashed: {}", pipeline_id, e);

                        // Update status to ERROR
                        let now = chrono::Utc::now()
                            .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
                        let _ = sqlx::query("UPDATE pipeline_metadata SET status = 'ERROR', last_error = $1, last_error_at = $2, updated_at = $2 WHERE pipeline_id = $3")
                            .bind(e.to_string())
                            .bind(now)
                            .bind(pipeline_id)
                            .execute(&db_pool)
                            .await
                            .map_err(|db_err| error!("Failed to update pipeline {} status: {}", pipeline_id, db_err));
                        let _ = sqlx::query("UPDATE pipelines SET status = 'PAUSE' WHERE id = $1")
                            .bind(pipeline_id)
                            .execute(&db_pool)
                            .await
                            .map_err(|db_err| {
                                error!("Failed to pause pipeline {}: {}", pipeline_id, db_err)
                            });
                    } else {
                        info!("Pipeline {} finished gracefully.", pipeline_id);
                    }
                }
                Err(e) => {
                    error!("Failed to start pipeline {}: {}", pipeline_id, e);
                    // Update status to ERROR on start failure
                    let now = chrono::Utc::now()
                        .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
                    let _ = sqlx::query("UPDATE pipeline_metadata SET status = 'ERROR', last_error = $1, last_error_at = $2, updated_at = $2 WHERE pipeline_id = $3")
                        .bind(e.to_string())
                        .bind(now)
                        .bind(pipeline_id)
                        .execute(&db_pool)
                        .await
                        .map_err(|db_err| error!("Failed to update pipeline {} status: {}", pipeline_id, db_err));

                    let _ = sqlx::query("UPDATE pipelines SET status = 'PAUSE' WHERE id = $1")
                        .bind(pipeline_id)
                        .execute(&db_pool)
                        .await
                        .map_err(|db_err| {
                            error!("Failed to pause pipeline {}: {}", pipeline_id, db_err)
                        });
                }
            }
        });

        Ok(handle)
    }

    async fn update_metadata(
        &self,
        pipeline_id: i32,
        status: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        let now =
            chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
        // Check if exists
        let exists: bool =
            sqlx::query("SELECT EXISTS(SELECT 1 FROM pipeline_metadata WHERE pipeline_id = $1)")
                .bind(pipeline_id)
                .fetch_one(&self.db_pool)
                .await?
                .get(0);

        if exists {
            sqlx::query("UPDATE pipeline_metadata SET status = $1, last_error = $2, last_error_at = CASE WHEN $2 IS NOT NULL THEN $3 ELSE last_error_at END, updated_at = $3 WHERE pipeline_id = $4")
                .bind(status)
                .bind(last_error)
                .bind(now)
                .bind(pipeline_id)
                .execute(&self.db_pool)
                .await?;
        } else {
            sqlx::query("INSERT INTO pipeline_metadata (pipeline_id, status, last_error, last_error_at) VALUES ($1, $2, $3, $4)")
                .bind(pipeline_id)
                .bind(status)
                .bind(last_error)
                .bind(if last_error.is_some() { Some(now) } else { None })
                .execute(&self.db_pool)
                .await?;
        }
        Ok(())
    }

    async fn update_last_start(&self, pipeline_id: i32) -> Result<()> {
        let now =
            chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
        sqlx::query("UPDATE pipeline_metadata SET last_start_at = $1 WHERE pipeline_id = $2")
            .bind(now)
            .bind(pipeline_id)
            .execute(&self.db_pool)
            .await?;
        Ok(())
    }

    async fn set_pipeline_status(&self, pipeline_id: i32, status: &str) -> Result<()> {
        sqlx::query("UPDATE pipelines SET status = $1 WHERE id = $2")
            .bind(status)
            .bind(pipeline_id)
            .execute(&self.db_pool)
            .await?;
        Ok(())
    }
}
