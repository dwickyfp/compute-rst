use super::client::SnowpipeClient;
use crate::config::SnowflakeConfig;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Cell, Event, TableId, TableRow};
use serde_json::{Value, json};
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug, Clone)]
struct SnowflakeSyncConfig {
    id: Option<i32>,
    target_table: String,
}

#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    client: Arc<Mutex<SnowpipeClient>>,
    current_token: Arc<Mutex<HashMap<String, String>>>, // Key is Target Table Name
    pg_pool: Pool<Postgres>,
    metadata_pool: Pool<Postgres>,
    pipeline_id: i32,
    pipeline_destination_id: i32,
    source_id: i32,
    real_table_cache: Arc<Mutex<HashMap<TableId, String>>>,
    column_cache: Arc<Mutex<HashMap<TableId, Vec<String>>>>,
}

// Helper: Convert ETL Cell to JSON Value
fn cell_to_json_value(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(v) => json!(v),
        Cell::String(v) => json!(v),
        Cell::I16(v) => json!(v),
        Cell::I32(v) => json!(v),
        Cell::I64(v) => json!(v),
        Cell::F32(v) => json!(v),
        Cell::F64(v) => json!(v),
        Cell::Bytes(v) => json!(format!("<bytes len={}>", v.len())),
        Cell::Json(v) => v.clone(),
        Cell::Numeric(v) => json!(v.to_string()),
        Cell::Uuid(v) => json!(v.to_string()), // Handle UUID properly
        Cell::Array(v) => match v {
            etl::types::ArrayCell::Bool(list) => json!(list),
            etl::types::ArrayCell::I16(list) => json!(list),
            etl::types::ArrayCell::I32(list) => json!(list),
            etl::types::ArrayCell::I64(list) => json!(list),
            etl::types::ArrayCell::F32(list) => json!(list),
            etl::types::ArrayCell::F64(list) => json!(list),
            etl::types::ArrayCell::String(list) => json!(list),
            etl::types::ArrayCell::Numeric(list) => json!(
                list.iter()
                    .map(|opt| opt.as_ref().map(|n| n.to_string()))
                    .collect::<Vec<_>>()
            ),
            etl::types::ArrayCell::Date(list) => json!(
                list.iter()
                    .map(|opt| opt.as_ref().map(|d| d.to_string()))
                    .collect::<Vec<_>>()
            ),
            etl::types::ArrayCell::TimestampTz(list) => json!(
                list.iter()
                    .map(|opt| opt.as_ref().map(|t| t.to_rfc3339()))
                    .collect::<Vec<_>>()
            ),
            etl::types::ArrayCell::Uuid(list) => json!(
                list.iter()
                    .map(|opt| opt.as_ref().map(|u| u.to_string()))
                    .collect::<Vec<_>>()
            ),
            _ => json!(format!("{:?}", v)),
        },
        Cell::Date(v) => json!(v.to_string()),
        Cell::Time(v) => json!(v.to_string()),
        Cell::Timestamp(v) => json!(v.to_string()), // NaiveDateTime uses to_string()
        Cell::TimestampTz(v) => json!(v.to_rfc3339()),
        _ => json!(format!("{:?}", cell)),
    }
}

// Helper: Convert TableRow to JSON object with column names
fn row_to_json_object(row: &TableRow, column_names: &[String], operation: &str) -> Value {
    let mut obj = serde_json::Map::new();

    // Add column values - convert to uppercase for Snowflake
    for (i, cell) in row.values.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| format!("COL_{}", i));
        obj.insert(col_name, cell_to_json_value(cell));
    }

    // Add operation column (uppercase for Snowflake)
    obj.insert("OPERATION".to_string(), json!(operation));

    // Add sync timestamp (uppercase for Snowflake)
    obj.insert(
        "SYNC_TIMESTAMP_ROSETTA".to_string(),
        json!(chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap())
            .to_rfc3339()),
    );

    Value::Object(obj)
}

impl SnowflakeDestination {
    pub fn new(
        config: SnowflakeConfig,
        pg_pool: Pool<Postgres>,
        metadata_pool: Pool<Postgres>,
        pipeline_id: i32,
        pipeline_destination_id: i32,
        source_id: i32,
    ) -> EtlResult<Self> {
        // Init client (akan hitung fingerprint di sini)
        let client = SnowpipeClient::new(config)
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Client init error: {}", e))?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            current_token: Arc::new(Mutex::new(HashMap::new())),
            pg_pool,
            metadata_pool,
            pipeline_id,
            pipeline_destination_id,
            source_id,
            real_table_cache: Arc::new(Mutex::new(HashMap::new())),
            column_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }



    async fn resolve_real_table_name(&self, table_id: TableId) -> String {
        let mut cache = self.real_table_cache.lock().await;
        if let Some(name) = cache.get(&table_id) {
            return name.clone();
        }

        // Query Postgres for table name
        let query = "SELECT relname FROM pg_class WHERE oid = $1";
        let row: Option<String> = sqlx::query_scalar(query)
            .bind(table_id.0 as i32)
            .fetch_optional(&self.pg_pool)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to query table name for TableId {}: {}", table_id, e);
                None
            });

        let table_name = row.unwrap_or_else(|| format!("UNKNOWN_{}", table_id));

        cache.insert(table_id, table_name.clone());
        table_name
    }

    async fn resolve_column_names(&self, table_id: TableId) -> Vec<String> {
        let mut cache = self.column_cache.lock().await;
        if let Some(columns) = cache.get(&table_id) {
            return columns.clone();
        }

        // Query Postgres for column names
        let query = r#"
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = $1))
              AND table_name = (SELECT relname FROM pg_class WHERE oid = $1)
            ORDER BY ordinal_position
        "#;

        let rows: Vec<(String,)> = sqlx::query_as(query)
            .bind(table_id.0 as i32)
            .fetch_all(&self.pg_pool)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to query column names for TableId {}: {}",
                    table_id, e
                );
                vec![]
            });

        let column_names: Vec<String> = rows.into_iter().map(|(name,)| name).collect();

        cache.insert(table_id, column_names.clone());
        column_names
    }
    pub async fn check_connection(&self) -> EtlResult<()> {
        // Check metadata pool connectivity as a proxy for health
        sqlx::query("SELECT 1")
            .execute(&self.metadata_pool)
            .await
            .map(|_| ())
            .map_err(|e| etl::etl_error!(etl::error::ErrorKind::Unknown, "Metadata pool check failed: {}", e))
    }

    async fn resolve_syncs(&self, source_table_name: &str) -> Vec<SnowflakeSyncConfig> {
        let query = "SELECT id, table_name_target FROM pipelines_destination_table_sync WHERE pipeline_destination_id = $1 AND table_name = $2";
        let rows: Vec<(i32, String)> = sqlx::query_as(query)
            .bind(self.pipeline_destination_id)
            .bind(source_table_name)
            .fetch_all(&self.metadata_pool)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to resolve syncs for source table {}: {}", source_table_name, e);
                vec![]
            });

        if rows.is_empty() {
             vec![]
        } else {
            rows.into_iter().map(|(id, target)| {
                let mut upper_target = target.to_uppercase();
                if !upper_target.starts_with("LANDING_") {
                    upper_target = format!("LANDING_{}", upper_target);
                }
                SnowflakeSyncConfig { id: Some(id), target_table: upper_target }
            }).collect()
        }
    }
}

impl Destination for SnowflakeDestination {
    fn name() -> &'static str {
        "snowflake_streaming_rest"
    }

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        info!("Truncate ignored (Append-only)");
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Resolve Source Table Name first
        let real_source_table_name = self.resolve_real_table_name(table_id).await;
        // Resolve all destination syncs for this source table
        let syncs = self.resolve_syncs(&real_source_table_name).await;

        let column_names = self.resolve_column_names(table_id).await;
        
        let json_rows: Vec<Value> = rows
            .iter()
            .map(|r| row_to_json_object(r, &column_names, "C"))
            .collect();

        for sync in syncs {
            let target_table_name = sync.target_table;
            let sync_id = sync.id;

            let result: EtlResult<()> = (|| async {
                let mut client = self.client.lock().await;
                let mut tokens = self.current_token.lock().await;

                let token = tokens.entry(target_table_name.clone()).or_insert_with(String::new);

                if token.is_empty() {
                    *token = client
                        .open_channel(&target_table_name, "default")
                        .await
                        .map_err(|e| {
                            error!("Open channel failed for {}: {}", target_table_name, e);
                            etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e)
                        })?;
                }

                let rows_for_insert = json_rows.clone();

                let next_token = client
                    .insert_rows(&target_table_name, "default", rows_for_insert, Some(token.clone()))
                    .await
                    .map_err(|e| {
                        error!("Insert rows failed for {}: {}", target_table_name, e);
                        etl_error!(ErrorKind::Unknown, "Write rows failed: {}", e)
                    })?;

                *token = next_token;
                Ok(())
            })().await;

            match result {
                Ok(_) => {
                    // Monitor data flow
                    let record_count = rows.len() as i64;
                    let now = chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());

                    if let Err(e) = sqlx::query(
                        "INSERT INTO data_flow_record_monitoring (pipeline_id, pipeline_destination_id, source_id, table_name, record_count, created_at, updated_at, pipeline_destination_table_sync_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    )
                    .bind(self.pipeline_id)
                    .bind(self.pipeline_destination_id)
                    .bind(self.source_id)
                    .bind(&real_source_table_name)
                    .bind(record_count)
                    .bind(now)
                    .bind(now)
                    .bind(sync_id)
                    .execute(&self.metadata_pool)
                    .await
                    {
                        error!("Failed to insert monitoring record for {}: {}", real_source_table_name, e);
                    }

                    if let Some(sid) = sync_id {
                         let _ = sqlx::query(
                            "UPDATE pipelines_destination_table_sync 
                             SET is_error = false, error_message = NULL, updated_at = NOW() 
                             WHERE id = $1",
                        )
                        .bind(sid)
                        .execute(&self.metadata_pool)
                        .await;
                    }
                }
                Err(e) => {
                    error!("Error processing sync for table {}: {}", real_source_table_name, e);
                    if let Some(sid) = sync_id {
                        let _ = sqlx::query(
                            "UPDATE pipelines_destination_table_sync 
                             SET is_error = true, error_message = $2, updated_at = NOW() 
                             WHERE id = $1",
                        )
                        .bind(sid)
                        .bind(e.to_string())
                        .execute(&self.metadata_pool)
                        .await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut events_by_table: HashMap<TableId, Vec<Event>> = HashMap::new();
        for event in events {
            let table_id = match &event {
                Event::Insert(i) => Some(i.table_id),
                Event::Update(u) => Some(u.table_id),
                Event::Delete(d) => Some(d.table_id),
                _ => None,
            };

            if let Some(tid) = table_id {
                events_by_table.entry(tid).or_default().push(event);
            }
        }

        for (table_id, events) in events_by_table {
            let real_source_table_name = self.resolve_real_table_name(table_id).await;
            let syncs = self.resolve_syncs(&real_source_table_name).await;
            
            let column_names = self.resolve_column_names(table_id).await;

            let record_count = events.len() as i64;
            let mut json_rows = Vec::new();

            for event in events {
                let row_obj = match event {
                    Event::Insert(i) => Some(row_to_json_object(&i.table_row, &column_names, "C")),
                    Event::Update(u) => Some(row_to_json_object(&u.table_row, &column_names, "U")),
                    Event::Delete(d) => {
                        if let Some((_, old_row)) = &d.old_table_row {
                            Some(row_to_json_object(old_row, &column_names, "D"))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                if let Some(obj) = row_obj {
                    json_rows.push(obj);
                }
            }
            
            if !json_rows.is_empty() {
                 for sync in syncs {
                    let target_table_name = sync.target_table;
                    let sync_id = sync.id;

                    let result: EtlResult<()> = (|| async {
                        let mut client = self.client.lock().await;
                        let mut tokens = self.current_token.lock().await;

                        let token = tokens.entry(target_table_name.clone()).or_insert_with(String::new);

                        if token.is_empty() {
                            *token = client
                                .open_channel(&target_table_name, "default")
                                .await
                                .map_err(|e| {
                                    error!("Open channel failed for {}: {}", target_table_name, e);
                                    etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e)
                                })?;
                        }
                        
                        let rows_for_insert = json_rows.clone();

                        let next_token = client
                            .insert_rows(&target_table_name, "default", rows_for_insert, Some(token.clone()))
                            .await
                            .map_err(|e| {
                                error!("Insert events failed for {}: {}", target_table_name, e);
                                etl_error!(ErrorKind::Unknown, "Write events failed: {}", e)
                            })?;
                        *token = next_token;
                        Ok(())
                    })().await;
                    
                    match result {
                        Ok(_) => {
                            let now = chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());
                            if let Err(e) = sqlx::query(
                                "INSERT INTO data_flow_record_monitoring (pipeline_id, pipeline_destination_id, source_id, table_name, record_count, created_at, updated_at, pipeline_destination_table_sync_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                            )
                            .bind(self.pipeline_id)
                            .bind(self.pipeline_destination_id)
                            .bind(self.source_id)
                            .bind(&real_source_table_name)
                            .bind(record_count)
                            .bind(now)
                            .bind(now)
                            .bind(sync_id)
                            .execute(&self.metadata_pool)
                            .await
                            {
                                error!("Failed to insert monitoring record for {}: {}", real_source_table_name, e);
                            }

                            if let Some(sid) = sync_id {
                                let _ = sqlx::query(
                                    "UPDATE pipelines_destination_table_sync 
                                     SET is_error = false, error_message = NULL, updated_at = NOW() 
                                     WHERE id = $1",
                                )
                                .bind(sid)
                                .execute(&self.metadata_pool)
                                .await;
                            }
                        }
                        Err(e) => {
                            error!("Error processing events for table {}: {}", real_source_table_name, e);
                             if let Some(sid) = sync_id {
                                let _ = sqlx::query(
                                    "UPDATE pipelines_destination_table_sync 
                                     SET is_error = true, error_message = $2, updated_at = NOW() 
                                     WHERE id = $1",
                                )
                                .bind(sid)
                                .bind(e.to_string())
                                .execute(&self.metadata_pool)
                                .await;
                            }
                        }
                    }
                 }
            }
        }

        Ok(())
    }
}
