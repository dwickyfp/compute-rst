use anyhow::Result;
use duckdb::Connection;
use etl::config::PgConnectionConfig;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Cell, Event, TableId, TableRow};
use secrecy::ExposeSecret;
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Convert ETL Cell to a string representation suitable for DuckDB/Postgres.
/// This follows the same pattern as Snowflake's cell_to_json_value function.
fn cell_to_string(cell: &Cell) -> Option<String> {
    match cell {
        Cell::Null => None,
        Cell::Bool(v) => Some(v.to_string()),
        Cell::String(v) => Some(v.clone()),
        Cell::I16(v) => Some(v.to_string()),
        Cell::I32(v) => Some(v.to_string()),
        Cell::I64(v) => Some(v.to_string()),
        Cell::F32(v) => Some(v.to_string()),
        Cell::F64(v) => Some(v.to_string()),
        Cell::Bytes(v) => Some(format!(
            "\\x{}",
            v.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        )),
        Cell::Json(v) => Some(v.to_string()),
        Cell::Numeric(v) => Some(v.to_string()),
        Cell::Uuid(v) => Some(v.to_string()),
        Cell::Array(v) => {
            // Convert arrays to Postgres array literal format
            match v {
                etl::types::ArrayCell::Bool(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|b| b.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::I16(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|n| n.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::I32(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|n| n.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::I64(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|n| n.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::F32(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|n| n.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::F64(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt.map(|n| n.to_string()).unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::String(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt
                            .as_ref()
                            .map(|s| format!("\"{}\"", s.replace("\"", "\\\"")))
                            .unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::Numeric(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt
                            .as_ref()
                            .map(|n| n.to_string())
                            .unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::Date(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt
                            .as_ref()
                            .map(|d| d.to_string())
                            .unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::TimestampTz(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt
                            .as_ref()
                            .map(|t| t.to_rfc3339())
                            .unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                etl::types::ArrayCell::Uuid(list) => Some(format!(
                    "{{{}}}",
                    list.iter()
                        .map(|opt| opt
                            .as_ref()
                            .map(|u| u.to_string())
                            .unwrap_or("NULL".to_string()))
                        .collect::<Vec<_>>()
                        .join(",")
                )),
                _ => Some(format!("{:?}", v)),
            }
        }
        Cell::Date(v) => Some(v.to_string()),
        Cell::Time(v) => Some(v.to_string()),
        Cell::Timestamp(v) => Some(v.to_string()),
        Cell::TimestampTz(v) => Some(v.to_rfc3339()),
        _ => Some(format!("{:?}", cell)),
    }
}

#[derive(Clone)]
pub struct PostgresDuckdbDestination {
    name: String,
    pg_config: PgConnectionConfig,
    db_pool: Pool<Postgres>,     // Main DB pool for fetching sync config
    source_pool: Pool<Postgres>, // Source DB pool for schema
    pipeline_destination_id: i32,
    pipeline_id: i32,
    source_id: i32,
    // Caches
    table_cache: Arc<Mutex<HashMap<TableId, String>>>,
    // Cache stores (name, type) pairs
    column_cache: Arc<Mutex<HashMap<TableId, Vec<(String, String)>>>>,
}

impl PostgresDuckdbDestination {
    pub fn new(
        name: String,
        pg_config: PgConnectionConfig,
        db_pool: Pool<Postgres>,
        source_pool: Pool<Postgres>,
        pipeline_destination_id: i32,
        pipeline_id: i32,
        source_id: i32,
    ) -> Result<Self> {
        Ok(Self {
            name,
            pg_config,
            db_pool,
            source_pool,
            pipeline_destination_id,
            pipeline_id,
            source_id,
            table_cache: Arc::new(Mutex::new(HashMap::new())),
            column_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    // ...
    async fn resolve_table_name(&self, table_id: TableId) -> String {
        let mut cache = self.table_cache.lock().await;
        if let Some(name) = cache.get(&table_id) {
            return name.clone();
        }

        let query = "SELECT cast($1::regclass as text)";
        let row: Option<String> = sqlx::query_scalar(query)
            .bind(table_id.0 as i32)
            .fetch_optional(&self.source_pool)
            .await
            .unwrap_or(None);

        let name = row.unwrap_or(format!("unknown_table_{}", table_id));
        // let sanitized = name.replace('.', "_");

        cache.insert(table_id, name.clone());
        name
    }

    async fn resolve_columns(&self, table_id: TableId) -> Vec<(String, String)> {
        let mut cache = self.column_cache.lock().await;
        if let Some(cols) = cache.get(&table_id) {
            return cols.clone();
        }

        // Query Postgres for column names and types
        // We prefer udt_name for array types (e.g. _varchar) or format_type
        let query = r#"
            SELECT column_name, udt_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = $1))
              AND table_name = (SELECT relname FROM pg_class WHERE oid = $1)
            ORDER BY ordinal_position
        "#;

        let rows: Vec<(String, String, String)> = sqlx::query_as(query)
            .bind(table_id.0 as i32)
            .fetch_all(&self.source_pool)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to query distinct columns for TableId {}: {}",
                    table_id, e
                );
                vec![]
            });

        let cols: Vec<(String, String)> = rows
            .into_iter()
            .map(|(name, udt, dtype)| {
                // Map types to Postgres castable types
                // Note: DuckDB's postgres_scanner allows some casts, but for others we need to use
                // generic types that DuckDB understands or that map cleanly.
                let final_type = if dtype == "ARRAY" {
                    // udt_name for arrays usually starts with underscore, e.g. _varchar, _int4
                    // We need to convert it to a type DuckDB understands (e.g. VARCHAR[], INTEGER[])
                    if udt.starts_with('_') {
                        let inner = &udt[1..];
                        match inner {
                            "int2" | "int4" => "INTEGER[]".to_string(), // DuckDB prefers INTEGER
                            "int8" => "BIGINT[]".to_string(),
                            "float4" | "float8" => "DOUBLE[]".to_string(),
                            "bool" => "BOOLEAN[]".to_string(),
                            "text" | "varchar" | "bpchar" | "char" => "VARCHAR[]".to_string(),
                            "json" | "jsonb" => "JSON[]".to_string(),
                            "uuid" => "UUID[]".to_string(),
                            // Fallback for others
                            _ => format!("{}[]", inner),
                        }
                    } else {
                        // Fallback if udt doesn't start with _
                        format!("{}[]", udt)
                    }
                } else {
                    match udt.as_str() {
                        "jsonb" | "json" => "JSON".to_string(),
                        "uuid" => "UUID".to_string(),
                        "timestamptz" | "timestamp" => "TIMESTAMPTZ".to_string(),
                        "geography" | "geometry" | "box2d" | "box3d" => "VARCHAR".to_string(), // Treat complex spatial types as strings (WKT)
                        "int2" | "int4" => "INTEGER".to_string(),
                        "int8" => "BIGINT".to_string(),
                        "float4" => "FLOAT".to_string(),
                        "float8" => "DOUBLE".to_string(),
                        "bool" => "BOOLEAN".to_string(),
                        _ => udt,
                    }
                };
                (name, final_type)
            })
            .collect();

        cache.insert(table_id, cols.clone());
        cols
    }

    async fn resolve_primary_key_columns(&self, table_id: TableId) -> Vec<String> {
        // Query Postgres for primary key columns
        let query = r#"
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1 AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        "#;

        let rows: Vec<(String,)> = sqlx::query_as(query)
            .bind(table_id.0 as i32)
            .fetch_all(&self.source_pool)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to query primary key for TableId {}: {}",
                    table_id, e
                );
                vec![]
            });

        rows.into_iter().map(|(n,)| n).collect()
    }

    pub async fn check_connection(&self) -> EtlResult<()> {
        // Try to initialize DuckDB connection which attempts to connect to Postgres
        self.get_duckdb_connection().map(|_| ()).map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::Unknown,
                "Connection check failed: {}",
                e
            )
        })
    }

    fn get_duckdb_connection(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL postgres; LOAD postgres;")?;

        let pg_dsn = format!(
            "dbname={} user={} host={} port={} password={}",
            self.pg_config.name,
            self.pg_config.username,
            self.pg_config.host,
            self.pg_config.port,
            self.pg_config
                .password
                .as_ref()
                .map(|p| p.expose_secret())
                .unwrap_or("")
        );

        // Sanitize name to be a valid SQL identifier (replace special chars with underscores)
        let sanitized_name = self
            .name
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();

        let attach_query = format!(
            "ATTACH '{}' AS pg_{} (TYPE POSTGRES);",
            pg_dsn, sanitized_name
        );
        conn.execute_batch(&attach_query)?;

        Ok(conn)
    }

    /// Convert a TableRow to a Vec of optional string params, handling array format conversion
    fn row_to_params(row: &TableRow, columns: &[(String, String)]) -> Vec<Option<String>> {
        row.values
            .iter()
            .zip(columns)
            .map(|(val, (_, col_type))| {
                let s = cell_to_string(val);
                if let Some(ref str_val) = s {
                    // Check if it's an array type and value is in Postgres format e.g. "{a,b}"
                    // DuckDB expects list format e.g. "[a, b]" for casting to array
                    if col_type.ends_with("[]")
                        && str_val.starts_with('{')
                        && str_val.ends_with('}')
                    {
                        let inner = &str_val[1..str_val.len() - 1];
                        return Some(format!("[{}]", inner));
                    }
                }
                s
            })
            .collect()
    }

    async fn get_table_sync_config(
        &self,
        table_name: &str,
    ) -> Result<Vec<(i32, String, String, String)>> {
        let rows = sqlx::query(
            "SELECT id, custom_sql, filter_sql, table_name_target FROM pipelines_destination_table_sync 
             WHERE pipeline_destination_id = $1 AND table_name = $2",
        )
        .bind(self.pipeline_destination_id)
        .bind(table_name)
        .fetch_all(&self.db_pool)
        .await?;

        let mut configs = Vec::new();

        for r in rows {
            let id: i32 = r.try_get("id")?;
            let custom_sql: Option<String> = r.try_get("custom_sql")?;
            let filter_sql: Option<String> = r.try_get("filter_sql")?;
            let table_name_target: String = r.try_get("table_name_target")?;

            info!(
                "Fetched sync config for '{}': target='{}' (pipeline_dest_id={})",
                table_name, table_name_target, self.pipeline_destination_id
            );

            configs.push((
                id,
                custom_sql.unwrap_or_default(),
                filter_sql.unwrap_or_default(),
                table_name_target,
            ));
        }

        Ok(configs)
    }
}

impl Destination for PostgresDuckdbDestination {
    fn name() -> &'static str {
        "postgres_duckdb"
    }

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        Ok(())
    }

    async fn write_table_rows(&self, _table_id: TableId, _rows: Vec<TableRow>) -> EtlResult<()> {
        warn!("write_table_rows not implemented for PostgresDuckdbDestination");
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut events_by_table: HashMap<TableId, Vec<&Event>> = HashMap::new();
        for event in &events {
            let tid = match event {
                Event::Insert(i) => Some(i.table_id),
                Event::Update(u) => Some(u.table_id),
                Event::Delete(d) => Some(d.table_id),
                _ => None,
            };
            if let Some(id) = tid {
                events_by_table.entry(id).or_default().push(event);
            }
        }

        // Collect all async data first (before creating DuckDB connection)
        #[allow(dead_code)]
        struct TableData {
            sync_id: Option<i32>,
            table_name: String, // Full name with schema (e.g., "public.table")
            table_name_target: String, // Target table name for destination
            schema_name: String, // Schema part (e.g., "public")
            short_table_name: String, // Target table name only (e.g., "table")
            columns: Vec<(String, String)>, // (name, type)
            pk_columns: Vec<String>,
            custom_sql: String,
            filter_sql: String,
            upsert_events: Vec<Vec<Option<String>>>, // INSERT/UPDATE row data
            delete_events: Vec<Vec<Option<String>>>, // DELETE row data (PKs only needed)
        }

        let mut tables_data: Vec<TableData> = Vec::new();

        for (table_id, table_events) in events_by_table {
            let table_name = self.resolve_table_name(table_id).await;
            let columns = self.resolve_columns(table_id).await;

            if columns.is_empty() {
                warn!("No columns found for table {}, skipping.", table_name);
                continue;
            }

            // Sync config
            let sync_configs = self
                .get_table_sync_config(&table_name)
                .await
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Fetch config error: {}", e))?;

            if sync_configs.is_empty() {
                info!("No sync config for {}, skipping.", table_name);
                continue;
            }

            // Get primary key columns for MERGE
            let pk_columns = self.resolve_primary_key_columns(table_id).await;

            // Pre-convert event rows to string params - separate upserts from deletes
            let mut upsert_events: Vec<Vec<Option<String>>> = Vec::new();
            let mut delete_events: Vec<Vec<Option<String>>> = Vec::new();

            for event in table_events {
                match event {
                    Event::Insert(i) => {
                        let params = Self::row_to_params(&i.table_row, &columns);
                        upsert_events.push(params);
                    }
                    Event::Update(u) => {
                        let params = Self::row_to_params(&u.table_row, &columns);
                        upsert_events.push(params);
                    }
                    Event::Delete(d) => {
                        // For deletes, we only need PK values
                        if let Some((_, row)) = d.old_table_row.as_ref() {
                            let params = Self::row_to_params(row, &columns);
                            delete_events.push(params);
                        }
                    }
                    _ => {}
                }
            }

            // Process each sync configuration (branch)
            for (sync_id, custom_sql, filter_sql, table_name_target) in sync_configs {
                // Parse target table name for schema/table parts
                let (schema_name, short_table_name) = if table_name_target.contains('.') {
                    let parts: Vec<&str> = table_name_target.splitn(2, '.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    // If target doesn't have schema, use schema from source table
                    let source_schema = if table_name.contains('.') {
                        table_name
                            .splitn(2, '.')
                            .next()
                            .unwrap_or("public")
                            .to_string()
                    } else {
                        "public".to_string()
                    };
                    (source_schema, table_name_target.clone())
                };

                tables_data.push(TableData {
                    sync_id: Some(sync_id),
                    table_name: table_name.clone(),
                    table_name_target,
                    schema_name,
                    short_table_name,
                    columns: columns.clone(),
                    pk_columns: pk_columns.clone(),
                    custom_sql,
                    filter_sql,
                    upsert_events: upsert_events.clone(),
                    delete_events: delete_events.clone(),
                });
            }
        }

        // Now create DuckDB connection and process synchronously (no await points)
        let conn = self
            .get_duckdb_connection()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "DuckDB Init: {}", e))?;

        // Sanitize destination name once
        let sanitized_dest_name: String = self
            .name
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        // Clone the pool for use in the closure/loop
        let db_pool = self.db_pool.clone();

        for data in tables_data {
            // We'll wrap processing in a function/block to catch errors
            let result = (|| -> EtlResult<()> {
                // Prepare DuckDB Table
                let create_cols = data
                    .columns
                    .iter()
                    .map(|(c, _)| format!("\"{}\" TEXT", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Err(e) = conn.execute_batch(&format!(
                    "CREATE OR REPLACE TABLE duckdb_updates ({});",
                    create_cols
                )) {
                    return Err(etl_error!(ErrorKind::Unknown, "Create table error: {}", e));
                }

                // Insert Data
                info!(
                    "Processing table {}: {} events, {} columns, pk_columns: {:?}",
                    data.table_name,
                    data.upsert_events.len(),
                    data.columns.len(),
                    data.pk_columns
                );

                let insert_sql = format!(
                    "INSERT INTO duckdb_updates VALUES ({})",
                    vec!["?"; data.columns.len()].join(", ")
                );
                let mut stmt = conn
                    .prepare(&insert_sql)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Prepare insert error: {}", e))?;

                let mut inserted_count = 0;
                for params in &data.upsert_events {
                    if let Err(e) = stmt.execute(duckdb::params_from_iter(params.clone())) {
                        warn!("Insert row error: {}", e);
                    } else {
                        inserted_count += 1;
                    }
                }
                info!("Inserted {} rows into duckdb_updates", inserted_count);

                // Apply filter logic
                let filtered_table = if !data.filter_sql.trim().is_empty() {
                    let safe_filter: String = data.filter_sql.trim().replace(";", "");
                    format!(
                        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM duckdb_updates WHERE {};",
                        data.table_name, safe_filter
                    )
                } else {
                    format!(
                        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM duckdb_updates;",
                        data.table_name
                    )
                };

                if let Err(e) = conn.execute_batch(&filtered_table) {
                    return Err(etl_error!(
                        ErrorKind::Unknown,
                        "Filter execution error: {}",
                        e
                    ));
                }

                // Execute custom SQL if provided - this transforms the source table data
                // custom_sql should be a SELECT statement that will replace the source-named table content
                if !data.custom_sql.trim().is_empty() {
                    // Custom SQL should be a CREATE TABLE or SELECT statement
                    // We wrap it to replace the source-named table with the transformed result
                    let safe_custom_sql = data.custom_sql.trim().trim_end_matches(';');

                    // Check if custom_sql is a SELECT statement (transform query)
                    let is_select = safe_custom_sql
                        .to_uppercase()
                        .trim_start()
                        .starts_with("SELECT");

                    if is_select {
                        // Wrap the SELECT to create/replace the source-named table
                        let transform_sql = format!(
                            "CREATE OR REPLACE TABLE \"{}\" AS {};",
                            data.table_name, safe_custom_sql
                        );
                        if let Err(e) = conn.execute_batch(&transform_sql) {
                            return Err(etl_error!(
                                ErrorKind::Unknown,
                                "Custom SQL execution error: {}",
                                e
                            ));
                        }
                    } else {
                        // Execute as-is (e.g., if user provides CREATE TABLE or other DDL)
                        if let Err(e) = conn.execute_batch(safe_custom_sql) {
                            return Err(etl_error!(
                                ErrorKind::Unknown,
                                "Custom SQL execution error: {}",
                                e
                            ));
                        }
                    }
                }

                // After filter_sql and custom_sql (if any), get the columns from the result table
                // This allows custom_sql to transform/select specific columns
                let result_columns: Vec<(String, String)> = {
                    let query = format!("DESCRIBE \"{}\"", data.table_name);
                    match conn.prepare(&query) {
                        Ok(mut stmt) => {
                            let rows: Vec<(String, String)> = stmt
                                .query_map([], |row| {
                                    let col_name: String = row.get(0)?;
                                    let col_type: String = row.get(1)?;
                                    Ok((col_name, col_type))
                                })
                                .map(|iter| iter.filter_map(|r| r.ok()).collect())
                                .unwrap_or_default();
                            if rows.is_empty() {
                                // Fallback to original columns if DESCRIBE fails
                                data.columns.clone()
                            } else {
                                rows
                            }
                        }
                        Err(_) => data.columns.clone(),
                    }
                };

                // Now upsert the result into target Postgres table
                // This applies to all cases (with or without custom_sql)
                if data.pk_columns.is_empty() {
                    warn!(
                        "No primary key found for table {}, using INSERT instead of MERGE.",
                        data.table_name
                    );
                    // Fallback to simple INSERT
                    let col_list = result_columns
                        .iter()
                        .map(|(c, _)| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", ");

                    // Use original column types for casting (not DuckDB VARCHAR types)
                    let select_list = result_columns
                        .iter()
                        .map(|(c, _)| {
                            // Find the original Postgres type for this column
                            let orig_type = data
                                .columns
                                .iter()
                                .find(|(orig_c, _)| orig_c == c)
                                .map(|(_, t)| t.as_str())
                                .unwrap_or("VARCHAR");
                            format!("\"{}\"::{}", c, orig_type)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    // Use proper path: pg_<dest>.<schema>."<table>"
                    let insert_sql = format!(
                        "INSERT INTO pg_{}.{}.\"{}\" ({}) SELECT {} FROM \"{}\";",
                        sanitized_dest_name,
                        data.schema_name,
                        data.short_table_name,
                        col_list,
                        select_list,
                        data.table_name
                    );
                    if let Err(e) = conn.execute_batch(&insert_sql) {
                        return Err(etl_error!(
                            ErrorKind::Unknown,
                            "INSERT execution error: {}",
                            e
                        ));
                    }
                } else {
                    // Use DELETE + INSERT pattern instead of MERGE
                    // DuckDB's postgres_scanner strips type casts when translating MERGE to UPDATE,
                    // causing type mismatch errors. DELETE+INSERT is more reliable.

                    let col_list = result_columns
                        .iter()
                        .map(|(c, _)| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", ");

                    // Use original column types for casting (not DuckDB VARCHAR types)
                    let select_list = result_columns
                        .iter()
                        .map(|(c, _)| {
                            // Find the original Postgres type for this column
                            let orig_type = data
                                .columns
                                .iter()
                                .find(|(orig_c, _)| orig_c == c)
                                .map(|(_, t)| t.as_str())
                                .unwrap_or("VARCHAR");
                            format!("\"{}\"::{}", c, orig_type)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    // Build WHERE clause for DELETE based on PKs
                    // Need to cast the PK column from DuckDB (TEXT) to match Postgres type
                    // Only use PKs that exist in the result columns
                    let available_pks: Vec<&String> = data
                        .pk_columns
                        .iter()
                        .filter(|pk| result_columns.iter().any(|(c, _)| c == *pk))
                        .collect();

                    if available_pks.is_empty() {
                        warn!(
                            "No primary key columns found in result for table {}, using INSERT only.",
                            data.table_name
                        );
                        let insert_sql = format!(
                            "INSERT INTO pg_{}.{}.\"{}\" ({}) SELECT {} FROM \"{}\";",
                            sanitized_dest_name,
                            data.schema_name,
                            data.short_table_name,
                            col_list,
                            select_list,
                            data.table_name
                        );
                        if let Err(e) = conn.execute_batch(&insert_sql) {
                            return Err(etl_error!(
                                ErrorKind::Unknown,
                                "INSERT execution error: {}",
                                e
                            ));
                        }
                    } else {
                        // Step 1: DELETE existing rows that match PKs
                        // The SQL for DELETE using IN clause
                        let pk_conditions = available_pks
                            .iter()
                            .map(|pk| {
                                let pk_type = data
                                    .columns
                                    .iter()
                                    .find(|(c, _)| c == *pk)
                                    .map(|(_, t)| t.as_str())
                                    .unwrap_or("BIGINT");
                                format!("\"{}\"::{}", pk, pk_type)
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        let delete_sql = format!(
                            "DELETE FROM pg_{}.{}.\"{}\" WHERE ({}) IN (SELECT {} FROM \"{}\");",
                            sanitized_dest_name,
                            data.schema_name,
                            data.short_table_name,
                            available_pks
                                .iter()
                                .map(|k| format!("\"{}\"", k))
                                .collect::<Vec<_>>()
                                .join(", "),
                            pk_conditions,
                            data.table_name
                        );

                        if let Err(e) = conn.execute_batch(&delete_sql) {
                            return Err(etl_error!(
                                ErrorKind::Unknown,
                                "DELETE execution error: {}",
                                e
                            ));
                        }

                        // Step 2: INSERT new rows
                        let insert_sql = format!(
                            "INSERT INTO pg_{}.{}.\"{}\" ({}) SELECT {} FROM \"{}\";",
                            sanitized_dest_name,
                            data.schema_name,
                            data.short_table_name,
                            col_list,
                            select_list,
                            data.table_name
                        );
                        if let Err(e) = conn.execute_batch(&insert_sql) {
                            return Err(etl_error!(
                                ErrorKind::Unknown,
                                "INSERT execution error: {}",
                                e
                            ));
                        }
                    }
                }

                if !data.delete_events.is_empty() && !data.pk_columns.is_empty() {
                    // Create a temp table for delete PKs
                    let delete_table_name = format!("{}_deletes", data.table_name);

                    // Build PK column definitions for delete temp table
                    let pk_col_defs = data
                        .pk_columns
                        .iter()
                        .map(|pk| format!("\"{}\" TEXT", pk))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let create_delete_table = format!(
                        "CREATE OR REPLACE TABLE \"{}\" ({});",
                        delete_table_name, pk_col_defs
                    );

                    if let Err(e) = conn.execute_batch(&create_delete_table) {
                        warn!("Create delete table error: {}", e);
                    } else {
                        // Insert delete PKs into temp table
                        let pk_indices: Vec<usize> = data
                            .pk_columns
                            .iter()
                            .filter_map(|pk| data.columns.iter().position(|(c, _)| c == pk))
                            .collect();

                        let insert_delete_sql = format!(
                            "INSERT INTO \"{}\" VALUES ({})",
                            delete_table_name,
                            vec!["?"; data.pk_columns.len()].join(", ")
                        );

                        if let Ok(mut stmt) = conn.prepare(&insert_delete_sql) {
                            for row_params in &data.delete_events {
                                let pk_values: Vec<Option<String>> = pk_indices
                                    .iter()
                                    .filter_map(|&i| row_params.get(i).cloned())
                                    .collect();
                                let _ = stmt.execute(duckdb::params_from_iter(pk_values));
                            }
                        }

                        // Execute DELETE based on PKs in temp table
                        let pk_match = data
                            .pk_columns
                            .iter()
                            .map(|pk| {
                                let pk_type = data
                                    .columns
                                    .iter()
                                    .find(|(c, _)| c == pk)
                                    .map(|(_, t)| t.as_str())
                                    .unwrap_or("BIGINT");
                                format!(
                                    "\"{}\" IN (SELECT \"{}\"::{} FROM \"{}\")",
                                    pk, pk, pk_type, delete_table_name
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(" AND ");

                        let delete_sql = format!(
                            "DELETE FROM pg_{}.{}.\"{}\" WHERE {};",
                            sanitized_dest_name, data.schema_name, data.short_table_name, pk_match
                        );

                        info!(
                            "Executing DELETE for {} delete events: {}",
                            data.delete_events.len(),
                            delete_sql
                        );
                        if let Err(e) = conn.execute_batch(&delete_sql) {
                            warn!("DELETE execution error for delete events: {}", e);
                        } else {
                            info!(
                                "DELETE completed successfully for {} rows",
                                data.delete_events.len()
                            );
                        }
                    }
                }
                Ok(())
            })();

            if result.is_ok() {
                let record_count = (data.upsert_events.len() + data.delete_events.len()) as i64;
                let now = chrono::Utc::now()
                    .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());

                if let Err(e) = sqlx::query(
                    "INSERT INTO data_flow_record_monitoring (pipeline_id, pipeline_destination_id, source_id, table_name, record_count, created_at, updated_at, pipeline_destination_table_sync_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(self.pipeline_id)
                .bind(self.pipeline_destination_id)
                .bind(self.source_id)
                .bind(&data.table_name)
                .bind(record_count)
                .bind(now)
                .bind(now)
                .bind(data.sync_id)
                .execute(&db_pool)
                .await
                {
                    error!(
                        "Failed to insert monitoring record for {}: {}",
                        data.table_name, e
                    );
                }
            }

            if let Some(sid) = data.sync_id {
                let pool = db_pool.clone();
                match result {
                    Ok(_) => {
                        let _ = sqlx::query(
                            "UPDATE pipelines_destination_table_sync 
                             SET is_error = false, error_message = NULL, updated_at = NOW() 
                             WHERE id = $1",
                        )
                        .bind(sid)
                        .execute(&pool)
                        .await;
                    }
                    Err(e) => {
                        error!("Error processing table {}: {}", data.table_name, e);
                        let _ = sqlx::query(
                            "UPDATE pipelines_destination_table_sync 
                             SET is_error = true, error_message = $2, updated_at = NOW() 
                             WHERE id = $1",
                        )
                        .bind(sid)
                        .bind(e.to_string())
                        .execute(&pool)
                        .await;
                        continue;
                    }
                }
            } else if let Err(e) = result {
                error!(
                    "Error processing table {} (no sync config): {}",
                    data.table_name, e
                );
            }
        }

        Ok(())
    }
}
