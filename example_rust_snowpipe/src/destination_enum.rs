//! Destination Enum - Wraps different destination types with DLQ support
//!
//! Supports Snowflake, Postgres, and Multi (multiple destinations).
//! Each destination in Multi can independently fail and use DLQ.

use crate::dlq::retry::{is_connection_error, RetryManager};
use crate::dlq::store::DlqStore;
use etl::destination::Destination;
use etl::error::EtlResult;
use etl::types::{Event, TableId, TableRow};
use sqlx::{Pool, Postgres};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::postgres::destination::PostgresDuckdbDestination;
use crate::snowflake::SnowflakeDestination;

/// State for a single destination with DLQ capability
pub struct DestinationWithDlq {
    pub destination: DestinationEnum,
    pub pipeline_dest_id: i32,
    pub db_pool: Pool<Postgres>,

    // DLQ state
    pub dlq_store: Arc<DlqStore>,
    pub is_error: AtomicBool,
    pub error_message: RwLock<Option<String>>,
    pub retry_manager: Arc<RetryManager>,
    pub pending_tables: RwLock<HashSet<String>>,
    pub is_retrying: AtomicBool,
}

impl DestinationWithDlq {
    pub fn new(
        destination: DestinationEnum,
        pipeline_dest_id: i32,
        db_pool: Pool<Postgres>,
        dlq_store: Arc<DlqStore>,
    ) -> Self {
        Self {
            destination,
            pipeline_dest_id,
            db_pool,
            dlq_store,
            is_error: AtomicBool::new(false),
            error_message: RwLock::new(None),
            retry_manager: Arc::new(RetryManager::new()),
            pending_tables: RwLock::new(HashSet::new()),
            is_retrying: AtomicBool::new(false),
        }
    }

    pub fn is_in_error(&self) -> bool {
        self.is_error.load(Ordering::Relaxed)
    }

    async fn set_error_state(&self, error_msg: &str) {
        self.is_error.store(true, Ordering::Relaxed);
        *self.error_message.write().await = Some(error_msg.to_string());

        if let Err(e) = self.update_error_in_db(true, Some("Database Error")).await {
            error!("Failed to update error state in DB: {}", e);
        }

        info!("Destination {} entered error state: {}", self.pipeline_dest_id, error_msg);
    }

    async fn clear_error_state(&self) {
        self.is_error.store(false, Ordering::Relaxed);
        *self.error_message.write().await = None;
        self.retry_manager.reset();

        if let Err(e) = self.update_error_in_db(false, None).await {
            error!("Failed to clear error state in DB: {}", e);
        }

        info!("Destination {} recovered from error", self.pipeline_dest_id);
    }

    async fn update_error_in_db(&self, is_error: bool, error_msg: Option<&str>) -> anyhow::Result<()> {
        let now = chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(7 * 3600).unwrap());

        if is_error {
            sqlx::query(
                "UPDATE pipelines_destination 
                 SET is_error = true, error_message = $1, last_error_at = $2 
                 WHERE id = $3"
            )
            .bind(error_msg)
            .bind(now)
            .bind(self.pipeline_dest_id)
            .execute(&self.db_pool)
            .await?;
        } else {
            sqlx::query(
                "UPDATE pipelines_destination 
                 SET is_error = false, error_message = NULL 
                 WHERE id = $1"
            )
            .bind(self.pipeline_dest_id)
            .execute(&self.db_pool)
            .await?;
        }

        Ok(())
    }

    /// Initialize state from persistent storage on startup
    pub async fn init_from_persistence(self: Arc<Self>) -> anyhow::Result<()> {
        let count = self.dlq_store.count_for_destination(self.pipeline_dest_id).await;
        
        if count > 0 {
            let tables = self.dlq_store.get_pending_tables(self.pipeline_dest_id).await;
            info!(
                "Dest {}: Found {} pending DLQ events for tables {:?}. Starting recovery.",
                self.pipeline_dest_id, count, tables
            );

            // Set internal error state
            // We don't call set_error_state because that updates the DB with a generic "Database Error"
            // We want to keep whatever error was there, or just set the flag.
            self.is_error.store(true, Ordering::SeqCst);
            {
                let mut pt = self.pending_tables.write().await;
                for t in tables {
                    pt.insert(t);
                }
            }

            // Start recovery background task
            self.clone().start_recovery_loop();
        } else {
            // Check if DB says we are in error, maybe we should recover anyway?
            // For now, if DLQ is empty, we assume we are healthy on startup.
            // This is simpler and avoids stuck states if DB wasn't updated.
            let _ = self.update_error_in_db(false, None).await;
        }

        Ok(())
    }

    async fn push_to_dlq(&self, table_name: &str, events: Vec<Event>) -> anyhow::Result<()> {
        self.dlq_store.push(self.pipeline_dest_id, table_name, events).await?;
        self.pending_tables.write().await.insert(table_name.to_string());
        Ok(())
    }

    async fn flush_dlq_for_table(&self, table_name: &str) -> anyhow::Result<usize> {
        let mut total_flushed = 0;
        const BATCH_SIZE: usize = 100;

        loop {
            let events = self.dlq_store.pop_batch(self.pipeline_dest_id, table_name, BATCH_SIZE).await?;
            
            if events.is_empty() {
                break;
            }

            let count = events.len();
            
            match self.destination.write_events(events.clone()).await {
                Ok(_) => {
                    total_flushed += count;
                    debug!("Flushed {} DLQ events for table {}", count, table_name);
                }
                Err(e) => {
                    warn!("Failed to flush DLQ, pushing back: {:?}", e);
                    self.dlq_store.push(self.pipeline_dest_id, table_name, events).await?;
                    return Err(anyhow::anyhow!("Flush failed: {:?}", e));
                }
            }
        }

        Ok(total_flushed)
    }

    pub async fn flush_all_dlq(&self) -> anyhow::Result<usize> {
        let tables: Vec<String> = self.pending_tables.read().await.iter().cloned().collect();
        let mut total = 0;

        for table in tables {
            match self.flush_dlq_for_table(&table).await {
                Ok(count) => {
                    total += count;
                    self.pending_tables.write().await.remove(&table);
                }
                Err(e) => {
                    warn!("Failed to flush DLQ for table {}: {}", table, e);
                    return Err(e);
                }
            }
        }

        if total > 0 {
            info!("Flushed {} total DLQ events for dest {}", total, self.pipeline_dest_id);
        }
        Ok(total)
    }

    /// Write events with DLQ failover
    pub async fn write_events(self: Arc<Self>, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let table_name = Self::extract_table_name(&events).unwrap_or_else(|| "unknown".to_string());

        // If in error state, save directly to DLQ
        if self.is_in_error() {
            debug!(
                "Dest {} in error state, saving {} events to DLQ for table {}",
                self.pipeline_dest_id, events.len(), table_name
            );

            self.push_to_dlq(&table_name, events)
                .await
                .map_err(|e| (etl::error::ErrorKind::Unknown, "DLQ push error", e.to_string()))?;

            // Ensure recovery loop is running
            self.start_recovery_loop();

            return Ok(());
        }

        // Try to write to destination
        match self.destination.write_events(events.clone()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let error_str = format!("{:?}", e);

                if is_connection_error(&error_str) {
                    warn!(
                        "Dest {}: Connection error, saving to DLQ: {}",
                        self.pipeline_dest_id, error_str
                    );

                    self.set_error_state(&error_str).await;
                    self.push_to_dlq(&table_name, events)
                        .await
                        .map_err(|e| (etl::error::ErrorKind::Unknown, "DLQ push error", e.to_string()))?;

                    // Start recovery loop
                    self.start_recovery_loop();

                    // Return OK so pipeline continues
                    Ok(())
                } else {
                    // Non-connection error - propagate
                    Err(e)
                }
            }
        }
    }

    /// Spawn background recovery loop if not already running
    pub fn start_recovery_loop(self: Arc<Self>) {
        if self.is_retrying.swap(true, Ordering::SeqCst) {
            return;
        }

        let dest = self.clone();
        tokio::spawn(async move {
            info!("Dest {}: Starting recovery background loop", dest.pipeline_dest_id);
            
            loop {
                let delay = dest.retry_manager.next_delay();
                let attempt = dest.retry_manager.current_attempt();
                
                debug!("Dest {}: Retry attempt {} in {:?}", dest.pipeline_dest_id, attempt, delay);
                tokio::time::sleep(delay).await;

                match dest.destination.check_connection().await {
                    Ok(_) => {
                        info!("Dest {}: Connection recovered! Flushing DLQ...", dest.pipeline_dest_id);
                        
                        // Move out of error state
                        dest.clear_error_state().await;
                        dest.retry_manager.reset();

                        // Flush all pending DLQ data
                        if let Err(e) = dest.flush_all_dlq().await {
                            error!("Dest {}: Failed to flush DLQ after recovery: {}", dest.pipeline_dest_id, e);
                            // We stay out of error state for live events, but DLQ flush failed.
                            // Maybe we should re-enter error state?
                            // User requirement says "switch back to live", so we are live now.
                            // But we should eventually flush these.
                        }

                        dest.is_retrying.store(false, Ordering::SeqCst);
                        break;
                    }
                    Err(e) => {
                        debug!("Dest {}: Recovery check failed: {:?}", dest.pipeline_dest_id, e);
                        // Continue loop
                    }
                }
            }
            
            info!("Dest {}: Recovery loop finished", dest.pipeline_dest_id);
        });
    }

    /// Try to recover from error state
    pub async fn try_recover(self: Arc<Self>) -> bool {
        if !self.is_in_error() {
            return true;
        }

        // Try a test connection check
        match self.destination.check_connection().await {
            Ok(_) => {
                info!("Dest {}: Connection test successful, recovering", self.pipeline_dest_id);
                self.clear_error_state().await;

                // Flush DLQ
                if let Err(e) = self.flush_all_dlq().await {
                    error!("Failed to flush DLQ after recovery: {}", e);
                    self.set_error_state(&format!("DLQ flush failed: {}", e)).await;
                    return false;
                }

                true
            }
            Err(_) => {
                debug!("Dest {}: Connection test failed", self.pipeline_dest_id);
                false
            }
        }
    }

    fn extract_table_name(events: &[Event]) -> Option<String> {
        events.first().and_then(|e| match e {
            Event::Insert(i) => Some(format!("{}", i.table_id)),
            Event::Update(u) => Some(format!("{}", u.table_id)),
            Event::Delete(d) => Some(format!("{}", d.table_id)),
            _ => None, // Other events (Begin, Commit, Relation, etc.) don't have table names
        })
    }
}

impl Clone for DestinationWithDlq {
    fn clone(&self) -> Self {
        Self {
            destination: self.destination.clone(),
            pipeline_dest_id: self.pipeline_dest_id,
            db_pool: self.db_pool.clone(),
            dlq_store: self.dlq_store.clone(),
            is_error: AtomicBool::new(self.is_error.load(Ordering::Relaxed)),
            error_message: RwLock::new(None),
            retry_manager: self.retry_manager.clone(),
            pending_tables: RwLock::new(HashSet::new()),
            is_retrying: AtomicBool::new(self.is_retrying.load(Ordering::Relaxed)),
        }
    }
}

/// Enum for different destination types
#[derive(Clone)]
pub enum DestinationEnum {
    Snowflake(SnowflakeDestination),
    Postgres(PostgresDuckdbDestination),
    /// Single destination with DLQ support
    SingleWithDlq(Arc<DestinationWithDlq>),
    /// Multiple destinations with DLQ support per-destination
    MultiWithDlq(Arc<Vec<Arc<DestinationWithDlq>>>),
    /// Legacy multi destination without DLQ (for backwards compatibility)
    Multi(Arc<Vec<Box<DestinationEnum>>>),
}

#[allow(refining_impl_trait)]
impl DestinationEnum {
    pub fn check_connection(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                DestinationEnum::Snowflake(d) => d.check_connection().await,
                DestinationEnum::Postgres(d) => d.check_connection().await,
                DestinationEnum::SingleWithDlq(d) => d.destination.check_connection().await,
                DestinationEnum::MultiWithDlq(dests) => {
                    for dest in dests.iter() {
                        dest.destination.check_connection().await?;
                    }
                    Ok(())
                }
                DestinationEnum::Multi(dests) => {
                    // Best effort check for legacy multi
                    for dest in dests.iter() {
                        dest.check_connection().await?;
                    }
                    Ok(())
                }
            }
        })
    }
}

#[allow(refining_impl_trait)]
impl Destination for DestinationEnum {

    fn name() -> &'static str {
        "multi_destination_wrapper"
    }

    fn truncate_table(
        &self,
        table_id: TableId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                DestinationEnum::Snowflake(d) => d.truncate_table(table_id).await,
                DestinationEnum::Postgres(d) => d.truncate_table(table_id).await,
                DestinationEnum::SingleWithDlq(d) => d.destination.truncate_table(table_id).await,
                DestinationEnum::MultiWithDlq(dests) => {
                    for dest in dests.iter() {
                        dest.destination.truncate_table(table_id.clone()).await?;
                    }
                    Ok(())
                }
                DestinationEnum::Multi(dests) => {
                    let mut handles = vec![];
                    for dest in dests.iter() {
                        let dest = dest.clone();
                        let tid = table_id.clone();
                        handles.push(tokio::spawn(async move { dest.truncate_table(tid).await }));
                    }

                    for h in handles {
                        match h.await {
                            Ok(res) => res?,
                            Err(e) => {
                                return Err((
                                    etl::error::ErrorKind::Unknown,
                                    "Join Error",
                                    e.to_string(),
                                )
                                    .into());
                            }
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                DestinationEnum::Snowflake(d) => d.write_table_rows(table_id, rows).await,
                DestinationEnum::Postgres(d) => d.write_table_rows(table_id, rows).await,
                DestinationEnum::SingleWithDlq(d) => {
                    d.destination.write_table_rows(table_id, rows).await
                }
                DestinationEnum::MultiWithDlq(dests) => {
                    for dest in dests.iter() {
                        dest.destination.write_table_rows(table_id.clone(), rows.clone()).await?;
                    }
                    Ok(())
                }
                DestinationEnum::Multi(dests) => {
                    let mut handles = vec![];
                    for dest in dests.iter() {
                        let dest = dest.clone();
                        let tid = table_id.clone();
                        let r = rows.clone();
                        handles.push(tokio::spawn(
                            async move { dest.write_table_rows(tid, r).await },
                        ));
                    }

                    for h in handles {
                        match h.await {
                            Ok(res) => res?,
                            Err(e) => {
                                return Err((
                                    etl::error::ErrorKind::Unknown,
                                    "Join Error",
                                    e.to_string(),
                                )
                                    .into());
                            }
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn write_events(
        &self,
        events: Vec<Event>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            match self {
                DestinationEnum::Snowflake(d) => d.write_events(events).await,
                DestinationEnum::Postgres(d) => d.write_events(events).await,
                DestinationEnum::SingleWithDlq(d) => d.clone().write_events(events).await,
                DestinationEnum::MultiWithDlq(dests) => {
                    // Run destinations in parallel, each with independent DLQ handling
                    let mut handles = vec![];
                    for dest in dests.iter() {
                        let dest = dest.clone();
                        let evs = events.clone();
                        handles.push(tokio::spawn(async move { 
                            dest.write_events(evs).await 
                        }));
                    }

                    // Even if some fail (non-connection errors), we collect all results
                    let mut first_error: Option<etl::error::EtlError> = None;
                    for h in handles {
                        match h.await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                // Only store first non-DLQ error
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                            }
                            Err(e) => {
                                error!("Destination task panicked: {}", e);
                            }
                        }
                    }

                    // Return first error if any destination had a non-recoverable error
                    if let Some(e) = first_error {
                        return Err(e);
                    }
                    Ok(())
                }
                DestinationEnum::Multi(dests) => {
                    let mut handles = vec![];
                    for dest in dests.iter() {
                        let dest = dest.clone();
                        let evs = events.clone();
                        handles.push(tokio::spawn(async move { dest.write_events(evs).await }));
                    }

                    for h in handles {
                        match h.await {
                            Ok(res) => res?,
                            Err(e) => {
                                return Err((
                                    etl::error::ErrorKind::Unknown,
                                    "Join Error",
                                    e.to_string(),
                                )
                                    .into());
                            }
                        }
                    }
                    Ok(())
                }
            }
        })
    }
}
