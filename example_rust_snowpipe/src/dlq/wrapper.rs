//! DLQ Destination Wrapper
//!
//! Wraps any destination with Dead Letter Queue failover capability.
//! On connection errors, events are saved to DLQ and replayed when connection recovers.

use crate::dlq::retry::{is_connection_error, RetryManager};
use crate::dlq::store::DlqStore;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::types::{Event, TableId, TableRow};
use sqlx::{Pool, Postgres};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Wrapper that adds DLQ capability to any destination
pub struct DlqDestinationWrapper<D: Destination + Clone + Send + Sync + 'static> {
    inner: D,
    dlq_store: Arc<DlqStore>,
    pipeline_dest_id: i32,
    db_pool: Pool<Postgres>,

    // Error state
    is_error: AtomicBool,
    error_message: RwLock<Option<String>>,
    retry_manager: Arc<RetryManager>,

    // Track tables with DLQ data
    pending_tables: RwLock<HashSet<String>>,
}

#[allow(dead_code)]
impl<D: Destination + Clone + Send + Sync + 'static> DlqDestinationWrapper<D> {
    /// Create a new DLQ wrapper
    pub fn new(
        inner: D,
        dlq_store: Arc<DlqStore>,
        pipeline_dest_id: i32,
        db_pool: Pool<Postgres>,
    ) -> Self {
        Self {
            inner,
            dlq_store,
            pipeline_dest_id,
            db_pool,
            is_error: AtomicBool::new(false),
            error_message: RwLock::new(None),
            retry_manager: Arc::new(RetryManager::new()),
            pending_tables: RwLock::new(HashSet::new()),
        }
    }

    /// Check if destination is in error state
    pub fn is_in_error(&self) -> bool {
        self.is_error.load(Ordering::Relaxed)
    }

    /// Set error state and update database
    async fn set_error_state(&self, error_msg: &str) {
        self.is_error.store(true, Ordering::Relaxed);
        *self.error_message.write().await = Some(error_msg.to_string());

        // Update database
        if let Err(e) = self.update_error_in_db(true, Some("Database Error")).await {
            error!("Failed to update error state in DB: {}", e);
        }

        info!(
            "Destination {} entered error state: {}",
            self.pipeline_dest_id, error_msg
        );
    }

    /// Clear error state and update database
    async fn clear_error_state(&self) {
        self.is_error.store(false, Ordering::Relaxed);
        *self.error_message.write().await = None;

        // Update database
        if let Err(e) = self.update_error_in_db(false, None).await {
            error!("Failed to clear error state in DB: {}", e);
        }

        info!("Destination {} recovered from error", self.pipeline_dest_id);
    }

    /// Update error status in database
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

    /// Try to flush DLQ for a specific table
    async fn flush_dlq_for_table(&self, table_name: &str) -> anyhow::Result<usize> {
        let mut total_flushed = 0;
        const BATCH_SIZE: usize = 100;

        loop {
            let events = self.dlq_store.pop_batch(self.pipeline_dest_id, table_name, BATCH_SIZE).await?;
            
            if events.is_empty() {
                break;
            }

            let count = events.len();
            
            // Try to write to destination
            match self.inner.write_events(events.clone()).await {
                Ok(_) => {
                    total_flushed += count;
                    debug!("Flushed {} DLQ events for table {}", count, table_name);
                }
                Err(e) => {
                    // Push events back to DLQ
                    warn!("Failed to flush DLQ, pushing back: {}", e);
                    self.dlq_store.push(self.pipeline_dest_id, table_name, events).await?;
                    return Err(anyhow::anyhow!("Flush failed: {}", e));
                }
            }
        }

        Ok(total_flushed)
    }

    /// Flush all pending DLQ data
    async fn flush_all_dlq(&self) -> anyhow::Result<usize> {
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

        info!("Flushed {} total DLQ events for dest {}", total, self.pipeline_dest_id);
        Ok(total)
    }

    /// Start retry loop for connection recovery
    fn spawn_retry_loop(self: Arc<Self>) {
        let wrapper = self.clone();
        let inner = self.inner.clone();
        let retry_manager = self.retry_manager.clone();

        retry_manager.clone().spawn_retry_loop(
            self.pipeline_dest_id,
            move || {
                let inner = inner.clone();
                async move {
                    // Simple health check - try an empty write
                    match inner.write_events(vec![]).await {
                        Ok(_) => true,
                        Err(e) => {
                            debug!("Health check failed: {}", e);
                            false
                        }
                    }
                }
            },
            move || {
                // On success callback
                let wrapper = wrapper.clone();
                tokio::spawn(async move {
                    // Clear error state
                    wrapper.clear_error_state().await;

                    // Flush DLQ
                    if let Err(e) = wrapper.flush_all_dlq().await {
                        error!("Failed to flush DLQ after recovery: {}", e);
                        // Re-enter error state
                        wrapper.set_error_state(&format!("DLQ flush failed: {}", e)).await;
                    }
                });
            },
        );
    }

    /// Extract table name from events
    fn extract_table_name(events: &[Event]) -> Option<String> {
        events.first().and_then(|e| match e {
            Event::Insert(i) => Some(format!("{}", i.table_id)),
            Event::Update(u) => Some(format!("{}", u.table_id)),
            Event::Delete(d) => Some(format!("{}", d.table_id)),
            _ => None, // Other events (Begin, Commit, Relation, etc.) don't have table names
        })
    }
}

impl<D: Destination + Clone + Send + Sync + 'static> Clone for DlqDestinationWrapper<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            dlq_store: self.dlq_store.clone(),
            pipeline_dest_id: self.pipeline_dest_id,
            db_pool: self.db_pool.clone(),
            is_error: AtomicBool::new(self.is_error.load(Ordering::Relaxed)),
            error_message: RwLock::new(None), // Not cloned
            retry_manager: self.retry_manager.clone(),
            pending_tables: RwLock::new(HashSet::new()), // Not cloned
        }
    }
}

#[allow(refining_impl_trait)]
impl<D: Destination + Clone + Send + Sync + 'static> Destination for DlqDestinationWrapper<D> {
    fn name() -> &'static str {
        "dlq_wrapper"
    }

    fn truncate_table(
        &self,
        table_id: TableId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            // Truncate doesn't go to DLQ - just pass through
            self.inner.truncate_table(table_id).await
        })
    }

    fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            // Pass through - this is typically not used for CDC
            self.inner.write_table_rows(table_id, rows).await
        })
    }

    fn write_events(
        &self,
        events: Vec<Event>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = EtlResult<()>> + Send + '_>> {
        Box::pin(async move {
            if events.is_empty() {
                return Ok(());
            }

            let table_name = Self::extract_table_name(&events)
                .unwrap_or_else(|| "unknown".to_string());

            // If in error state, save directly to DLQ
            if self.is_in_error() {
                debug!(
                    "Dest {} in error state, saving {} events to DLQ for table {}",
                    self.pipeline_dest_id,
                    events.len(),
                    table_name
                );

                self.dlq_store
                    .push(self.pipeline_dest_id, &table_name, events)
                    .await
                    .map_err(|e| {
                        (
                            ErrorKind::Unknown,
                            "DLQ push error",
                            e.to_string(),
                        )
                    })?;

                self.pending_tables.write().await.insert(table_name);
                return Ok(());
            }

            // Try to write to destination
            match self.inner.write_events(events.clone()).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    let error_str = format!("{:?}", e);

                    if is_connection_error(&error_str) {
                        // Connection error - save to DLQ and start retry
                        warn!(
                            "Dest {}: Connection error, saving to DLQ: {}",
                            self.pipeline_dest_id, error_str
                        );

                        // Set error state
                        self.set_error_state(&error_str).await;

                        // Save to DLQ
                        self.dlq_store
                            .push(self.pipeline_dest_id, &table_name, events)
                            .await
                            .map_err(|e| {
                                (
                                    ErrorKind::Unknown,
                                    "DLQ push error",
                                    e.to_string(),
                                )
                            })?;

                        self.pending_tables.write().await.insert(table_name);

                        // Start retry loop (in background)
                        // Note: Can't use Arc<Self> easily here, would need refactor
                        // For now, we'll handle retry in the manager

                        // Return OK so pipeline continues
                        Ok(())
                    } else {
                        // Non-connection error - propagate
                        Err(e)
                    }
                }
            }
        })
    }
}
