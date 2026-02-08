//! DLQ Store - Persistent Dead Letter Queue using fjall
//!
//! Stores events that failed to write to destination due to connection errors.
//! Events are serialized to JSON and persisted to fjall for durability.

use crate::dlq::serialization::SerializableEvent;
use anyhow::{Context, Result};
use etl::types::Event;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Dead Letter Queue store using fjall for persistence
pub struct DlqStore {
    /// Fjall DB
    db: Arc<Database>,
    /// Events keyspace: (dest_id, table, timestamp, uuid) -> serialized events
    events_ks: Keyspace,
    /// Metadata keyspace: (dest_id, table) -> count
    metadata_ks: Keyspace,
}

impl Clone for DlqStore {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            events_ks: self.events_ks.clone(),
            metadata_ks: self.metadata_ks.clone(),
        }
    }
}

impl DlqStore {
    /// Create a new DLQ store at the specified path
    pub fn new(base_path: &Path) -> Result<Self> {
        let dlq_path = base_path.join("dlq");
        std::fs::create_dir_all(&dlq_path)
            .context("Failed to create DLQ directory")?;

        let db = Database::builder(&dlq_path)
            .open()
            .context("Failed to open DLQ database")?;

        let db = Arc::new(db);

        // Create keyspaces
        let metadata_ks = db
            .keyspace("dlq_metadata", || KeyspaceCreateOptions::default())
            .context("Failed to create metadata keyspace")?;

        let events_ks = db
            .keyspace("dlq_events", || KeyspaceCreateOptions::default())
            .context("Failed to create events keyspace")?;

        info!("DLQ store initialized at {:?}", dlq_path);

        Ok(Self {
            db,
            events_ks,
            metadata_ks,
        })
    }

    /// Generate prefix key for a destination/table combination
    fn make_prefix(pipeline_dest_id: i32, table_name: &str) -> String {
        format!("{}:{}", pipeline_dest_id, table_name)
    }

    /// Push events to the DLQ for a specific destination and table
    pub async fn push(&self, pipeline_dest_id: i32, table_name: &str, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // 1. Serialize events
        let serializable_events: Vec<SerializableEvent> = events
            .into_iter()
            .map(SerializableEvent::from)
            .collect();
        
        let json_bytes = serde_json::to_vec(&serializable_events)
            .context("Failed to serialize events")?;

        // 2. Generate unique key (using timestamp + uuid to ensure order and uniqueness)
        let prefix = Self::make_prefix(pipeline_dest_id, table_name);
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let uuid = Uuid::new_v4();
        let key = format!("{}:{}:{}", prefix, timestamp, uuid);

        // 3. Write to DB
        self.events_ks.insert(&key, json_bytes)?;

        // 4. Update metadata count
        let count = self.get_stored_count(pipeline_dest_id, table_name) + serializable_events.len();
        self.update_count_metadata(pipeline_dest_id, table_name, count)?;

        debug!(
            "DLQ: Pushed {} events for dest {} table {}, total stored: {}",
            serializable_events.len(),
            pipeline_dest_id,
            table_name,
            count
        );

        Ok(())
    }

    /// Pop a batch of events from the DLQ (oldest first)
    /// Returns events and removes them from the store
    pub async fn pop_batch(&self, pipeline_dest_id: i32, table_name: &str, limit: usize) -> Result<Vec<Event>> {
        let prefix = Self::make_prefix(pipeline_dest_id, table_name);
        
        let mut all_events = Vec::new();
        let mut keys_to_delete = Vec::new();
        let mut events_count = 0;

        // Iterate manually. Item is Guard.
        for guard in self.events_ks.prefix(prefix.as_bytes()) {
             // into_inner() consumes the guard and returns (UserKey, UserValue)
             let (key_slice, value_slice) = guard.into_inner()?;
             let key = key_slice.to_vec();
             let value = value_slice.to_vec();

             // Deserialize batch
             let serializable_events: Vec<SerializableEvent> = match serde_json::from_slice(&value) {
                 Ok(evs) => evs,
                 Err(e) => {
                     warn!("Failed to deserialize DLQ entry: {}", e);
                     keys_to_delete.push(key);
                     continue;
                 }
             };

             if serializable_events.is_empty() {
                 keys_to_delete.push(key);
                 continue;
             }

             // Convert back to Event
             let mut events: Vec<Event> = serializable_events
                 .into_iter()
                 .map(Event::from)
                 .collect();

             if events_count + events.len() <= limit {
                 events_count += events.len();
                 all_events.append(&mut events);
                 keys_to_delete.push(key);
             } else {
                 let needed = limit - events_count;
                 let remaining = events.split_off(needed);
                 
                 all_events.append(&mut events);
                 // No need to increment events_count here as we break
                 
                 let remaining_serializable: Vec<SerializableEvent> = remaining
                     .into_iter()
                     .map(SerializableEvent::from)
                     .collect();
                     
                 if let Ok(new_json) = serde_json::to_vec(&remaining_serializable) {
                     let _ = self.events_ks.insert(&key, new_json);
                 }
                 
                 break;
             }
        }

        // Delete processed batches
        for key in keys_to_delete {
            let _ = self.events_ks.remove(key);
        }

        // Update metadata
        let current_total = self.get_stored_count(pipeline_dest_id, table_name);
        let new_total = current_total.saturating_sub(all_events.len());
        self.update_count_metadata(pipeline_dest_id, table_name, new_total)?;

        if !all_events.is_empty() {
            debug!(
                "DLQ: Popped {} events for dest {} table {}, remaining: {}",
                all_events.len(),
                pipeline_dest_id,
                table_name,
                new_total
            );
        }

        Ok(all_events)
    }

    /// Check if DLQ is empty for a destination/table
    pub async fn is_empty(&self, pipeline_dest_id: i32, table_name: &str) -> bool {
        self.get_stored_count(pipeline_dest_id, table_name) == 0
    }

    /// Count total event batches in DLQ for a destination (across all tables)
    pub async fn count_for_destination(&self, pipeline_dest_id: i32) -> usize {
        let prefix = format!("count:{}:", pipeline_dest_id);
        let mut total = 0;
        
        for guard in self.metadata_ks.prefix(prefix.as_bytes()) {
             if let Ok(value) = guard.value() {
                 let count: usize = String::from_utf8_lossy(&value).parse().unwrap_or(0);
                 total += count;
             }
        }
        total
    }

    /// Get all table names with pending DLQ entries for a destination
    pub async fn get_pending_tables(&self, pipeline_dest_id: i32) -> Vec<String> {
        let prefix = format!("count:{}:", pipeline_dest_id);
        let mut tables = Vec::new();
        
        for guard in self.metadata_ks.prefix(prefix.as_bytes()) {
             if let Ok((key, value)) = guard.into_inner() {
                 let count: usize = String::from_utf8_lossy(&value).parse().unwrap_or(0);
                 if count > 0 {
                     let key_str = String::from_utf8_lossy(&key);
                     let parts: Vec<&str> = key_str.splitn(3, ':').collect();
                     if parts.len() == 3 {
                         tables.push(parts[2].to_string());
                     }
                 }
             }
        }
        tables
    }

    /// Update count metadata in fjall
    fn update_count_metadata(&self, pipeline_dest_id: i32, table_name: &str, count: usize) -> Result<()> {
        let key = format!("count:{}:{}", pipeline_dest_id, table_name);
        
        if count == 0 {
            // Remove metadata if empty to keep it clean
            let _ = self.metadata_ks.remove(&key);
        } else {
             let value = count.to_string();
             self.metadata_ks.insert(&key, &value)
                .context("Failed to update DLQ count metadata")?;
        }
       
        // Persist asynchronously (best effort)
        // Note: Fjall flushes automatically in background, but we can hint
        // self.db.persist(PersistMode::Buffer)?;
        
        Ok(())
    }

    /// Get total event count from metadata
    pub fn get_stored_count(&self, pipeline_dest_id: i32, table_name: &str) -> usize {
        let key = format!("count:{}:{}", pipeline_dest_id, table_name);
        
        match self.metadata_ks.get(&key) {
            Ok(Some(bytes)) => {
                String::from_utf8_lossy(&bytes)
                    .parse()
                    .unwrap_or(0)
            }
            _ => 0,
        }
    }
}
