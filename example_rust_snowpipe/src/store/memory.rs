use etl::error::{EtlResult, ErrorKind};
use etl::etl_error;
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Default)]
struct TableEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    // Wajib pakai BTreeMap untuk StateStore requirement
    tables: Arc<Mutex<BTreeMap<TableId, TableEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        Ok(self
            .tables
            .lock()
            .await
            .get(table_id)
            .and_then(|e| e.schema.clone()))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        Ok(self
            .tables
            .lock()
            .await
            .values()
            .filter_map(|e| e.schema.clone())
            .collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn store_table_schema(&self, schema: TableSchema) -> EtlResult<()> {
        let mut lock = self.tables.lock().await;
        let id = schema.id;
        lock.entry(id).or_default().schema = Some(Arc::new(schema));
        Ok(())
    }
}

impl StateStore for CustomStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        Ok(self
            .tables
            .lock()
            .await
            .get(&table_id)
            .and_then(|e| e.state.clone()))
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        Ok(self
            .tables
            .lock()
            .await
            .iter()
            .filter_map(|(k, v)| v.state.clone().map(|s| (*k, s)))
            .collect())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        self.tables.lock().await.entry(table_id).or_default().state = Some(state);
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let state = self.tables.lock().await.get(&table_id).and_then(|e| e.state.clone());
        if let Some(s) = state {
            Ok(s)
        } else {
             Err(etl_error!(ErrorKind::Unknown, "Table state not found during rollback"))
        }
    }

    async fn get_table_mapping(&self, table_id: &TableId) -> EtlResult<Option<String>> {
        Ok(self
            .tables
            .lock()
            .await
            .get(table_id)
            .and_then(|e| e.mapping.clone()))
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        Ok(self
            .tables
            .lock()
            .await
            .iter()
            .filter_map(|(k, v)| v.mapping.clone().map(|m| (*k, m)))
            .collect())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn store_table_mapping(&self, table_id: TableId, mapping: String) -> EtlResult<()> {
        self.tables
            .lock()
            .await
            .entry(table_id)
            .or_default()
            .mapping = Some(mapping);
        Ok(())
    }
}

impl CleanupStore for CustomStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        self.tables.lock().await.remove(&table_id);
        Ok(())
    }
}
