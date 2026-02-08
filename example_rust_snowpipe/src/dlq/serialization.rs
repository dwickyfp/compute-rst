use etl::types::{Event, Cell, TableRow, TableId, InsertEvent, UpdateEvent, DeleteEvent, ArrayCell};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableEvent {
    Insert(SerializableInsert),
    Update(SerializableUpdate),
    Delete(SerializableDelete),
    Begin,
    Commit,
    Unsupported(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableInsert {
    pub table_id: u32,
    pub table_row: SerializableTableRow,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableUpdate {
    pub table_id: u32,
    pub table_row: SerializableTableRow,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableDelete {
    pub table_id: u32,
    pub old_table_row: Option<(bool, SerializableTableRow)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableTableRow {
    pub values: Vec<SerializableCell>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableCell {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Numeric(String), 
    Uuid(String),
    Array(SerializableArrayCell),
    Date(String),
    Time(String),
    Timestamp(String),
    TimestampTz(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerializableArrayCell {
    Bool(Vec<Option<bool>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    String(Vec<Option<String>>),
    Numeric(Vec<Option<String>>),
    Date(Vec<Option<String>>),
    TimestampTz(Vec<Option<String>>),
    Uuid(Vec<Option<String>>),
    Unknown(String),
}

impl From<Event> for SerializableEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Insert(i) => SerializableEvent::Insert(SerializableInsert {
                table_id: i.table_id.0,
                table_row: i.table_row.into(),
            }),
            Event::Update(u) => SerializableEvent::Update(SerializableUpdate {
                table_id: u.table_id.0,
                table_row: u.table_row.into(),
            }),
            Event::Delete(d) => SerializableEvent::Delete(SerializableDelete {
                table_id: d.table_id.0,
                old_table_row: d.old_table_row.map(|(is_old, row)| (is_old, row.into())),
            }),
            Event::Begin(_) => SerializableEvent::Begin,
            Event::Commit(_) => SerializableEvent::Commit,
            _ => SerializableEvent::Unsupported(format!("{:?}", event)),
        }
    }
}

impl From<TableRow> for SerializableTableRow {
    fn from(row: TableRow) -> Self {
        Self {
            values: row.values.into_iter().map(SerializableCell::from).collect(),
        }
    }
}

impl From<Cell> for SerializableCell {
    fn from(cell: Cell) -> Self {
        match cell {
            Cell::Null => SerializableCell::Null,
            Cell::Bool(v) => SerializableCell::Bool(v),
            Cell::String(v) => SerializableCell::String(v),
            Cell::I16(v) => SerializableCell::I16(v),
            Cell::I32(v) => SerializableCell::I32(v),
            Cell::I64(v) => SerializableCell::I64(v),
            Cell::F32(v) => SerializableCell::F32(v),
            Cell::F64(v) => SerializableCell::F64(v),
            Cell::Bytes(v) => SerializableCell::Bytes(v),
            Cell::Json(v) => SerializableCell::Json(v),
            Cell::Numeric(v) => SerializableCell::Numeric(v.to_string()),
            Cell::Uuid(v) => SerializableCell::Uuid(v.to_string()),
            Cell::Date(v) => SerializableCell::Date(v.to_string()),
            Cell::Time(v) => SerializableCell::Time(v.to_string()),
            Cell::Timestamp(v) => SerializableCell::Timestamp(v.to_string()),
            Cell::TimestampTz(v) => SerializableCell::TimestampTz(v.to_rfc3339()),
            Cell::Array(v) => SerializableCell::Array(SerializableArrayCell::from(v)),
            _ => SerializableCell::String(format!("{:?}", cell)),
        }
    }
}

impl From<ArrayCell> for SerializableArrayCell {
    fn from(cell: ArrayCell) -> Self {
        match cell {
            ArrayCell::Bool(v) => SerializableArrayCell::Bool(v),
            ArrayCell::I16(v) => SerializableArrayCell::I16(v),
            ArrayCell::I32(v) => SerializableArrayCell::I32(v),
            ArrayCell::I64(v) => SerializableArrayCell::I64(v),
            ArrayCell::F32(v) => SerializableArrayCell::F32(v),
            ArrayCell::F64(v) => SerializableArrayCell::F64(v),
            ArrayCell::String(v) => SerializableArrayCell::String(v),
            ArrayCell::Numeric(v) => SerializableArrayCell::Numeric(v.into_iter().map(|o| o.map(|n| n.to_string())).collect()),
            ArrayCell::Date(v) => SerializableArrayCell::Date(v.into_iter().map(|o| o.map(|d| d.to_string())).collect()),
            ArrayCell::TimestampTz(v) => SerializableArrayCell::TimestampTz(v.into_iter().map(|o| o.map(|t| t.to_rfc3339())).collect()),
            ArrayCell::Uuid(v) => SerializableArrayCell::Uuid(v.into_iter().map(|o| o.map(|u| u.to_string())).collect()),
            _ => SerializableArrayCell::Unknown(format!("{:?}", cell)),
        }
    }
}

// Convert back to ETL types
impl From<SerializableEvent> for Event {
    fn from(event: SerializableEvent) -> Self {
        match event {
            SerializableEvent::Insert(i) => Event::Insert(InsertEvent {
                table_id: TableId(i.table_id),
                table_row: i.table_row.into(),
                start_lsn: 0.into(),
                commit_lsn: 0.into(),
            }),
            SerializableEvent::Update(u) => Event::Update(UpdateEvent {
                table_id: TableId(u.table_id),
                table_row: u.table_row.into(),
                old_table_row: None,
                start_lsn: 0.into(),
                commit_lsn: 0.into(),
            }),
            SerializableEvent::Delete(d) => Event::Delete(DeleteEvent {
                table_id: TableId(d.table_id),
                old_table_row: d.old_table_row.map(|(is_old, row)| (is_old, row.into())),
                start_lsn: 0.into(),
                commit_lsn: 0.into(),
            }),
            SerializableEvent::Begin => Event::Begin(etl::types::BeginEvent { 
                start_lsn: 0.into(), 
                commit_lsn: 0.into(),
                timestamp: chrono::Utc::now().timestamp(),
                xid: 0,
            }),
            SerializableEvent::Commit => Event::Commit(etl::types::CommitEvent { 
                start_lsn: 0.into(), 
                commit_lsn: 0.into(), 
                end_lsn: 0.into(),
                flags: 0,
                timestamp: chrono::Utc::now().timestamp(),
            }),
            _ => Event::Unsupported,
        }
    }
}

impl From<SerializableTableRow> for TableRow {
    fn from(row: SerializableTableRow) -> Self {
        Self {
            values: row.values.into_iter().map(Cell::from).collect(),
        }
    }
}

impl From<SerializableCell> for Cell {
    fn from(cell: SerializableCell) -> Self {
        match cell {
            SerializableCell::Null => Cell::Null,
            SerializableCell::Bool(v) => Cell::Bool(v),
            SerializableCell::String(v) => Cell::String(v),
            SerializableCell::I16(v) => Cell::I16(v),
            SerializableCell::I32(v) => Cell::I32(v),
            SerializableCell::I64(v) => Cell::I64(v),
            SerializableCell::F32(v) => Cell::F32(v),
            SerializableCell::F64(v) => Cell::F64(v),
            SerializableCell::Bytes(v) => Cell::Bytes(v),
            SerializableCell::Json(v) => Cell::Json(v),
            SerializableCell::Numeric(v) => Cell::Numeric(v.parse().unwrap_or_default()),
            SerializableCell::Uuid(v) => Cell::Uuid(v.parse().unwrap_or_default()),
            SerializableCell::Date(v) => Cell::Date(v.parse().unwrap_or_default()),
            SerializableCell::Time(v) => Cell::Time(v.parse().unwrap_or_default()),
            SerializableCell::Timestamp(v) => Cell::Timestamp(v.parse().unwrap_or_default()),
            SerializableCell::TimestampTz(v) => Cell::TimestampTz(v.parse().ok()
                .unwrap_or_else(|| chrono::Utc::now())),
            SerializableCell::Array(v) => Cell::Array(ArrayCell::from(v)),
        }
    }
}

impl From<SerializableArrayCell> for ArrayCell {
    fn from(cell: SerializableArrayCell) -> Self {
        match cell {
            SerializableArrayCell::Bool(v) => ArrayCell::Bool(v),
            SerializableArrayCell::I16(v) => ArrayCell::I16(v),
            SerializableArrayCell::I32(v) => ArrayCell::I32(v),
            SerializableArrayCell::I64(v) => ArrayCell::I64(v),
            SerializableArrayCell::F32(v) => ArrayCell::F32(v),
            SerializableArrayCell::F64(v) => ArrayCell::F64(v),
            SerializableArrayCell::String(v) => ArrayCell::String(v),
            SerializableArrayCell::Numeric(v) => ArrayCell::Numeric(v.into_iter().map(|o| o.map(|s| s.parse().unwrap_or_default())).collect()),
            SerializableArrayCell::Date(v) => ArrayCell::Date(v.into_iter().map(|o| o.map(|s| s.parse().unwrap_or_default())).collect()),
            SerializableArrayCell::TimestampTz(v) => ArrayCell::TimestampTz(v.into_iter().map(|o| o.and_then(|s| s.parse().ok())).collect()),
            SerializableArrayCell::Uuid(v) => ArrayCell::Uuid(v.into_iter().map(|o| o.map(|s| s.parse().unwrap_or_default())).collect()),
            _ => ArrayCell::String(vec![]),
        }
    }
}
