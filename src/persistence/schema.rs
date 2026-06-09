//! Table schema and row value types shared by every [`Store`](super::Store).

/// A SQL column type. SQLite is dynamically typed, so these act as type
/// affinities / a best-guess mapping from event field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl SqlType {
    /// The SQL keyword used in a `CREATE TABLE` column definition.
    pub fn sql(&self) -> &'static str {
        match self {
            SqlType::Integer => "INTEGER",
            SqlType::Real => "REAL",
            SqlType::Text => "TEXT",
            SqlType::Blob => "BLOB",
            SqlType::Numeric => "NUMERIC",
        }
    }
}

/// A single column in a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: SqlType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// The schema for one event's table: a table name and its event-field columns.
///
/// The `block_number` column is implicit — every [`Store`](super::Store) adds
/// it — so a schema describes only the event's own fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: Vec::new(),
        }
    }

    /// Append a column (builder style).
    pub fn col(mut self, name: impl Into<String>, ty: SqlType) -> Self {
        self.columns.push(Column::new(name, ty));
        self
    }
}

/// A scalar value bound into / read out of a SQL cell.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

/// One row's worth of event-field values, ordered to match
/// [`TableSchema::columns`].
#[derive(Debug, Clone, PartialEq)]
pub struct Row(pub Vec<SqlValue>);
