//! Best-guess mapping between events and SQL rows.
//!
//! The table name comes from the event's Solidity signature; column names come
//! from the event's `serde` field names and column types are inferred from the
//! serialised JSON. This requires events to implement [`serde::Serialize`].

use std::collections::BTreeMap;

use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::schema::{Column, Row, SqlType, SqlValue, TableSchema};

/// Name of the implicit column holding each event's full JSON, used to
/// losslessly reconstruct the event when replaying from the database.
pub const PAYLOAD_COLUMN: &str = "_payload";

/// The best-guess table name for an event type, derived from its Solidity
/// signature: `ValueSet(uint256)` -> `value_set`.
pub fn table_name<E: SolEvent>() -> String {
    let signature = E::SIGNATURE;
    let name = signature.split('(').next().unwrap_or(signature);
    to_snake_case(name)
}

/// Map an event's top-level fields to `name -> (best-guess type, value)`,
/// sorted by field name (deterministic across runs).
fn field_map<E: Serialize>(event: &E) -> Result<BTreeMap<String, (SqlType, SqlValue)>> {
    let value = serde_json::to_value(event).context("event is not serialisable to JSON")?;
    let object = match value {
        Value::Object(map) => map,
        other => anyhow::bail!("event must serialise to a JSON object, got {other}"),
    };
    Ok(object
        .into_iter()
        .map(|(key, field)| (key, (infer_type(&field), json_to_sql(&field))))
        .collect())
}

/// The best-guess [`TableSchema`] and [`Row`] for a serialisable event:
/// one column per top-level field, named and typed from the serialised JSON.
pub fn derive<E>(event: &E) -> Result<(TableSchema, Row)>
where
    E: Serialize + SolEvent,
{
    let fields = field_map(event)?;
    let mut columns = Vec::with_capacity(fields.len());
    let mut values = Vec::with_capacity(fields.len());
    for (name, (ty, value)) in fields {
        columns.push(Column::new(name, ty));
        values.push(value);
    }

    Ok((
        TableSchema {
            table: table_name::<E>(),
            columns,
        },
        Row(values),
    ))
}

/// The schema and row for `event`, augmented with a [`PAYLOAD_COLUMN`] holding
/// the event's full JSON. The per-field columns make the table queryable; the
/// payload column makes replay lossless.
pub fn derive_record<E>(event: &E) -> Result<(TableSchema, Row)>
where
    E: Serialize + SolEvent,
{
    derive_record_with(event, None)
}

/// Like [`derive_record`], but honouring an optional schema `override_`.
///
/// When an override is given, its table name and columns are used: each
/// override column's value is looked up by name from the event's fields (a
/// missing field becomes `NULL`), so columns may be renamed-away, reordered, or
/// retyped without disturbing value alignment. The [`PAYLOAD_COLUMN`] is always
/// appended for lossless replay.
pub fn derive_record_with<E>(
    event: &E,
    override_: Option<&TableSchema>,
) -> Result<(TableSchema, Row)>
where
    E: Serialize + SolEvent,
{
    let fields = field_map(event)?;

    let (table, columns, mut values) = match override_ {
        Some(schema) => {
            let values = schema
                .columns
                .iter()
                .map(|col| {
                    fields
                        .get(&col.name)
                        .map(|(_, value)| value.clone())
                        .unwrap_or(SqlValue::Null)
                })
                .collect();
            (schema.table.clone(), schema.columns.clone(), values)
        }
        None => {
            let mut columns = Vec::with_capacity(fields.len());
            let mut values = Vec::with_capacity(fields.len());
            for (name, (ty, value)) in &fields {
                columns.push(Column::new(name.clone(), *ty));
                values.push(value.clone());
            }
            (table_name::<E>(), columns, values)
        }
    };

    let mut schema = TableSchema { table, columns };
    let payload = serde_json::to_string(event).context("event is not serialisable to JSON")?;
    schema
        .columns
        .push(Column::new(PAYLOAD_COLUMN, SqlType::Text));
    values.push(SqlValue::Text(payload));

    Ok((schema, Row(values)))
}

/// The schema used to read back stored events for replay: the table name plus
/// the single [`PAYLOAD_COLUMN`]. Needs no event instance, so replay can run
/// before any live event is seen.
pub fn payload_schema<E: SolEvent>() -> TableSchema {
    TableSchema::new(table_name::<E>()).col(PAYLOAD_COLUMN, SqlType::Text)
}

/// Reconstruct an event from a stored [`PAYLOAD_COLUMN`] value.
pub fn from_payload<E: DeserializeOwned>(payload: &str) -> Result<E> {
    serde_json::from_str(payload).context("stored payload is not valid for this event type")
}

/// Best-guess SQL type for a serialised field value.
fn infer_type(value: &Value) -> SqlType {
    match value {
        Value::Bool(_) => SqlType::Integer,
        Value::Number(n) if n.is_i64() || n.is_u64() => SqlType::Integer,
        Value::Number(_) => SqlType::Real,
        Value::String(_) => SqlType::Text,
        // Arrays, objects and null are stored as JSON text.
        _ => SqlType::Text,
    }
}

/// Convert a serialised field value into a [`SqlValue`].
fn json_to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(*b as i64),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(u) = n.as_u64() {
                SqlValue::Integer(u as i64)
            } else {
                SqlValue::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => SqlValue::Text(s.clone()),
        // Arrays / objects: keep the JSON text representation.
        other => SqlValue::Text(other.to_string()),
    }
}

/// Convert `PascalCase` / `camelCase` to `snake_case`.
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.char_indices() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
