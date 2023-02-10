//! A library for communicating with a libSQL database over HTTP.
//!
//! libsql-client is a lightweight HTTP-based driver for sqld,
//! which is a server mode for libSQL, which is an open-contribution fork of SQLite.
//!
//! libsql-client compiles to wasm32-unknown-unknown target, which makes it a great
//! driver for environments that run on WebAssembly.
//!
//! It is expected to become a general-purpose driver for communicating with sqld/libSQL,
//! but the only backend implemented at the moment is for Cloudflare Workers environment.

use std::collections::HashMap;
use std::iter::IntoIterator;

use anyhow::{anyhow, Result};

pub mod statement;
pub use statement::Statement;

pub mod value;
pub use value::Value;

pub mod connection;
pub use connection::{connect, Connection};

#[cfg(feature = "workers_backend")]
pub mod workers;

#[cfg(feature = "reqwest_backend")]
pub mod reqwest;

#[cfg(feature = "local_backend")]
pub mod local;

/// Metadata of a database request
#[derive(Clone, Debug, Default)]
pub struct Meta {
    pub duration: u64,
}

/// A database row
#[derive(Clone, Debug)]
pub struct Row {
    pub cells: HashMap<String, Value>,
}

/// Structure holding a set of rows returned from a query
/// and their corresponding column names
#[derive(Clone, Debug)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// Result of a database request - a set of rows or an error
#[derive(Clone, Debug)]
pub enum QueryResult {
    Error((String, Meta)),
    Success((ResultSet, Meta)),
}

pub fn parse_columns(columns: Vec<serde_json::Value>, result_idx: usize) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(columns.len());
    for (idx, column) in columns.into_iter().enumerate() {
        match column {
            serde_json::Value::String(column) => result.push(column),
            _ => {
                return Err(anyhow!(format!(
                    "Result {result_idx} column name {idx} not a string",
                )))
            }
        }
    }
    Ok(result)
}

pub fn parse_value(
    cell: serde_json::Value,
    result_idx: usize,
    row_idx: usize,
    cell_idx: usize,
) -> Result<Value> {
    match cell {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Number(v) => match v.as_i64() {
            Some(v) => Ok(Value::Integer(v)),
            None => match v.as_f64() {
                Some(v) => Ok(Value::Real(v)),
                None => Err(anyhow!(
                    "Result {result_idx} row {row_idx} cell {cell_idx} had unknown number value: {v}",
                )),
            },
        },
        serde_json::Value::String(v) => Ok(Value::Text(v)),
        _ => Err(anyhow!(
            "Result {result_idx} row {row_idx} cell {cell_idx} had unknown type",
        )),
    }
}

pub fn parse_rows(
    rows: Vec<serde_json::Value>,
    columns: &Vec<String>,
    result_idx: usize,
) -> Result<Vec<Row>> {
    let mut result = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        match row {
            serde_json::Value::Array(row) => {
                if row.len() != columns.len() {
                    return Err(anyhow!(
                        "Result {result_idx} row {idx} had wrong number of cells",
                    ));
                }
                let mut cells = HashMap::with_capacity(columns.len());
                for (cell_idx, value) in row.into_iter().enumerate() {
                    cells.insert(
                        columns[cell_idx].clone(),
                        parse_value(value, result_idx, idx, cell_idx)?,
                    );
                }
                result.push(Row { cells })
            }
            _ => return Err(anyhow!("Result {result_idx} row {idx} was not an array",)),
        }
    }
    Ok(result)
}

pub fn parse_query_result(result: serde_json::Value, idx: usize) -> Result<QueryResult> {
    match result {
        serde_json::Value::Object(obj) => {
            if let Some(err) = obj.get("error") {
                return match err {
                    serde_json::Value::Object(obj) => match obj.get("message") {
                        Some(serde_json::Value::String(msg)) => {
                            Ok(QueryResult::Error((msg.clone(), Meta::default())))
                        }
                        _ => Err(anyhow!("Result {idx} error message was not a string",)),
                    },
                    _ => Err(anyhow!("Result {idx} results was not an object",)),
                };
            }

            let results = obj.get("results");
            match results {
                Some(serde_json::Value::Object(obj)) => {
                    let columns = obj
                        .get("columns")
                        .ok_or_else(|| anyhow!(format!("Result {idx} had no columns")))?;
                    let rows = obj
                        .get("rows")
                        .ok_or_else(|| anyhow!(format!("Result {idx} had no rows")))?;
                    match (rows, columns) {
                        (serde_json::Value::Array(rows), serde_json::Value::Array(columns)) => {
                            let columns = parse_columns(columns.to_vec(), idx)?;
                            let rows = parse_rows(rows.to_vec(), &columns, idx)?;
                            Ok(QueryResult::Success((
                                ResultSet { columns, rows },
                                Meta::default(),
                            )))
                        }
                        _ => Err(anyhow!(
                            "Result {idx} had rows or columns that were not an array",
                        )),
                    }
                }
                Some(_) => Err(anyhow!("Result {idx} was not an object",)),
                None => Err(anyhow!("Result {idx} did not contain results or error",)),
            }
        }
        _ => Err(anyhow!("Result {idx} was not an object",)),
    }
}
