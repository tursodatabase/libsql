use std::convert::Infallible;
use std::str::FromStr;

use pgwire::api::{results::FieldInfo, Type as PgType};
use serde::{Deserialize, Serialize};

pub type QueryResult = Result<QueryResponse, QueryError>;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ty: Option<Type>,
}

impl From<Column> for FieldInfo {
    fn from(col: Column) -> Self {
        FieldInfo::new(
            col.name,
            None,
            None,
            col.ty.map(PgType::from).unwrap_or(PgType::UNKNOWN),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Type {
    Integer,
    Binary,
    Float,
    String,
    Null,
    Unknown,
}

impl From<Type> for PgType {
    fn from(other: Type) -> Self {
        match other {
            Type::Integer => PgType::INT8,
            Type::Binary => PgType::BYTEA,
            Type::Float => PgType::NUMERIC,
            Type::String => PgType::VARCHAR,
            Type::Null | Type::Unknown => PgType::UNKNOWN,
        }
    }
}

impl FromStr for Type {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "integer" => Type::Integer,
            "real" => Type::Float,
            "text" => Type::String,
            "blob" => Type::Binary,
            _ => Type::Unknown,
        })
    }
}

#[derive(Debug)]
pub struct Row {
    pub values: Vec<Value>,
}

/// Mirrors rusqlite::Value, but implement extra traits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<rusqlite::types::Value> for Value {
    fn from(other: rusqlite::types::Value) -> Self {
        use rusqlite::types::Value;

        match other {
            Value::Null => Self::Null,
            Value::Integer(i) => Self::Integer(i),
            Value::Real(x) => Self::Real(x),
            Value::Text(s) => Self::Text(s),
            Value::Blob(b) => Self::Blob(b),
        }
    }
}

#[derive(Debug)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub enum QueryResponse {
    Ack,
    ResultSet(ResultSet),
}

#[derive(Debug)]
pub enum Query {
    SimpleQuery(String),
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct QueryError {
    pub code: ErrorCode,
    pub msg: String,
}

impl QueryError {
    pub fn new(code: ErrorCode, msg: impl ToString) -> Self {
        Self {
            code,
            msg: msg.to_string(),
        }
    }
}

impl From<rusqlite::Error> for QueryError {
    fn from(other: rusqlite::Error) -> Self {
        Self::new(ErrorCode::SQLError, other)
    }
}

#[derive(Debug, Clone)]
pub enum ErrorCode {
    SQLError,
    TxBusy,
    TxTimeout,
    Internal,
}
