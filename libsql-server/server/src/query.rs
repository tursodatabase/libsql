use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use futures::stream;
use pgwire::api::results::{text_query_response, FieldInfo, Response, TextDataRowEncoder};
use pgwire::api::Type as PgType;
use pgwire::{error::PgWireResult, messages::data::DataRow};
use serde::{Deserialize, Serialize};

use crate::rpc::proxy::proxy_rpc::{
    error::ErrorCode as RpcErrorCode, Column as RpcColumn, Error as RpcError, ResultRows,
    Row as RpcRow, Type as RpcType, Value as RpcValue,
};

pub type QueryResult = Result<QueryResponse, QueryError>;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ty: Option<Type>,
}

impl From<Column> for RpcColumn {
    fn from(other: Column) -> Self {
        RpcColumn {
            name: other.name,
            ty: other.ty.map(|ty| RpcType::from(ty).into()),
        }
    }
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
    Blob,
    Real,
    Text,
    Null,
    Numeric,
    Unknown,
}

impl From<Type> for PgType {
    fn from(other: Type) -> Self {
        match other {
            Type::Integer => PgType::INT8,
            Type::Blob => PgType::BYTEA,
            Type::Real => PgType::FLOAT8,
            Type::Numeric => PgType::NUMERIC,
            Type::Text => PgType::TEXT,
            Type::Null | Type::Unknown => PgType::UNKNOWN,
        }
    }
}

impl From<Type> for RpcType {
    fn from(other: Type) -> Self {
        match other {
            Type::Integer => Self::Integer,
            Type::Blob => Self::Blob,
            Type::Real => Self::Real,
            Type::Text => Self::Text,
            Type::Null => Self::Null,
            Type::Numeric => Self::Numeric,
            Type::Unknown => Self::Unknown,
        }
    }
}

impl From<RpcType> for Type {
    fn from(other: RpcType) -> Self {
        match other {
            RpcType::Integer => Self::Integer,
            RpcType::Blob => Self::Blob,
            RpcType::Real => Self::Real,
            RpcType::Text => Self::Text,
            RpcType::Null => Self::Null,
            RpcType::Unknown => Self::Unknown,
            RpcType::Numeric => Self::Numeric,
        }
    }
}

impl FromStr for Type {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "integer" | "int" | "tinyint" | "smallint" | "mediumint" | "bigint"
            | "unsigned big int" | "int2" | "int8" => Type::Integer,
            "real" | "double" | "double precision" | "float" => Type::Real,
            "text" | "character" | "varchar" | "varying character" | "nchar"
            | "native character" | "nvarchar" | "clob" => Type::Text,
            "blob" => Type::Blob,
            "numeric" | "decimal" | "boolean" | "date" | "datetime" => Type::Numeric,
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

impl From<Value> for rusqlite::types::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(i) => Self::Integer(i),
            Value::Real(x) => Self::Real(x),
            Value::Text(s) => Self::Text(s),
            Value::Blob(b) => Self::Blob(b),
        }
    }
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

impl ResultSet {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

fn encode_row(row: Row) -> PgWireResult<DataRow> {
    let mut encoder = TextDataRowEncoder::new(row.values.len());
    for value in row.values {
        match value {
            Value::Null => {
                encoder.append_field(None::<&u8>)?;
            }
            Value::Integer(i) => {
                encoder.append_field(Some(&i))?;
            }
            Value::Real(f) => {
                encoder.append_field(Some(&f))?;
            }
            Value::Text(t) => {
                encoder.append_field(Some(&t))?;
            }
            Value::Blob(b) => {
                encoder.append_field(Some(&hex::encode(b)))?;
            }
        }
    }
    encoder.finish()
}

impl From<ResultSet> for Response {
    fn from(ResultSet { columns, rows }: ResultSet) -> Self {
        let field_infos = columns.into_iter().map(Into::into).collect();
        let data_row_stream = stream::iter(rows.into_iter().map(encode_row));
        Response::Query(text_query_response(field_infos, data_row_stream))
    }
}

impl From<ResultSet> for ResultRows {
    fn from(other: ResultSet) -> Self {
        let column_descriptions = other.columns.into_iter().map(Into::into).collect();
        let rows = other
            .rows
            .iter()
            .map(|row| RpcRow {
                values: row
                    .values
                    .iter()
                    .map(|v| bincode::serialize(v).unwrap())
                    .map(|data| RpcValue { data })
                    .collect(),
            })
            .collect();

        ResultRows {
            column_descriptions,
            rows,
        }
    }
}

impl From<ResultRows> for ResultSet {
    fn from(rows: ResultRows) -> Self {
        let columns = rows
            .column_descriptions
            .into_iter()
            .map(|c| Column {
                ty: Some(c.ty().into()),
                name: c.name,
            })
            .collect();

        let rows = rows
            .rows
            .into_iter()
            .map(|row| {
                row.values
                    .iter()
                    .map(|v| bincode::deserialize(&v.data).unwrap())
                    .collect::<Vec<_>>()
            })
            .map(|values| Row { values })
            .collect();

        Self { columns, rows }
    }
}

#[derive(Debug)]
pub enum QueryResponse {
    ResultSet(ResultSet),
}

#[derive(Debug)]
pub enum Query {
    SimpleQuery(String, Vec<Value>),
}

#[derive(Debug, Clone)]
pub struct QueryError {
    pub code: ErrorCode,
    pub msg: String,
}

impl std::error::Error for QueryError {}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl From<RpcError> for QueryError {
    fn from(other: RpcError) -> Self {
        let code = match other.code() {
            RpcErrorCode::SqlError => ErrorCode::SQLError,
            RpcErrorCode::TxBusy => ErrorCode::TxBusy,
            RpcErrorCode::TxTimeout => ErrorCode::TxTimeout,
            RpcErrorCode::Internal => ErrorCode::Internal,
        };

        Self::new(code, other.message)
    }
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
