use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

use anyhow::{anyhow, ensure, Context};
use futures::stream;
use pgwire::api::results::{query_response, DataRowEncoder, FieldFormat, FieldInfo, Response};
use pgwire::api::Type as PgType;
use pgwire::{error::PgWireResult, messages::data::DataRow};
use rusqlite::types::ToSqlOutput;
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::query_analysis::Statement;
use crate::rpc::proxy::rpc::{
    Column as RpcColumn, ResultRows, Row as RpcRow, Type as RpcType, Value as RpcValue,
};

pub type QueryResult = Result<QueryResponse, Error>;

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
            FieldFormat::Text,
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
    pub affected_row_count: u64,
    pub include_column_defs: bool,
}

impl ResultSet {
    pub fn empty(col_defs: bool) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_row_count: 0,
            include_column_defs: col_defs,
        }
    }
}

fn encode_row(row: Row) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(row.values.len());
    for value in row.values {
        match value {
            Value::Null => {
                encoder.encode_text_format_field(None::<&u8>)?;
            }
            Value::Integer(i) => {
                encoder.encode_text_format_field(Some(&i))?;
            }
            Value::Real(f) => {
                encoder.encode_text_format_field(Some(&f))?;
            }
            Value::Text(t) => {
                encoder.encode_text_format_field(Some(&t))?;
            }
            Value::Blob(b) => {
                encoder.encode_text_format_field(Some(&hex::encode(b)))?;
            }
        }
    }
    encoder.finish()
}

impl<'a> From<ResultSet> for Response<'a> {
    fn from(
        ResultSet {
            columns,
            rows,
            include_column_defs,
            ..
        }: ResultSet,
    ) -> Self {
        let field_infos = if include_column_defs {
            Some(columns.into_iter().map(Into::into).collect())
        } else {
            None
        };
        let data_row_stream = stream::iter(rows.into_iter().map(encode_row));
        Response::Query(query_response(field_infos, data_row_stream))
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
            affected_row_count: other.affected_row_count,
        }
    }
}

impl From<ResultRows> for ResultSet {
    fn from(result_rows: ResultRows) -> Self {
        let columns = result_rows
            .column_descriptions
            .into_iter()
            .map(|c| Column {
                ty: Some(c.ty().into()),
                name: c.name,
            })
            .collect();

        let rows = result_rows
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

        Self {
            columns,
            rows,
            affected_row_count: result_rows.affected_row_count,
            include_column_defs: true,
        }
    }
}

#[derive(Debug)]
pub enum QueryResponse {
    ResultSet(ResultSet),
}

#[derive(Debug)]
pub struct Query {
    pub stmt: Statement,
    pub params: Params,
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let val = match self {
            Value::Null => ToSqlOutput::Owned(rusqlite::types::Value::Null),
            Value::Integer(i) => ToSqlOutput::Owned(rusqlite::types::Value::Integer(*i)),
            Value::Real(x) => ToSqlOutput::Owned(rusqlite::types::Value::Real(*x)),
            Value::Text(s) => ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(s.as_bytes())),
            Value::Blob(b) => ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Blob(b)),
        };

        Ok(val)
    }
}

#[derive(Debug, Serialize)]
pub enum Params {
    Named(HashMap<String, Value>),
    Positional(Vec<Value>),
}

impl Params {}

impl Params {
    pub fn empty() -> Self {
        Self::Positional(Vec::new())
    }

    pub fn new_named(values: HashMap<String, Value>) -> Self {
        Self::Named(values)
    }

    pub fn new_positional(values: Vec<Value>) -> Self {
        Self::Positional(values)
    }

    pub fn get_pos(&self, pos: usize) -> Option<&Value> {
        assert!(pos > 0);
        match self {
            Params::Named(_) => None,
            Params::Positional(params) => params.get(pos - 1),
        }
    }

    pub fn get_named(&self, name: &str) -> Option<&Value> {
        match self {
            Params::Named(params) => params.get(name),
            Params::Positional(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Params::Named(params) => params.len(),
            Params::Positional(params) => params.len(),
        }
    }

    pub fn bind(&self, stmt: &mut rusqlite::Statement) -> anyhow::Result<()> {
        let param_count = stmt.parameter_count();
        ensure!(
            param_count >= self.len(),
            "too many parameters, expected {param_count} found {}",
            self.len()
        );

        if param_count > 0 {
            for index in 1..=param_count {
                let mut param_name = None;
                // get by name
                let maybe_value = match stmt.parameter_name(index) {
                    Some(name) => {
                        param_name = Some(name);
                        let mut chars = name.chars();
                        match chars.next() {
                            Some('?') => {
                                let pos = chars.as_str().parse::<usize>().context(
                                    "invalid parameter {name}: expected a numerical position after `?`",
                                )?;
                                self.get_pos(pos)
                            }
                            _ => self
                                .get_named(name)
                                .or_else(|| self.get_named(chars.as_str())),
                        }
                    }
                    None => self.get_pos(index),
                };

                if let Some(value) = maybe_value {
                    stmt.raw_bind_parameter(index, value)?;
                } else if let Some(name) = param_name {
                    return Err(anyhow!("value for parameter {} not found", name));
                } else {
                    return Err(anyhow!("value for parameter {} not found", index));
                }
            }
        }

        Ok(())
    }
}

pub type Queries = Vec<Query>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bind_params_positional_simple() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT ?").unwrap();
        let params = Params::new_positional(vec![Value::Integer(10)]);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10");
    }

    #[test]
    fn test_bind_params_positional_numbered() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT ? || ?2 || ?1").unwrap();
        let params = Params::new_positional(vec![Value::Integer(10), Value::Integer(20)]);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10 || 20 || 10");
    }

    #[test]
    fn test_bind_params_positional_named() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT :first || $second").unwrap();
        let mut params = HashMap::new();
        params.insert(":first".to_owned(), Value::Integer(10));
        params.insert("$second".to_owned(), Value::Integer(20));
        let params = Params::new_named(params);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10 || 20");
    }

    #[test]
    fn test_bind_params_positional_named_no_prefix() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT :first || $second").unwrap();
        let mut params = HashMap::new();
        params.insert("first".to_owned(), Value::Integer(10));
        params.insert("second".to_owned(), Value::Integer(20));
        let params = Params::new_named(params);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10 || 20");
    }

    #[test]
    fn test_bind_params_positional_named_conflict() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT :first || $first").unwrap();
        let mut params = HashMap::new();
        params.insert("first".to_owned(), Value::Integer(10));
        params.insert("$first".to_owned(), Value::Integer(20));
        let params = Params::new_named(params);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10 || 20");
    }

    #[test]
    fn test_bind_params_positional_named_repeated() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con
            .prepare("SELECT :first || $second || $first || $second")
            .unwrap();
        let mut params = HashMap::new();
        params.insert("first".to_owned(), Value::Integer(10));
        params.insert("$second".to_owned(), Value::Integer(20));
        let params = Params::new_named(params);
        params.bind(&mut stmt).unwrap();

        assert_eq!(stmt.expanded_sql().unwrap(), "SELECT 10 || 20 || 10 || 20");
    }

    #[test]
    fn test_bind_params_too_many_params() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT :first || $second").unwrap();
        let mut params = HashMap::new();
        params.insert(":first".to_owned(), Value::Integer(10));
        params.insert("$second".to_owned(), Value::Integer(20));
        params.insert("$oops".to_owned(), Value::Integer(20));
        let params = Params::new_named(params);
        assert!(params.bind(&mut stmt).is_err());
    }

    #[test]
    fn test_bind_params_too_few_params() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT :first || $second").unwrap();
        let mut params = HashMap::new();
        params.insert(":first".to_owned(), Value::Integer(10));
        let params = Params::new_named(params);
        assert!(params.bind(&mut stmt).is_err());
    }

    #[test]
    fn test_bind_params_invalid_positional() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = con.prepare("SELECT ?invalid").unwrap();
        let params = Params::empty();
        assert!(params.bind(&mut stmt).is_err());
    }
}
