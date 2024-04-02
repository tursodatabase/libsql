use std::collections::HashMap;

use anyhow::{anyhow, ensure, Context};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};

use crate::query_analysis::Statement;

/// Mirrors rusqlite::Value, but implement extra traits
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(*i),
            Value::Real(x) => ValueRef::Real(*x),
            Value::Text(s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(b) => ValueRef::Blob(b.as_slice()),
        }
    }
}

impl TryFrom<rusqlite::types::ValueRef<'_>> for Value {
    type Error = anyhow::Error;

    fn try_from(value: rusqlite::types::ValueRef<'_>) -> anyhow::Result<Value> {
        let val = match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(i) => Value::Integer(i),
            rusqlite::types::ValueRef::Real(x) => Value::Real(x),
            rusqlite::types::ValueRef::Text(s) => Value::Text(String::from_utf8(Vec::from(s))?),
            rusqlite::types::ValueRef::Blob(b) => Value::Blob(Vec::from(b)),
        };

        Ok(val)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Query {
    pub stmt: Statement,
    pub params: Params,
    pub want_rows: bool,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Params {
    Named(HashMap<String, Value>),
    Positional(Vec<Value>),
}

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
                    if stmt.is_explain() > 0 {
                        return Ok(());
                    } else {
                        return Err(anyhow!("value for parameter {} not found", name));
                    }
                } else {
                    if stmt.is_explain() > 0 {
                        return Ok(());
                    } else {
                        return Err(anyhow!("value for parameter {} not found", index));
                    }
                }
            }
        }

        Ok(())
    }
}

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
