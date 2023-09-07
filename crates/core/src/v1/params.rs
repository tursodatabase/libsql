use std::ffi::c_char;

use libsql_sys::ValueType;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

#[macro_export]
macro_rules! params {
    () => {
        $crate::Params::None
    };
    ($($value:expr),* $(,)?) => {
        $crate::Params::Positional(vec![$($value.into()),*])
    };
}

#[macro_export]
macro_rules! named_params {
    () => {
        $crate::Params::None
    };
    ($($param_name:literal: $value:expr),* $(,)?) => {
        $crate::Params::Named(vec![$(($param_name.to_string(), $crate::params::Value::from($value))),*])
    };
}

/// Convert an owned iterator into Params.
///
/// # Example
///
/// ```rust
/// # use libsql::{Connection, params_from_iter, Rows};
/// # async fn run(conn: &Connection) {
///
/// let iter = vec![1, 2, 3];
///
/// conn.query(
///     "SELECT * FROM users WHERE id IN (?1, ?2, ?3)",
///     params_from_iter(iter).unwrap()
/// )
/// .await
/// .unwrap();
/// # }
/// ```
pub fn params_from_iter<I>(iter: I) -> Result<Params>
where
    I: IntoIterator,
    I::Item: TryInto<Value>,
    <I::Item as TryInto<Value>>::Error: Into<crate::BoxError>,
{
    let vec = iter
        .into_iter()
        .map(|i| i.try_into())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::ToSqlConversionFailure(e.into()))?;

    Ok(Params::Positional(vec))
}

impl From<()> for Params {
    fn from(_: ()) -> Params {
        Params::None
    }
}

impl From<Vec<Value>> for Params {
    fn from(values: Vec<Value>) -> Params {
        Params::Positional(values)
    }
}

impl From<Vec<(String, Value)>> for Params {
    fn from(values: Vec<(String, Value)>) -> Params {
        Params::Named(values)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Text(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Value {
        Value::Blob(value)
    }
}

impl From<libsql_sys::Value> for Value {
    fn from(value: libsql_sys::Value) -> Value {
        match value.value_type() {
            ValueType::Null => Value::Null,
            ValueType::Integer => Value::Integer(value.int64()),
            ValueType::Real => Value::Real(value.double()),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    Value::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const c_char) };
                    let v = v.to_str().unwrap();
                    Value::Text(v.to_owned())
                }
            }
            ValueType::Blob => {
                let (len, blob) = (value.bytes(), value.blob());

                assert!(len >= 0, "unexpected negative bytes value from sqlite3");

                let mut v = Vec::with_capacity(len as usize);

                let slice: &[u8] =
                    unsafe { std::slice::from_raw_parts(blob as *const u8, len as usize) };
                v.extend_from_slice(slice);
                Value::Blob(v)
            }
        }
    }
}

// Heavily inspired by rusqlite's ValueRef
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

impl ValueRef<'_> {
    pub fn data_type(&self) -> ValueType {
        match *self {
            ValueRef::Null => ValueType::Null,
            ValueRef::Integer(_) => ValueType::Integer,
            ValueRef::Real(_) => ValueType::Real,
            ValueRef::Text(_) => ValueType::Text,
            ValueRef::Blob(_) => ValueType::Blob,
        }
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(vr: ValueRef<'_>) -> Value {
        match vr {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(r) => Value::Real(r),
            ValueRef::Text(s) => Value::Text(String::from_utf8_lossy(s).to_string()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &str) -> ValueRef<'_> {
        ValueRef::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(s: &[u8]) -> ValueRef<'_> {
        ValueRef::Blob(s)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(v: &'a Value) -> ValueRef<'a> {
        match *v {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(i),
            Value::Real(r) => ValueRef::Real(r),
            Value::Text(ref s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(ref b) => ValueRef::Blob(b),
        }
    }
}

impl<'a, T> From<Option<T>> for ValueRef<'a>
where
    T: Into<ValueRef<'a>>,
{
    #[inline]
    fn from(s: Option<T>) -> ValueRef<'a> {
        match s {
            Some(x) => x.into(),
            None => ValueRef::Null,
        }
    }
}

impl<'a> From<libsql_sys::Value> for ValueRef<'a> {
    fn from(value: libsql_sys::Value) -> ValueRef<'a> {
        match value.value_type() {
            ValueType::Null => ValueRef::Null,
            ValueType::Integer => ValueRef::Integer(value.int64()),
            ValueType::Real => todo!(),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    ValueRef::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const c_char) };
                    ValueRef::Text(v.to_bytes())
                }
            }
            ValueType::Blob => {
                let (len, blob) = (value.bytes(), value.blob());

                assert!(len >= 0, "unexpected negative bytes value from sqlite3");

                if len > 0 {
                    let slice: &[u8] =
                        unsafe { std::slice::from_raw_parts(blob as *const u8, len as usize) };
                    ValueRef::Blob(slice)
                } else {
                    ValueRef::Blob(&[])
                }
            }
        }
    }
}

#[cfg(feature = "replication")]
impl From<Params> for libsql_replication::pb::query::Params {
    fn from(params: Params) -> Self {
        use libsql_replication::pb;

        match params {
            Params::None => pb::query::Params::Positional(pb::Positional::default()),
            Params::Positional(values) => {
                let values = values
                    .iter()
                    .map(|v| bincode::serialize(v).unwrap())
                    .map(|data| pb::Value { data })
                    .collect::<Vec<_>>();
                pb::query::Params::Positional(pb::Positional { values })
            }
            Params::Named(_) => todo!(),
        }
    }
}
