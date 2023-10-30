use std::str::FromStr;

use crate::{Error, Result};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub enum ValueType {
    Integer = 1,
    Real,
    Text,
    Blob,
    Null,
}

impl FromStr for ValueType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<ValueType, Self::Err> {
        match s {
            "TEXT" => Ok(ValueType::Text),
            "INTEGER" => Ok(ValueType::Integer),
            "BLOB" => Ok(ValueType::Blob),
            "NULL" => Ok(ValueType::Null),
            "REAL" => Ok(ValueType::Real),
            _ => Err(()),
        }
    }
}

impl Value {
    /// Returns `true` if the value is [`Null`].
    ///
    /// [`Null`]: Value::Null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns `true` if the value is [`Integer`].
    ///
    /// [`Integer`]: Value::Integer
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    /// Returns `true` if the value is [`Real`].
    ///
    /// [`Real`]: Value::Real
    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Text`].
    ///
    /// [`Text`]: Value::Text
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&String> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Blob`].
    ///
    /// [`Blob`]: Value::Blob
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Value {
        Value::Integer(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Value {
        Value::Integer(value as i64)
    }
}

impl TryFrom<u64> for Value {
    type Error = crate::Error;

    fn try_from(value: u64) -> Result<Value> {
        if value > i64::MAX as u64 {
            Err(Error::ToSqlConversionFailure(
                "u64 is too large to fit in an i64".into(),
            ))
        } else {
            Ok(Value::Integer(value as i64))
        }
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Value {
        Value::Real(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Value {
        Value::Real(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::Text(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Value {
        Value::Blob(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Value {
        Value::Blob(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Integer(value as i64)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(inner) => inner.into(),
            None => Value::Null,
        }
    }
}

#[cfg(feature = "core")]
impl From<libsql_sys::Value> for Value {
    fn from(value: libsql_sys::Value) -> Value {
        match value.value_type().into() {
            ValueType::Null => Value::Null,
            ValueType::Integer => Value::Integer(value.int64()),
            ValueType::Real => Value::Real(value.double()),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    Value::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const std::ffi::c_char) };
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

    /// Returns `true` if the value ref is [`Null`].
    ///
    /// [`Null`]: ValueRef::Null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns `true` if the value ref is [`Integer`].
    ///
    /// [`Integer`]: ValueRef::Integer
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Real`].
    ///
    /// [`Real`]: ValueRef::Real
    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Text`].
    ///
    /// [`Text`]: ValueRef::Text
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&[u8]> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Blob`].
    ///
    /// [`Blob`]: ValueRef::Blob
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
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

#[cfg(feature = "core")]
impl<'a> From<libsql_sys::Value> for ValueRef<'a> {
    fn from(value: libsql_sys::Value) -> ValueRef<'a> {
        match value.value_type().into() {
            ValueType::Null => ValueRef::Null,
            ValueType::Integer => ValueRef::Integer(value.int64()),
            ValueType::Real => ValueRef::Real(value.double()),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    ValueRef::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const std::ffi::c_char) };
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

#[cfg(feature = "core")]
impl From<libsql_sys::ValueType> for ValueType {
    fn from(other: libsql_sys::ValueType) -> Self {
        match other {
            libsql_sys::ValueType::Integer => ValueType::Integer,
            libsql_sys::ValueType::Real => ValueType::Real,
            libsql_sys::ValueType::Text => ValueType::Text,
            libsql_sys::ValueType::Blob => ValueType::Blob,
            libsql_sys::ValueType::Null => ValueType::Null,
        }
    }
}

#[cfg(feature = "replication")]
impl TryFrom<libsql_replication::rpc::proxy::Value> for Value {
    type Error = Error;

    fn try_from(value: libsql_replication::rpc::proxy::Value) -> Result<Self> {
        bincode::deserialize(&value.data[..]).map_err(Error::from)
    }
}
