use super::{Type, Value};
use crate::types::{FromSqlError, FromSqlResult};

/// A non-owning [dynamic type value](http://sqlite.org/datatype3.html). Typically the
/// memory backing this value is owned by SQLite.
///
/// See [`Value`](Value) for an owning dynamic type value.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ValueRef<'a> {
    /// The value is a `NULL` value.
    Null,
    /// The value is a signed integer.
    Integer(i64),
    /// The value is a floating point number.
    Real(f64),
    /// The value is a text string.
    Text(&'a [u8]),
    /// The value is a blob of data
    Blob(&'a [u8]),
}

impl ValueRef<'_> {
    /// Returns SQLite fundamental datatype.
    #[inline]
    #[must_use]
    pub fn data_type(&self) -> Type {
        match *self {
            ValueRef::Null => Type::Null,
            ValueRef::Integer(_) => Type::Integer,
            ValueRef::Real(_) => Type::Real,
            ValueRef::Text(_) => Type::Text,
            ValueRef::Blob(_) => Type::Blob,
        }
    }
}

impl<'a> ValueRef<'a> {
    /// If `self` is case `Integer`, returns the integral value. Otherwise,
    /// returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_i64(&self) -> FromSqlResult<i64> {
        match *self {
            ValueRef::Integer(i) => Ok(i),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Null` returns None.
    /// If `self` is case `Integer`, returns the integral value.
    /// Otherwise returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_i64_or_null(&self) -> FromSqlResult<Option<i64>> {
        match *self {
            ValueRef::Null => Ok(None),
            ValueRef::Integer(i) => Ok(Some(i)),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Real`, returns the floating point value. Otherwise,
    /// returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_f64(&self) -> FromSqlResult<f64> {
        match *self {
            ValueRef::Real(f) => Ok(f),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Null` returns None.
    /// If `self` is case `Real`, returns the floating point value.
    /// Otherwise returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_f64_or_null(&self) -> FromSqlResult<Option<f64>> {
        match *self {
            ValueRef::Null => Ok(None),
            ValueRef::Real(f) => Ok(Some(f)),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Text`, returns the string value. Otherwise, returns
    /// [`Err(Error::InvalidColumnType)`](crate::Error::InvalidColumnType).
    #[inline]
    pub fn as_str(&self) -> FromSqlResult<&'a str> {
        match *self {
            ValueRef::Text(t) => {
                std::str::from_utf8(t).map_err(|e| FromSqlError::Other(Box::new(e)))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Null` returns None.
    /// If `self` is case `Text`, returns the string value.
    /// Otherwise returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_str_or_null(&self) -> FromSqlResult<Option<&'a str>> {
        match *self {
            ValueRef::Null => Ok(None),
            ValueRef::Text(t) => std::str::from_utf8(t)
                .map_err(|e| FromSqlError::Other(Box::new(e)))
                .map(Some),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Blob`, returns the byte slice. Otherwise, returns
    /// [`Err(Error::InvalidColumnType)`](crate::Error::InvalidColumnType).
    #[inline]
    pub fn as_blob(&self) -> FromSqlResult<&'a [u8]> {
        match *self {
            ValueRef::Blob(b) => Ok(b),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Null` returns None.
    /// If `self` is case `Blob`, returns the byte slice.
    /// Otherwise returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_blob_or_null(&self) -> FromSqlResult<Option<&'a [u8]>> {
        match *self {
            ValueRef::Null => Ok(None),
            ValueRef::Blob(b) => Ok(Some(b)),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// Returns the byte slice that makes up this ValueRef if it's either
    /// [`ValueRef::Blob`] or [`ValueRef::Text`].
    #[inline]
    pub fn as_bytes(&self) -> FromSqlResult<&'a [u8]> {
        match self {
            ValueRef::Text(s) | ValueRef::Blob(s) => Ok(s),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Null` returns None.
    /// If `self` is [`ValueRef::Blob`] or [`ValueRef::Text`] returns the byte
    /// slice that makes up this value
    #[inline]
    pub fn as_bytes_or_null(&self) -> FromSqlResult<Option<&'a [u8]>> {
        match *self {
            ValueRef::Null => Ok(None),
            ValueRef::Text(s) | ValueRef::Blob(s) => Ok(Some(s)),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl From<ValueRef<'_>> for Value {
    #[inline]
    #[track_caller]
    fn from(borrowed: ValueRef<'_>) -> Value {
        match borrowed {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(r) => Value::Real(r),
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).expect("invalid UTF-8");
                Value::Text(s.to_string())
            }
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    #[inline]
    fn from(s: &str) -> ValueRef<'_> {
        ValueRef::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    #[inline]
    fn from(s: &[u8]) -> ValueRef<'_> {
        ValueRef::Blob(s)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    #[inline]
    fn from(value: &'a Value) -> ValueRef<'a> {
        match *value {
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

#[cfg(any(feature = "functions", feature = "session", feature = "vtab"))]
impl<'a> ValueRef<'a> {
    pub(crate) unsafe fn from_value(value: *mut crate::ffi::sqlite3_value) -> ValueRef<'a> {
        use crate::ffi;
        use std::slice::from_raw_parts;

        match ffi::sqlite3_value_type(value) {
            ffi::SQLITE_NULL => ValueRef::Null,
            ffi::SQLITE_INTEGER => ValueRef::Integer(ffi::sqlite3_value_int64(value)),
            ffi::SQLITE_FLOAT => ValueRef::Real(ffi::sqlite3_value_double(value)),
            ffi::SQLITE_TEXT => {
                let text = ffi::sqlite3_value_text(value);
                let len = ffi::sqlite3_value_bytes(value);
                assert!(
                    !text.is_null(),
                    "unexpected SQLITE_TEXT value type with NULL data"
                );
                let s = from_raw_parts(text.cast::<u8>(), len as usize);
                ValueRef::Text(s)
            }
            ffi::SQLITE_BLOB => {
                let (blob, len) = (
                    ffi::sqlite3_value_blob(value),
                    ffi::sqlite3_value_bytes(value),
                );

                assert!(
                    len >= 0,
                    "unexpected negative return from sqlite3_value_bytes"
                );
                if len > 0 {
                    assert!(
                        !blob.is_null(),
                        "unexpected SQLITE_BLOB value type with NULL data"
                    );
                    ValueRef::Blob(from_raw_parts(blob.cast::<u8>(), len as usize))
                } else {
                    // The return value from sqlite3_value_blob() for a zero-length BLOB
                    // is a NULL pointer.
                    ValueRef::Blob(&[])
                }
            }
            _ => unreachable!("sqlite3_value_type returned invalid value"),
        }
    }

    // TODO sqlite3_value_nochange // 3.22.0 & VTab xUpdate
    // TODO sqlite3_value_frombind // 3.28.0
}
