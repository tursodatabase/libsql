use super::{ValueRef, Value};
use std::error::Error;
use std::fmt;

/// Enum listing possible errors from `FromSql` trait.
#[derive(Debug)]
pub enum FromSqlError {
    /// Error when an SQLite value is requested, but the type of the result cannot be converted to the
    /// requested Rust type.
    InvalidType,
    /// An error case available for implementors of the `FromSql` trait.
    Other(Box<Error + Send + Sync>),
}

impl fmt::Display for FromSqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FromSqlError::InvalidType => write!(f, "Invalid type"),
            FromSqlError::Other(ref err) => err.fmt(f),
        }
    }
}

impl Error for FromSqlError {
    fn description(&self) -> &str {
        match *self {
            FromSqlError::InvalidType => "invalid type",
            FromSqlError::Other(ref err) => err.description(),
        }
    }

    #[cfg_attr(feature="clippy", allow(match_same_arms))]
    fn cause(&self) -> Option<&Error> {
        match *self {
            FromSqlError::InvalidType => None,
            FromSqlError::Other(ref err) => Some(&**err),
        }
    }
}

/// Result type for implementors of the `FromSql` trait.
pub type FromSqlResult<T> = Result<T, FromSqlError>;

/// A trait for types that can be created from a SQLite value.
pub trait FromSql: Sized {
    fn column_result(value: ValueRef) -> FromSqlResult<Self>;
}

impl FromSql for i32 {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        i64::column_result(value).map(|i| i as i32)
    }
}

impl FromSql for i64 {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_i64()
    }
}

impl FromSql for f64 {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => Ok(i as f64),
            ValueRef::Real(f) => Ok(f),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl FromSql for bool {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        i64::column_result(value).map(|i| match i {
            0 => false,
            _ => true,
        })
    }
}

impl FromSql for String {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromSql for Vec<u8> {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_blob().map(|b| b.to_vec())
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        match value {
            ValueRef::Null => Ok(None),
            _ => FromSql::column_result(value).map(Some),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        Ok(value.into())
    }
}
