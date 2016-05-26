use super::{ValueRef, Value};
use ::Result;
use ::error::Error;

/// A trait for types that can be created from a SQLite value.
pub trait FromSql: Sized {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self>;
}

impl FromSql for i32 {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        i64::column_result(value, idx).map(|i| i as i32)
    }
}

impl FromSql for i64 {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        value.as_i64(idx)
    }
}

impl FromSql for f64 {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        match value {
            ValueRef::Integer(i) => Ok(i as f64),
            ValueRef::Real(f) => Ok(f),
            _ => Err(Error::InvalidColumnType(idx, value.data_type())),
        }
    }
}

impl FromSql for bool {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        i64::column_result(value, idx).map(|i| match i {
            0 => false,
            _ => true,
        })
    }
}

impl FromSql for String {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        value.as_str(idx).map(|s| s.to_string())
    }
}

impl FromSql for Vec<u8> {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        value.as_blob(idx).map(|b| b.to_vec())
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn column_result(value: ValueRef, idx: i32) -> Result<Self> {
        match value {
            ValueRef::Null => Ok(None),
            _ => FromSql::column_result(value, idx).map(Some),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef, _: i32) -> Result<Self> {
        Ok(value.into())
    }
}
