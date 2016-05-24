use super::{BorrowedValue, Value};
use ::Result;

pub trait FromSql: Sized {
    fn column_result(value: BorrowedValue) -> Result<Self>;
}

impl FromSql for i32 {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        i64::column_result(value).map(|i| i as i32)
    }
}

impl FromSql for i64 {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        value.as_i64()
    }
}

impl FromSql for f64 {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        value.as_f64()
    }
}

impl FromSql for bool {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        i64::column_result(value).map(|i| match i {
            0 => false,
            _ => true,
        })
    }
}

impl FromSql for String {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromSql for Vec<u8> {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        value.as_blob().map(|b| b.to_vec())
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        match value {
            BorrowedValue::Null => Ok(None),
            _ => FromSql::column_result(value).map(Some),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: BorrowedValue) -> Result<Self> {
        Ok(value.to_value())
    }
}
