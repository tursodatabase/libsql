use ::Result;
use ::error::Error;
use super::Value;

pub enum BorrowedValue<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> BorrowedValue<'a> {
    pub fn to_value(&self) -> Value {
        match *self {
            BorrowedValue::Null => Value::Null,
            BorrowedValue::Integer(i) => Value::Integer(i),
            BorrowedValue::Real(r) => Value::Real(r),
            BorrowedValue::Text(s) => Value::Text(s.to_string()),
            BorrowedValue::Blob(b) => Value::Blob(b.to_vec()),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match *self {
            BorrowedValue::Integer(i) => Ok(i),
            _ => Err(Error::InvalidColumnType),
        }
    }

    pub fn as_f64(&self) -> Result<f64> {
        match *self {
            BorrowedValue::Real(f) => Ok(f),
            _ => Err(Error::InvalidColumnType),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match *self {
            BorrowedValue::Text(ref t) => Ok(t),
            _ => Err(Error::InvalidColumnType),
        }
    }

    pub fn as_blob(&self) -> Result<&[u8]> {
        match *self {
            BorrowedValue::Blob(ref b) => Ok(b),
            _ => Err(Error::InvalidColumnType),
        }
    }
}
