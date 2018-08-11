use super::{Null, Type};

/// Owning [dynamic type value](http://sqlite.org/datatype3.html). Value's type is typically
/// dictated by SQLite (not by the caller).
///
/// See [`ValueRef`](enum.ValueRef.html) for a non-owning dynamic type value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The value is a `NULL` value.
    Null,
    /// The value is a signed integer.
    Integer(i64),
    /// The value is a floating point number.
    Real(f64),
    /// The value is a text string.
    Text(String),
    /// The value is a blob of data
    Blob(Vec<u8>),
}

impl From<Null> for Value {
    fn from(_: Null) -> Value {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(i: bool) -> Value {
        Value::Integer(i as i64)
    }
}

impl From<isize> for Value {
    fn from(i: isize) -> Value {
        Value::Integer(i as i64)
    }
}

macro_rules! from_i64(
    ($t:ty) => (
        impl From<$t> for Value {
            fn from(i: $t) -> Value {
                Value::Integer(i64::from(i))
            }
        }
    )
);

from_i64!(i8);
from_i64!(i16);
from_i64!(i32);
from_i64!(u8);
from_i64!(u16);
from_i64!(u32);

impl From<i64> for Value {
    fn from(i: i64) -> Value {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Value {
        Value::Real(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Text(s)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Value {
        Value::Blob(v)
    }
}

impl Value {
    pub fn data_type(&self) -> Type {
        match *self {
            Value::Null => Type::Null,
            Value::Integer(_) => Type::Integer,
            Value::Real(_) => Type::Real,
            Value::Text(_) => Type::Text,
            Value::Blob(_) => Type::Blob,
        }
    }
}
