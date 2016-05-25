use ::Result;
use ::error::Error;
use super::Value;

/// A non-owning [dynamic type value](http://sqlite.org/datatype3.html). Typically the
/// memory backing this value is owned by SQLite.
///
/// See [`Value`](enum.Value.html) for an owning dynamic type value.
#[derive(Copy,Clone,Debug,PartialEq)]
pub enum ValueRef<'a> {
    /// The value is a `NULL` value.
    Null,
    /// The value is a signed integer.
    Integer(i64),
    /// The value is a floating point number.
    Real(f64),
    /// The value is a text string.
    Text(&'a str),
    /// The value is a blob of data
    Blob(&'a [u8]),
}

impl<'a> ValueRef<'a> {
    /// If `self` is case `Integer`, returns the integral value. Otherwise, returns
    /// `Err(Error::InvalidColumnType)`.
    pub fn as_i64(&self) -> Result<i64> {
        match *self {
            ValueRef::Integer(i) => Ok(i),
            _ => Err(Error::InvalidColumnType),
        }
    }

    /// If `self` is case `Real`, returns the floating point value. Otherwise, returns
    /// `Err(Error::InvalidColumnType)`.
    pub fn as_f64(&self) -> Result<f64> {
        match *self {
            ValueRef::Real(f) => Ok(f),
            _ => Err(Error::InvalidColumnType),
        }
    }

    /// If `self` is case `Text`, returns the string value. Otherwise, returns
    /// `Err(Error::InvalidColumnType)`.
    pub fn as_str(&self) -> Result<&str> {
        match *self {
            ValueRef::Text(ref t) => Ok(t),
            _ => Err(Error::InvalidColumnType),
        }
    }

    /// If `self` is case `Blob`, returns the byte slice. Otherwise, returns
    /// `Err(Error::InvalidColumnType)`.
    pub fn as_blob(&self) -> Result<&[u8]> {
        match *self {
            ValueRef::Blob(ref b) => Ok(b),
            _ => Err(Error::InvalidColumnType),
        }
    }
}

impl<'a> From<ValueRef<'a>> for Value {
    fn from(borrowed: ValueRef) -> Value {
        match borrowed {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(r) => Value::Real(r),
            ValueRef::Text(s) => Value::Text(s.to_string()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> ValueRef<'a> {
        match *value {
            Value::Null => ValueRef::Null,
            Value::Integer(i) => ValueRef::Integer(i),
            Value::Real(r) => ValueRef::Real(r),
            Value::Text(ref s) => ValueRef::Text(s),
            Value::Blob(ref b) => ValueRef::Blob(b),
        }
    }
}
