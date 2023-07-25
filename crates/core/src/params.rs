use std::collections::HashMap;

use libsql_sys::ValueType;

pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

#[macro_export]
macro_rules! params {
    () => {
        Params::None
    };
    ($($value:expr),* $(,)?) => {
        Params::Positional(vec![$($value.into()),*])
    };
}

#[macro_export]
macro_rules! named_params {
    () => {
        Params::None
    };
    ($($param_name:literal: $value:expr),* $(,)?) => {
        Params::Named(vec![$(($param_name.to_string(), crate::params::Value::from($value))),*])
    };
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

pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
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

impl From<libsql_sys::Value> for Value {
    fn from(value: libsql_sys::Value) -> Value {
        match value.value_type() {
            ValueType::Null => Value::Null,
            ValueType::Integer => Value::Integer(value.int().into()),
            ValueType::Float => todo!(),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    Value::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const i8) };
                    let v = v.to_str().unwrap();
                    Value::Text(v.to_owned())
                }
            }
            ValueType::Blob => todo!(),
        }
    }
}
