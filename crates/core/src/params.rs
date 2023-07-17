use crate::raw;

pub enum Params {
    None,
    Positional(Vec<Value>),
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

impl From<raw::Value> for Value {
    fn from(value: raw::Value) -> Value {
        match value.value_type() {
            crate::rows::ValueType::Null => Value::Null,
            crate::rows::ValueType::Integer => Value::Integer(value.int().into()),
            crate::rows::ValueType::Float => todo!(),
            crate::rows::ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    Value::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const i8) };
                    let v = v.to_str().unwrap();
                    Value::Text(v.to_owned())
                }
            }
            crate::rows::ValueType::Blob => todo!(),
        }
    }
}
