//! `Value` represents libSQL values and types.
//! Each database row consists of one or more cell values.

/// Value of a single database cell
// FIXME: We need to support blobs as well
#[derive(Clone, Debug)]
pub enum Value {
    Text(String),
    Float(f64),
    Number(i64),
    Bool(bool),
    Null,
}

// FIXME: we should *not* rely on Display for serialization purposes
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Text(s) => write!(f, "\"{s}\""),
            Value::Float(d) => write!(f, "{d}"),
            Value::Number(n) => write!(f, "{n}"),
            Value::Bool(b) => write!(f, "{}", if *b { "1" } else { "0" }),
            Value::Null => write!(f, "null"),
        }
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Value {
        Value::Null
    }
}

macro_rules! impl_from_value {
    ($typename: ty, $variant: ident) => {
        impl From<$typename> for Value {
            fn from(t: $typename) -> Value {
                Value::$variant(t.into())
            }
        }
    };
}

impl_from_value!(String, Text);
impl_from_value!(&str, Text);

impl_from_value!(i8, Number);
impl_from_value!(i16, Number);
impl_from_value!(i32, Number);
impl_from_value!(i64, Number);

impl_from_value!(u8, Number);
impl_from_value!(u16, Number);
impl_from_value!(u32, Number);

impl_from_value!(f32, Float);
impl_from_value!(f64, Float);

impl_from_value!(bool, Bool);
