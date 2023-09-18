use std::ffi::c_char;

use libsql_sys::ValueType;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

mod sealed {
    pub trait Sealed {}
}

use sealed::Sealed;

/// Converts some type into paramters that can be passed
/// to libsql.
///
/// The trait is sealed and not designed to be implemented by hand
/// but instead provides a few ways to use it.
///
/// # Passing parameters to libsql
///
/// Many functions in this library let you pass parameters to libsql. Doing this
/// lets you avoid any risk of SQL injection, and is simpler than escaping
/// things manually. These functions generally contain some paramter that generically
/// accepts some implementation this trait.
///
/// # Positional parameters
///
/// These can be supplied in a few ways:
///
/// - For heterogeneous parameter lists of 16 or less items a tuple syntax is supported
///     by doing `(1, "foo")`.
/// - For hetergeneous parameter lists of 16 or greater, the [`libsql::params!`] is supported
///     by doing `libsql::params![1, "foo"]`.
/// - For homogeneous paramter types (where they are all the same type), const arrays are
///     supported by doing `[1, 2, 3]`.
///
/// # Example (positional)
///
/// ```rust,no_run
/// # use libsql::{Connection, params};
/// # async fn run(conn: Connection) -> libsql::Result<()> {
/// let mut stmt = conn.prepare("INSERT INTO test (a, b) VALUES (?1, ?2)").await?;
///
/// // Using a tuple:
/// stmt.execute((0, "foobar")).await?;
///
/// // Using `libsql::params!`:
/// stmt.execute(params![1i32, "blah"]).await?;
///
/// // array literal — non-references
/// stmt.execute([2i32, 3i32]).await?;
///
/// // array literal — references
/// stmt.execute(["foo", "bar"]).await?;
///
/// // Slice literal, references:
/// stmt.execute([2i32, 3i32]).await?;
///
/// #    Ok(())
/// # }
/// ```
///
/// # Named paramters
///
/// - For heterogeneous parameter lists of 16 or less items a tuple syntax is supported
///     by doing `(("key1", 1), ("key2", "foo"))`.
/// - For hetergeneous parameter lists of 16 or greater, the [`libsql::params!`] is supported
///     by doing `libsql::named_params!["key1": 1, "key2": "foo"]`.
/// - For homogeneous paramter types (where they are all the same type), const arrays are
///     supported by doing `[("key1", 1), ("key2, 2), ("key3", 3)]`.
///
/// # Example (named)
///
/// ```rust,no_run
/// # use libsql::{Connection, named_params};
/// # async fn run(conn: Connection) -> libsql::Result<()> {
/// let mut stmt = conn.prepare("INSERT INTO test (a, b) VALUES (:key1, :key2)").await?;
///
/// // Using a tuple:
/// stmt.execute(((":key1", 0), (":key2", "foobar"))).await?;
///
/// // Using `libsql::named_params!`:
/// stmt.execute(named_params! {":key1": 1i32, ":key2": "blah" }).await?;
///
/// // const array:
/// stmt.execute([(":key1", 2i32), (":key2", 3i32)]).await?;
///
/// #   Ok(())
/// # }
/// ```
pub trait IntoParams: Sealed {
    // Hide this because users should not be implementing this
    // themselves. We should consider sealing this trait.
    #[doc(hidden)]
    fn into_params(self) -> Result<Params>;
}

#[derive(Clone)]
#[doc(hidden)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

/// Convert an owned iterator into Params.
///
/// # Example
///
/// ```rust
/// # use libsql::{Connection, params_from_iter, Rows};
/// # async fn run(conn: &Connection) {
///
/// let iter = vec![1, 2, 3];
///
/// conn.query(
///     "SELECT * FROM users WHERE id IN (?1, ?2, ?3)",
///     params_from_iter(iter)
/// )
/// .await
/// .unwrap();
/// # }
/// ```
pub fn params_from_iter<I>(iter: I) -> impl IntoParams
where
    I: IntoIterator,
    I::Item: IntoValue,
{
    iter.into_iter().collect::<Vec<_>>()
}

impl Sealed for () {}
impl IntoParams for () {
    fn into_params(self) -> Result<Params> {
        Ok(Params::None)
    }
}

impl Sealed for Params {}
impl IntoParams for Params {
    fn into_params(self) -> Result<Params> {
        Ok(self)
    }
}

impl<T: IntoValue> Sealed for Vec<T> {}
impl<T: IntoValue> IntoParams for Vec<T> {
    fn into_params(self) -> Result<Params> {
        let values = self
            .into_iter()
            .map(|i| i.into_value())
            .collect::<Result<Vec<_>>>()?;

        Ok(Params::Positional(values))
    }
}

impl<T: IntoValue> Sealed for Vec<(String, T)> {}
impl<T: IntoValue> IntoParams for Vec<(String, T)> {
    fn into_params(self) -> Result<Params> {
        let values = self
            .into_iter()
            .map(|(k, v)| Ok((k, v.into_value()?)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Params::Named(values))
    }
}

impl<T: IntoValue, const N: usize> Sealed for [T; N] {}
impl<T: IntoValue, const N: usize> IntoParams for [T; N] {
    fn into_params(self) -> Result<Params> {
        self.into_iter().collect::<Vec<_>>().into_params()
    }
}

impl<T: IntoValue, const N: usize> Sealed for [(&str, T); N] {}
impl<T: IntoValue, const N: usize> IntoParams for [(&str, T); N] {
    fn into_params(self) -> Result<Params> {
        self.into_iter()
            // TODO: Pretty unfortunate that we need to allocate here when we know
            // the str is likely 'static. Maybe we should convert our param names
            // to be `Cow<'static, str>`?
            .map(|(k, v)| Ok((k.to_string(), v.into_value()?)))
            .collect::<Result<Vec<_>>>()?
            .into_params()
    }
}

impl<T: IntoValue + Clone, const N: usize> Sealed for &[T; N] {}
impl<T: IntoValue + Clone, const N: usize> IntoParams for &[T; N] {
    fn into_params(self) -> Result<Params> {
        self.iter().cloned().collect::<Vec<_>>().into_params()
    }
}

// NOTICE: heavily inspired by rusqlite
macro_rules! tuple_into_params {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> Sealed for ($($ftype,)*) where $($ftype: IntoValue,)* {}
        impl<$($ftype,)*> IntoParams for ($($ftype,)*) where $($ftype: IntoValue,)* {
            fn into_params(self) -> Result<Params> {
                let params = Params::Positional(vec![$(self.$field.into_value()?),*]);
                Ok(params)
            }
        }
    }
}

macro_rules! named_tuple_into_params {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> Sealed for ($((&str, $ftype),)*) where $($ftype: IntoValue,)* {}
        impl<$($ftype,)*> IntoParams for ($((&str, $ftype),)*) where $($ftype: IntoValue,)* {
            fn into_params(self) -> Result<Params> {
                let params = Params::Named(vec![$((self.$field.0.to_string(), self.$field.1.into_value()?)),*]);
                Ok(params)
            }
        }
    }
}

named_tuple_into_params!(2: (0 A), (1 B));
named_tuple_into_params!(3: (0 A), (1 B), (2 C));
named_tuple_into_params!(4: (0 A), (1 B), (2 C), (3 D));
named_tuple_into_params!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
named_tuple_into_params!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
named_tuple_into_params!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
named_tuple_into_params!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
named_tuple_into_params!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
named_tuple_into_params!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
named_tuple_into_params!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
named_tuple_into_params!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
named_tuple_into_params!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
named_tuple_into_params!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
named_tuple_into_params!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
named_tuple_into_params!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));

tuple_into_params!(2: (0 A), (1 B));
tuple_into_params!(3: (0 A), (1 B), (2 C));
tuple_into_params!(4: (0 A), (1 B), (2 C), (3 D));
tuple_into_params!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
tuple_into_params!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
tuple_into_params!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
tuple_into_params!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
tuple_into_params!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
tuple_into_params!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
tuple_into_params!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
tuple_into_params!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
tuple_into_params!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
tuple_into_params!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
tuple_into_params!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
tuple_into_params!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));

// TODO: Should we rename this to `ToSql` which makes less sense but
// matches the error variant we have in `Error`. Or should we change the
// error variant to match this breaking the few people that currently use
// this error variant.
pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

impl<T> IntoValue for T
where
    T: TryInto<Value>,
    T::Error: Into<crate::BoxError>,
{
    fn into_value(self) -> Result<Value> {
        self.try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))
    }
}

impl IntoValue for Result<Value> {
    fn into_value(self) -> Result<Value> {
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    /// Returns `true` if the value is [`Null`].
    ///
    /// [`Null`]: Value::Null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns `true` if the value is [`Integer`].
    ///
    /// [`Integer`]: Value::Integer
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    /// Returns `true` if the value is [`Real`].
    ///
    /// [`Real`]: Value::Real
    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Text`].
    ///
    /// [`Text`]: Value::Text
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&String> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Blob`].
    ///
    /// [`Blob`]: Value::Blob
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Integer(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Value {
        Value::Integer(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Value {
        Value::Integer(value as i64)
    }
}

impl TryFrom<u64> for Value {
    type Error = crate::Error;

    fn try_from(value: u64) -> Result<Value> {
        if value > i64::MAX as u64 {
            Err(Error::ToSqlConversionFailure(
                "u64 is too large to fit in an i64".into(),
            ))
        } else {
            Ok(Value::Integer(value as i64))
        }
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Value {
        Value::Real(value as f64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Value {
        Value::Real(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::Text(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Value {
        Value::Blob(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Value {
        Value::Blob(value)
    }
}

impl From<libsql_sys::Value> for Value {
    fn from(value: libsql_sys::Value) -> Value {
        match value.value_type() {
            ValueType::Null => Value::Null,
            ValueType::Integer => Value::Integer(value.int64()),
            ValueType::Real => Value::Real(value.double()),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    Value::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const c_char) };
                    let v = v.to_str().unwrap();
                    Value::Text(v.to_owned())
                }
            }
            ValueType::Blob => {
                let (len, blob) = (value.bytes(), value.blob());

                assert!(len >= 0, "unexpected negative bytes value from sqlite3");

                let mut v = Vec::with_capacity(len as usize);

                let slice: &[u8] =
                    unsafe { std::slice::from_raw_parts(blob as *const u8, len as usize) };
                v.extend_from_slice(slice);
                Value::Blob(v)
            }
        }
    }
}

// Heavily inspired by rusqlite's ValueRef
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

impl ValueRef<'_> {
    pub fn data_type(&self) -> ValueType {
        match *self {
            ValueRef::Null => ValueType::Null,
            ValueRef::Integer(_) => ValueType::Integer,
            ValueRef::Real(_) => ValueType::Real,
            ValueRef::Text(_) => ValueType::Text,
            ValueRef::Blob(_) => ValueType::Blob,
        }
    }

    /// Returns `true` if the value ref is [`Null`].
    ///
    /// [`Null`]: ValueRef::Null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns `true` if the value ref is [`Integer`].
    ///
    /// [`Integer`]: ValueRef::Integer
    #[must_use]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer(..))
    }

    pub fn as_integer(&self) -> Option<&i64> {
        if let Self::Integer(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Real`].
    ///
    /// [`Real`]: ValueRef::Real
    #[must_use]
    pub fn is_real(&self) -> bool {
        matches!(self, Self::Real(..))
    }

    pub fn as_real(&self) -> Option<&f64> {
        if let Self::Real(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Text`].
    ///
    /// [`Text`]: ValueRef::Text
    #[must_use]
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    pub fn as_text(&self) -> Option<&[u8]> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the value ref is [`Blob`].
    ///
    /// [`Blob`]: ValueRef::Blob
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(..))
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        if let Self::Blob(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(vr: ValueRef<'_>) -> Value {
        match vr {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(r) => Value::Real(r),
            ValueRef::Text(s) => Value::Text(String::from_utf8_lossy(s).to_string()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &str) -> ValueRef<'_> {
        ValueRef::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(s: &[u8]) -> ValueRef<'_> {
        ValueRef::Blob(s)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(v: &'a Value) -> ValueRef<'a> {
        match *v {
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

impl<'a> From<libsql_sys::Value> for ValueRef<'a> {
    fn from(value: libsql_sys::Value) -> ValueRef<'a> {
        match value.value_type() {
            ValueType::Null => ValueRef::Null,
            ValueType::Integer => ValueRef::Integer(value.int64()),
            ValueType::Real => ValueRef::Real(value.double()),
            ValueType::Text => {
                let v = value.text();
                if v.is_null() {
                    ValueRef::Null
                } else {
                    let v = unsafe { std::ffi::CStr::from_ptr(v as *const c_char) };
                    ValueRef::Text(v.to_bytes())
                }
            }
            ValueType::Blob => {
                let (len, blob) = (value.bytes(), value.blob());

                assert!(len >= 0, "unexpected negative bytes value from sqlite3");

                if len > 0 {
                    let slice: &[u8] =
                        unsafe { std::slice::from_raw_parts(blob as *const u8, len as usize) };
                    ValueRef::Blob(slice)
                } else {
                    ValueRef::Blob(&[])
                }
            }
        }
    }
}

#[cfg(feature = "replication")]
impl From<Params> for crate::replication::pb::query::Params {
    fn from(params: Params) -> Self {
        use crate::replication::pb;

        match params {
            Params::None => pb::query::Params::Positional(pb::Positional::default()),
            Params::Positional(values) => {
                let values = values
                    .iter()
                    .map(|v| bincode::serialize(v).unwrap())
                    .map(|data| pb::Value { data })
                    .collect::<Vec<_>>();
                pb::query::Params::Positional(pb::Positional { values })
            }
            Params::Named(values) => {
                let (names, values) = values
                    .into_iter()
                    .map(|(name, value)| {
                        let data = bincode::serialize(&value).unwrap();
                        let value = pb::Value { data };
                        (name, value)
                    })
                    .unzip();

                pb::query::Params::Named(pb::Named { names, values })
            }
        }
    }
}

#[cfg(feature = "replication")]
impl TryFrom<crate::replication::pb::Value> for Value {
    type Error = Error;

    fn try_from(value: crate::replication::pb::Value) -> Result<Self> {
        bincode::deserialize(&value.data[..]).map_err(Error::from)
    }
}

// TODO(lucio): Rework macros to take advantage of new traits
#[macro_export]
macro_rules! params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::params::IntoValue;
        [$($value.into_value()),*]

    }};
}

#[macro_export]
macro_rules! named_params {
    () => {
        ()
    };
    ($($param_name:literal: $value:expr),* $(,)?) => {{
        use $crate::params::IntoValue;
        [$(($param_name, $value.into_value())),*]
    }};
}
