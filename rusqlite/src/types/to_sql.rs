use super::{Null, Value, ValueRef};
#[cfg(feature = "array")]
use crate::vtab::array::Array;
use crate::Result;
use std::borrow::Cow;

/// `ToSqlOutput` represents the possible output types for implementors of the
/// `ToSql` trait.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToSqlOutput<'a> {
    /// A borrowed SQLite-representable value.
    Borrowed(ValueRef<'a>),

    /// An owned SQLite-representable value.
    Owned(Value),

    /// `feature = "blob"` A BLOB of the given length that is filled with
    /// zeroes.
    #[cfg(feature = "blob")]
    ZeroBlob(i32),

    /// `feature = "array"`
    #[cfg(feature = "array")]
    Array(Array),
}

// Generically allow any type that can be converted into a ValueRef
// to be converted into a ToSqlOutput as well.
impl<'a, T: ?Sized> From<&'a T> for ToSqlOutput<'a>
where
    &'a T: Into<ValueRef<'a>>,
{
    fn from(t: &'a T) -> Self {
        ToSqlOutput::Borrowed(t.into())
    }
}

// We cannot also generically allow any type that can be converted
// into a Value to be converted into a ToSqlOutput because of
// coherence rules (https://github.com/rust-lang/rust/pull/46192),
// so we'll manually implement it for all the types we know can
// be converted into Values.
macro_rules! from_value(
    ($t:ty) => (
        impl From<$t> for ToSqlOutput<'_> {
            fn from(t: $t) -> Self { ToSqlOutput::Owned(t.into())}
        }
    )
);
from_value!(String);
from_value!(Null);
from_value!(bool);
from_value!(i8);
from_value!(i16);
from_value!(i32);
from_value!(i64);
from_value!(isize);
from_value!(u8);
from_value!(u16);
from_value!(u32);
from_value!(f64);
from_value!(Vec<u8>);

// It would be nice if we could avoid the heap allocation (of the `Vec`) that
// `i128` needs in `Into<Value>`, but it's probably fine for the moment, and not
// worth adding another case to Value.
#[cfg(feature = "i128_blob")]
from_value!(i128);

#[cfg(feature = "uuid")]
from_value!(uuid::Uuid);

impl ToSql for ToSqlOutput<'_> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(match *self {
            ToSqlOutput::Borrowed(v) => ToSqlOutput::Borrowed(v),
            ToSqlOutput::Owned(ref v) => ToSqlOutput::Borrowed(ValueRef::from(v)),

            #[cfg(feature = "blob")]
            ToSqlOutput::ZeroBlob(i) => ToSqlOutput::ZeroBlob(i),
            #[cfg(feature = "array")]
            ToSqlOutput::Array(ref a) => ToSqlOutput::Array(a.clone()),
        })
    }
}

/// A trait for types that can be converted into SQLite values.
pub trait ToSql {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>>;
}

impl<T: ToSql + ToOwned + ?Sized> ToSql for Cow<'_, T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for Box<T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for std::rc::Rc<T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for std::sync::Arc<T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

// We should be able to use a generic impl like this:
//
// impl<T: Copy> ToSql for T where T: Into<Value> {
//     fn to_sql(&self) -> Result<ToSqlOutput> {
//         Ok(ToSqlOutput::from((*self).into()))
//     }
// }
//
// instead of the following macro, but this runs afoul of
// https://github.com/rust-lang/rust/issues/30191 and reports conflicting
// implementations even when there aren't any.

macro_rules! to_sql_self(
    ($t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(*self))
            }
        }
    )
);

to_sql_self!(Null);
to_sql_self!(bool);
to_sql_self!(i8);
to_sql_self!(i16);
to_sql_self!(i32);
to_sql_self!(i64);
to_sql_self!(isize);
to_sql_self!(u8);
to_sql_self!(u16);
to_sql_self!(u32);
to_sql_self!(f64);

#[cfg(feature = "i128_blob")]
to_sql_self!(i128);

#[cfg(feature = "uuid")]
to_sql_self!(uuid::Uuid);

impl<T: ?Sized> ToSql for &'_ T
where
    T: ToSql,
{
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        (*self).to_sql()
    }
}

impl ToSql for String {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_str()))
    }
}

impl ToSql for str {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}

impl ToSql for [u8] {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match *self {
            None => Ok(ToSqlOutput::from(Null)),
            Some(ref t) => t.to_sql(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::ToSql;

    fn is_to_sql<T: ToSql>() {}

    #[test]
    fn test_integral_types() {
        is_to_sql::<i8>();
        is_to_sql::<i16>();
        is_to_sql::<i32>();
        is_to_sql::<i64>();
        is_to_sql::<u8>();
        is_to_sql::<u16>();
        is_to_sql::<u32>();
    }

    #[test]
    fn test_cow_str() {
        use std::borrow::Cow;
        let s = "str";
        let cow: Cow<str> = Cow::Borrowed(s);
        let r = cow.to_sql();
        assert!(r.is_ok());
        let cow: Cow<str> = Cow::Owned::<str>(String::from(s));
        let r = cow.to_sql();
        assert!(r.is_ok());
        // Ensure this compiles.
        let _p: &[&dyn ToSql] = crate::params![cow];
    }

    #[test]
    fn test_box_dyn() {
        let s: Box<dyn ToSql> = Box::new("Hello world!");
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = ToSql::to_sql(&s);

        assert!(r.is_ok());
    }

    #[test]
    fn test_box_deref() {
        let s: Box<str> = "Hello world!".into();
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();

        assert!(r.is_ok());
    }

    #[test]
    fn test_box_direct() {
        let s: Box<str> = "Hello world!".into();
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = ToSql::to_sql(&s);

        assert!(r.is_ok());
    }

    #[test]
    fn test_cells() {
        use std::{rc::Rc, sync::Arc};

        let source_str: Box<str> = "Hello world!".into();

        let s: Rc<Box<str>> = Rc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<Box<str>> = Arc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<str> = Arc::from(&*source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<dyn ToSql> = Arc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Rc<str> = Rc::from(&*source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Rc<dyn ToSql> = Rc::new(source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());
    }

    #[cfg(feature = "i128_blob")]
    #[test]
    fn test_i128() {
        use crate::{Connection, NO_PARAMS};
        use std::i128;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (i128 BLOB, desc TEXT)")
            .unwrap();
        db.execute(
            "
            INSERT INTO foo(i128, desc) VALUES
                (?, 'zero'),
                (?, 'neg one'), (?, 'neg two'),
                (?, 'pos one'), (?, 'pos two'),
                (?, 'min'), (?, 'max')",
            &[0i128, -1i128, -2i128, 1i128, 2i128, i128::MIN, i128::MAX],
        )
        .unwrap();

        let mut stmt = db
            .prepare("SELECT i128, desc FROM foo ORDER BY i128 ASC")
            .unwrap();

        let res = stmt
            .query_map(NO_PARAMS, |row| {
                Ok((row.get::<_, i128>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            res,
            &[
                (i128::MIN, "min".to_owned()),
                (-2, "neg two".to_owned()),
                (-1, "neg one".to_owned()),
                (0, "zero".to_owned()),
                (1, "pos one".to_owned()),
                (2, "pos two".to_owned()),
                (i128::MAX, "max".to_owned()),
            ]
        );
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid() {
        use crate::{params, Connection};
        use uuid::Uuid;

        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (id BLOB CHECK(length(id) = 16), label TEXT);")
            .unwrap();

        let id = Uuid::new_v4();

        db.execute(
            "INSERT INTO foo (id, label) VALUES (?, ?)",
            params![id, "target"],
        )
        .unwrap();

        let mut stmt = db
            .prepare("SELECT id, label FROM foo WHERE id = ?")
            .unwrap();

        let mut rows = stmt.query(params![id]).unwrap();
        let row = rows.next().unwrap().unwrap();

        let found_id: Uuid = row.get_unwrap(0);
        let found_label: String = row.get_unwrap(1);

        assert_eq!(found_id, id);
        assert_eq!(found_label, "target");
    }
}
