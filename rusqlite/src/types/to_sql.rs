use super::{Null, Value, ValueRef};
use Result;

/// `ToSqlOutput` represents the possible output types for implementors of the `ToSql` trait.
#[derive(Clone,Debug,PartialEq)]
pub enum ToSqlOutput<'a> {
    /// A borrowed SQLite-representable value.
    Borrowed(ValueRef<'a>),

    /// An owned SQLite-representable value.
    Owned(Value),

    /// A BLOB of the given length that is filled with zeroes.
    #[cfg(feature = "blob")]
    ZeroBlob(i32),
}

impl<'a, T: ?Sized> From<&'a T> for ToSqlOutput<'a>
    where &'a T: Into<ValueRef<'a>>
{
    fn from(t: &'a T) -> Self {
        ToSqlOutput::Borrowed(t.into())
    }
}

impl<'a, T: Into<Value>> From<T> for ToSqlOutput<'a> {
    fn from(t: T) -> Self {
        ToSqlOutput::Owned(t.into())
    }
}

impl<'a> ToSql for ToSqlOutput<'a> {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(match *self {
               ToSqlOutput::Borrowed(v) => ToSqlOutput::Borrowed(v),
               ToSqlOutput::Owned(ref v) => ToSqlOutput::Borrowed(ValueRef::from(v)),

               #[cfg(feature = "blob")]
            ToSqlOutput::ZeroBlob(i) => ToSqlOutput::ZeroBlob(i),
           })
    }
}

/// A trait for types that can be converted into SQLite values.
pub trait ToSql {
    fn to_sql(&self) -> Result<ToSqlOutput>;
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
            fn to_sql(&self) -> Result<ToSqlOutput> {
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

impl<'a, T: ?Sized> ToSql for &'a T
    where &'a T: Into<ToSqlOutput<'a>>
{
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok((*self).into())
    }
}

impl ToSql for String {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.as_str()))
    }
}

impl ToSql for str {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}

impl ToSql for [u8] {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(self))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self) -> Result<ToSqlOutput> {
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
}
