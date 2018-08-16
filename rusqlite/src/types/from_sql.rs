use super::{Value, ValueRef};
use std::error::Error;
use std::fmt;

/// Enum listing possible errors from `FromSql` trait.
#[derive(Debug)]
pub enum FromSqlError {
    /// Error when an SQLite value is requested, but the type of the result
    /// cannot be converted to the requested Rust type.
    InvalidType,

    /// Error when the i64 value returned by SQLite cannot be stored into the
    /// requested type.
    OutOfRange(i64),

    /// An error case available for implementors of the `FromSql` trait.
    Other(Box<Error + Send + Sync>),
}

impl fmt::Display for FromSqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            FromSqlError::InvalidType => write!(f, "Invalid type"),
            FromSqlError::OutOfRange(i) => write!(f, "Value {} out of range", i),
            FromSqlError::Other(ref err) => err.fmt(f),
        }
    }
}

impl Error for FromSqlError {
    fn description(&self) -> &str {
        match *self {
            FromSqlError::InvalidType => "invalid type",
            FromSqlError::OutOfRange(_) => "value out of range",
            FromSqlError::Other(ref err) => err.description(),
        }
    }

    #[cfg_attr(feature = "clippy", allow(match_same_arms))]
    fn cause(&self) -> Option<&Error> {
        match *self {
            FromSqlError::Other(ref err) => err.cause(),
            FromSqlError::InvalidType | FromSqlError::OutOfRange(_) => None,
        }
    }
}

/// Result type for implementors of the `FromSql` trait.
pub type FromSqlResult<T> = Result<T, FromSqlError>;

/// A trait for types that can be created from a SQLite value.
///
/// Note that `FromSql` and `ToSql` are defined for most integral types, but
/// not `u64` or `usize`. This is intentional; SQLite returns integers as
/// signed 64-bit values, which cannot fully represent the range of these
/// types. Rusqlite would have to
/// decide how to handle negative values: return an error or reinterpret as a
/// very large postive numbers, neither of which
/// is guaranteed to be correct for everyone. Callers can work around this by
/// fetching values as i64 and then doing the interpretation themselves or by
/// defining a newtype and implementing `FromSql`/`ToSql` for it.
pub trait FromSql: Sized {
    fn column_result(value: ValueRef) -> FromSqlResult<Self>;
}

impl FromSql for isize {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        i64::column_result(value).and_then(|i| {
            if i < isize::min_value() as i64 || i > isize::max_value() as i64 {
                Err(FromSqlError::OutOfRange(i))
            } else {
                Ok(i as isize)
            }
        })
    }
}

macro_rules! from_sql_integral(
    ($t:ident) => (
        impl FromSql for $t {
            fn column_result(value: ValueRef) -> FromSqlResult<Self> {
                i64::column_result(value).and_then(|i| {
                    if i < i64::from($t::min_value()) || i > i64::from($t::max_value()) {
                        Err(FromSqlError::OutOfRange(i))
                    } else {
                        Ok(i as $t)
                    }
                })
            }
        }
    )
);

from_sql_integral!(i8);
from_sql_integral!(i16);
from_sql_integral!(i32);
from_sql_integral!(u8);
from_sql_integral!(u16);
from_sql_integral!(u32);

impl FromSql for i64 {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_i64()
    }
}

impl FromSql for f64 {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => Ok(i as f64),
            ValueRef::Real(f) => Ok(f),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl FromSql for bool {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        i64::column_result(value).map(|i| match i {
            0 => false,
            _ => true,
        })
    }
}

impl FromSql for String {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromSql for Vec<u8> {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value.as_blob().map(|b| b.to_vec())
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        match value {
            ValueRef::Null => Ok(None),
            _ => FromSql::column_result(value).map(Some),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        Ok(value.into())
    }
}

#[cfg(test)]
mod test {
    use super::FromSql;
    use {Connection, Error};

    fn checked_memory_handle() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn test_integral_ranges() {
        let db = checked_memory_handle();

        fn check_ranges<T>(db: &Connection, out_of_range: &[i64], in_range: &[i64])
        where
            T: Into<i64> + FromSql + ::std::fmt::Debug,
        {
            for n in out_of_range {
                let err = db
                    .query_row("SELECT ?", &[n], |r| r.get_checked::<_, T>(0))
                    .unwrap()
                    .unwrap_err();
                match err {
                    Error::IntegralValueOutOfRange(_, value) => assert_eq!(*n, value),
                    _ => panic!("unexpected error: {}", err),
                }
            }
            for n in in_range {
                assert_eq!(
                    *n,
                    db.query_row("SELECT ?", &[n], |r| r.get::<_, T>(0))
                        .unwrap()
                        .into()
                );
            }
        }

        check_ranges::<i8>(&db, &[-129, 128], &[-128, 0, 1, 127]);
        check_ranges::<i16>(&db, &[-32769, 32768], &[-32768, -1, 0, 1, 32767]);
        check_ranges::<i32>(
            &db,
            &[-2147483649, 2147483648],
            &[-2147483648, -1, 0, 1, 2147483647],
        );
        check_ranges::<u8>(&db, &[-2, -1, 256], &[0, 1, 255]);
        check_ranges::<u16>(&db, &[-2, -1, 65536], &[0, 1, 65535]);
        check_ranges::<u32>(&db, &[-2, -1, 4294967296], &[0, 1, 4294967295]);
    }
}
