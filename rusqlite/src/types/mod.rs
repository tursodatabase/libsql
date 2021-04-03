//! Traits dealing with SQLite data types.
//!
//! SQLite uses a [dynamic type system](https://www.sqlite.org/datatype3.html). Implementations of
//! the [`ToSql`] and [`FromSql`] traits are provided for the basic types that
//! SQLite provides methods for:
//!
//! * Strings (`String` and `&str`)
//! * Blobs (`Vec<u8>` and `&[u8]`)
//! * Numbers
//!
//! The number situation is a little complicated due to the fact that all
//! numbers in SQLite are stored as `INTEGER` (`i64`) or `REAL` (`f64`).
//!
//! [`ToSql`] and [`FromSql`] are implemented for all primitive number types.
//! [`FromSql`] has different behaviour depending on the SQL and Rust types, and
//! the value.
//!
//! * `INTEGER` to integer: returns an
//!   [`Error::IntegralValueOutOfRange`](crate::Error::IntegralValueOutOfRange)
//!   error if the value does not fit in the Rust type.
//! * `REAL` to integer: always returns an
//!   [`Error::InvalidColumnType`](crate::Error::InvalidColumnType) error.
//! * `INTEGER` to float: casts using `as` operator. Never fails.
//! * `REAL` to float: casts using `as` operator. Never fails.
//!
//! [`ToSql`] always succeeds except when storing a `u64` or `usize` value that
//! cannot fit in an `INTEGER` (`i64`). Also note that SQLite ignores column
//! types, so if you store an `i64` in a column with type `REAL` it will be
//! stored as an `INTEGER`, not a `REAL`.
//!
//! If the `time` feature is enabled, implementations are
//! provided for `time::OffsetDateTime` that use the RFC 3339 date/time format,
//! `"%Y-%m-%dT%H:%M:%S.%fZ"`, to store time values as strings.  These values
//! can be parsed by SQLite's builtin
//! [datetime](https://www.sqlite.org/lang_datefunc.html) functions.  If you
//! want different storage for datetimes, you can use a newtype.
#![cfg_attr(
    feature = "time",
    doc = r##"
For example, to store datetimes as `i64`s counting the number of seconds since
the Unix epoch:

```
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::Result;

pub struct DateTimeSql(pub time::OffsetDateTime);

impl FromSql for DateTimeSql {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        i64::column_result(value).map(|as_i64| {
            DateTimeSql(time::OffsetDateTime::from_unix_timestamp(as_i64))
        })
    }
}

impl ToSql for DateTimeSql {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(self.0.timestamp().into())
    }
}
```

"##
)]
//! [`ToSql`] and [`FromSql`] are also implemented for `Option<T>` where `T`
//! implements [`ToSql`] or [`FromSql`] for the cases where you want to know if
//! a value was NULL (which gets translated to `None`).

pub use self::from_sql::{FromSql, FromSqlError, FromSqlResult};
pub use self::to_sql::{ToSql, ToSqlOutput};
pub use self::value::Value;
pub use self::value_ref::ValueRef;

use std::fmt;

#[cfg(feature = "chrono")]
mod chrono;
mod from_sql;
#[cfg(feature = "serde_json")]
mod serde_json;
#[cfg(feature = "time")]
mod time;
mod to_sql;
#[cfg(feature = "url")]
mod url;
mod value;
mod value_ref;

/// Empty struct that can be used to fill in a query parameter as `NULL`.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result};
/// # use rusqlite::types::{Null};
///
/// fn insert_null(conn: &Connection) -> Result<usize> {
///     conn.execute("INSERT INTO people (name) VALUES (?)", [Null])
/// }
/// ```
#[derive(Copy, Clone)]
pub struct Null;

/// SQLite data types.
/// See [Fundamental Datatypes](https://sqlite.org/c3ref/c_blob.html).
#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    /// NULL
    Null,
    /// 64-bit signed integer
    Integer,
    /// 64-bit IEEE floating point number
    Real,
    /// String
    Text,
    /// BLOB
    Blob,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Type::Null => f.pad("Null"),
            Type::Integer => f.pad("Integer"),
            Type::Real => f.pad("Real"),
            Type::Text => f.pad("Text"),
            Type::Blob => f.pad("Blob"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Value;
    use crate::{params, Connection, Error, Result, Statement};
    use std::f64::EPSILON;
    use std::os::raw::{c_double, c_int};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (b BLOB, t TEXT, i INTEGER, f FLOAT, n)")?;
        Ok(db)
    }

    #[test]
    fn test_blob() -> Result<()> {
        let db = checked_memory_handle()?;

        let v1234 = vec![1u8, 2, 3, 4];
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&v1234])?;

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(v, v1234);
        Ok(())
    }

    #[test]
    fn test_empty_blob() -> Result<()> {
        let db = checked_memory_handle()?;

        let empty = vec![];
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&empty])?;

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(v, empty);
        Ok(())
    }

    #[test]
    fn test_str() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = "hello, world!";
        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s])?;

        let from: String = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;
        assert_eq!(from, s);
        Ok(())
    }

    #[test]
    fn test_string() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = "hello, world!";
        db.execute("INSERT INTO foo(t) VALUES (?)", [s.to_owned()])?;

        let from: String = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;
        assert_eq!(from, s);
        Ok(())
    }

    #[test]
    fn test_value() -> Result<()> {
        let db = checked_memory_handle()?;

        db.execute("INSERT INTO foo(i) VALUES (?)", [Value::Integer(10)])?;

        assert_eq!(
            10i64,
            db.query_row::<i64, _, _>("SELECT i FROM foo", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    fn test_option() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = Some("hello, world!");
        let b = Some(vec![1u8, 2, 3, 4]);

        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s])?;
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&b])?;

        let mut stmt = db.prepare("SELECT t, b FROM foo ORDER BY ROWID ASC")?;
        let mut rows = stmt.query([])?;

        {
            let row1 = rows.next()?.unwrap();
            let s1: Option<String> = row1.get_unwrap(0);
            let b1: Option<Vec<u8>> = row1.get_unwrap(1);
            assert_eq!(s.unwrap(), s1.unwrap());
            assert!(b1.is_none());
        }

        {
            let row2 = rows.next()?.unwrap();
            let s2: Option<String> = row2.get_unwrap(0);
            let b2: Option<Vec<u8>> = row2.get_unwrap(1);
            assert!(s2.is_none());
            assert_eq!(b, b2);
        }
        Ok(())
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_mismatched_types() -> Result<()> {
        fn is_invalid_column_type(err: Error) -> bool {
            matches!(err, Error::InvalidColumnType(..))
        }

        let db = checked_memory_handle()?;

        db.execute(
            "INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)",
            [],
        )?;

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo")?;
        let mut rows = stmt.query([])?;

        let row = rows.next()?.unwrap();

        // check the correct types come back as expected
        assert_eq!(vec![1, 2], row.get::<_, Vec<u8>>(0)?);
        assert_eq!("text", row.get::<_, String>(1)?);
        assert_eq!(1, row.get::<_, c_int>(2)?);
        assert!((1.5 - row.get::<_, c_double>(3)?).abs() < EPSILON);
        assert_eq!(row.get::<_, Option<c_int>>(4)?, None);
        assert_eq!(row.get::<_, Option<c_double>>(4)?, None);
        assert_eq!(row.get::<_, Option<String>>(4)?, None);

        // check some invalid types

        // 0 is actually a blob (Vec<u8>)
        assert!(is_invalid_column_type(
            row.get::<_, c_int>(0).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, c_int>(0).err().unwrap()
        ));
        assert!(is_invalid_column_type(row.get::<_, i64>(0).err().unwrap()));
        assert!(is_invalid_column_type(
            row.get::<_, c_double>(0).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, String>(0).err().unwrap()
        ));
        #[cfg(feature = "time")]
        assert!(is_invalid_column_type(
            row.get::<_, time::OffsetDateTime>(0).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Option<c_int>>(0).err().unwrap()
        ));

        // 1 is actually a text (String)
        assert!(is_invalid_column_type(
            row.get::<_, c_int>(1).err().unwrap()
        ));
        assert!(is_invalid_column_type(row.get::<_, i64>(1).err().unwrap()));
        assert!(is_invalid_column_type(
            row.get::<_, c_double>(1).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Vec<u8>>(1).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Option<c_int>>(1).err().unwrap()
        ));

        // 2 is actually an integer
        assert!(is_invalid_column_type(
            row.get::<_, String>(2).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Vec<u8>>(2).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Option<String>>(2).err().unwrap()
        ));

        // 3 is actually a float (c_double)
        assert!(is_invalid_column_type(
            row.get::<_, c_int>(3).err().unwrap()
        ));
        assert!(is_invalid_column_type(row.get::<_, i64>(3).err().unwrap()));
        assert!(is_invalid_column_type(
            row.get::<_, String>(3).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Vec<u8>>(3).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Option<c_int>>(3).err().unwrap()
        ));

        // 4 is actually NULL
        assert!(is_invalid_column_type(
            row.get::<_, c_int>(4).err().unwrap()
        ));
        assert!(is_invalid_column_type(row.get::<_, i64>(4).err().unwrap()));
        assert!(is_invalid_column_type(
            row.get::<_, c_double>(4).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, String>(4).err().unwrap()
        ));
        assert!(is_invalid_column_type(
            row.get::<_, Vec<u8>>(4).err().unwrap()
        ));
        #[cfg(feature = "time")]
        assert!(is_invalid_column_type(
            row.get::<_, time::OffsetDateTime>(4).err().unwrap()
        ));
        Ok(())
    }

    #[test]
    fn test_dynamic_type() -> Result<()> {
        use super::Value;
        let db = checked_memory_handle()?;

        db.execute(
            "INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)",
            [],
        )?;

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo")?;
        let mut rows = stmt.query([])?;

        let row = rows.next()?.unwrap();
        assert_eq!(Value::Blob(vec![1, 2]), row.get::<_, Value>(0)?);
        assert_eq!(Value::Text(String::from("text")), row.get::<_, Value>(1)?);
        assert_eq!(Value::Integer(1), row.get::<_, Value>(2)?);
        match row.get::<_, Value>(3)? {
            Value::Real(val) => assert!((1.5 - val).abs() < EPSILON),
            x => panic!("Invalid Value {:?}", x),
        }
        assert_eq!(Value::Null, row.get::<_, Value>(4)?);
        Ok(())
    }

    macro_rules! test_conversion {
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect $expected_value:expr) => {
            $db_etc.insert_statement.execute(params![$insert_value])?;
            let res = $db_etc
                .query_statement
                .query_row([], |row| row.get::<_, $get_type>(0));
            assert_eq!(res?, $expected_value);
            $db_etc.delete_statement.execute([])?;
        };
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect_from_sql_error) => {
            $db_etc.insert_statement.execute(params![$insert_value])?;
            let res = $db_etc
                .query_statement
                .query_row([], |row| row.get::<_, $get_type>(0));
            res.unwrap_err();
            $db_etc.delete_statement.execute([])?;
        };
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect_to_sql_error) => {
            $db_etc
                .insert_statement
                .execute(params![$insert_value])
                .unwrap_err();
        };
    }

    #[test]
    fn test_numeric_conversions() -> Result<()> {
        #![allow(clippy::float_cmp)]

        // Test what happens when we store an f32 and retrieve an i32 etc.
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (x)")?;

        // SQLite actually ignores the column types, so we just need to test
        // different numeric values.

        struct DbEtc<'conn> {
            insert_statement: Statement<'conn>,
            query_statement: Statement<'conn>,
            delete_statement: Statement<'conn>,
        }

        let mut db_etc = DbEtc {
            insert_statement: db.prepare("INSERT INTO foo VALUES (?1)")?,
            query_statement: db.prepare("SELECT x FROM foo")?,
            delete_statement: db.prepare("DELETE FROM foo")?,
        };

        // Basic non-converting test.
        test_conversion!(db_etc, 0u8, u8, expect 0u8);

        // In-range integral conversions.
        test_conversion!(db_etc, 100u8, i8, expect 100i8);
        test_conversion!(db_etc, 200u8, u8, expect 200u8);
        test_conversion!(db_etc, 100u16, i8, expect 100i8);
        test_conversion!(db_etc, 200u16, u8, expect 200u8);
        test_conversion!(db_etc, u32::MAX, u64, expect u32::MAX as u64);
        test_conversion!(db_etc, i64::MIN, i64, expect i64::MIN);
        test_conversion!(db_etc, i64::MAX, i64, expect i64::MAX);
        test_conversion!(db_etc, i64::MAX, u64, expect i64::MAX as u64);
        test_conversion!(db_etc, 100usize, usize, expect 100usize);
        test_conversion!(db_etc, 100u64, u64, expect 100u64);
        test_conversion!(db_etc, i64::MAX as u64, u64, expect i64::MAX as u64);

        // Out-of-range integral conversions.
        test_conversion!(db_etc, 200u8, i8, expect_from_sql_error);
        test_conversion!(db_etc, 400u16, i8, expect_from_sql_error);
        test_conversion!(db_etc, 400u16, u8, expect_from_sql_error);
        test_conversion!(db_etc, -1i8, u8, expect_from_sql_error);
        test_conversion!(db_etc, i64::MIN, u64, expect_from_sql_error);
        test_conversion!(db_etc, u64::MAX, i64, expect_to_sql_error);
        test_conversion!(db_etc, u64::MAX, u64, expect_to_sql_error);
        test_conversion!(db_etc, i64::MAX as u64 + 1, u64, expect_to_sql_error);

        // FromSql integer to float, always works.
        test_conversion!(db_etc, i64::MIN, f32, expect i64::MIN as f32);
        test_conversion!(db_etc, i64::MAX, f32, expect i64::MAX as f32);
        test_conversion!(db_etc, i64::MIN, f64, expect i64::MIN as f64);
        test_conversion!(db_etc, i64::MAX, f64, expect i64::MAX as f64);

        // FromSql float to int conversion, never works even if the actual value
        // is an integer.
        test_conversion!(db_etc, 0f64, i64, expect_from_sql_error);
        Ok(())
    }
}
