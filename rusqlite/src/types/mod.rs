//! Traits dealing with SQLite data types.
//!
//! SQLite uses a [dynamic type system](https://www.sqlite.org/datatype3.html). Implementations of
//! the `ToSql` and `FromSql` traits are provided for the basic types that SQLite provides methods
//! for:
//!
//! * C integers and doubles (`c_int` and `c_double`)
//! * Strings (`String` and `&str`)
//! * Blobs (`Vec<u8>` and `&[u8]`)
//!
//! Additionally, because it is such a common data type, implementations are provided for
//! `time::Timespec` that use a string for storage (using the same format string,
//! `"%Y-%m-%d %H:%M:%S"`, as SQLite's builtin
//! [datetime](https://www.sqlite.org/lang_datefunc.html) function.  Note that this storage
//! truncates timespecs to the nearest second. If you want different storage for timespecs, you can
//! use a newtype. For example, to store timespecs as doubles:
//!
//! `ToSql` and `FromSql` are also implemented for `Option<T>` where `T` implements `ToSql` or
//! `FromSql` for the cases where you want to know if a value was NULL (which gets translated to
//! `None`). If you get a value that was NULL in SQLite but you store it into a non-`Option` value
//! in Rust, you will get a "sensible" zero value - 0 for numeric types (including timespecs), an
//! empty string, or an empty vector of bytes.
//!
//! ```rust,ignore
//! extern crate rusqlite;
//! extern crate libc;
//!
//! use rusqlite::types::{FromSql, ToSql, sqlite3_stmt};
//! use rusqlite::{Result};
//! use libc::c_int;
//! use time;
//!
//! pub struct TimespecSql(pub time::Timespec);
//!
//! impl FromSql for TimespecSql {
//!     unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int)
//!             -> Result<TimespecSql> {
//!         let as_f64_result = FromSql::column_result(stmt, col);
//!         as_f64_result.map(|as_f64: f64| {
//!             TimespecSql(time::Timespec{ sec: as_f64.trunc() as i64,
//!                                         nsec: (as_f64.fract() * 1.0e9) as i32 })
//!         })
//!     }
//! }
//!
//! impl ToSql for TimespecSql {
//!     unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
//!         let TimespecSql(ts) = *self;
//!         let as_f64 = ts.sec as f64 + (ts.nsec as f64) / 1.0e9;
//!         as_f64.bind_parameter(stmt, col)
//!     }
//! }
//! ```

pub use ffi::sqlite3_stmt;
pub use ffi::sqlite3_column_type;
pub use ffi::{SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB, SQLITE_NULL};

pub use self::from_sql::FromSql;
pub use self::to_sql::ToSql;

mod from_sql;
mod to_sql;
mod time;
#[cfg(feature = "chrono")]
mod chrono;
#[cfg(feature = "serde_json")]
mod serde_json;

/// Empty struct that can be used to fill in a query parameter as `NULL`.
///
/// ## Example
///
/// ```rust,no_run
/// # extern crate libc;
/// # extern crate rusqlite;
/// # use rusqlite::{Connection, Result};
/// # use rusqlite::types::{Null};
/// # use libc::{c_int};
/// fn main() {
/// }
/// fn insert_null(conn: &Connection) -> Result<c_int> {
///     conn.execute("INSERT INTO people (name) VALUES (?)", &[&Null])
/// }
/// ```
#[derive(Copy,Clone)]
pub struct Null;

/// Dynamic type value (http://sqlite.org/datatype3.html)
/// Value's type is dictated by SQLite (not by the caller).
#[derive(Clone,Debug,PartialEq)]
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

#[cfg(test)]
#[cfg_attr(feature="clippy", allow(similar_names))]
mod test {
    extern crate time;

    use Connection;
    use Error;
    use libc::{c_int, c_double};
    use std::f64::EPSILON;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (b BLOB, t TEXT, i INTEGER, f FLOAT, n)").unwrap();
        db
    }

    #[test]
    fn test_blob() {
        let db = checked_memory_handle();

        let v1234 = vec![1u8, 2, 3, 4];
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&v1234]).unwrap();

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(v, v1234);
    }

    #[test]
    fn test_str() {
        let db = checked_memory_handle();

        let s = "hello, world!";
        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s.to_owned()]).unwrap();

        let from: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(from, s);
    }

    #[test]
    fn test_option() {
        let db = checked_memory_handle();

        let s = Some("hello, world!");
        let b = Some(vec![1u8, 2, 3, 4]);

        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s]).unwrap();
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&b]).unwrap();

        let mut stmt = db.prepare("SELECT t, b FROM foo ORDER BY ROWID ASC").unwrap();
        let mut rows = stmt.query(&[]).unwrap();

        {
            let row1 = rows.next().unwrap().unwrap();
            let s1: Option<String> = row1.get(0);
            let b1: Option<Vec<u8>> = row1.get(1);
            assert_eq!(s.unwrap(), s1.unwrap());
            assert!(b1.is_none());
        }

        {
            let row2 = rows.next().unwrap().unwrap();
            let s2: Option<String> = row2.get(0);
            let b2: Option<Vec<u8>> = row2.get(1);
            assert!(s2.is_none());
            assert_eq!(b, b2);
        }
    }

    #[test]
    #[cfg_attr(feature="clippy", allow(cyclomatic_complexity))]
    fn test_mismatched_types() {
        fn is_invalid_column_type(err: Error) -> bool {
            match err {
                Error::InvalidColumnType => true,
                _ => false,
            }
        }

        let db = checked_memory_handle();

        db.execute("INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)",
                     &[])
            .unwrap();

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo").unwrap();
        let mut rows = stmt.query(&[]).unwrap();

        let row = rows.next().unwrap().unwrap();

        // check the correct types come back as expected
        assert_eq!(vec![1, 2], row.get_checked::<i32, Vec<u8>>(0).unwrap());
        assert_eq!("text", row.get_checked::<i32, String>(1).unwrap());
        assert_eq!(1, row.get_checked::<i32, c_int>(2).unwrap());
        assert!((1.5 - row.get_checked::<i32, c_double>(3).unwrap()).abs() < EPSILON);
        assert!(row.get_checked::<i32, Option<c_int>>(4).unwrap().is_none());
        assert!(row.get_checked::<i32, Option<c_double>>(4).unwrap().is_none());
        assert!(row.get_checked::<i32, Option<String>>(4).unwrap().is_none());

        // check some invalid types

        // 0 is actually a blob (Vec<u8>)
        assert!(is_invalid_column_type(row.get_checked::<i32, c_int>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, c_int>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, i64>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, c_double>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, String>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, time::Timespec>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Option<c_int>>(0).err().unwrap()));

        // 1 is actually a text (String)
        assert!(is_invalid_column_type(row.get_checked::<i32, c_int>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, i64>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, c_double>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Vec<u8>>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Option<c_int>>(1).err().unwrap()));

        // 2 is actually an integer
        assert!(is_invalid_column_type(row.get_checked::<i32, c_double>(2).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, String>(2).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Vec<u8>>(2).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Option<c_double>>(2).err().unwrap()));

        // 3 is actually a float (c_double)
        assert!(is_invalid_column_type(row.get_checked::<i32, c_int>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, i64>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, String>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Vec<u8>>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Option<c_int>>(3).err().unwrap()));

        // 4 is actually NULL
        assert!(is_invalid_column_type(row.get_checked::<i32, c_int>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, i64>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, c_double>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, String>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, Vec<u8>>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get_checked::<i32, time::Timespec>(4).err().unwrap()));
    }

    #[test]
    fn test_dynamic_type() {
        use super::Value;
        let db = checked_memory_handle();

        db.execute("INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)",
                     &[])
            .unwrap();

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo").unwrap();
        let mut rows = stmt.query(&[]).unwrap();

        let row = rows.next().unwrap().unwrap();
        assert_eq!(Value::Blob(vec![1, 2]),
                   row.get_checked::<i32, Value>(0).unwrap());
        assert_eq!(Value::Text(String::from("text")),
                   row.get_checked::<i32, Value>(1).unwrap());
        assert_eq!(Value::Integer(1), row.get_checked::<i32, Value>(2).unwrap());
        match row.get_checked::<i32, Value>(3).unwrap() {
            Value::Real(val) => assert!((1.5 - val).abs() < EPSILON),
            x => panic!("Invalid Value {:?}", x),
        }
        assert_eq!(Value::Null, row.get_checked::<i32, Value>(4).unwrap());
    }
}
