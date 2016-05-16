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

use libc::{c_int, c_double, c_char};
use std::ffi::CStr;
use std::mem;
use std::str;
use super::ffi;
use super::{Result, Error, str_to_cstring};

pub use ffi::sqlite3_stmt;
pub use ffi::sqlite3_column_type;

pub use ffi::{SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB, SQLITE_NULL};

mod time;
#[cfg(feature = "chrono")]
mod chrono;
#[cfg(feature = "serde_json")]
mod serde_json;

/// A trait for types that can be converted into SQLite values.
pub trait ToSql {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int;
}

/// A trait for types that can be created from a SQLite value.
pub trait FromSql: Sized {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<Self>;

    /// FromSql types can implement this method and use sqlite3_column_type to check that
    /// the type reported by SQLite matches a type suitable for Self. This method is used
    /// by `Row::get_checked` to confirm that the column contains a valid type before
    /// attempting to retrieve the value.
    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true
    }
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl ToSql for $t {
            unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
                ffi::$f(stmt, col, *self)
            }
        }
    )
);

raw_to_impl!(c_int, sqlite3_bind_int); // i32
raw_to_impl!(i64, sqlite3_bind_int64);
raw_to_impl!(c_double, sqlite3_bind_double);

impl ToSql for bool {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        if *self {
            ffi::sqlite3_bind_int(stmt, col, 1)
        } else {
            ffi::sqlite3_bind_int(stmt, col, 0)
        }
    }
}

impl<'a> ToSql for &'a str {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let length = self.len();
        if length > ::std::i32::MAX as usize {
            return ffi::SQLITE_TOOBIG;
        }
        match str_to_cstring(self) {
            Ok(c_str) => {
                ffi::sqlite3_bind_text(stmt,
                                       col,
                                       c_str.as_ptr(),
                                       length as c_int,
                                       ffi::SQLITE_TRANSIENT())
            }
            Err(_) => ffi::SQLITE_MISUSE,
        }
    }
}

impl ToSql for String {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        (&self[..]).bind_parameter(stmt, col)
    }
}

impl<'a> ToSql for &'a [u8] {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        if self.len() > ::std::i32::MAX as usize {
            return ffi::SQLITE_TOOBIG;
        }
        ffi::sqlite3_bind_blob(stmt,
                               col,
                               mem::transmute(self.as_ptr()),
                               self.len() as c_int,
                               ffi::SQLITE_TRANSIENT())
    }
}

impl ToSql for Vec<u8> {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        (&self[..]).bind_parameter(stmt, col)
    }
}

impl<T: ToSql> ToSql for Option<T> {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        match *self {
            None => ffi::sqlite3_bind_null(stmt, col),
            Some(ref t) => t.bind_parameter(stmt, col),
        }
    }
}

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

impl ToSql for Null {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        ffi::sqlite3_bind_null(stmt, col)
    }
}

macro_rules! raw_from_impl(
    ($t:ty, $f:ident, $c:expr) => (
        impl FromSql for $t {
            unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<$t> {
                Ok(ffi::$f(stmt, col))
            }

            unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
                sqlite3_column_type(stmt, col) == $c
            }
        }
    )
);

raw_from_impl!(c_int, sqlite3_column_int, ffi::SQLITE_INTEGER); // i32
raw_from_impl!(i64, sqlite3_column_int64, ffi::SQLITE_INTEGER);
raw_from_impl!(c_double, sqlite3_column_double, ffi::SQLITE_FLOAT); // f64

impl FromSql for bool {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<bool> {
        match ffi::sqlite3_column_int(stmt, col) {
            0 => Ok(false),
            _ => Ok(true),
        }
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
        sqlite3_column_type(stmt, col) == ffi::SQLITE_INTEGER
    }
}

impl FromSql for String {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<String> {
        let c_text = ffi::sqlite3_column_text(stmt, col);
        if c_text.is_null() {
            Ok("".to_owned())
        } else {
            let c_slice = CStr::from_ptr(c_text as *const c_char).to_bytes();
            let utf8_str = try!(str::from_utf8(c_slice));
            Ok(utf8_str.into())
        }
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
        sqlite3_column_type(stmt, col) == ffi::SQLITE_TEXT
    }
}

impl FromSql for Vec<u8> {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<Vec<u8>> {
        use std::slice::from_raw_parts;
        let c_blob = ffi::sqlite3_column_blob(stmt, col);
        let len = ffi::sqlite3_column_bytes(stmt, col);

        // The documentation for sqlite3_column_bytes indicates it is always non-negative,
        // but we should assert here just to be sure.
        assert!(len >= 0,
                "unexpected negative return from sqlite3_column_bytes");
        let len = len as usize;

        Ok(from_raw_parts(mem::transmute(c_blob), len).to_vec())
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
        sqlite3_column_type(stmt, col) == ffi::SQLITE_BLOB
    }
}

impl<T: FromSql> FromSql for Option<T> {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<Option<T>> {
        if sqlite3_column_type(stmt, col) == ffi::SQLITE_NULL {
            Ok(None)
        } else {
            FromSql::column_result(stmt, col).map(Some)
        }
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
        sqlite3_column_type(stmt, col) == ffi::SQLITE_NULL ||
        T::column_has_valid_sqlite_type(stmt, col)
    }
}

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

impl FromSql for Value {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<Value> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => FromSql::column_result(stmt, col).map(Value::Text),
            ffi::SQLITE_INTEGER => Ok(Value::Integer(ffi::sqlite3_column_int64(stmt, col))),
            ffi::SQLITE_FLOAT => Ok(Value::Real(ffi::sqlite3_column_double(stmt, col))),
            ffi::SQLITE_NULL => Ok(Value::Null),
            ffi::SQLITE_BLOB => FromSql::column_result(stmt, col).map(Value::Blob),
            _ => Err(Error::InvalidColumnType),
        }
    }
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

        let row1 = rows.next().unwrap().unwrap();
        let s1: Option<String> = row1.get(0);
        let b1: Option<Vec<u8>> = row1.get(1);
        assert_eq!(s.unwrap(), s1.unwrap());
        assert!(b1.is_none());

        let row2 = rows.next().unwrap().unwrap();
        let s2: Option<String> = row2.get(0);
        let b2: Option<Vec<u8>> = row2.get(1);
        assert!(s2.is_none());
        assert_eq!(b, b2);
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
