extern crate time;

use libc::{c_int, c_double};
use std::c_str::{CString};
use std::mem;
use std::vec;
use super::ffi;
use super::{SqliteResult, SqliteError};

const SQLITE_DATETIME_FMT: &'static str = "%Y-%m-%d %H:%M:%S";

pub trait ToSql {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int;
}

pub trait FromSql {
    unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt, col: c_int) -> SqliteResult<Self>;
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl ToSql for $t {
            unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
                ffi::$f(stmt, col, *self)
            }
        }
    )
)

raw_to_impl!(c_int, sqlite3_bind_int)
raw_to_impl!(i64, sqlite3_bind_int64)
raw_to_impl!(c_double, sqlite3_bind_double)

impl<'a> ToSql for &'a str {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        self.with_c_str(|c_str| {
            ffi::sqlite3_bind_text(stmt, col, c_str, -1, Some(ffi::SQLITE_TRANSIENT()))
        })
    }
}

impl ToSql for String {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        self.as_slice().bind_parameter(stmt, col)
    }
}

impl<'a> ToSql for &'a [u8] {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        ffi::sqlite3_bind_blob(
            stmt, col, mem::transmute(self.as_ptr()), self.len() as c_int,
            Some(ffi::SQLITE_TRANSIENT()))
    }
}

impl ToSql for Vec<u8> {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        self.as_slice().bind_parameter(stmt, col)
    }
}

impl ToSql for time::Timespec {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        let time_str = time::at_utc(*self).strftime(SQLITE_DATETIME_FMT).unwrap();
        time_str.bind_parameter(stmt, col)
    }
}

impl<T: ToSql> ToSql for Option<T> {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        match *self {
            None => ffi::sqlite3_bind_null(stmt, col),
            Some(ref t) => t.bind_parameter(stmt, col),
        }
    }
}

pub struct Null;

impl ToSql for Null {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        ffi::sqlite3_bind_null(stmt, col)
    }
}

macro_rules! raw_from_impl(
    ($t:ty, $f:ident) => (
        impl FromSql for $t {
            unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt, col: c_int) -> SqliteResult<$t> {
                Ok(ffi::$f(stmt, col))
            }
        }
    )
)

raw_from_impl!(c_int, sqlite3_column_int)
raw_from_impl!(i64, sqlite3_column_int64)
raw_from_impl!(c_double, sqlite3_column_double)

impl FromSql for String {
    unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt, col: c_int) -> SqliteResult<String> {
        let c_text = ffi::sqlite3_column_text(stmt, col);
        if c_text.is_null() {
            Ok("".to_string())
        } else {
            match CString::new(mem::transmute(c_text), false).as_str() {
                Some(s) => Ok(s.to_string()),
                None => Err(SqliteError{ code: ffi::SQLITE_MISMATCH,
                    message: "Could not convert text to UTF-8".to_string() })
            }
        }
    }
}

impl FromSql for Vec<u8> {
    unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt, col: c_int) -> SqliteResult<Vec<u8>> {
        let c_blob = ffi::sqlite3_column_blob(stmt, col);
        let len = ffi::sqlite3_column_bytes(stmt, col);

        assert!(len >= 0); let len = len as uint;

        Ok(vec::raw::from_buf(mem::transmute(c_blob), len))
    }
}

impl FromSql for time::Timespec {
    unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt,
                            col: c_int) -> SqliteResult<time::Timespec> {
        let col_str = FromSql::column_result(stmt, col);
        col_str.and_then(|txt: String| {
            time::strptime(txt.as_slice(), SQLITE_DATETIME_FMT).map(|tm| {
                tm.to_timespec()
            }).map_err(|parse_error| {
                SqliteError{ code: ffi::SQLITE_MISMATCH, message: format!("{}", parse_error) }
            })
        })
    }
}

impl<T: FromSql> FromSql for Option<T> {
    unsafe fn column_result(stmt: *mut ffi::sqlite3_stmt, col: c_int) -> SqliteResult<Option<T>> {
        if ffi::sqlite3_column_type(stmt, col) == ffi::SQLITE_NULL {
            Ok(None)
        } else {
            FromSql::column_result(stmt, col).map(|t| Some(t))
        }
    }
}

#[cfg(test)]
mod test {
    use SqliteConnection;
    use super::time;

    fn checked_memory_handle() -> SqliteConnection {
        let db = SqliteConnection::open(":memory:").unwrap();
        db.execute_batch("CREATE TABLE foo (b BLOB, t TEXT)").unwrap();
        db
    }

    #[test]
    fn test_blob() {
        let db = checked_memory_handle();

        let v1234 = vec![1u8,2,3,4];
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&v1234]).unwrap();

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", [], |r| r.unwrap().get(0));
        assert_eq!(v, v1234);
    }

    #[test]
    fn test_str() {
        let db = checked_memory_handle();

        let s = "hello, world!";
        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s.to_string()]).unwrap();

        let from: String = db.query_row("SELECT t FROM foo", [], |r| r.unwrap().get(0));
        assert_eq!(from.as_slice(), s);
    }

    #[test]
    fn test_timespec() {
        let db = checked_memory_handle();

        let ts = time::Timespec{sec: 10_000, nsec: 0 };
        db.execute("INSERT INTO foo(t) VALUES (?)", &[&ts]).unwrap();

        let from: time::Timespec = db.query_row("SELECT t FROM foo", [], |r| r.unwrap().get(0));
        assert_eq!(from, ts);
    }

    #[test]
    fn test_option() {
        let db = checked_memory_handle();

        let s = Some("hello, world!");
        let b = Some(vec![1u8,2,3,4]);

        db.execute("INSERT INTO foo(t) VALUES (?)", &[&s]).unwrap();
        db.execute("INSERT INTO foo(b) VALUES (?)", &[&b]).unwrap();

        let mut stmt = db.prepare("SELECT t, b FROM foo ORDER BY ROWID ASC").unwrap();
        let mut rows = stmt.query([]).unwrap();

        let row1 = rows.next().unwrap().unwrap();
        let s1: Option<String> = row1.get(0);
        let b1: Option<Vec<u8>> = row1.get(1);
        assert_eq!(s.unwrap(), s1.unwrap().as_slice());
        assert!(b1.is_none());

        let row2 = rows.next().unwrap().unwrap();
        let s2: Option<String> = row2.get(0);
        let b2: Option<Vec<u8>> = row2.get(1);
        assert!(s2.is_none());
        assert_eq!(b, b2);
    }
}
