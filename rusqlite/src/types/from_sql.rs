use std::ffi::CStr;
use std::mem;
use std::str;

use libc::{c_char, c_double, c_int};

use super::Value;
use ffi::{sqlite3_stmt, sqlite3_column_type};
use ::{ffi, Result};
use ::error::Error;

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
