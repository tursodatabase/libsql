use std::mem;

use libc::{c_double, c_int};

use super::Null;
use ::{ffi, str_to_cstring};
use ffi::sqlite3_stmt;

/// A trait for types that can be converted into SQLite values.
pub trait ToSql {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int;
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

impl ToSql for Null {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        ffi::sqlite3_bind_null(stmt, col)
    }
}
