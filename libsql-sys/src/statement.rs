#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_int};
use std::sync::atomic::AtomicBool;

use crate::error::Result;

#[derive(Debug)]
pub struct Statement {
    pub raw_stmt: *mut crate::ffi::sqlite3_stmt,
    finalized: AtomicBool,
    tail: usize,
}

// Safety: works as long as libSQL is compiled and set up with SERIALIZABLE threading model, which is the default.
unsafe impl Sync for Statement {}

// Safety: works as long as libSQL is compiled and set up with SERIALIZABLE threading model, which is the default.
unsafe impl Send for Statement {}

impl Drop for Statement {
    fn drop(&mut self) {
        self.finalize();
    }
}

impl Statement {
    pub fn finalize(&self) {
        if !self
            .finalized
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            unsafe {
                crate::ffi::sqlite3_finalize(self.raw_stmt);
            }
        }
    }

    pub fn bind_null(&self, idx: i32) {
        unsafe {
            crate::ffi::sqlite3_bind_null(self.raw_stmt, idx);
        }
    }

    pub fn bind_int64(&self, idx: i32, value: i64) {
        unsafe {
            crate::ffi::sqlite3_bind_int64(self.raw_stmt, idx, value);
        }
    }

    pub fn bind_double(&self, idx: i32, value: f64) {
        unsafe {
            crate::ffi::sqlite3_bind_double(self.raw_stmt, idx, value);
        }
    }

    pub fn bind_text(&self, idx: i32, value: &[u8]) {
        unsafe {
            crate::ffi::sqlite3_bind_text(
                self.raw_stmt,
                idx,
                value.as_ptr() as *const c_char,
                value.len() as i32,
                SQLITE_TRANSIENT(),
            );
        }
    }

    pub fn bind_blob(&self, idx: i32, value: &[u8]) {
        unsafe {
            crate::ffi::sqlite3_bind_blob(
                self.raw_stmt,
                idx,
                value.as_ptr() as *const std::ffi::c_void,
                value.len() as i32,
                SQLITE_TRANSIENT(),
            );
        }
    }

    pub fn step(&self) -> std::ffi::c_int {
        unsafe { crate::ffi::sqlite3_step(self.raw_stmt) }
    }

    pub fn reset(&self) -> std::ffi::c_int {
        unsafe { crate::ffi::sqlite3_reset(self.raw_stmt) }
    }

    pub fn column_count(&self) -> i32 {
        unsafe { crate::ffi::sqlite3_column_count(self.raw_stmt) }
    }

    pub fn column_value(&self, idx: i32) -> crate::Value {
        let raw_value = unsafe { crate::ffi::sqlite3_column_value(self.raw_stmt, idx) };
        crate::Value { raw_value }
    }

    pub fn column_type(&self, idx: i32) -> i32 {
        unsafe { crate::ffi::sqlite3_column_type(self.raw_stmt, idx) }
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        let raw_name = unsafe { crate::ffi::sqlite3_column_name(self.raw_stmt, idx) };

        if raw_name.is_null() {
            return None;
        }

        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const c_char) };
        let raw_name = raw_name.to_str().unwrap();
        Some(raw_name)
    }

    pub fn column_origin_name(&self, idx: i32) -> Option<&str> {
        let raw_name = unsafe { crate::ffi::sqlite3_column_origin_name(self.raw_stmt, idx) };

        if raw_name.is_null() {
            return None;
        }

        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const c_char) };

        let raw_name = raw_name.to_str().unwrap();
        Some(raw_name)
    }

    pub fn column_table_name(&self, idx: i32) -> Option<&str> {
        let raw_name = unsafe { crate::ffi::sqlite3_column_table_name(self.raw_stmt, idx) };
        if raw_name.is_null() {
            return None;
        }
        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const c_char) };
        let raw_name = raw_name.to_str().unwrap();
        Some(raw_name)
    }

    pub fn column_database_name(&self, idx: i32) -> Option<&str> {
        let raw_name = unsafe { crate::ffi::sqlite3_column_database_name(self.raw_stmt, idx) };
        if raw_name.is_null() {
            return None;
        }
        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const c_char) };
        let raw_name = raw_name.to_str().unwrap();
        Some(raw_name)
    }

    pub fn column_decltype(&self, idx: i32) -> Option<&str> {
        let raw_name = unsafe { crate::ffi::sqlite3_column_decltype(self.raw_stmt, idx) };
        if raw_name.is_null() {
            return None;
        }
        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const c_char) };
        let raw_name = raw_name.to_str().unwrap();
        Some(raw_name)
    }

    pub fn bind_parameter_index(&self, name: &str) -> i32 {
        let raw_name = std::ffi::CString::new(name).unwrap();

        unsafe { crate::ffi::sqlite3_bind_parameter_index(self.raw_stmt, raw_name.as_ptr()) }
    }

    pub fn bind_parameter_count(&self) -> usize {
        unsafe { crate::ffi::sqlite3_bind_parameter_count(self.raw_stmt) as usize }
    }

    pub fn bind_parameter_name(&self, index: i32) -> Option<&str> {
        unsafe {
            let name = crate::ffi::sqlite3_bind_parameter_name(self.raw_stmt, index);
            if name.is_null() {
                None
            } else {
                // NOTICE: unwrap(), because SQLite promises it's valid UTF-8
                Some(std::ffi::CStr::from_ptr(name).to_str().unwrap())
            }
        }
    }

    pub fn get_status(&self, status: i32) -> i32 {
        unsafe { crate::ffi::sqlite3_stmt_status(self.raw_stmt, status, 0) }
    }

    pub fn is_explain(&self) -> i32 {
        unsafe { crate::ffi::sqlite3_stmt_isexplain(self.raw_stmt) }
    }

    pub fn readonly(&self) -> bool {
        unsafe { crate::ffi::sqlite3_stmt_readonly(self.raw_stmt) != 0 }
    }

    pub fn tail(&self) -> usize {
        self.tail
    }
}

pub unsafe fn prepare_stmt(raw: *mut crate::ffi::sqlite3, sql: &str) -> Result<Statement> {
    let mut raw_stmt = std::ptr::null_mut();
    let (c_sql, len) = str_for_sqlite(sql.as_bytes())?;
    let mut c_tail: *const c_char = std::ptr::null_mut();

    let err =
        unsafe { crate::ffi::sqlite3_prepare_v2(raw, c_sql, len, &mut raw_stmt, &mut c_tail) };

    // If the input text contains no SQL (if the input is an empty string or a
    // comment) then *ppStmt is set to NULL.
    let tail = if c_tail.is_null() {
        0
    } else {
        let n = (c_tail as isize) - (c_sql as isize);
        if n <= 0 || n >= len as isize {
            0
        } else {
            n as usize
        }
    };

    match err {
        crate::ffi::SQLITE_OK => Ok(Statement {
            raw_stmt,
            tail,
            finalized: AtomicBool::new(false),
        }),
        _ => Err(err.into()),
    }
}

/// Returns `Ok((string ptr, len as c_int, SQLITE_STATIC | SQLITE_TRANSIENT))`
/// normally.
/// Returns error if the string is too large for sqlite.
/// The `sqlite3_destructor_type` item is always `SQLITE_TRANSIENT` unless
/// the string was empty (in which case it's `SQLITE_STATIC`, and the ptr is
/// static).
fn str_for_sqlite(s: &[u8]) -> Result<(*const c_char, c_int)> {
    let len = len_as_c_int(s.len())?;
    let ptr = if len != 0 {
        s.as_ptr().cast::<c_char>()
    } else {
        // Return a pointer guaranteed to live forever
        "".as_ptr().cast::<c_char>()
    };
    Ok((ptr, len))
}

// Helper to cast to c_int safely, returning the correct error type if the cast
// failed.
fn len_as_c_int(len: usize) -> Result<c_int> {
    if len >= (c_int::MAX as usize) {
        Err(crate::Error::from(
            libsql_ffi::SQLITE_TOOBIG as std::ffi::c_int,
        ))
    } else {
        Ok(len as c_int)
    }
}

#[must_use]
#[allow(non_snake_case)]
pub fn SQLITE_TRANSIENT() -> crate::ffi::sqlite3_destructor_type {
    Some(unsafe { std::mem::transmute(-1_isize) })
}
