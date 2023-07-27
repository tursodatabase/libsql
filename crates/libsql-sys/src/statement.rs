#![allow(clippy::missing_safety_doc)]

use crate::error::Result;

#[derive(Debug)]
pub struct Statement {
    pub raw_stmt: *mut crate::ffi::sqlite3_stmt,
}

// Safety: works as long as libSQL is compiled and set up with SERIALIZABLE threading model, which is the default.
unsafe impl Sync for Statement {}

// Safety: works as long as libSQL is compiled and set up with SERIALIZABLE threading model, which is the default.
unsafe impl Send for Statement {}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.raw_stmt.is_null() {
            unsafe {
                crate::ffi::sqlite3_finalize(self.raw_stmt);
            }
        }
    }
}

impl Statement {
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

    pub fn bind_text(&self, idx: i32, value: &str) {
        unsafe {
            let value = value.as_bytes();
            crate::ffi::sqlite3_bind_text(
                self.raw_stmt,
                idx,
                value.as_ptr() as *const i8,
                value.len() as i32,
                None,
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
                None,
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

    pub fn column_name(&self, idx: i32) -> &str {
        let raw_name = unsafe { crate::ffi::sqlite3_column_name(self.raw_stmt, idx) };
        let raw_name = unsafe { std::ffi::CStr::from_ptr(raw_name as *const i8) };
        let raw_name = raw_name.to_str().unwrap();
        raw_name
    }

    pub fn bind_parameter_index(&self, name: &str) -> i32 {
        let raw_name = std::ffi::CString::new(name).unwrap();

        unsafe { crate::ffi::sqlite3_bind_parameter_index(self.raw_stmt, raw_name.as_ptr()) }
    }

    pub fn get_status(&self, status: i32) -> i32 {
        unsafe { crate::ffi::sqlite3_stmt_status(self.raw_stmt, status as i32, 0) }
    }
}

pub unsafe fn prepare_stmt(raw: *mut crate::ffi::sqlite3, sql: &str) -> Result<Statement> {
    let mut raw_stmt = std::ptr::null_mut();
    let err = unsafe {
        crate::ffi::sqlite3_prepare_v2(
            raw,
            sql.as_ptr() as *const i8,
            sql.len() as i32,
            &mut raw_stmt,
            std::ptr::null_mut(),
        )
    };
    match err as u32 {
        crate::ffi::SQLITE_OK => Ok(Statement { raw_stmt }),
        _ => Err(err.into()),
    }
}
