#![allow(clippy::missing_safety_doc)]

pub type Error = i32;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Statement {
    pub(crate) raw_stmt: *mut libsql_sys::ffi::sqlite3_stmt,
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.raw_stmt.is_null() {
            unsafe {
                libsql_sys::ffi::sqlite3_finalize(self.raw_stmt);
            }
        }
    }
}

impl Statement {
    pub fn bind_null(&self, idx: i32) {
        unsafe {
            libsql_sys::ffi::sqlite3_bind_null(self.raw_stmt, idx);
        }
    }

    pub fn bind_int64(&self, idx: i32, value: i64) {
        unsafe {
            libsql_sys::ffi::sqlite3_bind_int64(self.raw_stmt, idx, value);
        }
    }

    pub fn bind_double(&self, idx: i32, value: f64) {
        unsafe {
            libsql_sys::ffi::sqlite3_bind_double(self.raw_stmt, idx, value);
        }
    }

    pub fn bind_text(&self, idx: i32, value: &str) {
        unsafe {
            let value = value.as_bytes();
            libsql_sys::ffi::sqlite3_bind_text(
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
            libsql_sys::ffi::sqlite3_bind_blob(
                self.raw_stmt,
                idx,
                value.as_ptr() as *const std::ffi::c_void,
                value.len() as i32,
                None,
            );
        }
    }
}

pub unsafe fn prepare_stmt(raw: *mut libsql_sys::ffi::sqlite3, sql: &str) -> Result<Statement> {
    let mut raw_stmt = std::ptr::null_mut();
    let err = unsafe {
        libsql_sys::ffi::sqlite3_prepare_v2(
            raw,
            sql.as_ptr() as *const i8,
            sql.len() as i32,
            &mut raw_stmt,
            std::ptr::null_mut(),
        )
    };
    match err as u32 {
        libsql_sys::ffi::SQLITE_OK => Ok(Statement { raw_stmt }),
        _ => Err(err),
    }
}
