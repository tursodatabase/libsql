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
