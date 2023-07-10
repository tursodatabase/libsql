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
