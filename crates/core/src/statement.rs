use crate::{errors, Error, Result, Rows};

/// A prepared statement.
pub struct Statement {
    raw: *mut libsql_sys::sqlite3,
    raw_stmt: *mut libsql_sys::sqlite3_stmt,
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Statement> {
        let mut raw_stmt = std::ptr::null_mut();
        let err = unsafe {
            libsql_sys::sqlite3_prepare_v2(
                raw,
                sql.as_ptr() as *const i8,
                sql.len() as i32,
                &mut raw_stmt,
                std::ptr::null_mut(),
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => Ok(Statement { raw, raw_stmt }),
            _ => Err(Error::QueryFailed(format!(
                "Failed to prepare statement: `{}`: {}",
                sql,
                errors::sqlite_error_message(raw),
            ))),
        }
    }

    pub fn execute(&self) -> Option<Rows> {
        unsafe { libsql_sys::sqlite3_reset(self.raw_stmt) };
        Rows::execute(self.raw, self.raw_stmt)
    }
}
