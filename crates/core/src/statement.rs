use crate::{Error, Result, Rows};

/// A prepared statement.
pub struct Statement {
    raw: *mut libsql_sys::sqlite3_stmt,
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Statement> {
        let mut stmt = std::ptr::null_mut();
        let mut tail = std::ptr::null();
        let err = unsafe {
            libsql_sys::sqlite3_prepare_v2(
                raw,
                sql.as_ptr() as *const i8,
                sql.len() as i32,
                &mut stmt,
                &mut tail,
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => Ok(Statement { raw: stmt }),
            _ => Err(Error::QueryFailed(sql.to_owned())),
        }
    }

    pub fn execute(&self) -> Result<Rows> {
        let err = unsafe { libsql_sys::sqlite3_reset(self.raw) };
        assert_eq!(err as u32, libsql_sys::SQLITE_OK);
        loop {
            let err = unsafe { libsql_sys::sqlite3_step(self.raw) };
            match err as u32 {
                libsql_sys::SQLITE_ROW => continue,
                libsql_sys::SQLITE_DONE => return Ok(Rows {}),
                _ => todo!("sqlite3_step() returned {}", err),
            };
        }
    }
}
