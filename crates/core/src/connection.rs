use crate::{Database, Error, Result, RowsFuture, Statement};

use std::ffi::c_int;

/// A connection to a libSQL database.
pub struct Connection {
    pub(crate) raw: *mut libsql_sys::sqlite3,
}

unsafe impl Send for Connection {} // TODO: is this safe?

impl Connection {
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let url = db.url.clone();
        let err = unsafe {
            // FIXME: switch to libsql_sys
            libsql_sys::sqlite3_open_v2(
                url.as_ptr() as *const i8,
                &mut raw,
                libsql_sys::SQLITE_OPEN_READWRITE as c_int
                    | libsql_sys::SQLITE_OPEN_CREATE as c_int,
                std::ptr::null(),
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => {}
            _ => {
                return Err(Error::ConnectionFailed(url));
            }
        }
        Ok(Connection { raw })
    }

    pub fn disconnect(&self) {
        unsafe {
            libsql_sys::sqlite3_close_v2(self.raw);
        }
    }

    pub fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        Statement::prepare(self.raw, sql.into().as_str())
    }

    pub fn execute<S: Into<String>>(&self, sql: S) -> Result<()> {
        let stmt = Statement::prepare(self.raw, sql.into().as_str())?;
        let rows = stmt.execute()?;
        loop {
            match rows.next()? {
                Some(_) => {}
                None => break,
            }
        }
        Ok(())
    }

    pub fn execute_async<S: Into<String>>(&self, sql: S) -> RowsFuture {
        RowsFuture {
            raw: self.raw,
            sql: sql.into(),
        }
    }
}
