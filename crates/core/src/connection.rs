use crate::{Database, Error, Result, Rows, RowsFuture, Statement};

use std::ffi::c_int;

/// A connection to a libSQL database.
pub struct Connection {
    pub(crate) raw: *mut libsql_sys::sqlite3,
}

unsafe impl Send for Connection {} // TODO: is this safe?

impl Connection {
    /// Connect to the database.
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let url = db.url.clone();
        let err = unsafe {
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

    /// Disconnect from the database.
    pub fn disconnect(&self) {
        unsafe {
            libsql_sys::sqlite3_close_v2(self.raw);
        }
    }

    /// Prepare the SQL statement.
    pub fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        Statement::prepare(self.raw, sql.into().as_str())
    }

    /// Execute the SQL statement synchronously.
    ///
    /// If you execute a SQL query statement (e.g. `SELECT` statement) that
    /// returns rows, then this method returns `Some(Rows)`on success; otherwise
    /// this method returns `None`.
    ///
    /// This method blocks the thread until the SQL statement is executed.
    /// However, for SQL query statements, the method blocks only until the
    /// first row is available. To fetch all rows, you need to call `Rows::next()`
    /// consecutively.
    pub fn execute<S: Into<String>>(&self, sql: S) -> Result<Option<Rows>> {
        let stmt = Statement::prepare(self.raw, sql.into().as_str())?;
        Ok(stmt.execute())
    }

    /// Execute the SQL statement synchronously.
    ///
    /// This method never blocks the thread until, but instead returns a
    /// `RowsFuture` object immediately that can be used to deferredly
    /// execute the statement.
    pub fn execute_async<S: Into<String>>(&self, sql: S) -> RowsFuture {
        RowsFuture {
            raw: self.raw,
            sql: sql.into(),
        }
    }
}
