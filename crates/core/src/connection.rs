use crate::{Database, Error, Params, Result, Rows, RowsFuture, Statement};

use std::ffi::c_int;

/// A connection to a libSQL database.
pub struct Connection {
    pub(crate) raw: *mut libsql_sys::ffi::sqlite3,
}

unsafe impl Send for Connection {} // TODO: is this safe?

impl Connection {
    /// Connect to the database.
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let db_path = db.db_path.clone();
        let err = unsafe {
            libsql_sys::ffi::sqlite3_open_v2(
                std::ffi::CString::new(db_path.as_str())
                    .unwrap()
                    .as_c_str()
                    .as_ptr() as *const _,
                &mut raw,
                libsql_sys::ffi::SQLITE_OPEN_READWRITE as c_int
                    | libsql_sys::ffi::SQLITE_OPEN_CREATE as c_int,
                std::ptr::null(),
            )
        };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => {}
            _ => {
                return Err(Error::ConnectionFailed(db_path));
            }
        }
        Ok(Connection { raw })
    }

    /// Disconnect from the database.
    pub fn disconnect(&self) {
        unsafe {
            libsql_sys::ffi::sqlite3_close_v2(self.raw);
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
    pub fn execute<S, P>(&self, sql: S, params: P) -> Result<Option<Rows>>
    where
        S: Into<String>,
        P: Into<Params>,
    {
        let stmt = Statement::prepare(self.raw, sql.into().as_str())?;
        let params = params.into();
        Ok(stmt.execute(&params))
    }

    /// Execute the SQL statement synchronously.
    ///
    /// This method never blocks the thread until, but instead returns a
    /// `RowsFuture` object immediately that can be used to deferredly
    /// execute the statement.
    pub fn execute_async<S, P>(&self, sql: S, params: P) -> RowsFuture
    where
        S: Into<String>,
        P: Into<Params>,
    {
        RowsFuture {
            raw: self.raw,
            sql: sql.into(),
            params: params.into(),
        }
    }
}
