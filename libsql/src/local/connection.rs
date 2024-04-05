#![allow(dead_code)]

use crate::params::Params;

use super::{Database, Error, Result, Rows, RowsFuture, Statement, Transaction};

use crate::TransactionBehavior;

use libsql_sys::ffi;
use std::{ffi::c_int, fmt, sync::Arc};

/// A connection to a libSQL database.
#[derive(Clone)]
pub struct Connection {
    pub(crate) raw: *mut ffi::sqlite3,

    drop_ref: Arc<()>,

    #[cfg(feature = "replication")]
    pub(crate) writer: Option<crate::replication::Writer>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.disconnect()
    }
}

// SAFETY: This is safe because we compile sqlite3 w/ SQLITE_THREADSAFE=1
unsafe impl Send for Connection {}
// SAFETY: This is safe because we compile sqlite3 w/ SQLITE_THREADSAFE=1
unsafe impl Sync for Connection {}

impl Connection {
    /// Connect to the database.
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let db_path = db.db_path.clone();
        let err = unsafe {
            ffi::sqlite3_open_v2(
                std::ffi::CString::new(db_path.as_str())
                    .unwrap()
                    .as_c_str()
                    .as_ptr() as *const _,
                &mut raw,
                db.flags.bits() as c_int,
                std::ptr::null(),
            )
        };
        match err {
            ffi::SQLITE_OK => {}
            _ => {
                return Err(Error::ConnectionFailed(db_path));
            }
        }

        Ok(Connection {
            raw,
            drop_ref: Arc::new(()),
            #[cfg(feature = "replication")]
            writer: db.writer()?,
        })
    }

    /// Get a raw handle to the underlying libSQL connection
    pub fn handle(&self) -> *mut ffi::sqlite3 {
        self.raw
    }

    /// Create a connection from a raw handle to the underlying libSQL connection
    pub fn from_handle(raw: *mut ffi::sqlite3) -> Self {
        Self {
            raw,
            drop_ref: Arc::new(()),
            #[cfg(feature = "replication")]
            writer: None,
        }
    }

    /// Disconnect from the database.
    pub fn disconnect(&mut self) {
        if Arc::get_mut(&mut self.drop_ref).is_some() {
            unsafe { libsql_sys::ffi::sqlite3_close_v2(self.raw) };
        }
    }

    /// Prepare the SQL statement.
    pub fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        Statement::prepare(self.clone(), self.raw, sql.into().as_str())
    }

    /// Convenience method to run a prepared statement query.
    /// ## Example
    ///
    /// ```rust,no_run,ignore
    /// # use libsql::Result;
    /// # use libsql::v1::{Connection, Rows};
    /// # fn create_tables(conn: &Connection) -> Result<Option<Rows>> {
    /// conn.query("SELECT * FROM users WHERE name = ?1;", vec![libsql::Value::from(1)])
    /// # }
    /// ```
    pub fn query<S, P>(&self, sql: S, params: P) -> Result<Option<Rows>>
    where
        S: Into<String>,
        P: TryInto<Params>,
        P::Error: Into<crate::BoxError>,
    {
        let stmt = Statement::prepare(self.clone(), self.raw, sql.into().as_str())?;
        let params = params
            .try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
        let ret = stmt.query(&params)?;
        Ok(Some(ret))
    }

    /// Convenience method to run multiple SQL statements (that cannot take any
    /// parameters).
    ///
    /// ## Example
    ///
    /// ```rust,no_run,ignore
    /// # use libsql::Result;
    /// # use libsql::v1::Connection;
    /// # fn create_tables(conn: &Connection) -> Result<()> {
    /// conn.execute_batch(
    ///     "BEGIN;
    ///     CREATE TABLE foo(x INTEGER);
    ///     CREATE TABLE bar(y TEXT);
    ///     COMMIT;",
    /// )
    /// # }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute_batch<S>(&self, sql: S) -> Result<()>
    where
        S: Into<String>,
    {
        let sql = sql.into();
        let mut sql = sql.as_str();

        while !sql.is_empty() {
            let stmt = self.prepare(sql)?;

            if !stmt.inner.raw_stmt.is_null() {
                stmt.step()?;
            }

            let tail = stmt.tail();

            if tail == 0 || tail >= sql.len() {
                break;
            }

            sql = &sql[tail..];
        }

        Ok(())
    }

    /// Execute the SQL statement synchronously.
    ///
    /// If you execute a SQL query statement (e.g. `SELECT` statement) that
    /// returns the number of rows changed.
    ///
    /// This method blocks the thread until the SQL statement is executed.
    pub fn execute<S, P>(&self, sql: S, params: P) -> Result<u64>
    where
        S: Into<String>,
        P: TryInto<Params>,
        P::Error: Into<crate::BoxError>,
    {
        let stmt = Statement::prepare(self.clone(), self.raw, sql.into().as_str())?;
        let params = params
            .try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
        stmt.execute(&params)
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
            conn: self.clone(),
            sql: sql.into(),
            params: params.into(),
        }
    }

    /// Begin a new transaction in DEFERRED mode, which is the default.
    pub fn transaction(&self) -> Result<Transaction> {
        self.transaction_with_behavior(TransactionBehavior::Deferred)
    }

    /// Begin a new transaction in the given mode.
    pub fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        Transaction::begin(self.clone(), tx_behavior)
    }

    pub fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.raw) != 0 }
    }

    pub fn changes(&self) -> u64 {
        unsafe { ffi::sqlite3_changes64(self.raw) as u64 }
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.raw) }
    }

    #[cfg(feature = "replication")]
    pub(crate) fn writer(&self) -> Option<&crate::replication::Writer> {
        self.writer.as_ref()
    }

    #[cfg(feature = "replication")]
    pub(crate) fn new_connection_writer(&self) -> Option<crate::replication::Writer> {
        self.writer.as_ref().cloned().map(|mut w| {
            w.new_client_id();
            w
        })
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}
