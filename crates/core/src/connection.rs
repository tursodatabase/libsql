use crate::{errors::Error, Database, Result};

use std::ffi::c_int;

pub struct Connection {
    raw: *mut libsql_sys::sqlite3,
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

    pub fn execute<S: Into<String>>(&self, sql: S) -> Result<ResultSet> {
        let rs = ResultSet {
            raw: self.raw,
            sql: sql.into(),
        };
        rs.execute()?;
        Ok(rs)
    }

    pub fn execute_async<S: Into<String>>(&self, sql: S) -> ResultSet {
        ResultSet {
            raw: self.raw,
            sql: sql.into(),
        }
    }
}

pub struct ResultSet {
    raw: *mut libsql_sys::sqlite3,
    sql: String,
}

impl futures::Future for ResultSet {
    type Output = Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let ret = self.execute();
        std::task::Poll::Ready(ret)
    }
}

impl ResultSet {
    fn execute(&self) -> Result<()> {
        let err = unsafe {
            libsql_sys::sqlite3_exec(
                self.raw,
                self.sql.as_ptr() as *const i8,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => Ok(()),
            _ => Err(Error::QueryFailed(self.sql.to_owned())),
        }
    }

    pub fn wait(&mut self) -> Result<()> {
        futures::executor::block_on(self)
    }

    pub fn row_count(&self) -> i32 {
        0
    }

    pub fn column_count(&self) -> i32 {
        0
    }
}
