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

    pub fn prepare<S: Into<String>>(&self, sql: S) -> Result<Statement> {
        Statement::prepare(self.raw, sql.into().as_str())
    }

    pub fn execute<S: Into<String>>(&self, sql: S) -> Result<Rows> {
        Rows::execute(self.raw, sql.into().as_str())
    }

    pub fn execute_async<S: Into<String>>(&self, sql: S) -> RowsFuture {
        RowsFuture {
            raw: self.raw,
            sql: sql.into(),
        }
    }
}

pub struct Statement {
    raw: *mut libsql_sys::sqlite3_stmt,
}

impl Statement {
    fn prepare(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Statement> {
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

pub struct Rows {}

impl Rows {
    fn execute(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Rows> {
        let err = unsafe {
            libsql_sys::sqlite3_exec(
                raw,
                sql.as_ptr() as *const i8,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => Ok(Rows {}),
            _ => Err(Error::QueryFailed(sql.to_owned())),
        }
    }

    pub fn row_count(&self) -> i32 {
        0
    }

    pub fn column_count(&self) -> i32 {
        0
    }
}

pub struct RowsFuture {
    raw: *mut libsql_sys::sqlite3,
    sql: String,
}

impl RowsFuture {
    pub fn wait(&mut self) -> Result<Rows> {
        futures::executor::block_on(self)
    }
}

impl futures::Future for RowsFuture {
    type Output = Result<Rows>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let ret = Rows::execute(self.raw, &self.sql);
        std::task::Poll::Ready(ret)
    }
}
