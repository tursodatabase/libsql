use crate::{Error, Result};

/// Query result rows.
pub struct Rows {}

impl Rows {
    pub(crate) fn execute(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Rows> {
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
    pub(crate) raw: *mut libsql_sys::sqlite3,
    pub(crate) sql: String,
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
