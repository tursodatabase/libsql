use crate::{errors, Error, Result, Statement};

/// Query result rows.
pub struct Rows {
    pub(crate) raw: *mut libsql_sys::sqlite3,
    pub(crate) raw_stmt: *mut libsql_sys::sqlite3_stmt,
}

impl Rows {
    pub fn next(&self) -> Result<Option<Row>> {
        let err = unsafe { libsql_sys::sqlite3_step(self.raw_stmt) };
        match err as u32 {
            libsql_sys::SQLITE_ROW => Ok(Some(Row { raw: self.raw_stmt })),
            libsql_sys::SQLITE_DONE => Ok(None),
            libsql_sys::SQLITE_OK => Ok(None),
            _ => Err(Error::QueryFailed(format!(
                "Failed to fetch next row: {}",
                errors::sqlite_error_message(self.raw)
            ))),
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
        let stmt = Statement::prepare(self.raw, &self.sql)?;
        let ret = stmt.execute();
        std::task::Poll::Ready(ret)
    }
}

pub struct Row {
    pub(crate) raw: *mut libsql_sys::sqlite3_stmt,
}

impl Row {
    pub fn get<T>(&self, idx: i32) -> Result<T>
    where
        T: FromValue,
    {
        let val = unsafe { libsql_sys::sqlite3_column_value(self.raw, idx) };
        T::from_sql(val)
    }
}

pub trait FromValue {
    fn from_sql(val: *mut libsql_sys::sqlite3_value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for i32 {
    fn from_sql(val: *mut libsql_sys::sqlite3_value) -> Result<Self> {
        let ret = unsafe { libsql_sys::sqlite3_value_int(val) };
        Ok(ret)
    }
}
