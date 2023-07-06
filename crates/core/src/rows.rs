use std::cell::RefCell;

use crate::{errors, Error, Params, Result, Statement};

/// Query result rows.
pub struct Rows {
    pub(crate) raw: *mut libsql_sys::ffi::sqlite3,
    pub(crate) raw_stmt: *mut libsql_sys::ffi::sqlite3_stmt,
    err: RefCell<Option<i32>>,
}

unsafe impl Send for Rows {} // TODO: is this safe?

impl Rows {
    pub fn execute(
        raw: *mut libsql_sys::ffi::sqlite3,
        raw_stmt: *mut libsql_sys::ffi::sqlite3_stmt,
    ) -> Option<Rows> {
        let err = unsafe { libsql_sys::ffi::sqlite3_step(raw_stmt) };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => None,
            libsql_sys::ffi::SQLITE_DONE => None,
            _ => {
                Some(Rows {
                    raw,
                    raw_stmt,
                    err: RefCell::new(Some(err)),
                })
            }
        }
    }

    pub fn next(&self) -> Result<Option<Row>> {
        let err = match self.err.take() {
            Some(err) => err,
            None => unsafe { libsql_sys::ffi::sqlite3_step(self.raw_stmt) },
        };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => Ok(None),
            libsql_sys::ffi::SQLITE_DONE => Ok(None),
            libsql_sys::ffi::SQLITE_ROW => Ok(Some(Row { raw: self.raw_stmt })),
            _ => Err(Error::QueryFailed(format!(
                "Failed to fetch next row: {}",
                errors::sqlite_error_message(self.raw)
            ))),
        }
    }

    pub fn column_count(&self) -> i32 {
        unsafe { libsql_sys::ffi::sqlite3_column_count(self.raw_stmt) }
    }
}

pub struct RowsFuture {
    pub(crate) raw: *mut libsql_sys::ffi::sqlite3,
    pub(crate) sql: String,
    pub(crate) params: Params,
}

impl RowsFuture {
    pub fn wait(&mut self) -> Result<Option<Rows>> {
        futures::executor::block_on(self)
    }
}

impl futures::Future for RowsFuture {
    type Output = Result<Option<Rows>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let stmt = Statement::prepare(self.raw, &self.sql)?;
        let ret = stmt.execute(&self.params);
        std::task::Poll::Ready(Ok(ret))
    }
}

pub struct Row {
    pub(crate) raw: *mut libsql_sys::ffi::sqlite3_stmt,
}

impl Row {
    pub fn get<T>(&self, idx: i32) -> Result<T>
    where
        T: FromValue,
    {
        let val = unsafe { libsql_sys::ffi::sqlite3_column_value(self.raw, idx) };
        T::from_sql(val)
    }

    pub fn column_type(&self, idx: i32) -> Result<ValueType> {
        let val = unsafe { libsql_sys::ffi::sqlite3_column_type(self.raw, idx) };
        match val as u32 {
            libsql_sys::ffi::SQLITE_INTEGER => Ok(ValueType::Integer),
            libsql_sys::ffi::SQLITE_FLOAT => Ok(ValueType::Float),
            libsql_sys::ffi::SQLITE_BLOB => Ok(ValueType::Blob),
            libsql_sys::ffi::SQLITE_TEXT => Ok(ValueType::Text),
            libsql_sys::ffi::SQLITE_NULL => Ok(ValueType::Null),
            _ => Err(Error::UnknownColumnType(idx, val)),
        }
    }
}

pub enum ValueType {
    Integer,
    Float,
    Blob,
    Text,
    Null,
}

pub trait FromValue {
    fn from_sql(val: *mut libsql_sys::ffi::sqlite3_value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for i32 {
    fn from_sql(val: *mut libsql_sys::ffi::sqlite3_value) -> Result<Self> {
        let ret = unsafe { libsql_sys::ffi::sqlite3_value_int(val) };
        Ok(ret)
    }
}

impl FromValue for &str {
    fn from_sql(val: *mut libsql_sys::ffi::sqlite3_value) -> Result<Self> {
        let ret = unsafe { libsql_sys::ffi::sqlite3_value_text(val) };
        let ret = unsafe { std::ffi::CStr::from_ptr(ret as *const i8) };
        let ret = ret.to_str().unwrap();
        Ok(ret)
    }
}
