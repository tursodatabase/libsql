use crate::{errors, Error, Params, Result, Statement, Value};
use libsql_sys::ValueType;

use std::cell::RefCell;
use std::sync::Arc;

/// Query result rows.
#[derive(Debug)]
pub struct Rows {
    pub(crate) stmt: Arc<libsql_sys::Statement>,
    pub(crate) err: RefCell<Option<i32>>,
}

unsafe impl Send for Rows {} // TODO: is this safe?

impl Rows {
    pub fn next(&self) -> Result<Option<Row>> {
        let err = match self.err.take() {
            Some(err) => err,
            None => self.stmt.step(),
        };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => Ok(None),
            libsql_sys::ffi::SQLITE_DONE => Ok(None),
            libsql_sys::ffi::SQLITE_ROW => Ok(Some(Row {
                stmt: self.stmt.clone(),
            })),
            _ => Err(Error::FetchRowFailed(errors::error_from_code(err))),
        }
    }

    pub fn column_count(&self) -> i32 {
        self.stmt.column_count()
    }

    pub fn column_name(&self, idx: i32) -> &str {
        self.stmt.column_name(idx)
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
    pub(crate) stmt: Arc<libsql_sys::Statement>,
}

impl Row {
    pub fn get<T>(&self, idx: i32) -> Result<T>
    where
        T: FromValue,
    {
        let val = self.stmt.column_value(idx);
        T::from_sql(val)
    }

    pub fn get_value(&self, idx: i32) -> Result<Value> {
        let val = self.stmt.column_value(idx);
        Ok(val.into())
    }

    pub fn column_type(&self, idx: i32) -> Result<ValueType> {
        let val = self.stmt.column_type(idx);
        match val as u32 {
            libsql_sys::ffi::SQLITE_INTEGER => Ok(ValueType::Integer),
            libsql_sys::ffi::SQLITE_FLOAT => Ok(ValueType::Float),
            libsql_sys::ffi::SQLITE_BLOB => Ok(ValueType::Blob),
            libsql_sys::ffi::SQLITE_TEXT => Ok(ValueType::Text),
            libsql_sys::ffi::SQLITE_NULL => Ok(ValueType::Null),
            _ => Err(Error::UnknownColumnType(idx, val)),
        }
    }

    pub fn column_name(&self, idx: i32) -> &str {
        self.stmt.column_name(idx)
    }
}

pub trait FromValue {
    fn from_sql(val: libsql_sys::Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for i32 {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.int();
        Ok(ret)
    }
}

impl FromValue for &str {
    fn from_sql(val: libsql_sys::Value) -> Result<Self> {
        let ret = val.text();
        if ret.is_null() {
            return Err(Error::NullValue);
        }
        let ret = unsafe { std::ffi::CStr::from_ptr(ret as *const i8) };
        let ret = ret.to_str().unwrap();
        Ok(ret)
    }
}
