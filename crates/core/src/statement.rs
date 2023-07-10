use crate::{errors, Error, Params, Result, Rows, Value};

use std::cell::RefCell;
use std::rc::Rc;

/// A prepared statement.
pub struct Statement {
    inner: Rc<StatementInner>,
}

pub(crate) struct StatementInner {
    pub(crate) raw_stmt: *mut libsql_sys::ffi::sqlite3_stmt,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if !self.raw_stmt.is_null() {
            unsafe {
                libsql_sys::ffi::sqlite3_finalize(self.raw_stmt);
            }
        }
    }
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::ffi::sqlite3, sql: &str) -> Result<Statement> {
        let mut raw_stmt = std::ptr::null_mut();
        let err = unsafe {
            libsql_sys::ffi::sqlite3_prepare_v2(
                raw,
                sql.as_ptr() as *const i8,
                sql.len() as i32,
                &mut raw_stmt,
                std::ptr::null_mut(),
            )
        };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => Ok(Statement {
                inner: Rc::new(StatementInner { raw_stmt }),
            }),
            _ => Err(Error::PrepareFailed(
                sql.to_string(),
                errors::sqlite_error_message(raw),
            )),
        }
    }

    pub fn bind(&self, params: &Params) {
        match params {
            Params::None => {}
            Params::Positional(params) => {
                for (i, param) in params.iter().enumerate() {
                    let i = i as i32 + 1;
                    match param {
                        Value::Null => unsafe {
                            libsql_sys::ffi::sqlite3_bind_null(self.inner.raw_stmt, i);
                        },
                        Value::Integer(value) => unsafe {
                            libsql_sys::ffi::sqlite3_bind_int64(self.inner.raw_stmt, i, *value);
                        },
                        Value::Float(value) => unsafe {
                            libsql_sys::ffi::sqlite3_bind_double(self.inner.raw_stmt, i, *value);
                        },
                        Value::Text(value) => unsafe {
                            let value = value.as_bytes();
                            libsql_sys::ffi::sqlite3_bind_text(
                                self.inner.raw_stmt,
                                i,
                                value.as_ptr() as *const i8,
                                value.len() as i32,
                                None,
                            );
                        },
                        Value::Blob(value) => unsafe {
                            libsql_sys::ffi::sqlite3_bind_blob(
                                self.inner.raw_stmt,
                                i,
                                value.as_ptr() as *const std::ffi::c_void,
                                value.len() as i32,
                                None,
                            );
                        },
                    }
                }
            }
        }
    }

    pub fn execute(&self, params: &Params) -> Option<Rows> {
        self.bind(params);
        let err = unsafe { libsql_sys::ffi::sqlite3_step(self.inner.raw_stmt) };
        match err as u32 {
            libsql_sys::ffi::SQLITE_OK => None,
            libsql_sys::ffi::SQLITE_DONE => None,
            _ => Some(Rows {
                stmt: self.inner.clone(),
                err: RefCell::new(Some(err)),
            }),
        }
    }

    /// Reset the prepared statement to initial state for reuse.
    pub fn reset(&self) {
        unsafe { libsql_sys::ffi::sqlite3_reset(self.inner.raw_stmt) };
    }
}
