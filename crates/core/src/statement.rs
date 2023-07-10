use crate::{errors, raw, Error, Params, Result, Rows, Value};

use std::cell::RefCell;
use std::rc::Rc;

/// A prepared statement.
pub struct Statement {
    inner: Rc<raw::Statement>,
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::ffi::sqlite3, sql: &str) -> Result<Statement> {
        match unsafe { raw::prepare_stmt(raw, sql) } {
            Ok(stmt) => Ok(Statement {
                inner: Rc::new(stmt),
            }),
            Err(err) => Err(Error::PrepareFailed(
                sql.to_string(),
                errors::sqlite_code_to_error(err),
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
