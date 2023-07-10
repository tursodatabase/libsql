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
                        Value::Null => {
                            self.inner.bind_null(i);
                        }
                        Value::Integer(value) => {
                            self.inner.bind_int64(i, *value);
                        }
                        Value::Float(value) => {
                            self.inner.bind_double(i, *value);
                        }
                        Value::Text(value) => {
                            self.inner.bind_text(i, value);
                        }
                        Value::Blob(value) => {
                            self.inner.bind_blob(i, &value[..]);
                        }
                    }
                }
            }
        }
    }

    pub fn execute(&self, params: &Params) -> Option<Rows> {
        self.bind(params);
        let err = self.inner.step();
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
        self.inner.reset();
    }
}
