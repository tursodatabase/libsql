use crate::{errors, Error, Params, Result, Rows, Value};

use std::cell::RefCell;
use std::sync::Arc;

/// A prepared statement.
pub struct Statement {
    inner: Arc<libsql_sys::Statement>,
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::ffi::sqlite3, sql: &str) -> Result<Statement> {
        match unsafe { libsql_sys::prepare_stmt(raw, sql) } {
            Ok(stmt) => Ok(Statement {
                inner: Arc::new(stmt),
            }),
            Err(libsql_sys::Error::LibError(_err)) => Err(Error::PrepareFailed(
                sql.to_string(),
                errors::error_from_handle(raw),
            )),
            Err(err) => Err(Error::Misuse(format!(
                "Unexpected error while preparing statement: {err}"
            ))),
        }
    }

    pub fn bind(&self, params: &Params) {
        match params {
            Params::None => {}
            Params::Positional(params) => {
                for (i, param) in params.iter().enumerate() {
                    let i = i as i32 + 1;

                    self.bind_value(i, param);
                }
            }

            Params::Named(params) => {
                for (name, param) in params {
                    let i = self.inner.bind_parameter_index(&name);

                    self.bind_value(i, param);
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

    fn bind_value(&self, i: i32, param: &Value) {
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
