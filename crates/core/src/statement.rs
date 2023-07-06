use crate::{errors, Error, Params, Result, Rows, Value};

/// A prepared statement.
pub struct Statement {
    raw: *mut libsql_sys::sqlite3,
    raw_stmt: *mut libsql_sys::sqlite3_stmt,
}

impl Statement {
    pub(crate) fn prepare(raw: *mut libsql_sys::sqlite3, sql: &str) -> Result<Statement> {
        let mut raw_stmt = std::ptr::null_mut();
        let err = unsafe {
            libsql_sys::sqlite3_prepare_v2(
                raw,
                sql.as_ptr() as *const i8,
                sql.len() as i32,
                &mut raw_stmt,
                std::ptr::null_mut(),
            )
        };
        match err as u32 {
            libsql_sys::SQLITE_OK => Ok(Statement { raw, raw_stmt }),
            _ => Err(Error::QueryFailed(format!(
                "Failed to prepare statement: `{}`: {}",
                sql,
                errors::sqlite_error_message(raw),
            ))),
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
                            libsql_sys::sqlite3_bind_null(self.raw_stmt, i);
                        },
                        Value::Integer(value) => unsafe {
                            libsql_sys::sqlite3_bind_int64(self.raw_stmt, i, *value);
                        },
                        Value::Float(value) => unsafe {
                            libsql_sys::sqlite3_bind_double(self.raw_stmt, i, *value);
                        },
                        Value::Text(value) => unsafe {
                            let value = value.as_bytes();
                            libsql_sys::sqlite3_bind_text(
                                self.raw_stmt,
                                i,
                                value.as_ptr() as *const i8,
                                value.len() as i32,
                                None,
                            );
                        },
                        Value::Blob(value) => unsafe {
                            libsql_sys::sqlite3_bind_blob(
                                self.raw_stmt,
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
        unsafe { libsql_sys::sqlite3_reset(self.raw_stmt) };
        self.bind(&params);
        Rows::execute(self.raw, self.raw_stmt)
    }
}
