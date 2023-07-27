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
                    let i = self.inner.bind_parameter_index(name);

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
            Value::Real(value) => {
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

    pub fn get_status(&self, status: i32) -> i32 {
        self.inner.get_status(status)
    }
}

// NOTICE: Column is blatantly copy-pasted from rusqlite
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct Column<'stmt> {
    pub name: &'stmt str,
    pub decl_type: Option<&'stmt str>,
}

impl Column<'_> {
    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the type of the column (`None` for expression).
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type
    }
}

impl Statement {
    /// Get all the column names in the result set of the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    pub fn column_names(&self) -> Vec<&str> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n);
        for i in 0..n {
            let s = self.column_name(i);
            cols.push(s);
        }
        cols
    }

    /// Return the number of columns in the result set returned by the prepared
    /// statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    pub fn column_count(&self) -> usize {
        self.inner.column_count() as usize
    }

    /// Returns the name assigned to a particular column in the result set
    /// returned by the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    pub fn column_name(&self, col: usize) -> &str {
        self.inner.column_name(col as i32)
    }

    /// Returns the column index in the result set for a given column name.
    ///
    /// If there is no AS clause then the name of the column is unspecified and
    /// may change from one release of SQLite to the next.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// # Failure
    ///
    /// Will return an `Error::InvalidColumnName` when there is no column with
    /// the specified `name`.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        let bytes = name.as_bytes();
        let n = self.column_count() as i32;
        for i in 0..n {
            // Note: `column_name` is only fallible if `i` is out of bounds,
            // which we've already checked.
            if bytes.eq_ignore_ascii_case(self.inner.column_name(i).as_bytes()) {
                return Ok(i as usize);
            }
        }
        Err(Error::InvalidColumnName(name.to_string()))
    }

    /// Returns a slice describing the columns of the result of the query.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    pub fn columns(&self) -> Vec<Column> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n);
        for i in 0..n {
            let name = self.column_name(i);
            let decl_type = match self.inner.column_type(i as i32) as u32 {
                libsql_sys::ffi::SQLITE_NULL => Some("null"),
                libsql_sys::ffi::SQLITE_INTEGER => Some("integer"),
                libsql_sys::ffi::SQLITE_FLOAT => Some("float"),
                libsql_sys::ffi::SQLITE_TEXT => Some("text"),
                libsql_sys::ffi::SQLITE_BLOB => Some("blob"),
                _ => None,
            };
            cols.push(Column { name, decl_type });
        }
        cols
    }
}
