use std::ffi::CString;
use libc::c_int;

use super::ffi;

use {SqliteResult, SqliteError, SqliteConnection, SqliteStatement, SqliteRows, SqliteRow};
use types::ToSql;

impl SqliteConnection {
    /// Convenience method to prepare and execute a single SQL statement with named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn insert(conn: &SqliteConnection) -> SqliteResult<i32> {
    ///     conn.execute_named("INSERT INTO test (name) VALUES (:name)", &[(":name", &"one")])
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn execute_named(&self, sql: &str, params: &[(&str, &ToSql)]) -> SqliteResult<c_int> {
        self.prepare(sql).and_then(|mut stmt| stmt.execute_named(params))
    }

    /// Convenience method to execute a query with named parameter(s) that is expected to return
    /// a single row.
    ///
    /// If the query returns more than one row, all rows except the first are ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn query_named_row<T, F>(&self,
                                 sql: &str,
                                 params: &[(&str, &ToSql)],
                                 f: F)
                                 -> SqliteResult<T>
        where F: FnOnce(SqliteRow) -> T
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query_named(params));

        match rows.next() {
            Some(row) => row.map(f),
            None => {
                Err(SqliteError {
                    code: ffi::SQLITE_NOTICE,
                    message: "Query did not return a row".to_string(),
                })
            }
        }
    }
}

impl<'conn> SqliteStatement<'conn> {
    /// Return the index of an SQL parameter given its name.
    ///
    /// # Failure
    ///
    /// Return None if `name` is invalid or if no matching parameter is found.
    pub fn parameter_index(&self, name: &str) -> Option<i32> {
        unsafe {
            CString::new(name).ok().and_then(|c_name| {
                match ffi::sqlite3_bind_parameter_index(self.stmt, c_name.as_ptr()) {
                    0 => None, // A zero is returned if no matching parameter is found.
                    n => Some(n),
                }
            })

        }
    }

    /// Execute the prepared statement with named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn insert(conn: &SqliteConnection) -> SqliteResult<i32> {
    ///     let mut stmt = try!(conn.prepare("INSERT INTO test (name) VALUES (:name)"));
    ///     stmt.execute_named(&[(":name", &"one")])
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement returns rows (in
    /// which case `query` should be used instead), or the underling SQLite call fails.
    pub fn execute_named(&mut self, params: &[(&str, &ToSql)]) -> SqliteResult<c_int> {
        unsafe {
            try!(self.bind_named_parameters(params));
            self.execute_()
        }
    }

    /// Execute the prepared statement with named parameter(s), returning an iterator over the
    /// resulting rows.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult, SqliteRows};
    /// fn query(conn: &SqliteConnection) -> SqliteResult<()> {
    ///     let mut stmt = try!(conn.prepare("SELECT * FROM test where name = :name"));
    ///     let mut rows = try!(stmt.query_named(&[(":name", &"one")]));
    ///     for row in rows {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_named<'a>(&'a mut self,
                           params: &[(&str, &ToSql)])
                           -> SqliteResult<SqliteRows<'a>> {
        self.reset_if_needed();

        unsafe {
            try!(self.bind_named_parameters(params));
        }

        self.needs_reset = true;
        Ok(SqliteRows::new(self))
    }

    unsafe fn bind_named_parameters(&mut self, params: &[(&str, &ToSql)]) -> SqliteResult<()> {
        // Always check that the number of parameters is correct.
        assert!(params.len() as c_int == ffi::sqlite3_bind_parameter_count(self.stmt),
                "incorrect number of parameters to query(): expected {}, got {}",
                ffi::sqlite3_bind_parameter_count(self.stmt),
                params.len());

        // In debug, also sanity check that we got distinct parameter names.
        debug_assert!({
                          use std::collections::HashSet;

                          let mut s = HashSet::with_capacity(params.len());
                          for &(name, _) in params {
                              s.insert(name);
                          }

                          s.len() == params.len()
                      },
                      "named parameters must be unique");

        for &(name, value) in params {
            let i = try!(self.parameter_index(name).ok_or(SqliteError {
                code: ffi::SQLITE_MISUSE,
                message: format!("Invalid parameter name: {}", name),
            }));
            try!(self.conn.decode_result(value.bind_parameter(self.stmt, i)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use SqliteConnection;

    #[test]
    fn test_execute_named() {
        let db = SqliteConnection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &1i32)]).unwrap(),
                   1);
        assert_eq!(db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &2i32)]).unwrap(),
                   1);

        assert_eq!(3i32,
                   db.query_named_row("SELECT SUM(x) FROM foo WHERE x > :x",
                                      &[(":x", &0i32)],
                                      |r| r.get(0))
                     .unwrap());
    }

    #[test]
    fn test_stmt_execute_named() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("INSERT INTO test (name) VALUES (:name)").unwrap();
        stmt.execute_named(&[(":name", &"one")]).unwrap();

        assert_eq!(1i32,
                   db.query_named_row("SELECT COUNT(*) FROM test WHERE name = :name",
                                      &[(":name", &"one")],
                                      |r| r.get(0))
                     .unwrap());
    }

    #[test]
    fn test_query_named() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT * FROM test where name = :name").unwrap();
        stmt.query_named(&[(":name", &"one")]).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_panic_on_incorrect_number_of_parameters() {
        let db = SqliteConnection::open_in_memory().unwrap();

        let mut stmt = db.prepare("SELECT 1 WHERE 1 = :one AND 2 = :two").unwrap();
        let _ = stmt.query_named(&[(":one", &1i32)]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn test_debug_panic_on_incorrect_parameter_names() {
        let db = SqliteConnection::open_in_memory().unwrap();

        let mut stmt = db.prepare("SELECT 1 WHERE 1 = :one AND 2 = :two").unwrap();
        let _ = stmt.query_named(&[(":one", &1i32), (":one", &2i32)]);
    }
}
