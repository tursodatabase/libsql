use libc::c_int;

use super::ffi;

use {Result, Error, Connection, Statement, Rows, Row, str_to_cstring};
use types::ToSql;

impl Connection {
    /// Convenience method to prepare and execute a single SQL statement with named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert(conn: &Connection) -> Result<i32> {
    ///     conn.execute_named("INSERT INTO test (name) VALUES (:name)", &[(":name", &"one")])
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn execute_named(&self, sql: &str, params: &[(&str, &ToSql)]) -> Result<c_int> {
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
    pub fn query_row_named<T, F>(&self, sql: &str, params: &[(&str, &ToSql)], f: F) -> Result<T>
        where F: FnOnce(Row) -> T
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query_named(params));

        rows.get_expected_row().map(f)
    }
}

impl<'conn> Statement<'conn> {
    /// Return the index of an SQL parameter given its name.
    ///
    /// # Failure
    ///
    /// Will return Err if `name` is invalid. Will return Ok(None) if the name
    /// is valid but not a bound parameter of this statement.
    pub fn parameter_index(&self, name: &str) -> Result<Option<i32>> {
        let c_name = try!(str_to_cstring(name));
        let c_index = unsafe { ffi::sqlite3_bind_parameter_index(self.stmt, c_name.as_ptr()) };
        Ok(match c_index {
            0 => None, // A zero is returned if no matching parameter is found.
            n => Some(n),
        })
    }

    /// Execute the prepared statement with named parameter(s). If any parameters
    /// that were in the prepared statement are not included in `params`, they
    /// will continue to use the most-recently bound value from a previous call
    /// to `execute_named`, or `NULL` if they have never been bound.
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert(conn: &Connection) -> Result<i32> {
    ///     let mut stmt = try!(conn.prepare("INSERT INTO test (name) VALUES (:name)"));
    ///     stmt.execute_named(&[(":name", &"one")])
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement returns rows (in
    /// which case `query` should be used instead), or the underling SQLite call fails.
    pub fn execute_named(&mut self, params: &[(&str, &ToSql)]) -> Result<c_int> {
        try!(self.bind_parameters_named(params));
        unsafe { self.execute_() }
    }

    /// Execute the prepared statement with named parameter(s), returning an iterator over the
    /// resulting rows. If any parameters that were in the prepared statement are not included in
    /// `params`, they will continue to use the most-recently bound value from a previous call to
    /// `query_named`, or `NULL` if they have never been bound.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, Rows};
    /// fn query(conn: &Connection) -> Result<()> {
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
    pub fn query_named<'a>(&'a mut self, params: &[(&str, &ToSql)]) -> Result<Rows<'a>> {
        try!(self.bind_parameters_named(params));
        Ok(Rows::new(self))
    }

    fn bind_parameters_named(&mut self, params: &[(&str, &ToSql)]) -> Result<()> {
        for &(name, value) in params {
            if let Some(i) = try!(self.parameter_index(name)) {
                try!(self.conn.decode_result(unsafe { value.bind_parameter(self.stmt, i) }));
            } else {
                return Err(Error::InvalidParameterName(name.into()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use Connection;

    #[test]
    fn test_execute_named() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &1i32)]).unwrap(),
                   1);
        assert_eq!(db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &2i32)]).unwrap(),
                   1);

        assert_eq!(3i32,
                   db.query_row_named("SELECT SUM(x) FROM foo WHERE x > :x",
                                      &[(":x", &0i32)],
                                      |r| r.get(0))
                     .unwrap());
    }

    #[test]
    fn test_stmt_execute_named() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("INSERT INTO test (name) VALUES (:name)").unwrap();
        stmt.execute_named(&[(":name", &"one")]).unwrap();

        assert_eq!(1i32,
                   db.query_row_named("SELECT COUNT(*) FROM test WHERE name = :name",
                                      &[(":name", &"one")],
                                      |r| r.get(0))
                     .unwrap());
    }

    #[test]
    fn test_query_named() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT * FROM test where name = :name").unwrap();
        stmt.query_named(&[(":name", &"one")]).unwrap();
    }

    #[test]
    fn test_unbound_parameters_are_null() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("INSERT INTO test (x, y) VALUES (:x, :y)").unwrap();
        stmt.execute_named(&[(":x", &"one")]).unwrap();

        let result: Option<String> =
            db.query_row("SELECT y FROM test WHERE x = 'one'", &[], |row| row.get(0))
                .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_unbound_parameters_are_reused() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("INSERT INTO test (x, y) VALUES (:x, :y)").unwrap();
        stmt.execute_named(&[(":x", &"one")]).unwrap();
        stmt.execute_named(&[(":y", &"two")]).unwrap();

        let result: String =
            db.query_row("SELECT x FROM test WHERE y = 'two'", &[], |row| row.get(0))
                .unwrap();
        assert_eq!(result, "one");
    }
}
