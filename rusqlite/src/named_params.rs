use std::convert;
use std::result;
use libc::c_int;

use super::ffi;

use {Result, Error, Connection, Statement, MappedRows, AndThenRows, Rows, Row, str_to_cstring};
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

    /// Execute the prepared statement with named parameter(s), returning a handle for the
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
    ///     while let Some(row) = rows.next() {
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

    /// Execute the prepared statement with named parameter(s), returning an iterator over the
    /// result of calling the mapping function over the query's rows. If any parameters that were
    /// in the prepared statement are not included in `params`, they will continue to use the
    /// most-recently bound value from a previous call to `query_named`, or `NULL` if they have
    /// never been bound.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people WHERE id = :id"));
    ///     let rows = try!(stmt.query_map_named(&[(":id", &"one")], |row| row.get(0)));
    ///
    ///     let mut names = Vec::new();
    ///     for name_result in rows {
    ///         names.push(try!(name_result));
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_map_named<'a, T, F>(&'a mut self,
                                     params: &[(&str, &ToSql)],
                                     f: F)
                                     -> Result<MappedRows<'a, F>>
        where F: FnMut(&Row) -> T
    {
        let rows = try!(self.query_named(params));
        Ok(MappedRows {
            rows: rows,
            map: f,
        })
    }

    /// Execute the prepared statement with named parameter(s), returning an iterator over the
    /// result of calling the mapping function over the query's rows. If any parameters that were
    /// in the prepared statement are not included in `params`, they will continue to use the
    /// most-recently bound value from a previous call to `query_named`, or `NULL` if they have
    /// never been bound.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// struct Person { name: String };
    ///
    /// fn name_to_person(name: String) -> Result<Person> {
    ///     // ... check for valid name
    ///     Ok(Person{ name: name })
    /// }
    ///
    /// fn get_names(conn: &Connection) -> Result<Vec<Person>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people WHERE id = :id"));
    ///     let rows = try!(stmt.query_and_then_named(&[(":id", &"one")], |row| {
    ///         name_to_person(row.get(0))
    ///     }));
    ///
    ///     let mut persons = Vec::new();
    ///     for person_result in rows {
    ///         persons.push(try!(person_result));
    ///     }
    ///
    ///     Ok(persons)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_and_then_named<'a, T, E, F>(&'a mut self,
                                             params: &[(&str, &ToSql)],
                                             f: F)
                                             -> Result<AndThenRows<'a, F>>
        where E: convert::From<Error>,
              F: FnMut(&Row) -> result::Result<T, E>
    {
        let rows = try!(self.query_named(params));
        Ok(AndThenRows {
            rows: rows,
            map: f,
        })
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
    use error::Error;

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
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        "#;
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT id FROM test where name = :name").unwrap();
        let mut rows = stmt.query_named(&[(":name", &"one")]).unwrap();

        let id: i32 = rows.next().unwrap().unwrap().get(0);
        assert_eq!(1, id);
    }

    #[test]
    fn test_query_map_named() {
        let db = Connection::open_in_memory().unwrap();
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        "#;
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT id FROM test where name = :name").unwrap();
        let mut rows = stmt.query_map_named(&[(":name", &"one")], |row| {
            let id: i32 = row.get(0);
            2 * id
        }).unwrap();

        let doubled_id: i32 = rows.next().unwrap().unwrap();
        assert_eq!(2, doubled_id);
    }

    #[test]
    fn test_query_and_then_named() {

        let db = Connection::open_in_memory().unwrap();
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        INSERT INTO test(id, name) VALUES (2, "one");
        "#;
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT id FROM test where name = :name ORDER BY id ASC").unwrap();
        let mut rows = stmt.query_and_then_named(&[(":name", &"one")], |row| {
            let id: i32 = row.get(0);
            if id == 1 {
                Ok(id)
            } else {
                Err(Error::SqliteSingleThreadedMode)
            }
        }).unwrap();

        // first row should be Ok
        let doubled_id: i32 = rows.next().unwrap().unwrap();
        assert_eq!(1, doubled_id);

        // second row should be Err
        match rows.next().unwrap() {
            Ok(_) => panic!("invalid Ok"),
            Err(Error::SqliteSingleThreadedMode) => (),
            Err(_) => panic!("invalid Err"),
        }
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
