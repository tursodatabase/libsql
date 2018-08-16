use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
#[cfg(feature = "array")]
use std::rc::Rc;
use std::slice::from_raw_parts;
use std::{convert, fmt, mem, ptr, result, str};

use super::ffi;
use super::str_to_cstring;
use super::{
    AndThenRows, Connection, Error, MappedRows, RawStatement, Result, Row, Rows, ValueRef,
};
use types::{ToSql, ToSqlOutput};
#[cfg(feature = "array")]
use vtab::array::{free_array, ARRAY_TYPE};

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    stmt: RawStatement,
}

impl<'conn> Statement<'conn> {
    /// Get all the column names in the result set of the prepared statement.
    pub fn column_names(&self) -> Vec<&str> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n as usize);
        for i in 0..n {
            let slice = self.stmt.column_name(i);
            let s = str::from_utf8(slice.to_bytes()).unwrap();
            cols.push(s);
        }
        cols
    }

    /// Return the number of columns in the result set returned by the prepared
    /// statement.
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Returns the column index in the result set for a given column name.
    ///
    /// If there is no AS clause then the name of the column is unspecified and
    /// may change from one release of SQLite to the next.
    ///
    /// # Failure
    ///
    /// Will return an `Error::InvalidColumnName` when there is no column with
    /// the specified `name`.
    pub fn column_index(&self, name: &str) -> Result<usize> {
        let bytes = name.as_bytes();
        let n = self.column_count();
        for i in 0..n {
            if bytes.eq_ignore_ascii_case(self.stmt.column_name(i).to_bytes()) {
                return Ok(i);
            }
        }
        Err(Error::InvalidColumnName(String::from(name)))
    }

    /// Execute the prepared statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn update_rows(conn: &Connection) -> Result<()> {
    ///     let mut stmt = try!(conn.prepare("UPDATE foo SET bar = 'baz' WHERE qux = ?"));
    ///
    ///     try!(stmt.execute(&[&1i32]));
    ///     try!(stmt.execute(&[&2i32]));
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement
    /// returns rows (in which case `query` should be used instead), or the
    /// underling SQLite call fails.
    pub fn execute(&mut self, params: &[&ToSql]) -> Result<usize> {
        try!(self.bind_parameters(params));
        self.execute_with_bound_parameters()
    }

    /// Execute the prepared statement with named parameter(s). If any
    /// parameters that were in the prepared statement are not included in
    /// `params`, they will continue to use the most-recently bound value
    /// from a previous call to `execute_named`, or `NULL` if they have
    /// never been bound.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert(conn: &Connection) -> Result<usize> {
    ///     let mut stmt = try!(conn.prepare("INSERT INTO test (name) VALUES (:name)"));
    ///     stmt.execute_named(&[(":name", &"one")])
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement
    /// returns rows (in which case `query` should be used instead), or the
    /// underling SQLite call fails.
    pub fn execute_named(&mut self, params: &[(&str, &ToSql)]) -> Result<usize> {
        try!(self.bind_parameters_named(params));
        self.execute_with_bound_parameters()
    }

    /// Execute an INSERT and return the ROWID.
    ///
    /// # Note
    ///
    /// This function is a convenience wrapper around `execute()` intended for
    /// queries that insert a single item. It is possible to misuse this
    /// function in a way that it cannot detect, such as by calling it on a
    /// statement which _updates_ a single
    /// item rather than inserting one. Please don't do that.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no row is inserted or many rows are inserted.
    pub fn insert(&mut self, params: &[&ToSql]) -> Result<i64> {
        let changes = try!(self.execute(params));
        match changes {
            1 => Ok(self.conn.last_insert_rowid()),
            _ => Err(Error::StatementChangedRows(changes)),
        }
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// rows.
    ///
    /// Due to lifetime restricts, the rows handle returned by `query` does not
    /// implement the `Iterator` trait. Consider using `query_map` or
    /// `query_and_then` instead, which do.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people"));
    ///     let mut rows = try!(stmt.query(&[]));
    ///
    ///     let mut names = Vec::new();
    ///     while let Some(result_row) = rows.next() {
    ///         let row = try!(result_row);
    ///         names.push(row.get(0));
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query<'a>(&'a mut self, params: &[&ToSql]) -> Result<Rows<'a>> {
        try!(self.check_readonly());
        try!(self.bind_parameters(params));
        Ok(Rows::new(self))
    }

    /// Execute the prepared statement with named parameter(s), returning a
    /// handle for the resulting rows. If any parameters that were in the
    /// prepared statement are not included in `params`, they will continue
    /// to use the most-recently bound value from a previous
    /// call to `query_named`, or `NULL` if they have never been bound.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
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
        try!(self.check_readonly());
        try!(self.bind_parameters_named(params));
        Ok(Rows::new(self))
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, returning an iterator over the mapped function results.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people"));
    ///     let rows = try!(stmt.query_map(&[], |row| row.get(0)));
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
    pub fn query_map<'a, T, F>(&'a mut self, params: &[&ToSql], f: F) -> Result<MappedRows<'a, F>>
    where
        F: FnMut(&Row) -> T,
    {
        let rows = self.query(params)?;
        Ok(MappedRows::new(rows, f))
    }

    /// Execute the prepared statement with named parameter(s), returning an
    /// iterator over the result of calling the mapping function over the
    /// query's rows. If any parameters that were in the prepared statement
    /// are not included in `params`, they will continue to use the
    /// most-recently bound value from a previous call to `query_named`,
    /// or `NULL` if they have never been bound.
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
    pub fn query_map_named<'a, T, F>(
        &'a mut self,
        params: &[(&str, &ToSql)],
        f: F,
    ) -> Result<MappedRows<'a, F>>
    where
        F: FnMut(&Row) -> T,
    {
        let rows = self.query_named(params)?;
        Ok(MappedRows::new(rows, f))
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, where the function returns a `Result` with `Error` type
    /// implementing `std::convert::From<Error>` (so errors can be unified).
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_and_then<'a, T, E, F>(
        &'a mut self,
        params: &[&ToSql],
        f: F,
    ) -> Result<AndThenRows<'a, F>>
    where
        E: convert::From<Error>,
        F: FnMut(&Row) -> result::Result<T, E>,
    {
        let rows = self.query(params)?;
        Ok(AndThenRows::new(rows, f))
    }

    /// Execute the prepared statement with named parameter(s), returning an
    /// iterator over the result of calling the mapping function over the
    /// query's rows. If any parameters that were in the prepared statement
    /// are not included in
    /// `params`, they will
    /// continue to use the most-recently bound value from a previous call
    /// to `query_named`, or `NULL` if they have never been bound.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// struct Person {
    ///     name: String,
    /// };
    ///
    /// fn name_to_person(name: String) -> Result<Person> {
    ///     // ... check for valid name
    ///     Ok(Person { name: name })
    /// }
    ///
    /// fn get_names(conn: &Connection) -> Result<Vec<Person>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people WHERE id = :id"));
    ///     let rows =
    ///         try!(stmt.query_and_then_named(&[(":id", &"one")], |row| name_to_person(row.get(0))));
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
    pub fn query_and_then_named<'a, T, E, F>(
        &'a mut self,
        params: &[(&str, &ToSql)],
        f: F,
    ) -> Result<AndThenRows<'a, F>>
    where
        E: convert::From<Error>,
        F: FnMut(&Row) -> result::Result<T, E>,
    {
        let rows = self.query_named(params)?;
        Ok(AndThenRows::new(rows, f))
    }

    /// Return `true` if a query in the SQL statement it executes returns one
    /// or more rows and `false` if the SQL returns an empty set.
    pub fn exists(&mut self, params: &[&ToSql]) -> Result<bool> {
        let mut rows = try!(self.query(params));
        let exists = {
            match rows.next() {
                Some(_) => true,
                None => false,
            }
        };
        Ok(exists)
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn query_row<T, F>(&mut self, params: &[&ToSql], f: F) -> Result<T>
    where
        F: FnOnce(&Row) -> T,
    {
        let mut rows = try!(self.query(params));

        rows.get_expected_row().map(|r| f(&r))
    }

    /// Consumes the statement.
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn finalize(mut self) -> Result<()> {
        self.finalize_()
    }

    /// Return the index of an SQL parameter given its name.
    ///
    /// # Failure
    ///
    /// Will return Err if `name` is invalid. Will return Ok(None) if the name
    /// is valid but not a bound parameter of this statement.
    pub fn parameter_index(&self, name: &str) -> Result<Option<usize>> {
        let c_name = try!(str_to_cstring(name));
        Ok(self.stmt.bind_parameter_index(&c_name))
    }

    fn bind_parameters(&mut self, params: &[&ToSql]) -> Result<()> {
        assert_eq!(
            params.len(),
            self.stmt.bind_parameter_count(),
            "incorrect number of parameters to query(): expected {}, got {}",
            self.stmt.bind_parameter_count(),
            params.len()
        );

        for (i, p) in params.iter().enumerate() {
            try!(self.bind_parameter(*p, i + 1));
        }

        Ok(())
    }

    fn bind_parameters_named(&mut self, params: &[(&str, &ToSql)]) -> Result<()> {
        for &(name, value) in params {
            if let Some(i) = try!(self.parameter_index(name)) {
                try!(self.bind_parameter(value, i));
            } else {
                return Err(Error::InvalidParameterName(name.into()));
            }
        }
        Ok(())
    }

    fn bind_parameter(&self, param: &ToSql, col: usize) -> Result<()> {
        let value = try!(param.to_sql());

        let ptr = unsafe { self.stmt.ptr() };
        let value = match value {
            ToSqlOutput::Borrowed(v) => v,
            ToSqlOutput::Owned(ref v) => ValueRef::from(v),

            #[cfg(feature = "blob")]
            ToSqlOutput::ZeroBlob(len) => {
                return self
                    .conn
                    .decode_result(unsafe { ffi::sqlite3_bind_zeroblob(ptr, col as c_int, len) });
            }
            #[cfg(feature = "array")]
            ToSqlOutput::Array(a) => {
                return self.conn.decode_result(unsafe {
                    ffi::sqlite3_bind_pointer(
                        ptr,
                        col as c_int,
                        Rc::into_raw(a) as *mut c_void,
                        ARRAY_TYPE,
                        Some(free_array),
                    )
                });
            }
        };
        self.conn.decode_result(match value {
            ValueRef::Null => unsafe { ffi::sqlite3_bind_null(ptr, col as c_int) },
            ValueRef::Integer(i) => unsafe { ffi::sqlite3_bind_int64(ptr, col as c_int, i) },
            ValueRef::Real(r) => unsafe { ffi::sqlite3_bind_double(ptr, col as c_int, r) },
            ValueRef::Text(s) => unsafe {
                let length = s.len();
                if length > ::std::i32::MAX as usize {
                    ffi::SQLITE_TOOBIG
                } else {
                    let c_str = try!(str_to_cstring(s));
                    let destructor = if length > 0 {
                        ffi::SQLITE_TRANSIENT()
                    } else {
                        ffi::SQLITE_STATIC()
                    };
                    ffi::sqlite3_bind_text(
                        ptr,
                        col as c_int,
                        c_str.as_ptr(),
                        length as c_int,
                        destructor,
                    )
                }
            },
            ValueRef::Blob(b) => unsafe {
                let length = b.len();
                if length > ::std::i32::MAX as usize {
                    ffi::SQLITE_TOOBIG
                } else if length == 0 {
                    ffi::sqlite3_bind_zeroblob(ptr, col as c_int, 0)
                } else {
                    ffi::sqlite3_bind_blob(
                        ptr,
                        col as c_int,
                        b.as_ptr() as *const c_void,
                        length as c_int,
                        ffi::SQLITE_TRANSIENT(),
                    )
                }
            },
        })
    }

    fn execute_with_bound_parameters(&mut self) -> Result<usize> {
        let r = self.stmt.step();
        self.stmt.reset();
        match r {
            ffi::SQLITE_DONE => {
                if self.column_count() == 0 {
                    Ok(self.conn.changes())
                } else {
                    Err(Error::ExecuteReturnedResults)
                }
            }
            ffi::SQLITE_ROW => Err(Error::ExecuteReturnedResults),
            _ => Err(self.conn.decode_result(r).unwrap_err()),
        }
    }

    fn finalize_(&mut self) -> Result<()> {
        let mut stmt = RawStatement::new(ptr::null_mut());
        mem::swap(&mut stmt, &mut self.stmt);
        self.conn.decode_result(stmt.finalize())
    }

    #[cfg(not(feature = "bundled"))]
    fn check_readonly(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "bundled")]
    fn check_readonly(&self) -> Result<()> {
        if !self.stmt.readonly() {
            return Err(Error::InvalidQuery);
        }
        Ok(())
    }

    /// Returns a string containing the SQL text of prepared statement with
    /// bound parameters expanded.
    #[cfg(feature = "bundled")]
    pub fn expanded_sql(&self) -> Option<&str> {
        unsafe {
            self.stmt
                .expanded_sql()
                .map(|s| str::from_utf8_unchecked(s.to_bytes()))
        }
    }
}

impl<'conn> Into<RawStatement> for Statement<'conn> {
    fn into(mut self) -> RawStatement {
        let mut stmt = RawStatement::new(ptr::null_mut());
        mem::swap(&mut stmt, &mut self.stmt);
        stmt
    }
}

impl<'conn> fmt::Debug for Statement<'conn> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sql = str::from_utf8(self.stmt.sql().to_bytes());
        f.debug_struct("Statement")
            .field("conn", self.conn)
            .field("stmt", &self.stmt)
            .field("sql", &sql)
            .finish()
    }
}

impl<'conn> Drop for Statement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.finalize_();
    }
}

impl<'conn> Statement<'conn> {
    pub(crate) fn new(conn: &Connection, stmt: RawStatement) -> Statement {
        Statement { conn, stmt }
    }

    pub(crate) fn value_ref(&self, col: usize) -> ValueRef {
        let raw = unsafe { self.stmt.ptr() };

        match self.stmt.column_type(col) {
            ffi::SQLITE_NULL => ValueRef::Null,
            ffi::SQLITE_INTEGER => {
                ValueRef::Integer(unsafe { ffi::sqlite3_column_int64(raw, col as c_int) })
            }
            ffi::SQLITE_FLOAT => {
                ValueRef::Real(unsafe { ffi::sqlite3_column_double(raw, col as c_int) })
            }
            ffi::SQLITE_TEXT => {
                let s = unsafe {
                    let text = ffi::sqlite3_column_text(raw, col as c_int);
                    assert!(
                        !text.is_null(),
                        "unexpected SQLITE_TEXT column type with NULL data"
                    );
                    CStr::from_ptr(text as *const c_char)
                };

                // sqlite3_column_text returns UTF8 data, so our unwrap here should be fine.
                let s = s
                    .to_str()
                    .expect("sqlite3_column_text returned invalid UTF-8");
                ValueRef::Text(s)
            }
            ffi::SQLITE_BLOB => {
                let (blob, len) = unsafe {
                    (
                        ffi::sqlite3_column_blob(raw, col as c_int),
                        ffi::sqlite3_column_bytes(raw, col as c_int),
                    )
                };

                assert!(
                    len >= 0,
                    "unexpected negative return from sqlite3_column_bytes"
                );
                if len > 0 {
                    assert!(
                        !blob.is_null(),
                        "unexpected SQLITE_BLOB column type with NULL data"
                    );
                    ValueRef::Blob(unsafe { from_raw_parts(blob as *const u8, len as usize) })
                } else {
                    // The return value from sqlite3_column_blob() for a zero-length BLOB
                    // is a NULL pointer.
                    ValueRef::Blob(&[])
                }
            }
            _ => unreachable!("sqlite3_column_type returned invalid value"),
        }
    }

    pub(crate) fn step(&self) -> Result<bool> {
        match self.stmt.step() {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            code => Err(self.conn.decode_result(code).unwrap_err()),
        }
    }

    pub(crate) fn reset(&self) -> c_int {
        self.stmt.reset()
    }
}

#[cfg(test)]
mod test {
    use {Connection, Error, Result};

    #[test]
    fn test_execute_named() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(
            db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &1i32)])
                .unwrap(),
            1
        );
        assert_eq!(
            db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &2i32)])
                .unwrap(),
            1
        );

        assert_eq!(
            3i32,
            db.query_row_named::<i32, _>(
                "SELECT SUM(x) FROM foo WHERE x > :x",
                &[(":x", &0i32)],
                |r| r.get(0)
            ).unwrap()
        );
    }

    #[test]
    fn test_stmt_execute_named() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db
            .prepare("INSERT INTO test (name) VALUES (:name)")
            .unwrap();
        stmt.execute_named(&[(":name", &"one")]).unwrap();

        assert_eq!(
            1i32,
            db.query_row_named::<i32, _>(
                "SELECT COUNT(*) FROM test WHERE name = :name",
                &[(":name", &"one")],
                |r| r.get(0)
            ).unwrap()
        );
    }

    #[test]
    fn test_query_named() {
        let db = Connection::open_in_memory().unwrap();
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        "#;
        db.execute_batch(sql).unwrap();

        let mut stmt = db
            .prepare("SELECT id FROM test where name = :name")
            .unwrap();
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

        let mut stmt = db
            .prepare("SELECT id FROM test where name = :name")
            .unwrap();
        let mut rows = stmt
            .query_map_named(&[(":name", &"one")], |row| {
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

        let mut stmt = db
            .prepare("SELECT id FROM test where name = :name ORDER BY id ASC")
            .unwrap();
        let mut rows = stmt
            .query_and_then_named(&[(":name", &"one")], |row| {
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

        let mut stmt = db
            .prepare("INSERT INTO test (x, y) VALUES (:x, :y)")
            .unwrap();
        stmt.execute_named(&[(":x", &"one")]).unwrap();

        let result: Option<String> = db
            .query_row("SELECT y FROM test WHERE x = 'one'", &[], |row| row.get(0))
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_unbound_parameters_are_reused() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db
            .prepare("INSERT INTO test (x, y) VALUES (:x, :y)")
            .unwrap();
        stmt.execute_named(&[(":x", &"one")]).unwrap();
        stmt.execute_named(&[(":y", &"two")]).unwrap();

        let result: String = db
            .query_row("SELECT x FROM test WHERE y = 'two'", &[], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "one");
    }

    #[test]
    fn test_insert() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER UNIQUE)")
            .unwrap();
        let mut stmt = db
            .prepare("INSERT OR IGNORE INTO foo (x) VALUES (?)")
            .unwrap();
        assert_eq!(stmt.insert(&[&1i32]).unwrap(), 1);
        assert_eq!(stmt.insert(&[&2i32]).unwrap(), 2);
        match stmt.insert(&[&1i32]).unwrap_err() {
            Error::StatementChangedRows(0) => (),
            err => panic!("Unexpected error {}", err),
        }
        let mut multi = db
            .prepare("INSERT INTO foo (x) SELECT 3 UNION ALL SELECT 4")
            .unwrap();
        match multi.insert(&[]).unwrap_err() {
            Error::StatementChangedRows(2) => (),
            err => panic!("Unexpected error {}", err),
        }
    }

    #[test]
    fn test_insert_different_tables() {
        // Test for https://github.com/jgallagher/rusqlite/issues/171
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            r"
            CREATE TABLE foo(x INTEGER);
            CREATE TABLE bar(x INTEGER);
        ",
        ).unwrap();

        assert_eq!(
            db.prepare("INSERT INTO foo VALUES (10)")
                .unwrap()
                .insert(&[])
                .unwrap(),
            1
        );
        assert_eq!(
            db.prepare("INSERT INTO bar VALUES (10)")
                .unwrap()
                .insert(&[])
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_exists() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   END;";
        db.execute_batch(sql).unwrap();
        let mut stmt = db.prepare("SELECT 1 FROM foo WHERE x = ?").unwrap();
        assert!(stmt.exists(&[&1i32]).unwrap());
        assert!(stmt.exists(&[&2i32]).unwrap());
        assert!(!stmt.exists(&[&0i32]).unwrap());
    }

    #[test]
    fn test_query_row() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   INSERT INTO foo VALUES(2, 4);
                   END;";
        db.execute_batch(sql).unwrap();
        let mut stmt = db.prepare("SELECT y FROM foo WHERE x = ?").unwrap();
        let y: Result<i64> = stmt.query_row(&[&1i32], |r| r.get(0));
        assert_eq!(3i64, y.unwrap());
    }

    #[test]
    fn test_query_by_column_name() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql).unwrap();
        let mut stmt = db.prepare("SELECT y FROM foo").unwrap();
        let y: Result<i64> = stmt.query_row(&[], |r| r.get("y"));
        assert_eq!(3i64, y.unwrap());
    }

    #[test]
    fn test_query_by_column_name_ignore_case() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql).unwrap();
        let mut stmt = db.prepare("SELECT y as Y FROM foo").unwrap();
        let y: Result<i64> = stmt.query_row(&[], |r| r.get("y"));
        assert_eq!(3i64, y.unwrap());
    }

    #[test]
    #[cfg(feature = "bundled")]
    fn test_expanded_sql() {
        let db = Connection::open_in_memory().unwrap();
        let stmt = db.prepare("SELECT ?").unwrap();
        stmt.bind_parameter(&1, 1).unwrap();
        assert_eq!(Some("SELECT 1"), stmt.expanded_sql());
    }
}
