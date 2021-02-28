use std::iter::IntoIterator;
use std::os::raw::{c_int, c_void};
#[cfg(feature = "array")]
use std::rc::Rc;
use std::slice::from_raw_parts;
use std::{convert, fmt, mem, ptr, str};

use super::ffi;
use super::{len_as_c_int, str_for_sqlite};
use super::{
    AndThenRows, Connection, Error, MappedRows, Params, RawStatement, Result, Row, Rows, ValueRef,
};
use crate::types::{ToSql, ToSqlOutput};
#[cfg(feature = "array")]
use crate::vtab::array::{free_array, ARRAY_TYPE};

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    pub(crate) stmt: RawStatement,
}

impl Statement<'_> {
    /// Execute the prepared statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ### Use with positional parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, params};
    /// fn update_rows(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("UPDATE foo SET bar = 'baz' WHERE qux = ?")?;
    ///     // The `rusqlite::params!` macro is mostly useful when the parameters do not
    ///     // all have the same type, or if there are more than 32 parameters
    ///     // at once.
    ///     stmt.execute(params![1i32])?;
    ///     // However, it's not required, many cases are fine as:
    ///     stmt.execute(&[&2i32])?;
    ///     // Or even:
    ///     stmt.execute([2i32])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ### Use with named parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, named_params};
    /// fn insert(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("INSERT INTO test (key, value) VALUES (:key, :value)")?;
    ///     // The `rusqlite::named_params!` macro (like `params!`) is useful for heterogeneous
    ///     // sets of parameters (where all parameters are not the same type), or for queries
    ///     // with many (more than 32) statically known parameters.
    ///     stmt.execute(named_params!{ ":key": "one", ":val": 2 })?;
    ///     // However, named parameters can also be passed like:
    ///     stmt.execute(&[(":key", "three"), (":val", "four")])?;
    ///     // Or even: (note that a &T is required for the value type, currently)
    ///     stmt.execute(&[(":key", &100), (":val", &200)])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ### Use without parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, params};
    /// fn delete_all(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("DELETE FROM users")?;
    ///     stmt.execute([])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement
    /// returns rows (in which case `query` should be used instead), or the
    /// underlying SQLite call fails.
    #[inline]
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        params.__bind_in(self)?;
        self.execute_with_bound_parameters()
    }

    /// Execute the prepared statement with named parameter(s).
    ///
    /// Note: This function is deprecated in favor of [`Statement::execute`],
    /// which can now take named parameters directly.
    ///
    /// If any parameters that were in the prepared statement are not included
    /// in `params`, they will continue to use the most-recently bound value
    /// from a previous call to `execute_named`, or `NULL` if they have never
    /// been bound.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement
    /// returns rows (in which case `query` should be used instead), or the
    /// underlying SQLite call fails.
    #[deprecated = "You can use `execute` with named params now."]
    #[inline]
    pub fn execute_named(&mut self, params: &[(&str, &dyn ToSql)]) -> Result<usize> {
        self.execute(params)
    }

    /// Execute an INSERT and return the ROWID.
    ///
    /// # Note
    ///
    /// This function is a convenience wrapper around [`execute()`](Statement::execute) intended for
    /// queries that insert a single item. It is possible to misuse this
    /// function in a way that it cannot detect, such as by calling it on a
    /// statement which _updates_ a single
    /// item rather than inserting one. Please don't do that.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no row is inserted or many rows are inserted.
    #[inline]
    pub fn insert<P: Params>(&mut self, params: P) -> Result<i64> {
        let changes = self.execute(params)?;
        match changes {
            1 => Ok(self.conn.last_insert_rowid()),
            _ => Err(Error::StatementChangedRows(changes)),
        }
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// rows.
    ///
    /// Due to lifetime restricts, the rows handle returned by `query` does not
    /// implement the `Iterator` trait. Consider using [`query_map`](Statement::query_map) or
    /// [`query_and_then`](Statement::query_and_then) instead, which do.
    ///
    /// ## Example
    ///
    /// ### Use without parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people")?;
    ///     let mut rows = stmt.query([])?;
    ///
    ///     let mut names = Vec::new();
    ///     while let Some(row) = rows.next()? {
    ///         names.push(row.get(0)?);
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ### Use with positional parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn query(conn: &Connection, name: &str) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = ?")?;
    ///     let mut rows = stmt.query(rusqlite::params![name])?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Or, equivalently (but without the [`params!`] macro).
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn query(conn: &Connection, name: &str) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = ?")?;
    ///     let mut rows = stmt.query([name])?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ### Use with named parameters
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn query(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = :name")?;
    ///     let mut rows = stmt.query(&[(":name", "one")])?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Note, the `named_params!` macro is provided for syntactic convenience,
    /// and so the above example could also be written as:
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, named_params};
    /// fn query(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = :name")?;
    ///     let mut rows = stmt.query(named_params!{ ":name": "one" })?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn query<P: Params>(&mut self, params: P) -> Result<Rows<'_>> {
        params.__bind_in(self)?;
        Ok(Rows::new(self))
    }

    /// Execute the prepared statement with named parameter(s), returning a
    /// handle for the resulting rows.
    ///
    /// Note: This function is deprecated in favor of [`Statement::query`],
    /// which can now take named parameters directly.
    ///
    /// If any parameters that were in the prepared statement are not included
    /// in `params`, they will continue to use the most-recently bound value
    /// from a previous call to `query_named`, or `NULL` if they have never been
    /// bound.
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[deprecated = "You can use `query` with named params now."]
    pub fn query_named(&mut self, params: &[(&str, &dyn ToSql)]) -> Result<Rows<'_>> {
        self.query(params)
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, returning an iterator over the mapped function results.
    ///
    /// `f` is used to tranform the _streaming_ iterator into a _standard_
    /// iterator.
    ///
    /// This is equivalent to `stmt.query(params)?.mapped(f)`.
    ///
    /// ## Example
    ///
    /// ### Use with positional params
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people")?;
    ///     let rows = stmt.query_map([], |row| row.get(0))?;
    ///
    ///     let mut names = Vec::new();
    ///     for name_result in rows {
    ///         names.push(name_result?);
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ### Use with named params
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people WHERE id = :id")?;
    ///     let rows = stmt.query_map(&[(":id", &"one")], |row| row.get(0))?;
    ///
    ///     let mut names = Vec::new();
    ///     for name_result in rows {
    ///         names.push(name_result?);
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_map<T, P, F>(&mut self, params: P, f: F) -> Result<MappedRows<'_, F>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        self.query(params).map(|rows| rows.mapped(f))
    }

    /// Execute the prepared statement with named parameter(s), returning an
    /// iterator over the result of calling the mapping function over the
    /// query's rows.
    ///
    /// Note: This function is deprecated in favor of [`Statement::query_map`],
    /// which can now take named parameters directly.
    ///
    /// If any parameters that were in the prepared statement
    /// are not included in `params`, they will continue to use the
    /// most-recently bound value from a previous call to `query_named`,
    /// or `NULL` if they have never been bound.
    ///
    /// `f` is used to tranform the _streaming_ iterator into a _standard_
    /// iterator.
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[deprecated = "You can use `query_map` with named params now."]
    pub fn query_map_named<T, F>(
        &mut self,
        params: &[(&str, &dyn ToSql)],
        f: F,
    ) -> Result<MappedRows<'_, F>>
    where
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        self.query_map(params, f)
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, where the function returns a `Result` with `Error` type
    /// implementing `std::convert::From<Error>` (so errors can be unified).
    ///
    /// This is equivalent to `stmt.query(params)?.and_then(f)`.
    ///
    /// ## Example
    ///
    /// ### Use with named params
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
    ///     let mut stmt = conn.prepare("SELECT name FROM people WHERE id = :id")?;
    ///     let rows =
    ///         stmt.query_and_then(&[(":id", "one")], |row| name_to_person(row.get(0)?))?;
    ///
    ///     let mut persons = Vec::new();
    ///     for person_result in rows {
    ///         persons.push(person_result?);
    ///     }
    ///
    ///     Ok(persons)
    /// }
    /// ```
    ///
    /// ### Use with positional params
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people WHERE id = ?")?;
    ///     let rows = stmt.query_and_then(["one"], |row| row.get::<_, String>(0))?;
    ///
    ///     let mut persons = Vec::new();
    ///     for person_result in rows {
    ///         persons.push(person_result?);
    ///     }
    ///
    ///     Ok(persons)
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn query_and_then<T, E, P, F>(&mut self, params: P, f: F) -> Result<AndThenRows<'_, F>>
    where
        P: Params,
        E: convert::From<Error>,
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        self.query(params).map(|rows| rows.and_then(f))
    }

    /// Execute the prepared statement with named parameter(s), returning an
    /// iterator over the result of calling the mapping function over the
    /// query's rows.
    ///
    /// Note: This function is deprecated in favor of [`Statement::query_and_then`],
    /// which can now take named parameters directly.
    ///
    /// If any parameters that were in the prepared statement are not included
    /// in `params`, they will continue to use the most-recently bound value
    /// from a previous call to `query_named`, or `NULL` if they have never been
    /// bound.
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[deprecated = "You can use `query_and_then` with named params now."]
    pub fn query_and_then_named<T, E, F>(
        &mut self,
        params: &[(&str, &dyn ToSql)],
        f: F,
    ) -> Result<AndThenRows<'_, F>>
    where
        E: convert::From<Error>,
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        self.query_and_then(params, f)
    }

    /// Return `true` if a query in the SQL statement it executes returns one
    /// or more rows and `false` if the SQL returns an empty set.
    #[inline]
    pub fn exists<P: Params>(&mut self, params: P) -> Result<bool> {
        let mut rows = self.query(params)?;
        let exists = rows.next()?.is_some();
        Ok(exists)
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call [`.optional()`](crate::OptionalExtension::optional) on the result of
    /// this to get a `Result<Option<T>>` (requires that the trait `rusqlite::OptionalExtension`
    /// is imported).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn query_row<T, P, F>(&mut self, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut rows = self.query(params)?;

        rows.get_expected_row().and_then(|r| f(&r))
    }

    /// Convenience method to execute a query with named parameter(s) that is
    /// expected to return a single row.
    ///
    /// Note: This function is deprecated in favor of [`Statement::query_and_then`],
    /// which can now take named parameters directly.
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call [`.optional()`](crate::OptionalExtension::optional) on the result of
    /// this to get a `Result<Option<T>>` (requires that the trait `rusqlite::OptionalExtension`
    /// is imported).
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[deprecated = "You can use `query_row` with named params now."]
    pub fn query_row_named<T, F>(&mut self, params: &[(&str, &dyn ToSql)], f: F) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.query_row(params, f)
    }

    /// Consumes the statement.
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[inline]
    pub fn finalize(mut self) -> Result<()> {
        self.finalize_()
    }

    /// Return the (one-based) index of an SQL parameter given its name.
    ///
    /// Note that the initial ":" or "$" or "@" or "?" used to specify the
    /// parameter is included as part of the name.
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn example(conn: &Connection) -> Result<()> {
    ///     let stmt = conn.prepare("SELECT * FROM test WHERE name = :example")?;
    ///     let index = stmt.parameter_index(":example")?;
    ///     assert_eq!(index, Some(1));
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return Err if `name` is invalid. Will return Ok(None) if the name
    /// is valid but not a bound parameter of this statement.
    #[inline]
    pub fn parameter_index(&self, name: &str) -> Result<Option<usize>> {
        Ok(self.stmt.bind_parameter_index(name))
    }

    #[inline]
    pub(crate) fn bind_parameters<P>(&mut self, params: P) -> Result<()>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let expected = self.stmt.bind_parameter_count();
        let mut index = 0;
        for p in params.into_iter() {
            index += 1; // The leftmost SQL parameter has an index of 1.
            if index > expected {
                break;
            }
            self.bind_parameter(&p, index)?;
        }
        if index != expected {
            Err(Error::InvalidParameterCount(index, expected))
        } else {
            Ok(())
        }
    }

    #[inline]
    pub(crate) fn bind_parameters_named<T: ?Sized + ToSql>(
        &mut self,
        params: &[(&str, &T)],
    ) -> Result<()> {
        for &(name, value) in params {
            if let Some(i) = self.parameter_index(name)? {
                let ts: &dyn ToSql = &value;
                self.bind_parameter(ts, i)?;
            } else {
                return Err(Error::InvalidParameterName(name.into()));
            }
        }
        Ok(())
    }

    /// Return the number of parameters that can be bound to this statement.
    #[inline]
    pub fn parameter_count(&self) -> usize {
        self.stmt.bind_parameter_count()
    }

    /// Low level API to directly bind a parameter to a given index.
    ///
    /// Note that the index is one-based, that is, the first parameter index is
    /// 1 and not 0. This is consistent with the SQLite API and the values given
    /// to parameters bound as `?NNN`.
    ///
    /// The valid values for `one_based_col_index` begin at `1`, and end at
    /// [`Statement::parameter_count`], inclusive.
    ///
    /// # Caveats
    ///
    /// This should not generally be used, but is available for special cases
    /// such as:
    ///
    /// - binding parameters where a gap exists.
    /// - binding named and positional parameters in the same query.
    /// - separating parameter binding from query execution.
    ///
    /// Statements that have had their parameters bound this way should be
    /// queried or executed by [`Statement::raw_query`] or
    /// [`Statement::raw_execute`]. Other functions are not guaranteed to work.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn query(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test WHERE name = :name AND value > ?2")?;
    ///     let name_index = stmt.parameter_index(":name")?.expect("No such parameter");
    ///     stmt.raw_bind_parameter(name_index, "foo")?;
    ///     stmt.raw_bind_parameter(2, 100)?;
    ///     let mut rows = stmt.raw_query();
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn raw_bind_parameter<T: ToSql>(
        &mut self,
        one_based_col_index: usize,
        param: T,
    ) -> Result<()> {
        // This is the same as `bind_parameter` but slightly more ergonomic and
        // correctly takes `&mut self`.
        self.bind_parameter(&param, one_based_col_index)
    }

    /// Low level API to execute a statement given that all parameters were
    /// bound explicitly with the [`Statement::raw_bind_parameter`] API.
    ///
    /// # Caveats
    ///
    /// Any unbound parameters will have `NULL` as their value.
    ///
    /// This should not generally be used outside of special cases, and
    /// functions in the [`Statement::execute`] family should be preferred.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the executed statement returns rows (in which case
    /// `query` should be used instead), or the underlying SQLite call fails.
    #[inline]
    pub fn raw_execute(&mut self) -> Result<usize> {
        self.execute_with_bound_parameters()
    }

    /// Low level API to get `Rows` for this query given that all parameters
    /// were bound explicitly with the [`Statement::raw_bind_parameter`] API.
    ///
    /// # Caveats
    ///
    /// Any unbound parameters will have `NULL` as their value.
    ///
    /// This should not generally be used outside of special cases, and
    /// functions in the [`Statement::query`] family should be preferred.
    ///
    /// Note that if the SQL does not return results, [`Statement::raw_execute`]
    /// should be used instead.
    #[inline]
    pub fn raw_query(&mut self) -> Rows<'_> {
        Rows::new(self)
    }

    // generic because many of these branches can constant fold away.
    fn bind_parameter<P: ?Sized + ToSql>(&self, param: &P, col: usize) -> Result<()> {
        let value = param.to_sql()?;

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
                let (c_str, len, destructor) = str_for_sqlite(s)?;
                ffi::sqlite3_bind_text(ptr, col as c_int, c_str, len, destructor)
            },
            ValueRef::Blob(b) => unsafe {
                let length = len_as_c_int(b.len())?;
                if length == 0 {
                    ffi::sqlite3_bind_zeroblob(ptr, col as c_int, 0)
                } else {
                    ffi::sqlite3_bind_blob(
                        ptr,
                        col as c_int,
                        b.as_ptr() as *const c_void,
                        length,
                        ffi::SQLITE_TRANSIENT(),
                    )
                }
            },
        })
    }

    #[inline]
    fn execute_with_bound_parameters(&mut self) -> Result<usize> {
        self.check_update()?;
        let r = self.stmt.step();
        self.stmt.reset();
        match r {
            ffi::SQLITE_DONE => Ok(self.conn.changes()),
            ffi::SQLITE_ROW => Err(Error::ExecuteReturnedResults),
            _ => Err(self.conn.decode_result(r).unwrap_err()),
        }
    }

    #[inline]
    fn finalize_(&mut self) -> Result<()> {
        let mut stmt = unsafe { RawStatement::new(ptr::null_mut(), 0) };
        mem::swap(&mut stmt, &mut self.stmt);
        self.conn.decode_result(stmt.finalize())
    }

    #[cfg(all(feature = "modern_sqlite", feature = "extra_check"))]
    #[inline]
    fn check_update(&self) -> Result<()> {
        // sqlite3_column_count works for DML but not for DDL (ie ALTER)
        if self.column_count() > 0 && self.stmt.readonly() {
            return Err(Error::ExecuteReturnedResults);
        }
        Ok(())
    }

    #[cfg(all(not(feature = "modern_sqlite"), feature = "extra_check"))]
    #[inline]
    fn check_update(&self) -> Result<()> {
        // sqlite3_column_count works for DML but not for DDL (ie ALTER)
        if self.column_count() > 0 {
            return Err(Error::ExecuteReturnedResults);
        }
        Ok(())
    }

    #[cfg(not(feature = "extra_check"))]
    #[inline]
    #[allow(clippy::unnecessary_wraps)]
    fn check_update(&self) -> Result<()> {
        Ok(())
    }

    /// Returns a string containing the SQL text of prepared statement with
    /// bound parameters expanded.
    #[cfg(feature = "modern_sqlite")]
    pub fn expanded_sql(&self) -> Option<String> {
        self.stmt
            .expanded_sql()
            .map(|s| s.to_string_lossy().to_string())
    }

    /// Get the value for one of the status counters for this statement.
    #[inline]
    pub fn get_status(&self, status: StatementStatus) -> i32 {
        self.stmt.get_status(status, false)
    }

    /// Reset the value of one of the status counters for this statement,
    #[inline]
    /// returning the value it had before resetting.
    pub fn reset_status(&self, status: StatementStatus) -> i32 {
        self.stmt.get_status(status, true)
    }

    #[cfg(feature = "extra_check")]
    #[inline]
    pub(crate) fn check_no_tail(&self) -> Result<()> {
        if self.stmt.has_tail() {
            Err(Error::MultipleStatement)
        } else {
            Ok(())
        }
    }

    #[cfg(not(feature = "extra_check"))]
    #[inline]
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn check_no_tail(&self) -> Result<()> {
        Ok(())
    }

    /// Safety: This is unsafe, because using `sqlite3_stmt` after the
    /// connection has closed is illegal, but `RawStatement` does not enforce
    /// this, as it loses our protective `'conn` lifetime bound.
    #[inline]
    pub(crate) unsafe fn into_raw(mut self) -> RawStatement {
        let mut stmt = RawStatement::new(ptr::null_mut(), 0);
        mem::swap(&mut stmt, &mut self.stmt);
        stmt
    }
}

impl fmt::Debug for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sql = if self.stmt.is_null() {
            Ok("")
        } else {
            str::from_utf8(self.stmt.sql().unwrap().to_bytes())
        };
        f.debug_struct("Statement")
            .field("conn", self.conn)
            .field("stmt", &self.stmt)
            .field("sql", &sql)
            .finish()
    }
}

impl Drop for Statement<'_> {
    #[allow(unused_must_use)]
    #[inline]
    fn drop(&mut self) {
        self.finalize_();
    }
}

impl Statement<'_> {
    #[inline]
    pub(super) fn new(conn: &Connection, stmt: RawStatement) -> Statement<'_> {
        Statement { conn, stmt }
    }

    pub(super) fn value_ref(&self, col: usize) -> ValueRef<'_> {
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
                    // Quoting from "Using SQLite" book:
                    // To avoid problems, an application should first extract the desired type using
                    // a sqlite3_column_xxx() function, and then call the
                    // appropriate sqlite3_column_bytes() function.
                    let text = ffi::sqlite3_column_text(raw, col as c_int);
                    let len = ffi::sqlite3_column_bytes(raw, col as c_int);
                    assert!(
                        !text.is_null(),
                        "unexpected SQLITE_TEXT column type with NULL data"
                    );
                    from_raw_parts(text as *const u8, len as usize)
                };

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

    #[inline]
    pub(super) fn step(&self) -> Result<bool> {
        match self.stmt.step() {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            code => Err(self.conn.decode_result(code).unwrap_err()),
        }
    }

    #[inline]
    pub(super) fn reset(&self) -> c_int {
        self.stmt.reset()
    }
}

/// Prepared statement status counters.
///
/// See `https://www.sqlite.org/c3ref/c_stmtstatus_counter.html`
/// for explanations of each.
///
/// Note that depending on your version of SQLite, all of these
/// may not be available.
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatementStatus {
    /// Equivalent to SQLITE_STMTSTATUS_FULLSCAN_STEP
    FullscanStep = 1,
    /// Equivalent to SQLITE_STMTSTATUS_SORT
    Sort = 2,
    /// Equivalent to SQLITE_STMTSTATUS_AUTOINDEX
    AutoIndex = 3,
    /// Equivalent to SQLITE_STMTSTATUS_VM_STEP
    VmStep = 4,
    /// Equivalent to SQLITE_STMTSTATUS_REPREPARE
    RePrepare = 5,
    /// Equivalent to SQLITE_STMTSTATUS_RUN
    Run = 6,
    /// Equivalent to SQLITE_STMTSTATUS_MEMUSED
    MemUsed = 99,
}

#[cfg(test)]
mod test {
    use crate::types::ToSql;
    use crate::{params_from_iter, Connection, Error, Result};

    #[test]
    #[allow(deprecated)]
    fn test_execute_named() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        assert_eq!(
            db.execute_named("INSERT INTO foo(x) VALUES (:x)", &[(":x", &1i32)])?,
            1
        );
        assert_eq!(
            db.execute("INSERT INTO foo(x) VALUES (:x)", &[(":x", &2i32)])?,
            1
        );
        assert_eq!(
            db.execute(
                "INSERT INTO foo(x) VALUES (:x)",
                crate::named_params! {":x": 3i32}
            )?,
            1
        );

        assert_eq!(
            6i32,
            db.query_row_named::<i32, _>(
                "SELECT SUM(x) FROM foo WHERE x > :x",
                &[(":x", &0i32)],
                |r| r.get(0)
            )?
        );
        assert_eq!(
            5i32,
            db.query_row::<i32, _, _>(
                "SELECT SUM(x) FROM foo WHERE x > :x",
                &[(":x", &1i32)],
                |r| r.get(0)
            )?
        );
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_stmt_execute_named() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag \
                   INTEGER)";
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (name) VALUES (:name)")?;
        stmt.execute_named(&[(":name", &"one")])?;

        let mut stmt = db.prepare("SELECT COUNT(*) FROM test WHERE name = :name")?;
        assert_eq!(
            1i32,
            stmt.query_row_named::<i32, _>(&[(":name", &"one")], |r| r.get(0))?
        );
        assert_eq!(
            1i32,
            stmt.query_row::<i32, _, _>(&[(":name", "one")], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_query_named() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = :name")?;
        // legacy `_named` api
        {
            let mut rows = stmt.query_named(&[(":name", &"one")])?;
            let id: Result<i32> = rows.next()?.unwrap().get(0);
            assert_eq!(Ok(1), id);
        }

        // plain api
        {
            let mut rows = stmt.query(&[(":name", "one")])?;
            let id: Result<i32> = rows.next()?.unwrap().get(0);
            assert_eq!(Ok(1), id);
        }
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_query_map_named() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = :name")?;
        // legacy `_named` api
        {
            let mut rows = stmt.query_map_named(&[(":name", &"one")], |row| {
                let id: Result<i32> = row.get(0);
                id.map(|i| 2 * i)
            })?;

            let doubled_id: i32 = rows.next().unwrap()?;
            assert_eq!(2, doubled_id);
        }
        // plain api
        {
            let mut rows = stmt.query_map(&[(":name", "one")], |row| {
                let id: Result<i32> = row.get(0);
                id.map(|i| 2 * i)
            })?;

            let doubled_id: i32 = rows.next().unwrap()?;
            assert_eq!(2, doubled_id);
        }
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_query_and_then_named() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        INSERT INTO test(id, name) VALUES (2, "one");
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = :name ORDER BY id ASC")?;
        let mut rows = stmt.query_and_then_named(&[(":name", &"one")], |row| {
            let id: i32 = row.get(0)?;
            if id == 1 {
                Ok(id)
            } else {
                Err(Error::SqliteSingleThreadedMode)
            }
        })?;

        // first row should be Ok
        let doubled_id: i32 = rows.next().unwrap()?;
        assert_eq!(1, doubled_id);

        // second row should be Err
        #[allow(clippy::match_wild_err_arm)]
        match rows.next().unwrap() {
            Ok(_) => panic!("invalid Ok"),
            Err(Error::SqliteSingleThreadedMode) => (),
            Err(_) => panic!("invalid Err"),
        }
        Ok(())
    }

    #[test]
    fn test_query_and_then_by_name() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, "one");
        INSERT INTO test(id, name) VALUES (2, "one");
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = :name ORDER BY id ASC")?;
        let mut rows = stmt.query_and_then(&[(":name", "one")], |row| {
            let id: i32 = row.get(0)?;
            if id == 1 {
                Ok(id)
            } else {
                Err(Error::SqliteSingleThreadedMode)
            }
        })?;

        // first row should be Ok
        let doubled_id: i32 = rows.next().unwrap()?;
        assert_eq!(1, doubled_id);

        // second row should be Err
        #[allow(clippy::match_wild_err_arm)]
        match rows.next().unwrap() {
            Ok(_) => panic!("invalid Ok"),
            Err(Error::SqliteSingleThreadedMode) => (),
            Err(_) => panic!("invalid Err"),
        }
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn test_unbound_parameters_are_null() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (x, y) VALUES (:x, :y)")?;
        stmt.execute_named(&[(":x", &"one")])?;

        let result: Option<String> =
            db.query_row("SELECT y FROM test WHERE x = 'one'", [], |row| row.get(0))?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_raw_binding() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (name TEXT, value INTEGER)")?;
        {
            let mut stmt = db.prepare("INSERT INTO test (name, value) VALUES (:name, ?3)")?;

            let name_idx = stmt.parameter_index(":name")?.unwrap();
            stmt.raw_bind_parameter(name_idx, "example")?;
            stmt.raw_bind_parameter(3, 50i32)?;
            let n = stmt.raw_execute()?;
            assert_eq!(n, 1);
        }

        {
            let mut stmt = db.prepare("SELECT name, value FROM test WHERE value = ?2")?;
            stmt.raw_bind_parameter(2, 50)?;
            let mut rows = stmt.raw_query();
            {
                let row = rows.next()?.unwrap();
                let name: String = row.get(0)?;
                assert_eq!(name, "example");
                let value: i32 = row.get(1)?;
                assert_eq!(value, 50);
            }
            assert!(rows.next()?.is_none());
        }

        Ok(())
    }

    #[test]
    fn test_unbound_parameters_are_reused() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (x, y) VALUES (:x, :y)")?;
        stmt.execute(&[(":x", "one")])?;
        stmt.execute(&[(":y", "two")])?;

        let result: String =
            db.query_row("SELECT x FROM test WHERE y = 'two'", [], |row| row.get(0))?;
        assert_eq!(result, "one");
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER UNIQUE)")?;
        let mut stmt = db.prepare("INSERT OR IGNORE INTO foo (x) VALUES (?)")?;
        assert_eq!(stmt.insert([1i32])?, 1);
        assert_eq!(stmt.insert([2i32])?, 2);
        match stmt.insert([1i32]).unwrap_err() {
            Error::StatementChangedRows(0) => (),
            err => panic!("Unexpected error {}", err),
        }
        let mut multi = db.prepare("INSERT INTO foo (x) SELECT 3 UNION ALL SELECT 4")?;
        match multi.insert([]).unwrap_err() {
            Error::StatementChangedRows(2) => (),
            err => panic!("Unexpected error {}", err),
        }
        Ok(())
    }

    #[test]
    fn test_insert_different_tables() -> Result<()> {
        // Test for https://github.com/rusqlite/rusqlite/issues/171
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            r"
            CREATE TABLE foo(x INTEGER);
            CREATE TABLE bar(x INTEGER);
        ",
        )?;

        assert_eq!(db.prepare("INSERT INTO foo VALUES (10)")?.insert([])?, 1);
        assert_eq!(db.prepare("INSERT INTO bar VALUES (10)")?.insert([])?, 1);
        Ok(())
    }

    #[test]
    fn test_exists() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT 1 FROM foo WHERE x = ?")?;
        assert!(stmt.exists([1i32])?);
        assert!(stmt.exists([2i32])?);
        assert!(!stmt.exists([0i32])?);
        Ok(())
    }

    #[test]
    fn test_query_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   INSERT INTO foo VALUES(2, 4);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y FROM foo WHERE x = ?")?;
        let y: Result<i64> = stmt.query_row([1i32], |r| r.get(0));
        assert_eq!(3i64, y?);
        Ok(())
    }

    #[test]
    fn test_query_by_column_name() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y FROM foo")?;
        let y: Result<i64> = stmt.query_row([], |r| r.get("y"));
        assert_eq!(3i64, y?);
        Ok(())
    }

    #[test]
    fn test_query_by_column_name_ignore_case() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y as Y FROM foo")?;
        let y: Result<i64> = stmt.query_row([], |r| r.get("y"));
        assert_eq!(3i64, y?);
        Ok(())
    }

    #[test]
    #[cfg(feature = "modern_sqlite")]
    fn test_expanded_sql() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let stmt = db.prepare("SELECT ?")?;
        stmt.bind_parameter(&1, 1)?;
        assert_eq!(Some("SELECT 1".to_owned()), stmt.expanded_sql());
        Ok(())
    }

    #[test]
    fn test_bind_parameters() -> Result<()> {
        let db = Connection::open_in_memory()?;
        // dynamic slice:
        db.query_row(
            "SELECT ?1, ?2, ?3",
            &[&1u8 as &dyn ToSql, &"one", &Some("one")],
            |row| row.get::<_, u8>(0),
        )?;
        // existing collection:
        let data = vec![1, 2, 3];
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| {
            row.get::<_, u8>(0)
        })?;
        db.query_row(
            "SELECT ?1, ?2, ?3",
            params_from_iter(data.as_slice()),
            |row| row.get::<_, u8>(0),
        )?;
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(data), |row| {
            row.get::<_, u8>(0)
        })?;

        use std::collections::BTreeSet;
        let data: BTreeSet<String> = ["one", "two", "three"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| {
            row.get::<_, String>(0)
        })?;

        let data = [0; 3];
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| {
            row.get::<_, u8>(0)
        })?;
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(data.iter()), |row| {
            row.get::<_, u8>(0)
        })?;
        Ok(())
    }

    #[test]
    fn test_empty_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare("")?;
        assert_eq!(0, stmt.column_count());
        assert!(stmt.parameter_index("test").is_ok());
        assert!(stmt.step().is_err());
        stmt.reset();
        assert!(stmt.execute([]).is_err());
        Ok(())
    }

    #[test]
    fn test_comment_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.prepare("/*SELECT 1;*/")?;
        Ok(())
    }

    #[test]
    fn test_comment_and_sql_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let stmt = conn.prepare("/*...*/ SELECT 1;")?;
        assert_eq!(1, stmt.column_count());
        Ok(())
    }

    #[test]
    fn test_semi_colon_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let stmt = conn.prepare(";")?;
        assert_eq!(0, stmt.column_count());
        Ok(())
    }

    #[test]
    fn test_utf16_conversion() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.pragma_update(None, "encoding", &"UTF-16le")?;
        let encoding: String = db.pragma_query_value(None, "encoding", |row| row.get(0))?;
        assert_eq!("UTF-16le", encoding);
        db.execute_batch("CREATE TABLE foo(x TEXT)")?;
        let expected = "テスト";
        db.execute("INSERT INTO foo(x) VALUES (?)", &[&expected])?;
        let actual: String = db.query_row("SELECT x FROM foo", [], |row| row.get(0))?;
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn test_nul_byte() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let expected = "a\x00b";
        let actual: String = db.query_row("SELECT ?", [expected], |row| row.get(0))?;
        assert_eq!(expected, actual);
        Ok(())
    }
}
