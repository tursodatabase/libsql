use std::str;

use crate::{Error, Result, Row, Rows, Statement};

/// Information about a column of a SQLite query.
#[derive(Debug)]
pub struct Column<'stmt> {
    name: &'stmt str,
    decl_type: Option<&'stmt str>,
}

impl Column<'_> {
    /// Returns the name of the column.
    #[inline]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the type of the column (`None` for expression).
    #[inline]
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type
    }
}

impl Statement<'_> {
    /// Get all the column names in the result set of the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    pub fn column_names(&self) -> Vec<&str> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n as usize);
        for i in 0..n {
            let s = self.column_name_unwrap(i);
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
    #[inline]
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Check that column name reference lifetime is limited:
    /// https://www.sqlite.org/c3ref/column_name.html
    /// > The returned string pointer is valid...
    ///
    /// `column_name` reference can become invalid if `stmt` is reprepared
    /// (because of schema change) when `query_row` is called. So we assert
    /// that a compilation error happens if this reference is kept alive:
    /// ```compile_fail
    /// use rusqlite::{Connection, Result};
    /// fn main() -> Result<()> {
    ///     let db = Connection::open_in_memory()?;
    ///     let mut stmt = db.prepare("SELECT 1 as x")?;
    ///     let column_name = stmt.column_name(0)?;
    ///     let x = stmt.query_row([], |r| r.get::<_, i64>(0))?; // E0502
    ///     assert_eq!(1, x);
    ///     assert_eq!("x", column_name);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub(super) fn column_name_unwrap(&self, col: usize) -> &str {
        // Just panic if the bounds are wrong for now, we never call this
        // without checking first.
        self.column_name(col).expect("Column out of bounds")
    }

    /// Returns the name assigned to a particular column in the result set
    /// returned by the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Panics when column name is not valid UTF-8.
    #[inline]
    pub fn column_name(&self, col: usize) -> Result<&str> {
        self.stmt
            .column_name(col)
            .ok_or(Error::InvalidColumnIndex(col))
            .map(|slice| {
                str::from_utf8(slice.to_bytes()).expect("Invalid UTF-8 sequence in column name")
            })
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
    #[inline]
    pub fn column_index(&self, name: &str) -> Result<usize> {
        let bytes = name.as_bytes();
        let n = self.column_count();
        for i in 0..n {
            // Note: `column_name` is only fallible if `i` is out of bounds,
            // which we've already checked.
            if bytes.eq_ignore_ascii_case(self.stmt.column_name(i).unwrap().to_bytes()) {
                return Ok(i);
            }
        }
        Err(Error::InvalidColumnName(String::from(name)))
    }

    /// Returns a slice describing the columns of the result of the query.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    #[cfg(feature = "column_decltype")]
    pub fn columns(&self) -> Vec<Column> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n as usize);
        for i in 0..n {
            let name = self.column_name_unwrap(i);
            let slice = self.stmt.column_decltype(i);
            let decl_type = slice.map(|s| {
                str::from_utf8(s.to_bytes()).expect("Invalid UTF-8 sequence in column declaration")
            });
            cols.push(Column { name, decl_type });
        }
        cols
    }
}

impl<'stmt> Rows<'stmt> {
    /// Get all the column names.
    #[inline]
    pub fn column_names(&self) -> Option<Vec<&str>> {
        self.stmt.map(Statement::column_names)
    }

    /// Return the number of columns.
    #[inline]
    pub fn column_count(&self) -> Option<usize> {
        self.stmt.map(Statement::column_count)
    }

    /// Return the name of the column.
    #[inline]
    pub fn column_name(&self, col: usize) -> Option<Result<&str>> {
        self.stmt.map(|stmt| stmt.column_name(col))
    }

    /// Return the index of the column.
    #[inline]
    pub fn column_index(&self, name: &str) -> Option<Result<usize>> {
        self.stmt.map(|stmt| stmt.column_index(name))
    }

    /// Returns a slice describing the columns of the Rows.
    #[inline]
    #[cfg(feature = "column_decltype")]
    pub fn columns(&self) -> Option<Vec<Column>> {
        self.stmt.map(Statement::columns)
    }
}

impl<'stmt> Row<'stmt> {
    /// Get all the column names of the Row.
    #[inline]
    pub fn column_names(&self) -> Vec<&str> {
        self.stmt.column_names()
    }

    /// Return the number of columns in the current row.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Return the name of the column.
    #[inline]
    pub fn column_name(&self, col: usize) -> Result<&str> {
        self.stmt.column_name(col)
    }

    /// Return the index of the column.
    #[inline]
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.stmt.column_index(name)
    }

    /// Returns a slice describing the columns of the Row.
    #[inline]
    #[cfg(feature = "column_decltype")]
    pub fn columns(&self) -> Vec<Column> {
        self.stmt.columns()
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};

    #[test]
    #[cfg(feature = "column_decltype")]
    fn test_columns() -> Result<()> {
        use super::Column;

        let db = Connection::open_in_memory()?;
        let query = db.prepare("SELECT * FROM sqlite_master")?;
        let columns = query.columns();
        let column_names: Vec<&str> = columns.iter().map(Column::name).collect();
        assert_eq!(
            column_names.as_slice(),
            &["type", "name", "tbl_name", "rootpage", "sql"]
        );
        let column_types: Vec<Option<&str>> = columns.iter().map(Column::decl_type).collect();
        assert_eq!(
            &column_types[..3],
            &[Some("text"), Some("text"), Some("text"),]
        );
        Ok(())
    }

    #[test]
    fn test_column_name_in_error() -> Result<()> {
        use crate::{types::Type, Error};
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "BEGIN;
             CREATE TABLE foo(x INTEGER, y TEXT);
             INSERT INTO foo VALUES(4, NULL);
             END;",
        )?;
        let mut stmt = db.prepare("SELECT x as renamed, y FROM foo")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        match row.get::<_, String>(0).unwrap_err() {
            Error::InvalidColumnType(idx, name, ty) => {
                assert_eq!(idx, 0);
                assert_eq!(name, "renamed");
                assert_eq!(ty, Type::Integer);
            }
            e => {
                panic!("Unexpected error type: {:?}", e);
            }
        }
        match row.get::<_, String>("y").unwrap_err() {
            Error::InvalidColumnType(idx, name, ty) => {
                assert_eq!(idx, 1);
                assert_eq!(name, "y");
                assert_eq!(ty, Type::Null);
            }
            e => {
                panic!("Unexpected error type: {:?}", e);
            }
        }
        Ok(())
    }

    /// `column_name` reference should stay valid until `stmt` is reprepared (or
    /// reset) even if DB schema is altered (SQLite documentation is
    /// ambiguous here because it says reference "is valid until (...) the next
    /// call to sqlite3_column_name() or sqlite3_column_name16() on the same
    /// column.". We assume that reference is valid if only `sqlite3_column_name()` is used):
    #[test]
    #[cfg(feature = "modern_sqlite")]
    fn test_column_name_reference() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE y (x);")?;
        let stmt = db.prepare("SELECT x FROM y;")?;
        let column_name = stmt.column_name(0)?;
        assert_eq!("x", column_name);
        db.execute_batch("ALTER TABLE y RENAME COLUMN x TO z;")?;
        // column name is not refreshed until statement is re-prepared
        let same_column_name = stmt.column_name(0)?;
        assert_eq!(same_column_name, column_name);
        Ok(())
    }
}
