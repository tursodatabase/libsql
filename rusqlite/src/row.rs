use std::marker::PhantomData;
use std::{convert, result};

use super::{Error, Result, Statement};
use types::{FromSql, FromSqlError};

/// An handle for the resulting rows of a query.
pub struct Rows<'stmt> {
    stmt: Option<&'stmt Statement<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    fn reset(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            stmt.reset();
        }
    }

    /// Attempt to get the next row from the query. Returns `Some(Ok(Row))` if
    /// there is another row, `Some(Err(...))` if there was an error
    /// getting the next row, and `None` if all rows have been retrieved.
    ///
    /// ## Note
    ///
    /// This interface is not compatible with Rust's `Iterator` trait, because
    /// the lifetime of the returned row is tied to the lifetime of `self`.
    /// This is a "streaming iterator". For a more natural interface,
    /// consider using `query_map` or `query_and_then` instead, which
    /// return types that implement `Iterator`.
    #[cfg_attr(feature = "cargo-clippy", allow(should_implement_trait))] // cannot implement Iterator
    pub fn next<'a>(&'a mut self) -> Option<Result<Row<'a, 'stmt>>> {
        self.stmt.and_then(|stmt| match stmt.step() {
            Ok(true) => Some(Ok(Row {
                stmt,
                phantom: PhantomData,
            })),
            Ok(false) => {
                self.reset();
                None
            }
            Err(err) => {
                self.reset();
                Some(Err(err))
            }
        })
    }
}

impl<'stmt> Rows<'stmt> {
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Rows<'stmt> {
        Rows { stmt: Some(stmt) }
    }

    pub(crate) fn get_expected_row<'a>(&'a mut self) -> Result<Row<'a, 'stmt>> {
        match self.next() {
            Some(row) => row,
            None => Err(Error::QueryReturnedNoRows),
        }
    }
}

impl<'stmt> Drop for Rows<'stmt> {
    fn drop(&mut self) {
        self.reset();
    }
}

/// An iterator over the mapped resulting rows of a query.
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<'stmt, T, F> MappedRows<'stmt, F>
where
    F: FnMut(&Row) -> T,
{
    pub(crate) fn new(rows: Rows<'stmt>, f: F) -> MappedRows<'stmt, F> {
        MappedRows { rows, map: f }
    }
}

impl<'conn, T, F> Iterator for MappedRows<'conn, F>
where
    F: FnMut(&Row) -> T,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows
            .next()
            .map(|row_result| row_result.map(|row| (map)(&row)))
    }
}

/// An iterator over the mapped resulting rows of a query, with an Error type
/// unifying with Error.
pub struct AndThenRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<'stmt, T, E, F> AndThenRows<'stmt, F>
where
    F: FnMut(&Row) -> result::Result<T, E>,
{
    pub(crate) fn new(rows: Rows<'stmt>, f: F) -> AndThenRows<'stmt, F> {
        AndThenRows { rows, map: f }
    }
}

impl<'stmt, T, E, F> Iterator for AndThenRows<'stmt, F>
where
    E: convert::From<Error>,
    F: FnMut(&Row) -> result::Result<T, E>,
{
    type Item = result::Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .map(|row_result| row_result.map_err(E::from).and_then(|row| (map)(&row)))
    }
}

/// A single result row of a query.
pub struct Row<'a, 'stmt> {
    stmt: &'stmt Statement<'stmt>,
    phantom: PhantomData<&'a ()>,
}

impl<'a, 'stmt> Row<'a, 'stmt> {
    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Panics if calling `row.get_checked(idx)` would return an error,
    /// including:
    ///
    ///    * If the underlying SQLite column type is not a valid type as a
    ///      source for `T`
    ///    * If the underlying SQLite integral value is
    ///      outside the range representable by `T`
    ///    * If `idx` is outside the range of columns in the
    ///      returned query
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> T {
        self.get_checked(idx).unwrap()
    }

    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnType` if the underlying SQLite column
    /// type is not a valid type as a source for `T`.
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column
    /// name for this row.
    pub fn get_checked<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        let idx = try!(idx.idx(self.stmt));
        let value = self.stmt.value_ref(idx);
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => Error::InvalidColumnType(idx, value.data_type()),
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
            FromSqlError::Other(err) => {
                Error::FromSqlConversionFailure(idx as usize, value.data_type(), err)
            }
        })
    }

    /// Return the number of columns in the current row.
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement) -> Result<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Result<usize> {
        if *self >= stmt.column_count() {
            Err(Error::InvalidColumnIndex(*self))
        } else {
            Ok(*self)
        }
    }
}

impl<'a> RowIndex for &'a str {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Result<usize> {
        stmt.column_index(*self)
    }
}
