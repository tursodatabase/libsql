use fallible_iterator::FallibleIterator;
use fallible_streaming_iterator::FallibleStreamingIterator;
use std::convert;

use super::{Error, Result, Statement};
use crate::types::{FromSql, FromSqlError, ValueRef};

/// An handle for the resulting rows of a query.
pub struct Rows<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
    row: Option<Row<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    fn reset(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            stmt.reset();
        }
    }

    /// Attempt to get the next row from the query. Returns `Ok(Some(Row))` if
    /// there is another row, `Err(...)` if there was an error
    /// getting the next row, and `Ok(None)` if all rows have been retrieved.
    ///
    /// ## Note
    ///
    /// This interface is not compatible with Rust's `Iterator` trait, because
    /// the lifetime of the returned row is tied to the lifetime of `self`.
    /// This is a fallible "streaming iterator". For a more natural interface,
    /// consider using `query_map` or `query_and_then` instead, which
    /// return types that implement `Iterator`.
    #[allow(clippy::should_implement_trait)] // cannot implement Iterator
    pub fn next(&mut self) -> Result<Option<&Row<'stmt>>> {
        self.advance()?;
        Ok((*self).get())
    }

    pub fn map<F, B>(self, f: F) -> Map<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        Map { rows: self, f }
    }

    /// Map over this `Rows`, converting it to a [`MappedRows`], which
    /// implements `Iterator`.
    pub fn mapped<F, B>(self, f: F) -> MappedRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        MappedRows { rows: self, map: f }
    }

    /// Map over this `Rows` with a fallible function, converting it to a
    /// [`AndThenRows`], which implements `Iterator` (instead of
    /// `FallibleStreamingIterator`).
    pub fn and_then<F, T, E>(self, f: F) -> AndThenRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        AndThenRows { rows: self, map: f }
    }
}

impl<'stmt> Rows<'stmt> {
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Rows<'stmt> {
        Rows {
            stmt: Some(stmt),
            row: None,
        }
    }

    pub(crate) fn get_expected_row(&mut self) -> Result<&Row<'stmt>> {
        match self.next()? {
            Some(row) => Ok(row),
            None => Err(Error::QueryReturnedNoRows),
        }
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        self.reset();
    }
}

pub struct Map<'stmt, F> {
    rows: Rows<'stmt>,
    f: F,
}

impl<F, B> FallibleIterator for Map<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<B>,
{
    type Error = Error;
    type Item = B;

    fn next(&mut self) -> Result<Option<B>> {
        match self.rows.next()? {
            Some(v) => Ok(Some((self.f)(v)?)),
            None => Ok(None),
        }
    }
}

/// An iterator over the mapped resulting rows of a query.
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<'stmt, T, F> MappedRows<'stmt, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    pub(crate) fn new(rows: Rows<'stmt>, f: F) -> MappedRows<'stmt, F> {
        MappedRows { rows, map: f }
    }
}

impl<T, F> Iterator for MappedRows<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.and_then(|row| (map)(&row)))
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
    F: FnMut(&Row<'_>) -> Result<T, E>,
{
    pub(crate) fn new(rows: Rows<'stmt>, f: F) -> AndThenRows<'stmt, F> {
        AndThenRows { rows, map: f }
    }
}

impl<T, E, F> Iterator for AndThenRows<'_, F>
where
    E: convert::From<Error>,
    F: FnMut(&Row<'_>) -> Result<T, E>,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.map_err(E::from).and_then(|row| (map)(&row)))
    }
}

impl<'stmt> FallibleStreamingIterator for Rows<'stmt> {
    type Error = Error;
    type Item = Row<'stmt>;

    fn advance(&mut self) -> Result<()> {
        match self.stmt {
            Some(ref stmt) => match stmt.step() {
                Ok(true) => {
                    self.row = Some(Row { stmt });
                    Ok(())
                }
                Ok(false) => {
                    self.reset();
                    self.row = None;
                    Ok(())
                }
                Err(e) => {
                    self.reset();
                    self.row = None;
                    Err(e)
                }
            },
            None => {
                self.row = None;
                Ok(())
            }
        }
    }

    fn get(&self) -> Option<&Row<'stmt>> {
        self.row.as_ref()
    }
}

/// A single result row of a query.
pub struct Row<'stmt> {
    pub(crate) stmt: &'stmt Statement<'stmt>,
}

impl<'stmt> Row<'stmt> {
    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Panics if calling `row.get(idx)` would return an error,
    /// including:
    ///
    /// * If the underlying SQLite column type is not a valid type as a source
    ///   for `T`
    /// * If the underlying SQLite integral value is outside the range
    ///   representable by `T`
    /// * If `idx` is outside the range of columns in the returned query
    pub fn get_unwrap<I: RowIndex, T: FromSql>(&self, idx: I) -> T {
        self.get(idx).unwrap()
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
    ///
    /// If the result type is i128 (which requires the `i128_blob` feature to be
    /// enabled), and the underlying SQLite column is a blob whose size is not
    /// 16 bytes, `Error::InvalidColumnType` will also be returned.
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        let idx = idx.idx(self.stmt)?;
        let value = self.stmt.value_ref(idx);
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => Error::InvalidColumnType(
                idx,
                self.stmt.column_name_unwrap(idx).into(),
                value.data_type(),
            ),
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
            FromSqlError::Other(err) => {
                Error::FromSqlConversionFailure(idx as usize, value.data_type(), err)
            }
            #[cfg(feature = "i128_blob")]
            FromSqlError::InvalidI128Size(_) => Error::InvalidColumnType(
                idx,
                self.stmt.column_name_unwrap(idx).into(),
                value.data_type(),
            ),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => Error::InvalidColumnType(
                idx,
                self.stmt.column_name_unwrap(idx).into(),
                value.data_type(),
            ),
        })
    }

    /// Get the value of a particular column of the result row as a `ValueRef`,
    /// allowing data to be read out of a row without copying.
    ///
    /// This `ValueRef` is valid only as long as this Row, which is enforced by
    /// it's lifetime. This means that while this method is completely safe,
    /// it can be somewhat difficult to use, and most callers will be better
    /// served by `get` or `get`.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column
    /// name for this row.
    pub fn get_raw_checked<I: RowIndex>(&self, idx: I) -> Result<ValueRef<'_>> {
        let idx = idx.idx(self.stmt)?;
        // Narrowing from `ValueRef<'stmt>` (which `self.stmt.value_ref(idx)`
        // returns) to `ValueRef<'a>` is needed because it's only valid until
        // the next call to sqlite3_step.
        let val_ref = self.stmt.value_ref(idx);
        Ok(val_ref)
    }

    /// Get the value of a particular column of the result row as a `ValueRef`,
    /// allowing data to be read out of a row without copying.
    ///
    /// This `ValueRef` is valid only as long as this Row, which is enforced by
    /// it's lifetime. This means that while this method is completely safe,
    /// it can be difficult to use, and most callers will be better served by
    /// `get` or `get`.
    ///
    /// ## Failure
    ///
    /// Panics if calling `row.get_raw_checked(idx)` would return an error,
    /// including:
    ///
    /// * If `idx` is outside the range of columns in the returned query.
    /// * If `idx` is not a valid column name for this row.
    pub fn get_raw<I: RowIndex>(&self, idx: I) -> ValueRef<'_> {
        self.get_raw_checked(idx).unwrap()
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize> {
        if *self >= stmt.column_count() {
            Err(Error::InvalidColumnIndex(*self))
        } else {
            Ok(*self)
        }
    }
}

impl RowIndex for &'_ str {
    #[inline]
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize> {
        stmt.column_index(*self)
    }
}
