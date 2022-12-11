use fallible_iterator::FallibleIterator;
use fallible_streaming_iterator::FallibleStreamingIterator;
use std::convert;

use super::{Error, Result, Statement};
use crate::types::{FromSql, FromSqlError, ValueRef};

/// An handle for the resulting rows of a query.
#[must_use = "Rows is lazy and will do nothing unless consumed"]
pub struct Rows<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
    row: Option<Row<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    #[inline]
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
    /// consider using [`query_map`](crate::Statement::query_map) or
    /// [`query_and_then`](crate::Statement::query_and_then) instead, which
    /// return types that implement `Iterator`.
    #[allow(clippy::should_implement_trait)] // cannot implement Iterator
    #[inline]
    pub fn next(&mut self) -> Result<Option<&Row<'stmt>>> {
        self.advance()?;
        Ok((*self).get())
    }

    /// Map over this `Rows`, converting it to a [`Map`], which
    /// implements `FallibleIterator`.
    /// ```rust,no_run
    /// use fallible_iterator::FallibleIterator;
    /// # use rusqlite::{Result, Statement};
    /// fn query(stmt: &mut Statement) -> Result<Vec<i64>> {
    ///     let rows = stmt.query([])?;
    ///     rows.map(|r| r.get(0)).collect()
    /// }
    /// ```
    // FIXME Hide FallibleStreamingIterator::map
    #[inline]
    pub fn map<F, B>(self, f: F) -> Map<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        Map { rows: self, f }
    }

    /// Map over this `Rows`, converting it to a [`MappedRows`], which
    /// implements `Iterator`.
    #[inline]
    pub fn mapped<F, B>(self, f: F) -> MappedRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        MappedRows { rows: self, map: f }
    }

    /// Map over this `Rows` with a fallible function, converting it to a
    /// [`AndThenRows`], which implements `Iterator` (instead of
    /// `FallibleStreamingIterator`).
    #[inline]
    pub fn and_then<F, T, E>(self, f: F) -> AndThenRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        AndThenRows { rows: self, map: f }
    }

    /// Give access to the underlying statement
    #[must_use]
    pub fn as_ref(&self) -> Option<&Statement<'stmt>> {
        self.stmt
    }
}

impl<'stmt> Rows<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Rows<'stmt> {
        Rows {
            stmt: Some(stmt),
            row: None,
        }
    }

    #[inline]
    pub(crate) fn get_expected_row(&mut self) -> Result<&Row<'stmt>> {
        match self.next()? {
            Some(row) => Ok(row),
            None => Err(Error::QueryReturnedNoRows),
        }
    }
}

impl Drop for Rows<'_> {
    #[inline]
    fn drop(&mut self) {
        self.reset();
    }
}

/// `F` is used to transform the _streaming_ iterator into a _fallible_
/// iterator.
#[must_use = "iterators are lazy and do nothing unless consumed"]
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

    #[inline]
    fn next(&mut self) -> Result<Option<B>> {
        match self.rows.next()? {
            Some(v) => Ok(Some((self.f)(v)?)),
            None => Ok(None),
        }
    }
}

/// An iterator over the mapped resulting rows of a query.
///
/// `F` is used to transform the _streaming_ iterator into a _standard_
/// iterator.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, F> Iterator for MappedRows<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.and_then(map))
    }
}

/// An iterator over the mapped resulting rows of a query, with an Error type
/// unifying with Error.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct AndThenRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, E, F> Iterator for AndThenRows<'_, F>
where
    E: From<Error>,
    F: FnMut(&Row<'_>) -> Result<T, E>,
{
    type Item = Result<T, E>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.map_err(E::from).and_then(map))
    }
}

/// `FallibleStreamingIterator` differs from the standard library's `Iterator`
/// in two ways:
/// * each call to `next` (`sqlite3_step`) can fail.
/// * returned `Row` is valid until `next` is called again or `Statement` is
///   reset or finalized.
///
/// While these iterators cannot be used with Rust `for` loops, `while let`
/// loops offer a similar level of ergonomics:
/// ```rust,no_run
/// # use rusqlite::{Result, Statement};
/// fn query(stmt: &mut Statement) -> Result<()> {
///     let mut rows = stmt.query([])?;
///     while let Some(row) = rows.next()? {
///         // scan columns value
///     }
///     Ok(())
/// }
/// ```
impl<'stmt> FallibleStreamingIterator for Rows<'stmt> {
    type Error = Error;
    type Item = Row<'stmt>;

    #[inline]
    fn advance(&mut self) -> Result<()> {
        if let Some(stmt) = self.stmt {
            match stmt.step() {
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
            }
        } else {
            self.row = None;
            Ok(())
        }
    }

    #[inline]
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
    /// Panics if calling [`row.get(idx)`](Row::get) would return an error,
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
                Error::FromSqlConversionFailure(idx, value.data_type(), err)
            }
            FromSqlError::InvalidBlobSize { .. } => {
                Error::FromSqlConversionFailure(idx, value.data_type(), Box::new(err))
            }
        })
    }

    /// Get the value of a particular column of the result row as a `ValueRef`,
    /// allowing data to be read out of a row without copying.
    ///
    /// This `ValueRef` is valid only as long as this Row, which is enforced by
    /// it's lifetime. This means that while this method is completely safe,
    /// it can be somewhat difficult to use, and most callers will be better
    /// served by [`get`](Row::get) or [`get_unwrap`](Row::get_unwrap).
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column
    /// name for this row.
    pub fn get_ref<I: RowIndex>(&self, idx: I) -> Result<ValueRef<'_>> {
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
    /// [`get`](Row::get) or [`get_unwrap`](Row::get_unwrap).
    ///
    /// ## Failure
    ///
    /// Panics if calling [`row.get_ref(idx)`](Row::get_ref) would return an
    /// error, including:
    ///
    /// * If `idx` is outside the range of columns in the returned query.
    /// * If `idx` is not a valid column name for this row.
    pub fn get_ref_unwrap<I: RowIndex>(&self, idx: I) -> ValueRef<'_> {
        self.get_ref(idx).unwrap()
    }
}

impl<'stmt> AsRef<Statement<'stmt>> for Row<'stmt> {
    fn as_ref(&self) -> &Statement<'stmt> {
        self.stmt
    }
}

/// Debug `Row` like an ordered `Map<Result<&str>, Result<(Type, ValueRef)>>`
/// with column name as key except that for `Type::Blob` only its size is
/// printed (not its content).
impl<'stmt> std::fmt::Debug for Row<'stmt> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dm = f.debug_map();
        for c in 0..self.stmt.column_count() {
            let name = self.stmt.column_name(c);
            dm.key(&name);
            let value = self.get_ref(c);
            match value {
                Ok(value) => {
                    let dt = value.data_type();
                    match value {
                        ValueRef::Null => {
                            dm.value(&(dt, ()));
                        }
                        ValueRef::Integer(i) => {
                            dm.value(&(dt, i));
                        }
                        ValueRef::Real(f) => {
                            dm.value(&(dt, f));
                        }
                        ValueRef::Text(s) => {
                            dm.value(&(dt, String::from_utf8_lossy(s)));
                        }
                        ValueRef::Blob(b) => {
                            dm.value(&(dt, b.len()));
                        }
                    }
                }
                Err(ref _err) => {
                    dm.value(&value);
                }
            }
        }
        dm.finish()
    }
}

mod sealed {
    /// This trait exists just to ensure that the only impls of `trait Params`
    /// that are allowed are ones in this crate.
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
}

/// A trait implemented by types that can index into columns of a row.
///
/// It is only implemented for `usize` and `&str`.
pub trait RowIndex: sealed::Sealed {
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
        stmt.column_index(self)
    }
}

macro_rules! tuple_try_from_row {
    ($($field:ident),*) => {
        impl<'a, $($field,)*> convert::TryFrom<&'a Row<'a>> for ($($field,)*) where $($field: FromSql,)* {
            type Error = crate::Error;

            // we end with index += 1, which rustc warns about
            // unused_variables and unused_mut are allowed for ()
            #[allow(unused_assignments, unused_variables, unused_mut)]
            fn try_from(row: &'a Row<'a>) -> Result<Self> {
                let mut index = 0;
                $(
                    #[allow(non_snake_case)]
                    let $field = row.get::<_, $field>(index)?;
                    index += 1;
                )*
                Ok(($($field,)*))
            }
        }
    }
}

macro_rules! tuples_try_from_row {
    () => {
        // not very useful, but maybe some other macro users will find this helpful
        tuple_try_from_row!();
    };
    ($first:ident $(, $remaining:ident)*) => {
        tuple_try_from_row!($first $(, $remaining)*);
        tuples_try_from_row!($($remaining),*);
    };
}

tuples_try_from_row!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

#[cfg(test)]
mod tests {
    #![allow(clippy::redundant_closure)] // false positives due to lifetime issues; clippy issue #5594
    use crate::{Connection, Result};

    #[test]
    fn test_try_from_row_for_tuple_1() -> Result<()> {
        use crate::ToSql;
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE test (a INTEGER)",
            crate::params_from_iter(std::iter::empty::<&dyn ToSql>()),
        )?;
        conn.execute("INSERT INTO test VALUES (42)", [])?;
        let val = conn.query_row("SELECT a FROM test", [], |row| <(u32,)>::try_from(row))?;
        assert_eq!(val, (42,));
        let fail = conn.query_row("SELECT a FROM test", [], |row| <(u32, u32)>::try_from(row));
        fail.unwrap_err();
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_2() -> Result<()> {
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        conn.execute("CREATE TABLE test (a INTEGER, b INTEGER)", [])?;
        conn.execute("INSERT INTO test VALUES (42, 47)", [])?;
        let val = conn.query_row("SELECT a, b FROM test", [], |row| {
            <(u32, u32)>::try_from(row)
        })?;
        assert_eq!(val, (42, 47));
        let fail = conn.query_row("SELECT a, b FROM test", [], |row| {
            <(u32, u32, u32)>::try_from(row)
        });
        fail.unwrap_err();
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_16() -> Result<()> {
        use std::convert::TryFrom;

        let create_table = "CREATE TABLE test (
            a INTEGER,
            b INTEGER,
            c INTEGER,
            d INTEGER,
            e INTEGER,
            f INTEGER,
            g INTEGER,
            h INTEGER,
            i INTEGER,
            j INTEGER,
            k INTEGER,
            l INTEGER,
            m INTEGER,
            n INTEGER,
            o INTEGER,
            p INTEGER
        )";

        let insert_values = "INSERT INTO test VALUES (
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15
        )";

        type BigTuple = (
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
        );

        let conn = Connection::open_in_memory()?;
        conn.execute(create_table, [])?;
        conn.execute(insert_values, [])?;
        let val = conn.query_row("SELECT * FROM test", [], |row| BigTuple::try_from(row))?;
        // Debug is not implemented for tuples of 16
        assert_eq!(val.0, 0);
        assert_eq!(val.1, 1);
        assert_eq!(val.2, 2);
        assert_eq!(val.3, 3);
        assert_eq!(val.4, 4);
        assert_eq!(val.5, 5);
        assert_eq!(val.6, 6);
        assert_eq!(val.7, 7);
        assert_eq!(val.8, 8);
        assert_eq!(val.9, 9);
        assert_eq!(val.10, 10);
        assert_eq!(val.11, 11);
        assert_eq!(val.12, 12);
        assert_eq!(val.13, 13);
        assert_eq!(val.14, 14);
        assert_eq!(val.15, 15);

        // We don't test one bigger because it's unimplemented
        Ok(())
    }
}
