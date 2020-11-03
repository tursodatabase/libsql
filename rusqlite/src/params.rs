use crate::{Result, Statement, ToSql};

mod sealed {
    /// This trait exists just to ensure that the only impls of `trait Params`
    /// that are allowed are ones in this crate.
    pub trait Sealed {}
}
// must not be `pub use`.
use sealed::Sealed;

/// Trait used for parameter sets passed into SQL statements/queries.
///
/// Currently, this trait can only be implemented inside this crate.
pub trait Params: Sealed {
    /// Binds the parameters to the statement. It is unlikely calling this
    /// explicitly will do what you want. Please use `Statement::query` or
    /// similar directly.
    // For now, just hide the function in the docs...
    #[doc(hidden)]
    fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()>;
}

// Explicitly impl for empty array. Critically, for `conn.execute([])` to be
// unambiguous, this must be the *only* implementation for an empty array. This
// avoids `NO_PARAMS` being a necessary part of the API.
impl Sealed for [&dyn ToSql; 0] {}
impl Params for [&dyn ToSql; 0] {
    #[inline]
    fn bind_in(self, _: &mut Statement<'_>) -> Result<()> {
        Ok(())
    }
}

impl Sealed for &[&dyn ToSql] {}
impl Params for &[&dyn ToSql] {
    #[inline]
    fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters(self)
    }
}

impl Sealed for &[(&str, &dyn ToSql)] {}
impl Params for &[(&str, &dyn ToSql)] {
    #[inline]
    fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters_named(self)
    }
}

macro_rules! impl_for_array_ref {
    ($($N:literal)+) => {$(
        impl<T: ToSql + ?Sized> Sealed for &[&T; $N] {}
        impl<T: ToSql + ?Sized> Params for &[&T; $N] {
            fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters(self)
            }
        }
        impl<T: ToSql + ?Sized> Sealed for &[(&str, &T); $N] {}
        impl<T: ToSql + ?Sized> Params for &[(&str, &T); $N] {
            fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters_named(self)
            }
        }
        impl<T: ToSql> Sealed for [T; $N] {}
        impl<T: ToSql> Params for [T; $N] {
            fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters(&self)
            }
        }
    )+};
}
impl_for_array_ref!(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17
    18 19 20 21 22 23 24 25 26 27 29 30 31 32
);

/// Adapter type which allows any iterator over [`ToSql`] values to implement
/// [`Params`].
///
/// This struct is created by the [`params_from_iter`] function.
///
/// This can be useful if you have something like an `&[String]` (of unknown
/// length), and you want to use them with an API that wants something
/// implementing `Params`. This way, you can avoid having to allocate storage
/// for something like a `&[&dyn ToSql]`.
///
/// This essentially is only ever actually needed when dynamically generating
/// SQL — static SQL (by definition) has the number of parameters known
/// statically. As dynamically generating SQL is itself pretty advanced, this
/// API is itself for advanced use cases (See "Realistic use case" in the
/// examples).
///
/// # Example
///
/// ## Basic usage
///
/// ```rust,no_run
/// use rusqlite::{Connection, Result, params_from_iter};
/// use std::collections::BTreeSet;
///
/// fn query(conn: &Connection, ids: &BTreeSet<String>) -> Result<()> {
///     assert_eq!(ids.len(), 3, "Unrealistic sample code");
///
///     let mut stmt = conn.prepare("SELECT * FROM users WHERE id IN (?, ?, ?)")?;
///     let _rows = stmt.query(params_from_iter(ids.iter()))?;
///
///     // use _rows...
///     Ok(())
/// }
/// ```
///
/// ## Realistic use case
///
/// Here's how you'd use `ParamsFromIter` to call a function with no `_iter`
/// equivalent, e.g. [`Statement::exists`].
///
/// ```rust,no_run
/// use rusqlite::{Connection, Result};
///
/// pub fn any_active_users(conn: &Connection, usernames: &[String]) -> Result<bool> {
///     if usernames.is_empty() {
///         return Ok(false);
///     }
///
///     // Note: `repeat_vars` never returns anything attacker-controlled, so
///     // it's fine to use it in a dynamically-built SQL string.
///     let vars = repeat_vars(usernames.len());
///
///     let sql = format!(
///         // In practice this would probably be better as an `EXISTS` query.
///         "SELECT 1 FROM user WHERE is_active AND name IN ({}) LIMIT 1",
///         vars,
///     );
///     let mut stmt = conn.prepare(&sql)?;
///     stmt.exists(rusqlite::params_from_iter(usernames))
/// }
///
/// // Helper function to return a comma-separated sequence of `?`.
/// // - `repeat_vars(0) => panic!(...)`
/// // - `repeat_vars(1) => "?"`
/// // - `repeat_vars(2) => "?,?"`
/// // - `repeat_vars(3) => "?,?,?"`
/// // - ...
/// fn repeat_vars(count: usize) -> String {
///     assert_ne!(count, 0);
///     let mut s = "?,".repeat(count);
///     // Remove trailing comma
///     s.pop();
///     s
/// }
/// ```
///
/// That is fairly complex, and even so would need even more work to be fully
/// production-ready:
///
/// - production code should ensure `usernames` isn't so large that it will
///   surpass [`conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER)`][limits])
///   (chunking if too large).
///
/// - `repeat_vars` can be implemented in a way that avoids needing to allocate
///   a String.
///
/// [limits]: crate::Connection::limit
///
/// This complexity reflects the fact that `ParamsFromIter` is mainly intended
/// for advanced use cases — most of the time you should know how many
/// parameters you have statically.
#[derive(Clone, Debug)]
pub struct ParamsFromIter<I>(I);

/// Constructor function for a [`ParamsFromIter`]. See its documentation for
/// more.
#[inline]
pub fn params_from_iter<I>(iter: I) -> ParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    ParamsFromIter(iter)
}

impl<I> Sealed for ParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
}

impl<I> Params for ParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    #[inline]
    fn bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters(self.0)
    }
}
