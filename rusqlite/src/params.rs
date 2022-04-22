use crate::{Result, Statement, ToSql};

mod sealed {
    /// This trait exists just to ensure that the only impls of `trait Params`
    /// that are allowed are ones in this crate.
    pub trait Sealed {}
}
use sealed::Sealed;

/// Trait used for [sets of parameter][params] passed into SQL
/// statements/queries.
///
/// [params]: https://www.sqlite.org/c3ref/bind_blob.html
///
/// Note: Currently, this trait can only be implemented inside this crate.
/// Additionally, it's methods (which are `doc(hidden)`) should currently not be
/// considered part of the stable API, although it's possible they will
/// stabilize in the future.
///
/// # Passing parameters to SQLite
///
/// Many functions in this library let you pass parameters to SQLite. Doing this
/// lets you avoid any risk of SQL injection, and is simpler than escaping
/// things manually. Aside from deprecated functions and a few helpers, this is
/// indicated by the function taking a generic argument that implements `Params`
/// (this trait).
///
/// ## Positional parameters
///
/// For cases where you want to pass a list of parameters where the number of
/// parameters is known at compile time, this can be done in one of the
/// following ways:
///
/// - For small lists of parameters up to 16 items, they may alternatively be
///   passed as a tuple, as in `thing.query((1, "foo"))`.
///
///     This is somewhat inconvenient for a single item, since you need a
///     weird-looking trailing comma: `thing.query(("example",))`. That case is
///     perhaps more cleanly expressed as `thing.query(["example"])`.
///
/// - Using the [`rusqlite::params!`](crate::params!) macro, e.g.
///   `thing.query(rusqlite::params![1, "foo", bar])`. This is mostly useful for
///   heterogeneous lists where the number of parameters greater than 16, or
///   homogenous lists of paramters where the number of parameters exceeds 32.
///
/// - For small homogeneous lists of parameters, they can either be passed as:
///
///     - an array, as in `thing.query([1i32, 2, 3, 4])` or `thing.query(["foo",
///       "bar", "baz"])`.
///
///     - a reference to an array of references, as in `thing.query(&["foo",
///       "bar", "baz"])` or `thing.query(&[&1i32, &2, &3])`.
///
///         (Note: in this case we don't implement this for slices for coherence
///         reasons, so it really is only for the "reference to array" types —
///         hence why the number of parameters must be <= 32 or you need to
///         reach for `rusqlite::params!`)
///
///     Unfortunately, in the current design it's not possible to allow this for
///     references to arrays of non-references (e.g. `&[1i32, 2, 3]`). Code like
///     this should instead either use `params!`, an array literal, a `&[&dyn
///     ToSql]` or if none of those work, [`ParamsFromIter`].
///
/// - As a slice of `ToSql` trait object references, e.g. `&[&dyn ToSql]`. This
///   is mostly useful for passing parameter lists around as arguments without
///   having every function take a generic `P: Params`.
///
/// ### Example (positional)
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, params};
/// fn update_rows(conn: &Connection) -> Result<()> {
///     let mut stmt = conn.prepare("INSERT INTO test (a, b) VALUES (?, ?)")?;
///
///     // Using a tuple:
///     stmt.execute((0, "foobar"))?;
///
///     // Using `rusqlite::params!`:
///     stmt.execute(params![1i32, "blah"])?;
///
///     // array literal — non-references
///     stmt.execute([2i32, 3i32])?;
///
///     // array literal — references
///     stmt.execute(["foo", "bar"])?;
///
///     // Slice literal, references:
///     stmt.execute(&[&2i32, &3i32])?;
///
///     // Note: The types behind the references don't have to be `Sized`
///     stmt.execute(&["foo", "bar"])?;
///
///     // However, this doesn't work (see above):
///     // stmt.execute(&[1i32, 2i32])?;
///     Ok(())
/// }
/// ```
///
/// ## Named parameters
///
/// SQLite lets you name parameters using a number of conventions (":foo",
/// "@foo", "$foo"). You can pass named parameters in to SQLite using rusqlite
/// in a few ways:
///
/// - Using the [`rusqlite::named_params!`](crate::named_params!) macro, as in
///   `stmt.execute(named_params!{ ":name": "foo", ":age": 99 })`. Similar to
///   the `params` macro, this is most useful for heterogeneous lists of
///   parameters, or lists where the number of parameters exceeds 32.
///
/// - As a slice of `&[(&str, &dyn ToSql)]`. This is what essentially all of
///   these boil down to in the end, conceptually at least. In theory you can
///   pass this as `stmt`.
///
/// - As array references, similar to the positional params. This looks like
///   `thing.query(&[(":foo", &1i32), (":bar", &2i32)])` or
///   `thing.query(&[(":foo", "abc"), (":bar", "def")])`.
///
/// Note: Unbound named parameters will be left to the value they previously
/// were bound with, falling back to `NULL` for parameters which have never been
/// bound.
///
/// ### Example (named)
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, named_params};
/// fn insert(conn: &Connection) -> Result<()> {
///     let mut stmt = conn.prepare("INSERT INTO test (key, value) VALUES (:key, :value)")?;
///     // Using `rusqlite::params!`:
///     stmt.execute(named_params! { ":key": "one", ":val": 2 })?;
///     // Alternatively:
///     stmt.execute(&[(":key", "three"), (":val", "four")])?;
///     // Or:
///     stmt.execute(&[(":key", &100), (":val", &200)])?;
///     Ok(())
/// }
/// ```
///
/// ## No parameters
///
/// You can just use an empty tuple or the empty array literal to run a query
/// that accepts no parameters. (The `rusqlite::NO_PARAMS` constant which was
/// common in previous versions of this library is no longer needed, and is now
/// deprecated).
///
/// ### Example (no parameters)
///
/// The empty tuple:
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, params};
/// fn delete_all_users(conn: &Connection) -> Result<()> {
///     // You may also use `()`.
///     conn.execute("DELETE FROM users", ())?;
///     Ok(())
/// }
/// ```
///
/// The empty array:
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, params};
/// fn delete_all_users(conn: &Connection) -> Result<()> {
///     // Just use an empty array (e.g. `[]`) for no params.
///     conn.execute("DELETE FROM users", [])?;
///     Ok(())
/// }
/// ```
///
/// ## Dynamic parameter list
///
/// If you have a number of parameters which is unknown at compile time (for
/// example, building a dynamic query at runtime), you have two choices:
///
/// - Use a `&[&dyn ToSql]`. This is often annoying to construct if you don't
///   already have this type on-hand.
/// - Use the [`ParamsFromIter`] type. This essentially lets you wrap an
///   iterator some `T: ToSql` with something that implements `Params`. The
///   usage of this looks like `rusqlite::params_from_iter(something)`.
///
/// A lot of the considerations here are similar either way, so you should see
/// the [`ParamsFromIter`] documentation for more info / examples.
pub trait Params: Sealed {
    // XXX not public api, might not need to expose.
    //
    // Binds the parameters to the statement. It is unlikely calling this
    // explicitly will do what you want. Please use `Statement::query` or
    // similar directly.
    //
    // For now, just hide the function in the docs...
    #[doc(hidden)]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()>;
}

// Explicitly impl for empty array. Critically, for `conn.execute([])` to be
// unambiguous, this must be the *only* implementation for an empty array. This
// avoids `NO_PARAMS` being a necessary part of the API.
//
// This sadly prevents `impl<T: ToSql, const N: usize> Params for [T; N]`, which
// forces people to use `params![...]` or `rusqlite::params_from_iter` for long
// homogenous lists of parameters. This is not that big of a deal, but is
// unfortunate, especially because I mostly did it because I wanted a simple
// syntax for no-params that didnt require importing -- the empty tuple fits
// that nicely, but I didn't think of it until much later.
//
// Admittedly, if we did have the generic impl, then we *wouldn't* support the
// empty array literal as a parameter, since the `T` there would fail to be
// inferred. The error message here would probably be quite bad, and so on
// further thought, probably would end up causing *more* surprises, not less.
impl Sealed for [&(dyn ToSql + Send + Sync); 0] {}
impl Params for [&(dyn ToSql + Send + Sync); 0] {
    #[inline]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.ensure_parameter_count(0)
    }
}

impl Sealed for &[&dyn ToSql] {}
impl Params for &[&dyn ToSql] {
    #[inline]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters(self)
    }
}

impl Sealed for &[(&str, &dyn ToSql)] {}
impl Params for &[(&str, &dyn ToSql)] {
    #[inline]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters_named(self)
    }
}

// Manual impls for the empty and singleton tuple, although the rest are covered
// by macros.
impl Sealed for () {}
impl Params for () {
    #[inline]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.ensure_parameter_count(0)
    }
}

// I'm pretty sure you could tweak the `single_tuple_impl` to accept this.
impl<T: ToSql> Sealed for (T,) {}
impl<T: ToSql> Params for (T,) {
    #[inline]
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.ensure_parameter_count(1)?;
        stmt.raw_bind_parameter(1, self.0)?;
        Ok(())
    }
}

macro_rules! single_tuple_impl {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> Sealed for ($($ftype,)*) where $($ftype: ToSql,)* {}
        impl<$($ftype,)*> Params for ($($ftype,)*) where $($ftype: ToSql,)* {
            fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.ensure_parameter_count($count)?;
                $({
                    debug_assert!($field < $count);
                    stmt.raw_bind_parameter($field + 1, self.$field)?;
                })+
                Ok(())
            }
        }
    }
}

// We use a the macro for the rest, but don't bother with trying to implement it
// in a single invocation (it's possible to do, but my attempts were almost the
// same amount of code as just writing it out this way, and much more dense --
// it is a more complicated case than the TryFrom macro we have for row->tuple).
//
// Note that going up to 16 (rather than the 12 that the impls in the stdlib
// usually support) is just because we did the same in the `TryFrom<Row>` impl.
// I didn't catch that then, but there's no reason to remove it, and it seems
// nice to be consistent here; this way putting data in the database and getting
// data out of the database are more symmetric in a (mostly superficial) sense.
single_tuple_impl!(2: (0 A), (1 B));
single_tuple_impl!(3: (0 A), (1 B), (2 C));
single_tuple_impl!(4: (0 A), (1 B), (2 C), (3 D));
single_tuple_impl!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
single_tuple_impl!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
single_tuple_impl!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
single_tuple_impl!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
single_tuple_impl!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
single_tuple_impl!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
single_tuple_impl!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
single_tuple_impl!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
single_tuple_impl!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
single_tuple_impl!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
single_tuple_impl!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
single_tuple_impl!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));

macro_rules! impl_for_array_ref {
    ($($N:literal)+) => {$(
        // These are already generic, and there's a shedload of them, so lets
        // avoid the compile time hit from making them all inline for now.
        impl<T: ToSql + ?Sized> Sealed for &[&T; $N] {}
        impl<T: ToSql + ?Sized> Params for &[&T; $N] {
            fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters(self)
            }
        }
        impl<T: ToSql + ?Sized> Sealed for &[(&str, &T); $N] {}
        impl<T: ToSql + ?Sized> Params for &[(&str, &T); $N] {
            fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters_named(self)
            }
        }
        impl<T: ToSql> Sealed for [T; $N] {}
        impl<T: ToSql> Params for [T; $N] {
            #[inline]
            fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
                stmt.bind_parameters(&self)
            }
        }
    )+};
}

// Following libstd/libcore's (old) lead, implement this for arrays up to `[_;
// 32]`. Note `[_; 0]` is intentionally omitted for coherence reasons, see the
// note above the impl of `[&dyn ToSql; 0]` for more information.
//
// Note that this unfortunately means we can't use const generics here, but I
// don't really think it matters -- users who hit that can use `params!` anyway.
impl_for_array_ref!(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17
    18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
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
/// use rusqlite::{params_from_iter, Connection, Result};
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
/// Here's how you'd use `ParamsFromIter` to call [`Statement::exists`] with a
/// dynamic number of parameters.
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
///   surpass [`conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER)`][limits]),
///   chunking if too large. (Note that the limits api requires rusqlite to have
///   the "limits" feature).
///
/// - `repeat_vars` can be implemented in a way that avoids needing to allocate
///   a String.
///
/// - Etc...
///
/// [limits]: crate::Connection::limit
///
/// This complexity reflects the fact that `ParamsFromIter` is mainly intended
/// for advanced use cases — most of the time you should know how many
/// parameters you have statically (and if you don't, you're either doing
/// something tricky, or should take a moment to think about the design).
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
    fn __bind_in(self, stmt: &mut Statement<'_>) -> Result<()> {
        stmt.bind_parameters(self.0)
    }
}
