//! `feature = "functions"` Create or redefine SQL functions.
//!
//! # Example
//!
//! Adding a `regexp` function to a connection in which compiled regular
//! expressions are cached in a `HashMap`. For an alternative implementation
//! that uses SQLite's [Function Auxilliary Data](https://www.sqlite.org/c3ref/get_auxdata.html) interface
//! to avoid recompiling regular expressions, see the unit tests for this
//! module.
//!
//! ```rust
//! use regex::Regex;
//! use rusqlite::functions::FunctionFlags;
//! use rusqlite::{Connection, Error, Result};
//! use std::sync::Arc;
//! type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
//!
//! fn add_regexp_function(db: &Connection) -> Result<()> {
//!     db.create_scalar_function(
//!         "regexp",
//!         2,
//!         FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
//!         move |ctx| {
//!             assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");
//!             let regexp: Arc<Regex> = ctx.get_or_create_aux(0, |vr| -> Result<_, BoxError> {
//!                 Ok(Regex::new(vr.as_str()?)?)
//!             })?;
//!             let is_match = {
//!                 let text = ctx
//!                     .get_raw(1)
//!                     .as_str()
//!                     .map_err(|e| Error::UserFunctionError(e.into()))?;
//!
//!                 regexp.is_match(text)
//!             };
//!
//!             Ok(is_match)
//!         },
//!     )
//! }
//!
//! fn main() -> Result<()> {
//!     let db = Connection::open_in_memory()?;
//!     add_regexp_function(&db)?;
//!
//!     let is_match: bool =
//!         db.query_row("SELECT regexp('[aeiou]*', 'aaaaeeeiii')", [], |row| {
//!             row.get(0)
//!         })?;
//!
//!     assert!(is_match);
//!     Ok(())
//! }
//! ```
use std::any::Any;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::raw::{c_int, c_void};
use std::panic::{catch_unwind, RefUnwindSafe, UnwindSafe};
use std::ptr;
use std::slice;
use std::sync::Arc;

use crate::ffi;
use crate::ffi::sqlite3_context;
use crate::ffi::sqlite3_value;

use crate::context::set_result;
use crate::types::{FromSql, FromSqlError, ToSql, ValueRef};

use crate::{str_to_cstring, Connection, Error, InnerConnection, Result};

unsafe fn report_error(ctx: *mut sqlite3_context, err: &Error) {
    // Extended constraint error codes were added in SQLite 3.7.16. We don't have
    // an explicit feature check for that, and this doesn't really warrant one.
    // We'll use the extended code if we're on the bundled version (since it's
    // at least 3.17.0) and the normal constraint error code if not.
    #[cfg(feature = "modern_sqlite")]
    fn constraint_error_code() -> i32 {
        ffi::SQLITE_CONSTRAINT_FUNCTION
    }
    #[cfg(not(feature = "modern_sqlite"))]
    fn constraint_error_code() -> i32 {
        ffi::SQLITE_CONSTRAINT
    }

    match *err {
        Error::SqliteFailure(ref err, ref s) => {
            ffi::sqlite3_result_error_code(ctx, err.extended_code);
            if let Some(Ok(cstr)) = s.as_ref().map(|s| str_to_cstring(s)) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
        }
        _ => {
            ffi::sqlite3_result_error_code(ctx, constraint_error_code());
            if let Ok(cstr) = str_to_cstring(&err.to_string()) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
        }
    }
}

unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    drop(Box::from_raw(p as *mut T));
}

/// `feature = "functions"` Context is a wrapper for the SQLite function
/// evaluation context.
pub struct Context<'a> {
    ctx: *mut sqlite3_context,
    args: &'a [*mut sqlite3_value],
}

impl Context<'_> {
    /// Returns the number of arguments to the function.
    #[inline]
    pub fn len(&self) -> usize {
        self.args.len()
    }

    /// Returns `true` when there is no argument.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    /// Returns the `idx`th argument as a `T`.
    ///
    /// # Failure
    ///
    /// Will panic if `idx` is greater than or equal to [`self.len()`](Context::len).
    ///
    /// Will return Err if the underlying SQLite type cannot be converted to a
    /// `T`.
    pub fn get<T: FromSql>(&self, idx: usize) -> Result<T> {
        let arg = self.args[idx];
        let value = unsafe { ValueRef::from_value(arg) };
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => {
                Error::InvalidFunctionParameterType(idx, value.data_type())
            }
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
            FromSqlError::Other(err) => {
                Error::FromSqlConversionFailure(idx, value.data_type(), err)
            }
            #[cfg(feature = "i128_blob")]
            FromSqlError::InvalidI128Size(_) => {
                Error::FromSqlConversionFailure(idx, value.data_type(), Box::new(err))
            }
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Error::FromSqlConversionFailure(idx, value.data_type(), Box::new(err))
            }
        })
    }

    /// Returns the `idx`th argument as a `ValueRef`.
    ///
    /// # Failure
    ///
    /// Will panic if `idx` is greater than or equal to [`self.len()`](Context::len).
    #[inline]
    pub fn get_raw(&self, idx: usize) -> ValueRef<'_> {
        let arg = self.args[idx];
        unsafe { ValueRef::from_value(arg) }
    }

    /// Fetch or insert the auxilliary data associated with a particular
    /// parameter. This is intended to be an easier-to-use way of fetching it
    /// compared to calling [`get_aux`](Context::get_aux) and [`set_aux`](Context::set_aux) separately.
    ///
    /// See `https://www.sqlite.org/c3ref/get_auxdata.html` for a discussion of
    /// this feature, or the unit tests of this module for an example.
    pub fn get_or_create_aux<T, E, F>(&self, arg: c_int, func: F) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
        F: FnOnce(ValueRef<'_>) -> Result<T, E>,
    {
        if let Some(v) = self.get_aux(arg)? {
            Ok(v)
        } else {
            let vr = self.get_raw(arg as usize);
            self.set_aux(
                arg,
                func(vr).map_err(|e| Error::UserFunctionError(e.into()))?,
            )
        }
    }

    /// Sets the auxilliary data associated with a particular parameter. See
    /// `https://www.sqlite.org/c3ref/get_auxdata.html` for a discussion of
    /// this feature, or the unit tests of this module for an example.
    pub fn set_aux<T: Send + Sync + 'static>(&self, arg: c_int, value: T) -> Result<Arc<T>> {
        let orig: Arc<T> = Arc::new(value);
        let inner: AuxInner = orig.clone();
        let outer = Box::new(inner);
        let raw: *mut AuxInner = Box::into_raw(outer);
        unsafe {
            ffi::sqlite3_set_auxdata(
                self.ctx,
                arg,
                raw as *mut _,
                Some(free_boxed_value::<AuxInner>),
            )
        };
        Ok(orig)
    }

    /// Gets the auxilliary data that was associated with a given parameter via
    /// [`set_aux`](Context::set_aux). Returns `Ok(None)` if no data has been associated, and
    /// Ok(Some(v)) if it has. Returns an error if the requested type does not
    /// match.
    pub fn get_aux<T: Send + Sync + 'static>(&self, arg: c_int) -> Result<Option<Arc<T>>> {
        let p = unsafe { ffi::sqlite3_get_auxdata(self.ctx, arg) as *const AuxInner };
        if p.is_null() {
            Ok(None)
        } else {
            let v: AuxInner = AuxInner::clone(unsafe { &*p });
            v.downcast::<T>()
                .map(Some)
                .map_err(|_| Error::GetAuxWrongType)
        }
    }

    /// Get the db connection handle via [sqlite3_context_db_handle](https://www.sqlite.org/c3ref/context_db_handle.html)
    ///
    /// # Safety
    ///
    /// This function is marked unsafe because there is a potential for other
    /// references to the connection to be sent across threads, [see this comment](https://github.com/rusqlite/rusqlite/issues/643#issuecomment-640181213).
    pub unsafe fn get_connection(&self) -> Result<ConnectionRef<'_>> {
        let handle = ffi::sqlite3_context_db_handle(self.ctx);
        Ok(ConnectionRef {
            conn: Connection::from_handle(handle)?,
            phantom: PhantomData,
        })
    }
}

/// A reference to a connection handle with a lifetime bound to something.
pub struct ConnectionRef<'ctx> {
    // comes from Connection::from_handle(sqlite3_context_db_handle(...))
    // and is non-owning
    conn: Connection,
    phantom: PhantomData<&'ctx Context<'ctx>>,
}

impl Deref for ConnectionRef<'_> {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        &self.conn
    }
}

type AuxInner = Arc<dyn Any + Send + Sync + 'static>;

/// `feature = "functions"` Aggregate is the callback interface for user-defined
/// aggregate function.
///
/// `A` is the type of the aggregation context and `T` is the type of the final
/// result. Implementations should be stateless.
pub trait Aggregate<A, T>
where
    A: RefUnwindSafe + UnwindSafe,
    T: ToSql,
{
    /// Initializes the aggregation context. Will be called prior to the first
    /// call to [`step()`](Aggregate::step) to set up the context for an invocation of the
    /// function. (Note: `init()` will not be called if there are no rows.)
    fn init(&self, _: &mut Context<'_>) -> Result<A>;

    /// "step" function called once for each row in an aggregate group. May be
    /// called 0 times if there are no rows.
    fn step(&self, _: &mut Context<'_>, _: &mut A) -> Result<()>;

    /// Computes and returns the final result. Will be called exactly once for
    /// each invocation of the function. If [`step()`](Aggregate::step) was called at least
    /// once, will be given `Some(A)` (the same `A` as was created by
    /// [`init`](Aggregate::init) and given to [`step`](Aggregate::step)); if [`step()`](Aggregate::step) was not called (because
    /// the function is running against 0 rows), will be given `None`.
    ///
    /// The passed context will have no arguments.
    fn finalize(&self, _: &mut Context<'_>, _: Option<A>) -> Result<T>;
}

/// `feature = "window"` WindowAggregate is the callback interface for
/// user-defined aggregate window function.
#[cfg(feature = "window")]
pub trait WindowAggregate<A, T>: Aggregate<A, T>
where
    A: RefUnwindSafe + UnwindSafe,
    T: ToSql,
{
    /// Returns the current value of the aggregate. Unlike xFinal, the
    /// implementation should not delete any context.
    fn value(&self, _: Option<&A>) -> Result<T>;

    /// Removes a row from the current window.
    fn inverse(&self, _: &mut Context<'_>, _: &mut A) -> Result<()>;
}

bitflags::bitflags! {
    /// Function Flags.
    /// See [sqlite3_create_function](https://sqlite.org/c3ref/create_function.html)
    /// and [Function Flags](https://sqlite.org/c3ref/c_deterministic.html) for details.
    #[repr(C)]
    pub struct FunctionFlags: ::std::os::raw::c_int {
        /// Specifies UTF-8 as the text encoding this SQL function prefers for its parameters.
        const SQLITE_UTF8     = ffi::SQLITE_UTF8;
        /// Specifies UTF-16 using little-endian byte order as the text encoding this SQL function prefers for its parameters.
        const SQLITE_UTF16LE  = ffi::SQLITE_UTF16LE;
        /// Specifies UTF-16 using big-endian byte order as the text encoding this SQL function prefers for its parameters.
        const SQLITE_UTF16BE  = ffi::SQLITE_UTF16BE;
        /// Specifies UTF-16 using native byte order as the text encoding this SQL function prefers for its parameters.
        const SQLITE_UTF16    = ffi::SQLITE_UTF16;
        /// Means that the function always gives the same output when the input parameters are the same.
        const SQLITE_DETERMINISTIC = ffi::SQLITE_DETERMINISTIC;
        /// Means that the function may only be invoked from top-level SQL.
        const SQLITE_DIRECTONLY    = 0x0000_0008_0000; // 3.30.0
        /// Indicates to SQLite that a function may call `sqlite3_value_subtype()` to inspect the sub-types of its arguments.
        const SQLITE_SUBTYPE       = 0x0000_0010_0000; // 3.30.0
        /// Means that the function is unlikely to cause problems even if misused.
        const SQLITE_INNOCUOUS     = 0x0000_0020_0000; // 3.31.0
    }
}

impl Default for FunctionFlags {
    #[inline]
    fn default() -> FunctionFlags {
        FunctionFlags::SQLITE_UTF8
    }
}

impl Connection {
    /// `feature = "functions"` Attach a user-defined scalar function to
    /// this database connection.
    ///
    /// `fn_name` is the name the function will be accessible from SQL.
    /// `n_arg` is the number of arguments to the function. Use `-1` for a
    /// variable number. If the function always returns the same value
    /// given the same input, `deterministic` should be `true`.
    ///
    /// The function will remain available until the connection is closed or
    /// until it is explicitly removed via [`remove_function`](Connection::remove_function).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rusqlite::{Connection, Result};
    /// # use rusqlite::functions::FunctionFlags;
    /// fn scalar_function_example(db: Connection) -> Result<()> {
    ///     db.create_scalar_function(
    ///         "halve",
    ///         1,
    ///         FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
    ///         |ctx| {
    ///             let value = ctx.get::<f64>(0)?;
    ///             Ok(value / 2f64)
    ///         },
    ///     )?;
    ///
    ///     let six_halved: f64 = db.query_row("SELECT halve(6)", [], |r| r.get(0))?;
    ///     assert_eq!(six_halved, 3f64);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be attached to the connection.
    #[inline]
    pub fn create_scalar_function<'c, F, T>(
        &'c self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        x_func: F,
    ) -> Result<()>
    where
        F: FnMut(&Context<'_>) -> Result<T> + Send + UnwindSafe + 'c,
        T: ToSql,
    {
        self.db
            .borrow_mut()
            .create_scalar_function(fn_name, n_arg, flags, x_func)
    }

    /// `feature = "functions"` Attach a user-defined aggregate function to this
    /// database connection.
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be attached to the connection.
    #[inline]
    pub fn create_aggregate_function<A, D, T>(
        &self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        aggr: D,
    ) -> Result<()>
    where
        A: RefUnwindSafe + UnwindSafe,
        D: Aggregate<A, T>,
        T: ToSql,
    {
        self.db
            .borrow_mut()
            .create_aggregate_function(fn_name, n_arg, flags, aggr)
    }

    /// `feature = "window"` Attach a user-defined aggregate window function to
    /// this database connection.
    ///
    /// See `https://sqlite.org/windowfunctions.html#udfwinfunc` for more
    /// information.
    #[cfg(feature = "window")]
    #[inline]
    pub fn create_window_function<A, W, T>(
        &self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        aggr: W,
    ) -> Result<()>
    where
        A: RefUnwindSafe + UnwindSafe,
        W: WindowAggregate<A, T>,
        T: ToSql,
    {
        self.db
            .borrow_mut()
            .create_window_function(fn_name, n_arg, flags, aggr)
    }

    /// `feature = "functions"` Removes a user-defined function from this
    /// database connection.
    ///
    /// `fn_name` and `n_arg` should match the name and number of arguments
    /// given to [`create_scalar_function`](Connection::create_scalar_function) or [`create_aggregate_function`](Connection::create_aggregate_function).
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be removed.
    #[inline]
    pub fn remove_function(&self, fn_name: &str, n_arg: c_int) -> Result<()> {
        self.db.borrow_mut().remove_function(fn_name, n_arg)
    }
}

impl InnerConnection {
    fn create_scalar_function<'c, F, T>(
        &'c mut self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        x_func: F,
    ) -> Result<()>
    where
        F: FnMut(&Context<'_>) -> Result<T> + Send + UnwindSafe + 'c,
        T: ToSql,
    {
        unsafe extern "C" fn call_boxed_closure<F, T>(
            ctx: *mut sqlite3_context,
            argc: c_int,
            argv: *mut *mut sqlite3_value,
        ) where
            F: FnMut(&Context<'_>) -> Result<T>,
            T: ToSql,
        {
            let r = catch_unwind(|| {
                let boxed_f: *mut F = ffi::sqlite3_user_data(ctx) as *mut F;
                assert!(!boxed_f.is_null(), "Internal error - null function pointer");
                let ctx = Context {
                    ctx,
                    args: slice::from_raw_parts(argv, argc as usize),
                };
                (*boxed_f)(&ctx)
            });
            let t = match r {
                Err(_) => {
                    report_error(ctx, &Error::UnwindingPanic);
                    return;
                }
                Ok(r) => r,
            };
            let t = t.as_ref().map(|t| ToSql::to_sql(t));

            match t {
                Ok(Ok(ref value)) => set_result(ctx, value),
                Ok(Err(err)) => report_error(ctx, &err),
                Err(err) => report_error(ctx, err),
            }
        }

        let boxed_f: *mut F = Box::into_raw(Box::new(x_func));
        let c_name = str_to_cstring(fn_name)?;
        let r = unsafe {
            ffi::sqlite3_create_function_v2(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                flags.bits(),
                boxed_f as *mut c_void,
                Some(call_boxed_closure::<F, T>),
                None,
                None,
                Some(free_boxed_value::<F>),
            )
        };
        self.decode_result(r)
    }

    fn create_aggregate_function<A, D, T>(
        &mut self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        aggr: D,
    ) -> Result<()>
    where
        A: RefUnwindSafe + UnwindSafe,
        D: Aggregate<A, T>,
        T: ToSql,
    {
        let boxed_aggr: *mut D = Box::into_raw(Box::new(aggr));
        let c_name = str_to_cstring(fn_name)?;
        let r = unsafe {
            ffi::sqlite3_create_function_v2(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                flags.bits(),
                boxed_aggr as *mut c_void,
                None,
                Some(call_boxed_step::<A, D, T>),
                Some(call_boxed_final::<A, D, T>),
                Some(free_boxed_value::<D>),
            )
        };
        self.decode_result(r)
    }

    #[cfg(feature = "window")]
    fn create_window_function<A, W, T>(
        &mut self,
        fn_name: &str,
        n_arg: c_int,
        flags: FunctionFlags,
        aggr: W,
    ) -> Result<()>
    where
        A: RefUnwindSafe + UnwindSafe,
        W: WindowAggregate<A, T>,
        T: ToSql,
    {
        let boxed_aggr: *mut W = Box::into_raw(Box::new(aggr));
        let c_name = str_to_cstring(fn_name)?;
        let r = unsafe {
            ffi::sqlite3_create_window_function(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                flags.bits(),
                boxed_aggr as *mut c_void,
                Some(call_boxed_step::<A, W, T>),
                Some(call_boxed_final::<A, W, T>),
                Some(call_boxed_value::<A, W, T>),
                Some(call_boxed_inverse::<A, W, T>),
                Some(free_boxed_value::<W>),
            )
        };
        self.decode_result(r)
    }

    fn remove_function(&mut self, fn_name: &str, n_arg: c_int) -> Result<()> {
        let c_name = str_to_cstring(fn_name)?;
        let r = unsafe {
            ffi::sqlite3_create_function_v2(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                ffi::SQLITE_UTF8,
                ptr::null_mut(),
                None,
                None,
                None,
                None,
            )
        };
        self.decode_result(r)
    }
}

unsafe fn aggregate_context<A>(ctx: *mut sqlite3_context, bytes: usize) -> Option<*mut *mut A> {
    let pac = ffi::sqlite3_aggregate_context(ctx, bytes as c_int) as *mut *mut A;
    if pac.is_null() {
        return None;
    }
    Some(pac)
}

unsafe extern "C" fn call_boxed_step<A, D, T>(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) where
    A: RefUnwindSafe + UnwindSafe,
    D: Aggregate<A, T>,
    T: ToSql,
{
    let pac = match aggregate_context(ctx, ::std::mem::size_of::<*mut A>()) {
        Some(pac) => pac,
        None => {
            ffi::sqlite3_result_error_nomem(ctx);
            return;
        }
    };

    let r = catch_unwind(|| {
        let boxed_aggr: *mut D = ffi::sqlite3_user_data(ctx) as *mut D;
        assert!(
            !boxed_aggr.is_null(),
            "Internal error - null aggregate pointer"
        );
        let mut ctx = Context {
            ctx,
            args: slice::from_raw_parts(argv, argc as usize),
        };

        if (*pac as *mut A).is_null() {
            *pac = Box::into_raw(Box::new((*boxed_aggr).init(&mut ctx)?));
        }

        (*boxed_aggr).step(&mut ctx, &mut **pac)
    });
    let r = match r {
        Err(_) => {
            report_error(ctx, &Error::UnwindingPanic);
            return;
        }
        Ok(r) => r,
    };
    match r {
        Ok(_) => {}
        Err(err) => report_error(ctx, &err),
    };
}

#[cfg(feature = "window")]
unsafe extern "C" fn call_boxed_inverse<A, W, T>(
    ctx: *mut sqlite3_context,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) where
    A: RefUnwindSafe + UnwindSafe,
    W: WindowAggregate<A, T>,
    T: ToSql,
{
    let pac = match aggregate_context(ctx, ::std::mem::size_of::<*mut A>()) {
        Some(pac) => pac,
        None => {
            ffi::sqlite3_result_error_nomem(ctx);
            return;
        }
    };

    let r = catch_unwind(|| {
        let boxed_aggr: *mut W = ffi::sqlite3_user_data(ctx) as *mut W;
        assert!(
            !boxed_aggr.is_null(),
            "Internal error - null aggregate pointer"
        );
        let mut ctx = Context {
            ctx,
            args: slice::from_raw_parts(argv, argc as usize),
        };
        (*boxed_aggr).inverse(&mut ctx, &mut **pac)
    });
    let r = match r {
        Err(_) => {
            report_error(ctx, &Error::UnwindingPanic);
            return;
        }
        Ok(r) => r,
    };
    match r {
        Ok(_) => {}
        Err(err) => report_error(ctx, &err),
    };
}

unsafe extern "C" fn call_boxed_final<A, D, T>(ctx: *mut sqlite3_context)
where
    A: RefUnwindSafe + UnwindSafe,
    D: Aggregate<A, T>,
    T: ToSql,
{
    // Within the xFinal callback, it is customary to set N=0 in calls to
    // sqlite3_aggregate_context(C,N) so that no pointless memory allocations occur.
    let a: Option<A> = match aggregate_context(ctx, 0) {
        Some(pac) => {
            if (*pac as *mut A).is_null() {
                None
            } else {
                let a = Box::from_raw(*pac);
                Some(*a)
            }
        }
        None => None,
    };

    let r = catch_unwind(|| {
        let boxed_aggr: *mut D = ffi::sqlite3_user_data(ctx) as *mut D;
        assert!(
            !boxed_aggr.is_null(),
            "Internal error - null aggregate pointer"
        );
        let mut ctx = Context { ctx, args: &mut [] };
        (*boxed_aggr).finalize(&mut ctx, a)
    });
    let t = match r {
        Err(_) => {
            report_error(ctx, &Error::UnwindingPanic);
            return;
        }
        Ok(r) => r,
    };
    let t = t.as_ref().map(|t| ToSql::to_sql(t));
    match t {
        Ok(Ok(ref value)) => set_result(ctx, value),
        Ok(Err(err)) => report_error(ctx, &err),
        Err(err) => report_error(ctx, err),
    }
}

#[cfg(feature = "window")]
unsafe extern "C" fn call_boxed_value<A, W, T>(ctx: *mut sqlite3_context)
where
    A: RefUnwindSafe + UnwindSafe,
    W: WindowAggregate<A, T>,
    T: ToSql,
{
    // Within the xValue callback, it is customary to set N=0 in calls to
    // sqlite3_aggregate_context(C,N) so that no pointless memory allocations occur.
    let a: Option<&A> = match aggregate_context(ctx, 0) {
        Some(pac) => {
            if (*pac as *mut A).is_null() {
                None
            } else {
                let a = &**pac;
                Some(a)
            }
        }
        None => None,
    };

    let r = catch_unwind(|| {
        let boxed_aggr: *mut W = ffi::sqlite3_user_data(ctx) as *mut W;
        assert!(
            !boxed_aggr.is_null(),
            "Internal error - null aggregate pointer"
        );
        (*boxed_aggr).value(a)
    });
    let t = match r {
        Err(_) => {
            report_error(ctx, &Error::UnwindingPanic);
            return;
        }
        Ok(r) => r,
    };
    let t = t.as_ref().map(|t| ToSql::to_sql(t));
    match t {
        Ok(Ok(ref value)) => set_result(ctx, value),
        Ok(Err(err)) => report_error(ctx, &err),
        Err(err) => report_error(ctx, err),
    }
}

#[cfg(test)]
mod test {
    use regex::Regex;
    use std::f64::EPSILON;
    use std::os::raw::c_double;

    #[cfg(feature = "window")]
    use crate::functions::WindowAggregate;
    use crate::functions::{Aggregate, Context, FunctionFlags};
    use crate::{Connection, Error, Result};

    fn half(ctx: &Context<'_>) -> Result<c_double> {
        assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");
        let value = ctx.get::<c_double>(0)?;
        Ok(value / 2f64)
    }

    #[test]
    fn test_function_half() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_scalar_function(
            "half",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            half,
        )?;
        let result: Result<f64> = db.query_row("SELECT half(6)", [], |r| r.get(0));

        assert!((3f64 - result?).abs() < EPSILON);
        Ok(())
    }

    #[test]
    fn test_remove_function() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_scalar_function(
            "half",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            half,
        )?;
        let result: Result<f64> = db.query_row("SELECT half(6)", [], |r| r.get(0));
        assert!((3f64 - result?).abs() < EPSILON);

        db.remove_function("half", 1)?;
        let result: Result<f64> = db.query_row("SELECT half(6)", [], |r| r.get(0));
        assert!(result.is_err());
        Ok(())
    }

    // This implementation of a regexp scalar function uses SQLite's auxilliary data
    // (https://www.sqlite.org/c3ref/get_auxdata.html) to avoid recompiling the regular
    // expression multiple times within one query.
    fn regexp_with_auxilliary(ctx: &Context<'_>) -> Result<bool> {
        assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");
        type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
        let regexp: std::sync::Arc<Regex> = ctx
            .get_or_create_aux(0, |vr| -> Result<_, BoxError> {
                Ok(Regex::new(vr.as_str()?)?)
            })?;

        let is_match = {
            let text = ctx
                .get_raw(1)
                .as_str()
                .map_err(|e| Error::UserFunctionError(e.into()))?;

            regexp.is_match(text)
        };

        Ok(is_match)
    }

    #[test]
    fn test_function_regexp_with_auxilliary() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "BEGIN;
             CREATE TABLE foo (x string);
             INSERT INTO foo VALUES ('lisa');
             INSERT INTO foo VALUES ('lXsi');
             INSERT INTO foo VALUES ('lisX');
             END;",
        )?;
        db.create_scalar_function(
            "regexp",
            2,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            regexp_with_auxilliary,
        )?;

        let result: Result<bool> =
            db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')", [], |r| r.get(0));

        assert_eq!(true, result?);

        let result: Result<i64> = db.query_row(
            "SELECT COUNT(*) FROM foo WHERE regexp('l.s[aeiouy]', x) == 1",
            [],
            |r| r.get(0),
        );

        assert_eq!(2, result?);
        Ok(())
    }

    #[test]
    fn test_varargs_function() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_scalar_function(
            "my_concat",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |ctx| {
                let mut ret = String::new();

                for idx in 0..ctx.len() {
                    let s = ctx.get::<String>(idx)?;
                    ret.push_str(&s);
                }

                Ok(ret)
            },
        )?;

        for &(expected, query) in &[
            ("", "SELECT my_concat()"),
            ("onetwo", "SELECT my_concat('one', 'two')"),
            ("abc", "SELECT my_concat('a', 'b', 'c')"),
        ] {
            let result: String = db.query_row(query, [], |r| r.get(0))?;
            assert_eq!(expected, result);
        }
        Ok(())
    }

    #[test]
    fn test_get_aux_type_checking() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_scalar_function("example", 2, FunctionFlags::default(), |ctx| {
            if !ctx.get::<bool>(1)? {
                ctx.set_aux::<i64>(0, 100)?;
            } else {
                assert_eq!(ctx.get_aux::<String>(0), Err(Error::GetAuxWrongType));
                assert_eq!(*ctx.get_aux::<i64>(0)?.unwrap(), 100);
            }
            Ok(true)
        })?;

        let res: bool = db.query_row(
            "SELECT example(0, i) FROM (SELECT 0 as i UNION SELECT 1)",
            [],
            |r| r.get(0),
        )?;
        // Doesn't actually matter, we'll assert in the function if there's a problem.
        assert!(res);
        Ok(())
    }

    struct Sum;
    struct Count;

    impl Aggregate<i64, Option<i64>> for Sum {
        fn init(&self, _: &mut Context<'_>) -> Result<i64> {
            Ok(0)
        }

        fn step(&self, ctx: &mut Context<'_>, sum: &mut i64) -> Result<()> {
            *sum += ctx.get::<i64>(0)?;
            Ok(())
        }

        fn finalize(&self, _: &mut Context<'_>, sum: Option<i64>) -> Result<Option<i64>> {
            Ok(sum)
        }
    }

    impl Aggregate<i64, i64> for Count {
        fn init(&self, _: &mut Context<'_>) -> Result<i64> {
            Ok(0)
        }

        fn step(&self, _ctx: &mut Context<'_>, sum: &mut i64) -> Result<()> {
            *sum += 1;
            Ok(())
        }

        fn finalize(&self, _: &mut Context<'_>, sum: Option<i64>) -> Result<i64> {
            Ok(sum.unwrap_or(0))
        }
    }

    #[test]
    fn test_sum() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_aggregate_function(
            "my_sum",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            Sum,
        )?;

        // sum should return NULL when given no columns (contrast with count below)
        let no_result = "SELECT my_sum(i) FROM (SELECT 2 AS i WHERE 1 <> 1)";
        let result: Option<i64> = db.query_row(no_result, [], |r| r.get(0))?;
        assert!(result.is_none());

        let single_sum = "SELECT my_sum(i) FROM (SELECT 2 AS i UNION ALL SELECT 2)";
        let result: i64 = db.query_row(single_sum, [], |r| r.get(0))?;
        assert_eq!(4, result);

        let dual_sum = "SELECT my_sum(i), my_sum(j) FROM (SELECT 2 AS i, 1 AS j UNION ALL SELECT \
                        2, 1)";
        let result: (i64, i64) = db.query_row(dual_sum, [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        assert_eq!((4, 2), result);
        Ok(())
    }

    #[test]
    fn test_count() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.create_aggregate_function(
            "my_count",
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            Count,
        )?;

        // count should return 0 when given no columns (contrast with sum above)
        let no_result = "SELECT my_count(i) FROM (SELECT 2 AS i WHERE 1 <> 1)";
        let result: i64 = db.query_row(no_result, [], |r| r.get(0))?;
        assert_eq!(result, 0);

        let single_sum = "SELECT my_count(i) FROM (SELECT 2 AS i UNION ALL SELECT 2)";
        let result: i64 = db.query_row(single_sum, [], |r| r.get(0))?;
        assert_eq!(2, result);
        Ok(())
    }

    #[cfg(feature = "window")]
    impl WindowAggregate<i64, Option<i64>> for Sum {
        fn inverse(&self, ctx: &mut Context<'_>, sum: &mut i64) -> Result<()> {
            *sum -= ctx.get::<i64>(0)?;
            Ok(())
        }

        fn value(&self, sum: Option<&i64>) -> Result<Option<i64>> {
            Ok(sum.copied())
        }
    }

    #[test]
    #[cfg(feature = "window")]
    fn test_window() -> Result<()> {
        use fallible_iterator::FallibleIterator;

        let db = Connection::open_in_memory()?;
        db.create_window_function(
            "sumint",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            Sum,
        )?;
        db.execute_batch(
            "CREATE TABLE t3(x, y);
             INSERT INTO t3 VALUES('a', 4),
                     ('b', 5),
                     ('c', 3),
                     ('d', 8),
                     ('e', 1);",
        )?;

        let mut stmt = db.prepare(
            "SELECT x, sumint(y) OVER (
                   ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                 ) AS sum_y
                 FROM t3 ORDER BY x;",
        )?;

        let results: Vec<(String, i64)> = stmt
            .query([])?
            .map(|row| Ok((row.get("x")?, row.get("sum_y")?)))
            .collect()?;
        let expected = vec![
            ("a".to_owned(), 9),
            ("b".to_owned(), 12),
            ("c".to_owned(), 16),
            ("d".to_owned(), 12),
            ("e".to_owned(), 9),
        ];
        assert_eq!(expected, results);
        Ok(())
    }
}
