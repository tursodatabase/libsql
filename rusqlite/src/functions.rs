//! Create or redefine SQL functions.
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
//! extern crate libsqlite3_sys;
//! extern crate rusqlite;
//! extern crate regex;
//!
//! use regex::Regex;
//! use rusqlite::{Connection, Error, Result};
//! use std::collections::HashMap;
//!
//! fn add_regexp_function(db: &Connection) -> Result<()> {
//!     let mut cached_regexes = HashMap::new();
//!     db.create_scalar_function("regexp", 2, true, move |ctx| {
//!         let regex_s = try!(ctx.get::<String>(0));
//!         let entry = cached_regexes.entry(regex_s.clone());
//!         let regex = {
//!             use std::collections::hash_map::Entry::{Occupied, Vacant};
//!             match entry {
//!                 Occupied(occ) => occ.into_mut(),
//!                 Vacant(vac) => match Regex::new(&regex_s) {
//!                     Ok(r) => vac.insert(r),
//!                     Err(err) => return Err(Error::UserFunctionError(Box::new(err))),
//!                 },
//!             }
//!         };
//!
//!         let text = try!(ctx.get::<String>(1));
//!         Ok(regex.is_match(&text))
//!     })
//! }
//!
//! fn main() {
//!     let db = Connection::open_in_memory().unwrap();
//!     add_regexp_function(&db).unwrap();
//!
//!     let is_match: bool = db
//!         .query_row("SELECT regexp('[aeiou]*', 'aaaaeeeiii')", &[], |row| {
//!             row.get(0)
//!         }).unwrap();
//!
//!     assert!(is_match);
//! }
//! ```
use std::error::Error as StdError;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::slice;

use ffi;
use ffi::sqlite3_context;
use ffi::sqlite3_value;

use context::set_result;
use types::{FromSql, FromSqlError, ToSql, ValueRef};

use {str_to_cstring, Connection, Error, InnerConnection, Result};

unsafe fn report_error(ctx: *mut sqlite3_context, err: &Error) {
    // Extended constraint error codes were added in SQLite 3.7.16. We don't have
    // an explicit feature check for that, and this doesn't really warrant one.
    // We'll use the extended code if we're on the bundled version (since it's
    // at least 3.17.0) and the normal constraint error code if not.
    #[cfg(feature = "bundled")]
    fn constraint_error_code() -> i32 {
        ffi::SQLITE_CONSTRAINT_FUNCTION
    }
    #[cfg(not(feature = "bundled"))]
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
            if let Ok(cstr) = str_to_cstring(err.description()) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
        }
    }
}

unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    drop(Box::from_raw(p as *mut T));
}

/// Context is a wrapper for the SQLite function evaluation context.
pub struct Context<'a> {
    ctx: *mut sqlite3_context,
    args: &'a [*mut sqlite3_value],
}

impl<'a> Context<'a> {
    /// Returns the number of arguments to the function.
    pub fn len(&self) -> usize {
        self.args.len()
    }

    /// Returns `true` when there is no argument.
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    /// Returns the `idx`th argument as a `T`.
    ///
    /// # Failure
    ///
    /// Will panic if `idx` is greater than or equal to `self.len()`.
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
        })
    }

    /// Sets the auxilliary data associated with a particular parameter. See
    /// https://www.sqlite.org/c3ref/get_auxdata.html for a discussion of
    /// this feature, or the unit tests of this module for an example.
    pub fn set_aux<T>(&self, arg: c_int, value: T) {
        let boxed = Box::into_raw(Box::new(value));
        unsafe {
            ffi::sqlite3_set_auxdata(
                self.ctx,
                arg,
                boxed as *mut c_void,
                Some(free_boxed_value::<T>),
            )
        };
    }

    /// Gets the auxilliary data that was associated with a given parameter
    /// via `set_aux`. Returns `None` if no data has been associated.
    ///
    /// # Unsafety
    ///
    /// This function is unsafe as there is no guarantee that the type `T`
    /// requested matches the type `T` that was provided to `set_aux`. The
    /// types must be identical.
    pub unsafe fn get_aux<T>(&self, arg: c_int) -> Option<&T> {
        let p = ffi::sqlite3_get_auxdata(self.ctx, arg) as *mut T;
        if p.is_null() {
            None
        } else {
            Some(&*p)
        }
    }
}

/// Aggregate is the callback interface for user-defined aggregate function.
///
/// `A` is the type of the aggregation context and `T` is the type of the final
/// result. Implementations should be stateless.
pub trait Aggregate<A, T>
where
    T: ToSql,
{
    /// Initializes the aggregation context. Will be called prior to the first
    /// call to `step()` to set up the context for an invocation of the
    /// function. (Note: `init()` will not be called if there are no rows.)
    fn init(&self) -> A;

    /// "step" function called once for each row in an aggregate group. May be
    /// called 0 times if there are no rows.
    fn step(&self, &mut Context, &mut A) -> Result<()>;

    /// Computes and returns the final result. Will be called exactly once for
    /// each invocation of the function. If `step()` was called at least
    /// once, will be given `Some(A)` (the same `A` as was created by
    /// `init` and given to `step`); if `step()` was not called (because
    /// the function is running against 0 rows), will be given `None`.
    fn finalize(&self, Option<A>) -> Result<T>;
}

impl Connection {
    /// Attach a user-defined scalar function to this database connection.
    ///
    /// `fn_name` is the name the function will be accessible from SQL.
    /// `n_arg` is the number of arguments to the function. Use `-1` for a
    /// variable number. If the function always returns the same value
    /// given the same input, `deterministic` should be `true`.
    ///
    /// The function will remain available until the connection is closed or
    /// until it is explicitly removed via `remove_function`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rusqlite::{Connection, Result};
    /// fn scalar_function_example(db: Connection) -> Result<()> {
    ///     try!(db.create_scalar_function("halve", 1, true, |ctx| {
    ///         let value = try!(ctx.get::<f64>(0));
    ///         Ok(value / 2f64)
    ///     }));
    ///
    ///     let six_halved: f64 = try!(db.query_row("SELECT halve(6)", &[], |r| r.get(0)));
    ///     assert_eq!(six_halved, 3f64);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be attached to the connection.
    pub fn create_scalar_function<F, T>(
        &self,
        fn_name: &str,
        n_arg: c_int,
        deterministic: bool,
        x_func: F,
    ) -> Result<()>
    where
        F: FnMut(&Context) -> Result<T> + Send + 'static,
        T: ToSql,
    {
        self.db
            .borrow_mut()
            .create_scalar_function(fn_name, n_arg, deterministic, x_func)
    }

    /// Attach a user-defined aggregate function to this database connection.
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be attached to the connection.
    pub fn create_aggregate_function<A, D, T>(
        &self,
        fn_name: &str,
        n_arg: c_int,
        deterministic: bool,
        aggr: D,
    ) -> Result<()>
    where
        D: Aggregate<A, T>,
        T: ToSql,
    {
        self.db
            .borrow_mut()
            .create_aggregate_function(fn_name, n_arg, deterministic, aggr)
    }

    /// Removes a user-defined function from this database connection.
    ///
    /// `fn_name` and `n_arg` should match the name and number of arguments
    /// given to `create_scalar_function` or `create_aggregate_function`.
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be removed.
    pub fn remove_function(&self, fn_name: &str, n_arg: c_int) -> Result<()> {
        self.db.borrow_mut().remove_function(fn_name, n_arg)
    }
}

impl InnerConnection {
    fn create_scalar_function<F, T>(
        &mut self,
        fn_name: &str,
        n_arg: c_int,
        deterministic: bool,
        x_func: F,
    ) -> Result<()>
    where
        F: FnMut(&Context) -> Result<T> + Send + 'static,
        T: ToSql,
    {
        unsafe extern "C" fn call_boxed_closure<F, T>(
            ctx: *mut sqlite3_context,
            argc: c_int,
            argv: *mut *mut sqlite3_value,
        ) where
            F: FnMut(&Context) -> Result<T>,
            T: ToSql,
        {
            let ctx = Context {
                ctx,
                args: slice::from_raw_parts(argv, argc as usize),
            };
            let boxed_f: *mut F = ffi::sqlite3_user_data(ctx.ctx) as *mut F;
            assert!(!boxed_f.is_null(), "Internal error - null function pointer");

            let t = (*boxed_f)(&ctx);
            let t = t.as_ref().map(|t| ToSql::to_sql(t));

            match t {
                Ok(Ok(ref value)) => set_result(ctx.ctx, value),
                Ok(Err(err)) => report_error(ctx.ctx, &err),
                Err(err) => report_error(ctx.ctx, err),
            }
        }

        let boxed_f: *mut F = Box::into_raw(Box::new(x_func));
        let c_name = try!(str_to_cstring(fn_name));
        let mut flags = ffi::SQLITE_UTF8;
        if deterministic {
            flags |= ffi::SQLITE_DETERMINISTIC;
        }
        let r = unsafe {
            ffi::sqlite3_create_function_v2(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                flags,
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
        deterministic: bool,
        aggr: D,
    ) -> Result<()>
    where
        D: Aggregate<A, T>,
        T: ToSql,
    {
        unsafe fn aggregate_context<A>(
            ctx: *mut sqlite3_context,
            bytes: usize,
        ) -> Option<*mut *mut A> {
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
            D: Aggregate<A, T>,
            T: ToSql,
        {
            let boxed_aggr: *mut D = ffi::sqlite3_user_data(ctx) as *mut D;
            assert!(
                !boxed_aggr.is_null(),
                "Internal error - null aggregate pointer"
            );

            let pac = match aggregate_context(ctx, ::std::mem::size_of::<*mut A>()) {
                Some(pac) => pac,
                None => {
                    ffi::sqlite3_result_error_nomem(ctx);
                    return;
                }
            };

            if (*pac as *mut A).is_null() {
                *pac = Box::into_raw(Box::new((*boxed_aggr).init()));
            }

            let mut ctx = Context {
                ctx,
                args: slice::from_raw_parts(argv, argc as usize),
            };

            match (*boxed_aggr).step(&mut ctx, &mut **pac) {
                Ok(_) => {}
                Err(err) => report_error(ctx.ctx, &err),
            };
        }

        unsafe extern "C" fn call_boxed_final<A, D, T>(ctx: *mut sqlite3_context)
        where
            D: Aggregate<A, T>,
            T: ToSql,
        {
            let boxed_aggr: *mut D = ffi::sqlite3_user_data(ctx) as *mut D;
            assert!(
                !boxed_aggr.is_null(),
                "Internal error - null aggregate pointer"
            );

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

            let t = (*boxed_aggr).finalize(a);
            let t = t.as_ref().map(|t| ToSql::to_sql(t));
            match t {
                Ok(Ok(ref value)) => set_result(ctx, value),
                Ok(Err(err)) => report_error(ctx, &err),
                Err(err) => report_error(ctx, err),
            }
        }

        let boxed_aggr: *mut D = Box::into_raw(Box::new(aggr));
        let c_name = try!(str_to_cstring(fn_name));
        let mut flags = ffi::SQLITE_UTF8;
        if deterministic {
            flags |= ffi::SQLITE_DETERMINISTIC;
        }
        let r = unsafe {
            ffi::sqlite3_create_function_v2(
                self.db(),
                c_name.as_ptr(),
                n_arg,
                flags,
                boxed_aggr as *mut c_void,
                None,
                Some(call_boxed_step::<A, D, T>),
                Some(call_boxed_final::<A, D, T>),
                Some(free_boxed_value::<D>),
            )
        };
        self.decode_result(r)
    }

    fn remove_function(&mut self, fn_name: &str, n_arg: c_int) -> Result<()> {
        let c_name = try!(str_to_cstring(fn_name));
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

#[cfg(test)]
mod test {
    extern crate regex;

    use self::regex::Regex;
    use std::collections::HashMap;
    use std::f64::EPSILON;
    use std::os::raw::c_double;

    use functions::{Aggregate, Context};
    use {Connection, Error, Result};

    fn half(ctx: &Context) -> Result<c_double> {
        assert!(ctx.len() == 1, "called with unexpected number of arguments");
        let value = try!(ctx.get::<c_double>(0));
        Ok(value / 2f64)
    }

    #[test]
    fn test_function_half() {
        let db = Connection::open_in_memory().unwrap();
        db.create_scalar_function("half", 1, true, half).unwrap();
        let result: Result<f64> = db.query_row("SELECT half(6)", &[], |r| r.get(0));

        assert!((3f64 - result.unwrap()).abs() < EPSILON);
    }

    #[test]
    fn test_remove_function() {
        let db = Connection::open_in_memory().unwrap();
        db.create_scalar_function("half", 1, true, half).unwrap();
        let result: Result<f64> = db.query_row("SELECT half(6)", &[], |r| r.get(0));
        assert!((3f64 - result.unwrap()).abs() < EPSILON);

        db.remove_function("half", 1).unwrap();
        let result: Result<f64> = db.query_row("SELECT half(6)", &[], |r| r.get(0));
        assert!(result.is_err());
    }

    // This implementation of a regexp scalar function uses SQLite's auxilliary data
    // (https://www.sqlite.org/c3ref/get_auxdata.html) to avoid recompiling the regular
    // expression multiple times within one query.
    fn regexp_with_auxilliary(ctx: &Context) -> Result<bool> {
        assert!(ctx.len() == 2, "called with unexpected number of arguments");

        let saved_re: Option<&Regex> = unsafe { ctx.get_aux(0) };
        let new_re = match saved_re {
            None => {
                let s = try!(ctx.get::<String>(0));
                match Regex::new(&s) {
                    Ok(r) => Some(r),
                    Err(err) => return Err(Error::UserFunctionError(Box::new(err))),
                }
            }
            Some(_) => None,
        };

        let is_match = {
            let re = saved_re.unwrap_or_else(|| new_re.as_ref().unwrap());

            let text = try!(ctx.get::<String>(1));
            re.is_match(&text)
        };

        if let Some(re) = new_re {
            ctx.set_aux(0, re);
        }

        Ok(is_match)
    }

    #[test]
    fn test_function_regexp_with_auxilliary() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "BEGIN;
             CREATE TABLE foo (x string);
             INSERT INTO foo VALUES ('lisa');
             INSERT INTO foo VALUES ('lXsi');
             INSERT INTO foo VALUES ('lisX');
             END;",
        ).unwrap();
        db.create_scalar_function("regexp", 2, true, regexp_with_auxilliary)
            .unwrap();

        let result: Result<bool> =
            db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')", &[], |r| r.get(0));

        assert_eq!(true, result.unwrap());

        let result: Result<i64> = db.query_row(
            "SELECT COUNT(*) FROM foo WHERE regexp('l.s[aeiouy]', x) == 1",
            &[],
            |r| r.get(0),
        );

        assert_eq!(2, result.unwrap());
    }

    #[test]
    fn test_function_regexp_with_hashmap_cache() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "BEGIN;
             CREATE TABLE foo (x string);
             INSERT INTO foo VALUES ('lisa');
             INSERT INTO foo VALUES ('lXsi');
             INSERT INTO foo VALUES ('lisX');
             END;",
        ).unwrap();

        // This implementation of a regexp scalar function uses a captured HashMap
        // to keep cached regular expressions around (even across multiple queries)
        // until the function is removed.
        let mut cached_regexes = HashMap::new();
        db.create_scalar_function("regexp", 2, true, move |ctx| {
            assert!(ctx.len() == 2, "called with unexpected number of arguments");

            let regex_s = try!(ctx.get::<String>(0));
            let entry = cached_regexes.entry(regex_s.clone());
            let regex = {
                use std::collections::hash_map::Entry::{Occupied, Vacant};
                match entry {
                    Occupied(occ) => occ.into_mut(),
                    Vacant(vac) => match Regex::new(&regex_s) {
                        Ok(r) => vac.insert(r),
                        Err(err) => return Err(Error::UserFunctionError(Box::new(err))),
                    },
                }
            };

            let text = try!(ctx.get::<String>(1));
            Ok(regex.is_match(&text))
        }).unwrap();

        let result: Result<bool> =
            db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')", &[], |r| r.get(0));

        assert_eq!(true, result.unwrap());

        let result: Result<i64> = db.query_row(
            "SELECT COUNT(*) FROM foo WHERE regexp('l.s[aeiouy]', x) == 1",
            &[],
            |r| r.get(0),
        );

        assert_eq!(2, result.unwrap());
    }

    #[test]
    fn test_varargs_function() {
        let db = Connection::open_in_memory().unwrap();
        db.create_scalar_function("my_concat", -1, true, |ctx| {
            let mut ret = String::new();

            for idx in 0..ctx.len() {
                let s = try!(ctx.get::<String>(idx));
                ret.push_str(&s);
            }

            Ok(ret)
        }).unwrap();

        for &(expected, query) in &[
            ("", "SELECT my_concat()"),
            ("onetwo", "SELECT my_concat('one', 'two')"),
            ("abc", "SELECT my_concat('a', 'b', 'c')"),
        ] {
            let result: String = db.query_row(query, &[], |r| r.get(0)).unwrap();
            assert_eq!(expected, result);
        }
    }

    struct Sum;
    struct Count;

    impl Aggregate<i64, Option<i64>> for Sum {
        fn init(&self) -> i64 {
            0
        }

        fn step(&self, ctx: &mut Context, sum: &mut i64) -> Result<()> {
            *sum += try!(ctx.get::<i64>(0));
            Ok(())
        }

        fn finalize(&self, sum: Option<i64>) -> Result<Option<i64>> {
            Ok(sum)
        }
    }

    impl Aggregate<i64, i64> for Count {
        fn init(&self) -> i64 {
            0
        }

        fn step(&self, _ctx: &mut Context, sum: &mut i64) -> Result<()> {
            *sum += 1;
            Ok(())
        }

        fn finalize(&self, sum: Option<i64>) -> Result<i64> {
            Ok(sum.unwrap_or(0))
        }
    }

    #[test]
    fn test_sum() {
        let db = Connection::open_in_memory().unwrap();
        db.create_aggregate_function("my_sum", 1, true, Sum)
            .unwrap();

        // sum should return NULL when given no columns (contrast with count below)
        let no_result = "SELECT my_sum(i) FROM (SELECT 2 AS i WHERE 1 <> 1)";
        let result: Option<i64> = db.query_row(no_result, &[], |r| r.get(0)).unwrap();
        assert!(result.is_none());

        let single_sum = "SELECT my_sum(i) FROM (SELECT 2 AS i UNION ALL SELECT 2)";
        let result: i64 = db.query_row(single_sum, &[], |r| r.get(0)).unwrap();
        assert_eq!(4, result);

        let dual_sum = "SELECT my_sum(i), my_sum(j) FROM (SELECT 2 AS i, 1 AS j UNION ALL SELECT \
                        2, 1)";
        let result: (i64, i64) = db
            .query_row(dual_sum, &[], |r| (r.get(0), r.get(1)))
            .unwrap();
        assert_eq!((4, 2), result);
    }

    #[test]
    fn test_count() {
        let db = Connection::open_in_memory().unwrap();
        db.create_aggregate_function("my_count", -1, true, Count)
            .unwrap();

        // count should return 0 when given no columns (contrast with sum above)
        let no_result = "SELECT my_count(i) FROM (SELECT 2 AS i WHERE 1 <> 1)";
        let result: i64 = db.query_row(no_result, &[], |r| r.get(0)).unwrap();
        assert_eq!(result, 0);

        let single_sum = "SELECT my_count(i) FROM (SELECT 2 AS i UNION ALL SELECT 2)";
        let result: i64 = db.query_row(single_sum, &[], |r| r.get(0)).unwrap();
        assert_eq!(2, result);
    }
}
