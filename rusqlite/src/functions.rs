//! Create or redefine SQL functions.
//!
//! # Example
//!
//! Adding a `regexp` function to a connection in which compiled regular expressions
//! are cached in a `HashMap`. For an alternative implementation that uses SQLite's
//! [Function Auxilliary Data](https://www.sqlite.org/c3ref/get_auxdata.html) interface
//! to avoid recompiling regular expressions, see the unit tests for this module.
//!
//! ```rust
//! extern crate libsqlite3_sys;
//! extern crate rusqlite;
//! extern crate regex;
//!
//! use rusqlite::{Connection, Error, SqliteResult};
//! use std::collections::HashMap;
//! use regex::Regex;
//!
//! fn add_regexp_function(db: &Connection) -> SqliteResult<()> {
//!     let mut cached_regexes = HashMap::new();
//!     db.create_scalar_function("regexp", 2, true, move |ctx| {
//!         let regex_s = try!(ctx.get::<String>(0));
//!         let entry = cached_regexes.entry(regex_s.clone());
//!         let regex = {
//!             use std::collections::hash_map::Entry::{Occupied, Vacant};
//!             match entry {
//!                 Occupied(occ) => occ.into_mut(),
//!                 Vacant(vac) => {
//!                     let r = try!(Regex::new(&regex_s).map_err(|e| Error {
//!                         code: libsqlite3_sys::SQLITE_ERROR,
//!                         message: format!("Invalid regular expression: {}", e),
//!                     }));
//!                     vac.insert(r)
//!                 }
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
//!     let is_match = db.query_row("SELECT regexp('[aeiou]*', 'aaaaeeeiii')", &[],
//!                                 |row| row.get::<bool>(0)).unwrap();
//!
//!     assert!(is_match);
//! }
//! ```
use std::ffi::CStr;
use std::mem;
use std::ptr;
use std::slice;
use std::str;
use libc::{c_int, c_double, c_char, c_void};

use ffi;
pub use ffi::sqlite3_context;
pub use ffi::sqlite3_value;
pub use ffi::sqlite3_value_type;
pub use ffi::sqlite3_value_numeric_type;

use types::Null;

use {SqliteResult, Error, Connection, str_to_cstring, InnerConnection};

/// A trait for types that can be converted into the result of an SQL function.
pub trait ToResult {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context);
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl ToResult for $t {
            unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
                ffi::$f(ctx, *self)
            }
        }
    )
);

raw_to_impl!(c_int, sqlite3_result_int);
raw_to_impl!(i64, sqlite3_result_int64);
raw_to_impl!(c_double, sqlite3_result_double);

impl<'a> ToResult for bool {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        match *self {
            true => ffi::sqlite3_result_int(ctx, 1),
            _ => ffi::sqlite3_result_int(ctx, 0),
        }
    }
}


impl<'a> ToResult for &'a str {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        let length = self.len();
        if length > ::std::i32::MAX as usize {
            ffi::sqlite3_result_error_toobig(ctx);
            return;
        }
        match str_to_cstring(self) {
            Ok(c_str) => {
                ffi::sqlite3_result_text(ctx,
                                         c_str.as_ptr(),
                                         length as c_int,
                                         ffi::SQLITE_TRANSIENT())
            }
            Err(_) => ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_MISUSE), // TODO sqlite3_result_error
        }
    }
}

impl ToResult for String {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        (&self[..]).set_result(ctx)
    }
}

impl<'a> ToResult for &'a [u8] {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        if self.len() > ::std::i32::MAX as usize {
            ffi::sqlite3_result_error_toobig(ctx);
            return;
        }
        ffi::sqlite3_result_blob(ctx,
                                 mem::transmute(self.as_ptr()),
                                 self.len() as c_int,
                                 ffi::SQLITE_TRANSIENT())
    }
}

impl ToResult for Vec<u8> {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        (&self[..]).set_result(ctx)
    }
}

impl<T: ToResult> ToResult for Option<T> {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        match *self {
            None => ffi::sqlite3_result_null(ctx),
            Some(ref t) => t.set_result(ctx),
        }
    }
}

impl ToResult for Null {
    unsafe fn set_result(&self, ctx: *mut sqlite3_context) {
        ffi::sqlite3_result_null(ctx)
    }
}


// sqlite3_result_error_code, c_int
// sqlite3_result_error_nomem
// sqlite3_result_error_toobig
// sqlite3_result_error, *const c_char, c_int
// sqlite3_result_zeroblob
// sqlite3_result_value

/// A trait for types that can be created from a SQLite function parameter value.
pub trait FromValue: Sized {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<Self>;

    /// FromValue types can implement this method and use sqlite3_value_type to check that
    /// the type reported by SQLite matches a type suitable for Self. This method is used
    /// by `Context::get` to confirm that the parameter contains a valid type before
    /// attempting to retrieve the value.
    unsafe fn parameter_has_valid_sqlite_type(_: *mut sqlite3_value) -> bool {
        true
    }
}


macro_rules! raw_from_impl(
    ($t:ty, $f:ident, $c:expr) => (
        impl FromValue for $t {
            unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<$t> {
                Ok(ffi::$f(v))
            }

            unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
                sqlite3_value_numeric_type(v) == $c
            }
        }
    )
);

raw_from_impl!(c_int, sqlite3_value_int, ffi::SQLITE_INTEGER);
raw_from_impl!(i64, sqlite3_value_int64, ffi::SQLITE_INTEGER);

impl FromValue for bool {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<bool> {
        match ffi::sqlite3_value_int(v) {
            0 => Ok(false),
            _ => Ok(true),
        }
    }

    unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
        sqlite3_value_numeric_type(v) == ffi::SQLITE_INTEGER
    }
}

impl FromValue for c_double {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<c_double> {
        Ok(ffi::sqlite3_value_double(v))
    }

    unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
        sqlite3_value_numeric_type(v) == ffi::SQLITE_FLOAT ||
        sqlite3_value_numeric_type(v) == ffi::SQLITE_INTEGER
    }
}

impl FromValue for String {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<String> {
        let c_text = ffi::sqlite3_value_text(v);
        if c_text.is_null() {
            Ok("".to_string())
        } else {
            let c_slice = CStr::from_ptr(c_text as *const c_char).to_bytes();
            let utf8_str = str::from_utf8(c_slice);
            utf8_str.map(|s| s.to_string())
                    .map_err(|e| {
                        Error {
                            code: 0,
                            message: e.to_string(),
                        }
                    })
        }
    }

    unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
        sqlite3_value_type(v) == ffi::SQLITE_TEXT
    }
}

impl FromValue for Vec<u8> {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<Vec<u8>> {
        use std::slice::from_raw_parts;
        let c_blob = ffi::sqlite3_value_blob(v);
        let len = ffi::sqlite3_value_bytes(v);

        assert!(len >= 0,
                "unexpected negative return from sqlite3_value_bytes");
        let len = len as usize;

        Ok(from_raw_parts(mem::transmute(c_blob), len).to_vec())
    }

    unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
        sqlite3_value_type(v) == ffi::SQLITE_BLOB
    }
}

impl<T: FromValue> FromValue for Option<T> {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<Option<T>> {
        if sqlite3_value_type(v) == ffi::SQLITE_NULL {
            Ok(None)
        } else {
            FromValue::parameter_value(v).map(|t| Some(t))
        }
    }

    unsafe fn parameter_has_valid_sqlite_type(v: *mut sqlite3_value) -> bool {
        sqlite3_value_type(v) == ffi::SQLITE_NULL || T::parameter_has_valid_sqlite_type(v)
    }
}

unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    let _: Box<T> = Box::from_raw(mem::transmute(p));
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

    /// Returns the `idx`th argument as a `T`.
    ///
    /// # Failure
    ///
    /// Will panic if `idx` is greater than or equal to `self.len()`.
    ///
    /// Will return Err if the underlying SQLite type cannot be converted to a `T`.
    pub fn get<T: FromValue>(&self, idx: usize) -> SqliteResult<T> {
        let arg = self.args[idx];
        unsafe {
            if T::parameter_has_valid_sqlite_type(arg) {
                T::parameter_value(arg)
            } else {
                Err(Error {
                    code: ffi::SQLITE_MISMATCH,
                    message: "Invalid value type".to_string(),
                })
            }
        }
    }

    /// Sets the auxilliary data associated with a particular parameter. See
    /// https://www.sqlite.org/c3ref/get_auxdata.html for a discussion of
    /// this feature, or the unit tests of this module for an example.
    pub fn set_aux<T>(&self, arg: c_int, value: T) {
        let boxed = Box::into_raw(Box::new(value));
        unsafe {
            ffi::sqlite3_set_auxdata(self.ctx,
                                     arg,
                                     mem::transmute(boxed),
                                     Some(mem::transmute(free_boxed_value::<T>)))
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

impl Connection {
    /// Attach a user-defined scalar function to this database connection.
    ///
    /// `fn_name` is the name the function will be accessible from SQL.
    /// `n_arg` is the number of arguments to the function. Use `-1` for a variable
    /// number. If the function always returns the same value given the same
    /// input, `deterministic` should be `true`.
    ///
    /// The function will remain available until the connection is closed or
    /// until it is explicitly removed via `remove_function`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rusqlite::{Connection, SqliteResult};
    /// # type c_double = f64;
    /// fn scalar_function_example(db: Connection) -> SqliteResult<()> {
    ///     try!(db.create_scalar_function("halve", 1, true, |ctx| {
    ///         let value = try!(ctx.get::<c_double>(0));
    ///         Ok(value / 2f64)
    ///     }));
    ///
    ///     let six_halved = try!(db.query_row("SELECT halve(6)", &[], |r| r.get::<f64>(0)));
    ///     assert_eq!(six_halved, 3f64);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be attached to the connection.
    pub fn create_scalar_function<F, T>(&self,
                                        fn_name: &str,
                                        n_arg: c_int,
                                        deterministic: bool,
                                        x_func: F)
                                        -> SqliteResult<()>
        where F: FnMut(&Context) -> SqliteResult<T>,
              T: ToResult
    {
        self.db.borrow_mut().create_scalar_function(fn_name, n_arg, deterministic, x_func)
    }

    /// Removes a user-defined function from this database connection.
    ///
    /// `fn_name` and `n_arg` should match the name and number of arguments
    /// given to `create_scalar_function`.
    ///
    /// # Failure
    ///
    /// Will return Err if the function could not be removed.
    pub fn remove_function(&self, fn_name: &str, n_arg: c_int) -> SqliteResult<()> {
        self.db.borrow_mut().remove_function(fn_name, n_arg)
    }
}

impl InnerConnection {
    fn create_scalar_function<F, T>(&mut self,
                                    fn_name: &str,
                                    n_arg: c_int,
                                    deterministic: bool,
                                    x_func: F)
                                    -> SqliteResult<()>
        where F: FnMut(&Context) -> SqliteResult<T>,
              T: ToResult
    {
        extern "C" fn call_boxed_closure<F, T>(ctx: *mut sqlite3_context,
                                               argc: c_int,
                                               argv: *mut *mut sqlite3_value)
            where F: FnMut(&Context) -> SqliteResult<T>,
                  T: ToResult
        {
            unsafe {
                let ctx = Context {
                    ctx: ctx,
                    args: slice::from_raw_parts(argv, argc as usize),
                };
                let boxed_f: *mut F = mem::transmute(ffi::sqlite3_user_data(ctx.ctx));
                assert!(!boxed_f.is_null(), "Internal error - null function pointer");
                match (*boxed_f)(&ctx) {
                    Ok(r) => r.set_result(ctx.ctx),
                    Err(e) => {
                        ffi::sqlite3_result_error_code(ctx.ctx, e.code);
                        if let Ok(cstr) = str_to_cstring(&e.message) {
                            ffi::sqlite3_result_error(ctx.ctx, cstr.as_ptr(), -1);
                        }
                    }
                }
            }
        }

        let boxed_f: *mut F = Box::into_raw(Box::new(x_func));
        let c_name = try!(str_to_cstring(fn_name));
        let mut flags = ffi::SQLITE_UTF8;
        if deterministic {
            flags |= ffi::SQLITE_DETERMINISTIC;
        }
        let r = unsafe {
            ffi::sqlite3_create_function_v2(self.db(),
                                            c_name.as_ptr(),
                                            n_arg,
                                            flags,
                                            mem::transmute(boxed_f),
                                            Some(call_boxed_closure::<F, T>),
                                            None,
                                            None,
                                            Some(mem::transmute(free_boxed_value::<F>)))
        };
        self.decode_result(r)
    }

    fn remove_function(&mut self, fn_name: &str, n_arg: c_int) -> SqliteResult<()> {
        let c_name = try!(str_to_cstring(fn_name));
        let r = unsafe {
            ffi::sqlite3_create_function_v2(self.db(),
                                            c_name.as_ptr(),
                                            n_arg,
                                            ffi::SQLITE_UTF8,
                                            ptr::null_mut(),
                                            None,
                                            None,
                                            None,
                                            None)
        };
        self.decode_result(r)
    }
}

#[cfg(test)]
mod test {
    extern crate regex;

    use std::collections::HashMap;
    use libc::c_double;
    use self::regex::Regex;

    use {Connection, Error, SqliteResult};
    use ffi;
    use functions::Context;

    fn half(ctx: &Context) -> SqliteResult<c_double> {
        assert!(ctx.len() == 1, "called with unexpected number of arguments");
        let value = try!(ctx.get::<c_double>(0));
        Ok(value / 2f64)
    }

    #[test]
    fn test_function_half() {
        let db = Connection::open_in_memory().unwrap();
        db.create_scalar_function("half", 1, true, half).unwrap();
        let result = db.query_row("SELECT half(6)", &[], |r| r.get::<f64>(0));

        assert_eq!(3f64, result.unwrap());
    }

    #[test]
    fn test_remove_function() {
        let db = Connection::open_in_memory().unwrap();
        db.create_scalar_function("half", 1, true, half).unwrap();
        let result = db.query_row("SELECT half(6)", &[], |r| r.get::<f64>(0));
        assert_eq!(3f64, result.unwrap());

        db.remove_function("half", 1).unwrap();
        let result = db.query_row("SELECT half(6)", &[], |r| r.get::<f64>(0));
        assert!(result.is_err());
    }

    // This implementation of a regexp scalar function uses SQLite's auxilliary data
    // (https://www.sqlite.org/c3ref/get_auxdata.html) to avoid recompiling the regular
    // expression multiple times within one query.
    fn regexp_with_auxilliary(ctx: &Context) -> SqliteResult<bool> {
        assert!(ctx.len() == 2, "called with unexpected number of arguments");

        let saved_re: Option<&Regex> = unsafe { ctx.get_aux(0) };
        let new_re = match saved_re {
            None => {
                let s = try!(ctx.get::<String>(0));
                let r = try!(Regex::new(&s).map_err(|e| {
                    Error {
                        code: ffi::SQLITE_ERROR,
                        message: format!("Invalid regular expression: {}", e),
                    }
                }));
                Some(r)
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
    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn test_function_regexp_with_auxilliary() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("BEGIN;
                         CREATE TABLE foo (x string);
                         INSERT INTO foo VALUES ('lisa');
                         INSERT INTO foo VALUES ('lXsi');
                         INSERT INTO foo VALUES ('lisX');
                         END;").unwrap();
        db.create_scalar_function("regexp", 2, true, regexp_with_auxilliary).unwrap();

        let result = db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')",
                                  &[],
                                  |r| r.get::<bool>(0));

        assert_eq!(true, result.unwrap());

        let result = db.query_row("SELECT COUNT(*) FROM foo WHERE regexp('l.s[aeiouy]', x) == 1",
                                  &[],
                                  |r| r.get::<i64>(0));

        assert_eq!(2, result.unwrap());
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn test_function_regexp_with_hashmap_cache() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("BEGIN;
                         CREATE TABLE foo (x string);
                         INSERT INTO foo VALUES ('lisa');
                         INSERT INTO foo VALUES ('lXsi');
                         INSERT INTO foo VALUES ('lisX');
                         END;").unwrap();

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
                    Vacant(vac) => {
                        let r = try!(Regex::new(&regex_s).map_err(|e| Error {
                            code: ffi::SQLITE_ERROR,
                            message: format!("Invalid regular expression: {}", e),
                        }));
                        vac.insert(r)
                    }
                }
            };

            let text = try!(ctx.get::<String>(1));
            Ok(regex.is_match(&text))
        }).unwrap();

        let result = db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')",
                                  &[],
                                  |r| r.get::<bool>(0));

        assert_eq!(true, result.unwrap());

        let result = db.query_row("SELECT COUNT(*) FROM foo WHERE regexp('l.s[aeiouy]', x) == 1",
                                  &[],
                                  |r| r.get::<i64>(0));

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
          })
          .unwrap();

        for &(expected, query) in &[("", "SELECT my_concat()"),
                                    ("onetwo", "SELECT my_concat('one', 'two')"),
                                    ("abc", "SELECT my_concat('a', 'b', 'c')")] {
            let result: String = db.query_row(query, &[], |r| r.get(0)).unwrap();
            assert_eq!(expected, result);
        }
    }
}
