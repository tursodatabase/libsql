//! Create or redefine SQL functions
use std::ffi::{CStr};
use std::mem;
use std::ptr;
use std::str;
use libc::{c_int, c_double, c_char};

use ffi;
pub use ffi::sqlite3_context as sqlite3_context;
pub use ffi::sqlite3_value as sqlite3_value;
pub use ffi::sqlite3_value_type as sqlite3_value_type;
pub use ffi::sqlite3_value_numeric_type as sqlite3_value_numeric_type;

use types::Null;

use {SqliteResult, SqliteError, SqliteConnection, str_to_cstring, InnerSqliteConnection};

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
            return
        }
        match str_to_cstring(self) {
            Ok(c_str) => ffi::sqlite3_result_text(ctx, c_str.as_ptr(), length as c_int,
                                                ffi::SQLITE_TRANSIENT()),
            Err(_)    => ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_MISUSE), // TODO sqlite3_result_error
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
            return
        }
        ffi::sqlite3_result_blob(
            ctx, mem::transmute(self.as_ptr()), self.len() as c_int, ffi::SQLITE_TRANSIENT())
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
pub trait FromValue {
    unsafe fn parameter_value(v: *mut sqlite3_value) -> SqliteResult<Self>;

    /// FromValue types can implement this method and use sqlite3_value_type to check that
    /// the type reported by SQLite matches a type suitable for Self. This method is used
    /// by `???` to confirm that the parameter contains a valid type before
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
        sqlite3_value_numeric_type(v) == ffi::SQLITE_FLOAT || sqlite3_value_numeric_type(v) == ffi::SQLITE_INTEGER
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
            utf8_str
                .map(|s| { s.to_string() })
                .map_err(|e| { SqliteError{code: 0, message: e.to_string()} })
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

        assert!(len >= 0, "unexpected negative return from sqlite3_value_bytes");
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
        sqlite3_value_type(v) == ffi::SQLITE_NULL ||
            T::parameter_has_valid_sqlite_type(v)
    }
}

// sqlite3_user_data
// sqlite3_get_auxdata
// sqlite3_set_auxdata

pub type ScalarFunc =
    Option<extern "C" fn (ctx: *mut sqlite3_context, argc: c_int, argv: *mut *mut sqlite3_value)>;

impl SqliteConnection {
    // TODO pApp
    pub fn create_scalar_function(&self, fn_name: &str, n_arg: c_int, deterministic: bool, x_func: ScalarFunc) -> SqliteResult<()> {
        self.db.borrow_mut().create_scalar_function(fn_name, n_arg, deterministic, x_func)
    }
}

impl InnerSqliteConnection {
    pub fn create_scalar_function(&mut self, fn_name: &str, n_arg: c_int, deterministic: bool, x_func: ScalarFunc) -> SqliteResult<()> {
        let c_name = try!(str_to_cstring(fn_name));
        let mut flags = ffi::SQLITE_UTF8;
        if deterministic {
            flags |= ffi::SQLITE_DETERMINISTIC;
        }
        let r = unsafe {
            ffi::sqlite3_create_function_v2(self.db(), c_name.as_ptr(), n_arg, flags, ptr::null_mut(), x_func, None, None, None)
        };
        self.decode_result(r)
    }
}

#[cfg(test)]
mod test {
    extern crate regex;

    use std::boxed::Box;
    use std::ffi::{CString};
    use std::mem;
    use libc::{c_int, c_double, c_void};
    use self::regex::Regex;

    use SqliteConnection;
    use ffi;
    use ffi::sqlite3_context as sqlite3_context;
    use ffi::sqlite3_value as sqlite3_value;
    use functions::{FromValue,ToResult};

    extern "C" fn half(ctx: *mut sqlite3_context, _: c_int, argv: *mut *mut sqlite3_value) {
        unsafe {
            let arg = *argv.offset(0);
            if c_double::parameter_has_valid_sqlite_type(arg) {
                let value = c_double::parameter_value(arg).unwrap() / 2f64;
                value.set_result(ctx);
            } else {
                ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_MISMATCH);
            }
        }
    }

    #[test]
    fn test_half() {
        let db = SqliteConnection::open_in_memory().unwrap();
        db.create_scalar_function("half", 1, true, Some(half)).unwrap();
        let result = db.query_row("SELECT half(6)",
                                           &[],
                                           |r| r.get::<f64>(0));

        assert_eq!(3f64, result.unwrap());
    }

    extern "C" fn regexp_free(raw: *mut c_void) {
        unsafe {
            Box::from_raw(raw);
        }
    }

    extern "C" fn regexp(ctx: *mut sqlite3_context, _: c_int, argv: *mut *mut sqlite3_value) {
        unsafe {
            let mut re_ptr = ffi::sqlite3_get_auxdata(ctx, 0) as *const Regex;
            let mut re_opt = None;
            if re_ptr.is_null() {
                let raw = String::parameter_value(*argv.offset(0));
                if raw.is_err() {
                    let msg = CString::new(format!("{}", raw.unwrap_err())).unwrap();
                    ffi::sqlite3_result_error(ctx, msg.as_ptr(), -1);
                    return
                }
                let comp = Regex::new(raw.unwrap().as_ref());
                if comp.is_err() {
                    let msg = CString::new(format!("{}", comp.unwrap_err())).unwrap();
                    ffi::sqlite3_result_error(ctx, msg.as_ptr(), -1);
                    return
                }
                let re = comp.unwrap();
                re_ptr = &re as *const Regex;
                re_opt = Some(re);
            }

            let text = String::parameter_value(*argv.offset(1));
            if text.is_ok() {
                let text = text.unwrap();
                (*re_ptr).is_match(text.as_ref()).set_result(ctx);
            }

            if re_opt.is_some() {
                ffi::sqlite3_set_auxdata(ctx, 0, mem::transmute(Box::into_raw(Box::new(re_opt.unwrap()))), Some(regexp_free));
            }
        }
    }

    #[test]
    fn test_regexp() {
        let db = SqliteConnection::open_in_memory().unwrap();
        db.create_scalar_function("regexp", 2, true, Some(regexp)).unwrap();
        let result = db.query_row("SELECT regexp('l.s[aeiouy]', 'lisa')",
                                           &[],
                                           |r| r.get::<bool>(0));

        assert_eq!(true, result.unwrap());
    }
}