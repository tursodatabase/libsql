//! Code related to `sqlite3_context` common to `functions` and `vtab` modules.

use std::error::Error as StdError;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

use ffi;
use ffi::sqlite3_context;
use ffi::sqlite3_value;

use types::{ToSqlOutput, ValueRef};
use {str_to_cstring, Error};

impl<'a> ValueRef<'a> {
    pub unsafe fn from_value(value: *mut sqlite3_value) -> ValueRef<'a> {
        use std::slice::from_raw_parts;

        match ffi::sqlite3_value_type(value) {
            ffi::SQLITE_NULL => ValueRef::Null,
            ffi::SQLITE_INTEGER => ValueRef::Integer(ffi::sqlite3_value_int64(value)),
            ffi::SQLITE_FLOAT => ValueRef::Real(ffi::sqlite3_value_double(value)),
            ffi::SQLITE_TEXT => {
                let text = ffi::sqlite3_value_text(value);
                assert!(
                    !text.is_null(),
                    "unexpected SQLITE_TEXT value type with NULL data"
                );
                let s = CStr::from_ptr(text as *const c_char);

                // sqlite3_value_text returns UTF8 data, so our unwrap here should be fine.
                let s = s.to_str()
                    .expect("sqlite3_value_text returned invalid UTF-8");
                ValueRef::Text(s)
            }
            ffi::SQLITE_BLOB => {
                let (blob, len) = (
                    ffi::sqlite3_value_blob(value),
                    ffi::sqlite3_value_bytes(value),
                );

                assert!(
                    len >= 0,
                    "unexpected negative return from sqlite3_value_bytes"
                );
                if len > 0 {
                    assert!(
                        !blob.is_null(),
                        "unexpected SQLITE_BLOB value type with NULL data"
                    );
                    ValueRef::Blob(from_raw_parts(blob as *const u8, len as usize))
                } else {
                    // The return value from sqlite3_value_blob() for a zero-length BLOB
                    // is a NULL pointer.
                    ValueRef::Blob(&[])
                }
            }
            _ => unreachable!("sqlite3_value_type returned invalid value"),
        }
    }
}

pub unsafe fn set_result<'a>(ctx: *mut sqlite3_context, result: &ToSqlOutput<'a>) {
    let value = match *result {
        ToSqlOutput::Borrowed(v) => v,
        ToSqlOutput::Owned(ref v) => ValueRef::from(v),

        #[cfg(feature = "blob")]
        ToSqlOutput::ZeroBlob(len) => {
            return ffi::sqlite3_result_zeroblob(ctx, len);
        }
    };

    match value {
        ValueRef::Null => ffi::sqlite3_result_null(ctx),
        ValueRef::Integer(i) => ffi::sqlite3_result_int64(ctx, i),
        ValueRef::Real(r) => ffi::sqlite3_result_double(ctx, r),
        ValueRef::Text(s) => {
            let length = s.len();
            if length > ::std::i32::MAX as usize {
                ffi::sqlite3_result_error_toobig(ctx);
            } else {
                let c_str = match str_to_cstring(s) {
                    Ok(c_str) => c_str,
                    // TODO sqlite3_result_error
                    Err(_) => return ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_MISUSE),
                };
                let destructor = if length > 0 {
                    ffi::SQLITE_TRANSIENT()
                } else {
                    ffi::SQLITE_STATIC()
                };
                ffi::sqlite3_result_text(ctx, c_str.as_ptr(), length as c_int, destructor);
            }
        }
        ValueRef::Blob(b) => {
            let length = b.len();
            if length > ::std::i32::MAX as usize {
                ffi::sqlite3_result_error_toobig(ctx);
            } else if length == 0 {
                ffi::sqlite3_result_zeroblob(ctx, 0)
            } else {
                ffi::sqlite3_result_blob(
                    ctx,
                    b.as_ptr() as *const c_void,
                    length as c_int,
                    ffi::SQLITE_TRANSIENT(),
                );
            }
        }
    }
}

pub unsafe fn report_error(ctx: *mut sqlite3_context, err: &Error) {
    // Extended constraint error codes were added in SQLite 3.7.16. We don't have an explicit
    // feature check for that, and this doesn't really warrant one. We'll use the extended code
    // if we're on the bundled version (since it's at least 3.17.0) and the normal constraint
    // error code if not.
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
