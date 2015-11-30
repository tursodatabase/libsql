//! Tracing and profiling functions. Error and warning log.

use libc::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::ptr;
use std::str;

use super::ffi;
use {SqliteError, SqliteResult, SqliteConnection};

/// Set up the process-wide SQLite error logging callback.
/// This function is marked unsafe for two reasons:
///
/// * The function is not threadsafe. No other SQLite calls may be made while
///   `config_log` is running, and multiple threads may not call `config_log`
///   simultaneously.
/// * The provided `callback` itself function has two requirements:
///     * It must not invoke any SQLite calls.
///     * It must be threadsafe if SQLite is used in a multithreaded way.
///
/// cf [The Error And Warning Log](http://sqlite.org/errlog.html).
pub unsafe fn config_log(callback: Option<fn(c_int, &str)>) -> SqliteResult<()> {
    extern "C" fn log_callback(p_arg: *mut c_void, err: c_int, msg: *const c_char) {
        let c_slice = unsafe { CStr::from_ptr(msg).to_bytes() };
        let callback: fn(c_int, &str) = unsafe { mem::transmute(p_arg) };

        if let Ok(s) = str::from_utf8(c_slice) {
            callback(err, s);
        }
    }

    let rc = match callback {
        Some(f) => {
            let p_arg: *mut c_void = mem::transmute(f);
            ffi::sqlite3_config(ffi::SQLITE_CONFIG_LOG, Some(log_callback), p_arg)
        },
        None => {
            let nullptr: *mut c_void = ptr::null_mut();
            ffi::sqlite3_config(ffi::SQLITE_CONFIG_LOG, nullptr, nullptr)
        }
    };

    if rc != ffi::SQLITE_OK {
        return Err(SqliteError{ code: rc, message: "sqlite3_config(SQLITE_CONFIG_LOG, ...)".to_string() });
    }

    Ok(())
}

/// Write a message into the error log established by `config_log`.
pub fn log(err_code: c_int, msg: &str) {
    let msg = CString::new(msg).expect("SQLite log messages cannot contain embedded zeroes");
    unsafe {
        ffi::sqlite3_log(err_code, msg.as_ptr());
    }
}

/// The trace callback function signature.
pub type TraceCallback =
    Option<extern "C" fn (p_arg: *mut c_void, z_sql: *const c_char)>;
/// The profile callback function signature.
pub type ProfileCallback =
    Option<extern "C" fn (p_arg: *mut c_void, z_sql: *const c_char, nanoseconds: u64)>;

impl SqliteConnection {
    /// Register or clear a callback function that can be used for tracing the execution of SQL statements.
    ///
    /// Prepared statement placeholders are replaced/logged with their assigned values.
    /// There can only be a single tracer defined for each database connection.
    /// Setting a new tracer clears the old one.
    pub fn trace(&mut self, x_trace: TraceCallback) {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_trace(c.db(), x_trace, ptr::null_mut()); }
    }
    /// Register or clear a callback function that can be used for profiling the execution of SQL statements.
    ///
    /// There can only be a single profiler defined for each database connection.
    /// Setting a new profiler clears the old one.
    pub fn profile(&mut self, x_profile: ProfileCallback) {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_profile(c.db(), x_profile, ptr::null_mut()); }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use SqliteConnection;

    extern "C" fn profile_callback(_: *mut ::libc::c_void, sql: *const ::libc::c_char, nanoseconds: u64) {
        use std::time::Duration;
        unsafe {
            let c_slice = ::std::ffi::CStr::from_ptr(sql).to_bytes();
            let d = Duration::from_millis(nanoseconds / 1_000_000);
            let _ = writeln!(::std::io::stderr(), "PROFILE: {:?} ({:?})", ::std::str::from_utf8(c_slice), d);
        }
    }

    #[test]
    fn test_profile() {
        let mut db = SqliteConnection::open_in_memory().unwrap();
        db.profile(Some(profile_callback));
        db.execute_batch("PRAGMA application_id = 1").unwrap();
    }
}
