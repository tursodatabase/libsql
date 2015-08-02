use libc::{c_char, c_int, c_void};
use std::ffi::CString;
use std::ptr;

use super::ffi;
use {SqliteError, SqliteResult, SqliteConnection};

pub type LogCallback =
    Option<extern "C" fn (udp: *mut c_void, err: c_int, msg: *const c_char)>;

/// Set up the error logging callback
///
/// cf [The Error And Warning Log](http://sqlite.org/errlog.html).
pub fn config_log(cb: LogCallback) -> SqliteResult<()> {
    let rc = unsafe {
        let p_arg: *mut c_void = ptr::null_mut();
        ffi::sqlite3_config(ffi::SQLITE_CONFIG_LOG, cb, p_arg)
    };
    if rc != ffi::SQLITE_OK {
        return Err(SqliteError{ code: rc, message: "sqlite3_config(SQLITE_CONFIG_LOG, ...)".to_string() });
    }
    Ok(())
}

/// Write a message into the error log established by `config_log`.
pub fn log(err_code: c_int, msg: &str) {
    let msg = CString::new(msg).unwrap();
    unsafe {
        ffi::sqlite3_log(err_code, msg.as_ptr());
    }
}

pub type TraceCallback =
    Option<extern "C" fn (p_arg: *mut c_void, z_sql: *const c_char)>;
pub type ProfileCallback =
    Option<extern "C" fn (p_arg: *mut c_void, z_sql: *const c_char, nanoseconds: u64)>;
impl SqliteConnection {
    /// Register or clear a callback function that can be used for tracing the execution of SQL statements.
    /// Prepared statement placeholders are replaced/logged with their assigned values.
    /// There can only be a single tracer defined for each database connection.
    /// Setting a new tracer clears the old one.
    pub fn trace(&mut self, x_trace: TraceCallback) {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_trace(c.db(), x_trace, ptr::null_mut()); }
    }
    /// Register or clear a callback function that can be used for profiling the execution of SQL statements.
    /// There can only be a single profiler defined for each database connection.
    /// Setting a new profiler clears the old one.
    pub fn profile(&mut self, x_profile: ProfileCallback) {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_profile(c.db(), x_profile, ptr::null_mut()); }
    }
}

#[cfg(test)]
mod test {
    use libc::{c_char, c_int, c_void};
    use std::ffi::CStr;
    use std::io::Write;
    use std::str;

    use ffi;
    use SqliteConnection;

    extern "C" fn log_callback(_: *mut c_void, err: c_int, msg: *const c_char) {
        unsafe {
            let c_slice = CStr::from_ptr(msg).to_bytes();
            let _ = writeln!(::std::io::stderr(), "{}: {:?}", err, str::from_utf8(c_slice));
        }
    }

    #[test]
    fn test_log() {
        if true { // To avoid freezing tests
            return
        }
        unsafe { ffi::sqlite3_shutdown() };
        super::config_log(Some(log_callback)).unwrap();
        //super::log(ffi::SQLITE_NOTICE, "message from rusqlite");
        super::config_log(None).unwrap();
    }

    extern "C" fn trace_callback(_: *mut ::libc::c_void, sql: *const ::libc::c_char) {
        unsafe {
            let c_slice = ::std::ffi::CStr::from_ptr(sql).to_bytes();
            let _ = writeln!(::std::io::stderr(), "TRACE: {:?}", ::std::str::from_utf8(c_slice));
        }
    }

    #[test]
    fn test_trace() {
        let mut db = SqliteConnection::open_in_memory().unwrap();
        db.trace(Some(trace_callback));
        db.execute_batch("PRAGMA application_id = 1").unwrap();
    }
}