use std::ffi::CStr;
use std::ptr;
use std::os::raw::c_int;
use super::ffi;
use super::unlock_notify;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
#[derive(Debug)]
pub struct RawStatement(*mut ffi::sqlite3_stmt);

impl RawStatement {
    pub fn new(stmt: *mut ffi::sqlite3_stmt) -> RawStatement {
        RawStatement(stmt)
    }

    pub unsafe fn ptr(&self) -> *mut ffi::sqlite3_stmt {
        self.0
    }

    pub fn column_count(&self) -> c_int {
        unsafe { ffi::sqlite3_column_count(self.0) }
    }

    pub fn column_type(&self, idx: c_int) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.0, idx) }
    }

    pub fn column_name(&self, idx: c_int) -> &CStr {
        unsafe { CStr::from_ptr(ffi::sqlite3_column_name(self.0, idx)) }
    }

    pub fn step(&self) -> c_int {
        if cfg!(feature = "unlock_notify") {
            let db = unsafe { ffi::sqlite3_db_handle(self.0) };
            let mut rc;
            loop {
                rc = unsafe { ffi::sqlite3_step(self.0) };
                if !unlock_notify::is_locked(db, rc) {
                    break;
                }
                rc = unlock_notify::wait_for_unlock_notify(db);
                if rc != ffi::SQLITE_OK {
                    break;
                }
                self.reset();
            }
            rc
        } else {
            unsafe { ffi::sqlite3_step(self.0) }
        }
    }

    pub fn reset(&self) -> c_int {
        unsafe { ffi::sqlite3_reset(self.0) }
    }

    pub fn bind_parameter_count(&self) -> c_int {
        unsafe { ffi::sqlite3_bind_parameter_count(self.0) }
    }

    pub fn bind_parameter_index(&self, name: &CStr) -> Option<c_int> {
        let r = unsafe { ffi::sqlite3_bind_parameter_index(self.0, name.as_ptr()) };
        match r {
            0 => None,
            i => Some(i),
        }
    }

    pub fn clear_bindings(&self) -> c_int {
        unsafe { ffi::sqlite3_clear_bindings(self.0) }
    }

    pub fn sql(&self) -> &CStr {
        unsafe { CStr::from_ptr(ffi::sqlite3_sql(self.0)) }
    }

    pub fn finalize(mut self) -> c_int {
        self.finalize_()
    }

    fn finalize_(&mut self) -> c_int {
        let r = unsafe { ffi::sqlite3_finalize(self.0) };
        self.0 = ptr::null_mut();
        r
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.finalize_();
    }
}
