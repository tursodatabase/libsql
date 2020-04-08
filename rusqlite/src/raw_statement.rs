use super::ffi;
use super::unlock_notify;
use super::StatementStatus;
use std::ffi::CStr;
use std::os::raw::c_int;
use std::ptr;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
#[derive(Debug)]
pub struct RawStatement(*mut ffi::sqlite3_stmt, bool);

impl RawStatement {
    pub unsafe fn new(stmt: *mut ffi::sqlite3_stmt, tail: bool) -> RawStatement {
        RawStatement(stmt, tail)
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    pub unsafe fn ptr(&self) -> *mut ffi::sqlite3_stmt {
        self.0
    }

    pub fn column_count(&self) -> usize {
        unsafe { ffi::sqlite3_column_count(self.0) as usize }
    }

    pub fn column_type(&self, idx: usize) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.0, idx as c_int) }
    }

    pub fn column_decltype(&self, idx: usize) -> Option<&CStr> {
        unsafe {
            let decltype = ffi::sqlite3_column_decltype(self.0, idx as c_int);
            if decltype.is_null() {
                None
            } else {
                Some(CStr::from_ptr(decltype))
            }
        }
    }

    pub fn column_name(&self, idx: usize) -> Option<&CStr> {
        let idx = idx as c_int;
        if idx < 0 || idx >= self.column_count() as c_int {
            return None;
        }
        unsafe {
            let ptr = ffi::sqlite3_column_name(self.0, idx);
            // If ptr is null here, it's an OOM, so there's probably nothing
            // meaningful we can do. Just assert instead of returning None.
            assert!(
                !ptr.is_null(),
                "Null pointer from sqlite3_column_name: Out of memory?"
            );
            Some(CStr::from_ptr(ptr))
        }
    }

    pub fn step(&self) -> c_int {
        if cfg!(feature = "unlock_notify") {
            let db = unsafe { ffi::sqlite3_db_handle(self.0) };
            let mut rc;
            loop {
                rc = unsafe { ffi::sqlite3_step(self.0) };
                if unsafe { !unlock_notify::is_locked(db, rc) } {
                    break;
                }
                rc = unsafe { unlock_notify::wait_for_unlock_notify(db) };
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

    pub fn bind_parameter_count(&self) -> usize {
        unsafe { ffi::sqlite3_bind_parameter_count(self.0) as usize }
    }

    pub fn bind_parameter_index(&self, name: &CStr) -> Option<usize> {
        let r = unsafe { ffi::sqlite3_bind_parameter_index(self.0, name.as_ptr()) };
        match r {
            0 => None,
            i => Some(i as usize),
        }
    }

    pub fn clear_bindings(&self) -> c_int {
        unsafe { ffi::sqlite3_clear_bindings(self.0) }
    }

    pub fn sql(&self) -> Option<&CStr> {
        if self.0.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ffi::sqlite3_sql(self.0)) })
        }
    }

    pub fn finalize(mut self) -> c_int {
        self.finalize_()
    }

    fn finalize_(&mut self) -> c_int {
        let r = unsafe { ffi::sqlite3_finalize(self.0) };
        self.0 = ptr::null_mut();
        r
    }

    #[cfg(feature = "modern_sqlite")] // 3.7.4
    pub fn readonly(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_readonly(self.0) != 0 }
    }

    /// `CStr` must be freed
    #[cfg(feature = "modern_sqlite")] // 3.14.0
    pub unsafe fn expanded_sql(&self) -> Option<&CStr> {
        let ptr = ffi::sqlite3_expanded_sql(self.0);
        if ptr.is_null() {
            None
        } else {
            Some(CStr::from_ptr(ptr))
        }
    }

    pub fn get_status(&self, status: StatementStatus, reset: bool) -> i32 {
        assert!(!self.0.is_null());
        unsafe { ffi::sqlite3_stmt_status(self.0, status as i32, reset as i32) }
    }

    pub fn has_tail(&self) -> bool {
        self.1
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.finalize_();
    }
}
