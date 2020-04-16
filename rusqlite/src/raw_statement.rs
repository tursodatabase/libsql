use super::ffi;
use super::unlock_notify;
use super::StatementStatus;
#[cfg(feature = "modern_sqlite")]
use crate::util::SqliteMallocString;
use std::ffi::CStr;
use std::os::raw::c_int;
use std::ptr;
use std::sync::Arc;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
#[derive(Debug)]
pub struct RawStatement {
    ptr: *mut ffi::sqlite3_stmt,
    tail: bool,
    // Cached indices of named parameters, computed on the fly.
    cache: crate::util::ParamIndexCache,
    // Cached SQL (trimmed) that we use as the key when we're in the statement
    // cache. This is None for statements which didn't come from the statement
    // cache.
    //
    // This is probably the same as `self.sql()` in most cases, but we don't
    // care either way -- It's a better cache key as it is anyway since it's the
    // actual source we got from rust.
    //
    // One example of a case where the result of `sqlite_sql` and the value in
    // `statement_cache_key` might differ is if the statement has a `tail`.
    statement_cache_key: Option<Arc<str>>,
}

impl RawStatement {
    pub unsafe fn new(stmt: *mut ffi::sqlite3_stmt, tail: bool) -> RawStatement {
        RawStatement {
            ptr: stmt,
            tail,
            cache: Default::default(),
            statement_cache_key: None,
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub(crate) fn set_statement_cache_key(&mut self, p: impl Into<Arc<str>>) {
        self.statement_cache_key = Some(p.into());
    }

    pub(crate) fn statement_cache_key(&self) -> Option<Arc<str>> {
        self.statement_cache_key.clone()
    }

    pub unsafe fn ptr(&self) -> *mut ffi::sqlite3_stmt {
        self.ptr
    }

    pub fn column_count(&self) -> usize {
        // Note: Can't cache this as it changes if the schema is altered.
        unsafe { ffi::sqlite3_column_count(self.ptr) as usize }
    }

    pub fn column_type(&self, idx: usize) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.ptr, idx as c_int) }
    }

    pub fn column_decltype(&self, idx: usize) -> Option<&CStr> {
        unsafe {
            let decltype = ffi::sqlite3_column_decltype(self.ptr, idx as c_int);
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
            let ptr = ffi::sqlite3_column_name(self.ptr, idx);
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
            let db = unsafe { ffi::sqlite3_db_handle(self.ptr) };
            let mut rc;
            loop {
                rc = unsafe { ffi::sqlite3_step(self.ptr) };
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
            unsafe { ffi::sqlite3_step(self.ptr) }
        }
    }

    pub fn reset(&self) -> c_int {
        unsafe { ffi::sqlite3_reset(self.ptr) }
    }

    pub fn bind_parameter_count(&self) -> usize {
        unsafe { ffi::sqlite3_bind_parameter_count(self.ptr) as usize }
    }

    pub fn bind_parameter_index(&self, name: &str) -> Option<usize> {
        self.cache.get_or_insert_with(name, |param_cstr| {
            let r = unsafe { ffi::sqlite3_bind_parameter_index(self.ptr, param_cstr.as_ptr()) };
            match r {
                0 => None,
                i => Some(i as usize),
            }
        })
    }

    pub fn clear_bindings(&self) -> c_int {
        unsafe { ffi::sqlite3_clear_bindings(self.ptr) }
    }

    pub fn sql(&self) -> Option<&CStr> {
        if self.ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ffi::sqlite3_sql(self.ptr)) })
        }
    }

    pub fn finalize(mut self) -> c_int {
        self.finalize_()
    }

    fn finalize_(&mut self) -> c_int {
        let r = unsafe { ffi::sqlite3_finalize(self.ptr) };
        self.ptr = ptr::null_mut();
        r
    }

    #[cfg(feature = "modern_sqlite")] // 3.7.4
    pub fn readonly(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_readonly(self.ptr) != 0 }
    }

    #[cfg(feature = "modern_sqlite")] // 3.14.0
    pub(crate) fn expanded_sql(&self) -> Option<SqliteMallocString> {
        unsafe { SqliteMallocString::from_raw(ffi::sqlite3_expanded_sql(self.ptr)) }
    }

    pub fn get_status(&self, status: StatementStatus, reset: bool) -> i32 {
        assert!(!self.ptr.is_null());
        unsafe { ffi::sqlite3_stmt_status(self.ptr, status as i32, reset as i32) }
    }

    pub fn has_tail(&self) -> bool {
        self.tail
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.finalize_();
    }
}
