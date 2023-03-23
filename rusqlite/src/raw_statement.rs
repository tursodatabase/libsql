use super::ffi;
use super::StatementStatus;
use crate::util::ParamIndexCache;
use crate::util::SqliteMallocString;
use std::ffi::CStr;
use std::os::raw::c_int;
use std::ptr;
use std::sync::Arc;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
#[derive(Debug)]
pub struct RawStatement {
    ptr: *mut ffi::sqlite3_stmt,
    tail: usize,
    // Cached indices of named parameters, computed on the fly.
    cache: ParamIndexCache,
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
    #[inline]
    pub unsafe fn new(stmt: *mut ffi::sqlite3_stmt, tail: usize) -> RawStatement {
        RawStatement {
            ptr: stmt,
            tail,
            cache: ParamIndexCache::default(),
            statement_cache_key: None,
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline]
    pub(crate) fn set_statement_cache_key(&mut self, p: impl Into<Arc<str>>) {
        self.statement_cache_key = Some(p.into());
    }

    #[inline]
    pub(crate) fn statement_cache_key(&self) -> Option<Arc<str>> {
        self.statement_cache_key.clone()
    }

    #[inline]
    pub unsafe fn ptr(&self) -> *mut ffi::sqlite3_stmt {
        self.ptr
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        // Note: Can't cache this as it changes if the schema is altered.
        unsafe { ffi::sqlite3_column_count(self.ptr) as usize }
    }

    #[inline]
    pub fn column_type(&self, idx: usize) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.ptr, idx as c_int) }
    }

    #[inline]
    #[cfg(feature = "column_decltype")]
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

    #[inline]
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

    #[inline]
    #[cfg(not(feature = "unlock_notify"))]
    pub fn step(&self) -> c_int {
        unsafe { ffi::sqlite3_step(self.ptr) }
    }

    #[cfg(feature = "unlock_notify")]
    pub fn step(&self) -> c_int {
        use crate::unlock_notify;
        let mut db = ptr::null_mut::<ffi::sqlite3>();
        loop {
            unsafe {
                let mut rc = ffi::sqlite3_step(self.ptr);
                // Bail out early for success and errors unrelated to locking. We
                // still need check `is_locked` after this, but checking now lets us
                // avoid one or two (admittedly cheap) calls into SQLite that we
                // don't need to make.
                if (rc & 0xff) != ffi::SQLITE_LOCKED {
                    break rc;
                }
                if db.is_null() {
                    db = ffi::sqlite3_db_handle(self.ptr);
                }
                if !unlock_notify::is_locked(db, rc) {
                    break rc;
                }
                rc = unlock_notify::wait_for_unlock_notify(db);
                if rc != ffi::SQLITE_OK {
                    break rc;
                }
                self.reset();
            }
        }
    }

    #[inline]
    pub fn reset(&self) -> c_int {
        unsafe { ffi::sqlite3_reset(self.ptr) }
    }

    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        unsafe { ffi::sqlite3_bind_parameter_count(self.ptr) as usize }
    }

    #[inline]
    pub fn bind_parameter_index(&self, name: &str) -> Option<usize> {
        self.cache.get_or_insert_with(name, |param_cstr| {
            let r = unsafe { ffi::sqlite3_bind_parameter_index(self.ptr, param_cstr.as_ptr()) };
            match r {
                0 => None,
                i => Some(i as usize),
            }
        })
    }

    #[inline]
    pub fn bind_parameter_name(&self, index: i32) -> Option<&CStr> {
        unsafe {
            let name = ffi::sqlite3_bind_parameter_name(self.ptr, index);
            if name.is_null() {
                None
            } else {
                Some(CStr::from_ptr(name))
            }
        }
    }

    #[inline]
    pub fn clear_bindings(&self) {
        unsafe {
            ffi::sqlite3_clear_bindings(self.ptr);
        } // rc is always SQLITE_OK
    }

    #[inline]
    pub fn sql(&self) -> Option<&CStr> {
        if self.ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ffi::sqlite3_sql(self.ptr)) })
        }
    }

    #[inline]
    pub fn finalize(mut self) -> c_int {
        self.finalize_()
    }

    #[inline]
    fn finalize_(&mut self) -> c_int {
        let r = unsafe { ffi::sqlite3_finalize(self.ptr) };
        self.ptr = ptr::null_mut();
        r
    }

    // does not work for PRAGMA
    #[inline]
    pub fn readonly(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_readonly(self.ptr) != 0 }
    }

    #[inline]
    pub(crate) fn expanded_sql(&self) -> Option<SqliteMallocString> {
        unsafe { SqliteMallocString::from_raw(ffi::sqlite3_expanded_sql(self.ptr)) }
    }

    #[inline]
    pub fn get_status(&self, status: StatementStatus, reset: bool) -> i32 {
        assert!(!self.ptr.is_null());
        unsafe { ffi::sqlite3_stmt_status(self.ptr, status as i32, reset as i32) }
    }

    #[inline]
    #[cfg(feature = "extra_check")]
    pub fn has_tail(&self) -> bool {
        self.tail != 0
    }

    #[inline]
    pub fn tail(&self) -> usize {
        self.tail
    }

    #[inline]
    #[cfg(feature = "modern_sqlite")] // 3.28.0
    pub fn is_explain(&self) -> i32 {
        unsafe { ffi::sqlite3_stmt_isexplain(self.ptr) }
    }

    // TODO sqlite3_normalized_sql (https://sqlite.org/c3ref/expanded_sql.html) // 3.27.0 + SQLITE_ENABLE_NORMALIZE
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.finalize_();
    }
}
