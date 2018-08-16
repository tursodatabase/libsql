//! Commit, Data Change and Rollback Notification Callbacks
#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use ffi;

use {Connection, InnerConnection};

/// Authorizer Action Codes
#[derive(Debug, PartialEq)]
pub enum Action {
    UNKNOWN = -1,
    SQLITE_CREATE_INDEX = ffi::SQLITE_CREATE_INDEX as isize,
    SQLITE_CREATE_TABLE = ffi::SQLITE_CREATE_TABLE as isize,
    SQLITE_CREATE_TEMP_INDEX = ffi::SQLITE_CREATE_TEMP_INDEX as isize,
    SQLITE_CREATE_TEMP_TABLE = ffi::SQLITE_CREATE_TEMP_TABLE as isize,
    SQLITE_CREATE_TEMP_TRIGGER = ffi::SQLITE_CREATE_TEMP_TRIGGER as isize,
    SQLITE_CREATE_TEMP_VIEW = ffi::SQLITE_CREATE_TEMP_VIEW as isize,
    SQLITE_CREATE_TRIGGER = ffi::SQLITE_CREATE_TRIGGER as isize,
    SQLITE_CREATE_VIEW = ffi::SQLITE_CREATE_VIEW as isize,
    SQLITE_DELETE = ffi::SQLITE_DELETE as isize,
    SQLITE_DROP_INDEX = ffi::SQLITE_DROP_INDEX as isize,
    SQLITE_DROP_TABLE = ffi::SQLITE_DROP_TABLE as isize,
    SQLITE_DROP_TEMP_INDEX = ffi::SQLITE_DROP_TEMP_INDEX as isize,
    SQLITE_DROP_TEMP_TABLE = ffi::SQLITE_DROP_TEMP_TABLE as isize,
    SQLITE_DROP_TEMP_TRIGGER = ffi::SQLITE_DROP_TEMP_TRIGGER as isize,
    SQLITE_DROP_TEMP_VIEW = ffi::SQLITE_DROP_TEMP_VIEW as isize,
    SQLITE_DROP_TRIGGER = ffi::SQLITE_DROP_TRIGGER as isize,
    SQLITE_DROP_VIEW = ffi::SQLITE_DROP_VIEW as isize,
    SQLITE_INSERT = ffi::SQLITE_INSERT as isize,
    SQLITE_PRAGMA = ffi::SQLITE_PRAGMA as isize,
    SQLITE_READ = ffi::SQLITE_READ as isize,
    SQLITE_SELECT = ffi::SQLITE_SELECT as isize,
    SQLITE_TRANSACTION = ffi::SQLITE_TRANSACTION as isize,
    SQLITE_UPDATE = ffi::SQLITE_UPDATE as isize,
    SQLITE_ATTACH = ffi::SQLITE_ATTACH as isize,
    SQLITE_DETACH = ffi::SQLITE_DETACH as isize,
    SQLITE_ALTER_TABLE = ffi::SQLITE_ALTER_TABLE as isize,
    SQLITE_REINDEX = ffi::SQLITE_REINDEX as isize,
    SQLITE_ANALYZE = ffi::SQLITE_ANALYZE as isize,
    SQLITE_CREATE_VTABLE = ffi::SQLITE_CREATE_VTABLE as isize,
    SQLITE_DROP_VTABLE = ffi::SQLITE_DROP_VTABLE as isize,
    SQLITE_FUNCTION = ffi::SQLITE_FUNCTION as isize,
    SQLITE_SAVEPOINT = ffi::SQLITE_SAVEPOINT as isize,
    SQLITE_COPY = ffi::SQLITE_COPY as isize,
    SQLITE_RECURSIVE = 33,
}

impl From<i32> for Action {
    fn from(code: i32) -> Action {
        match code {
            ffi::SQLITE_CREATE_INDEX => Action::SQLITE_CREATE_INDEX,
            ffi::SQLITE_CREATE_TABLE => Action::SQLITE_CREATE_TABLE,
            ffi::SQLITE_CREATE_TEMP_INDEX => Action::SQLITE_CREATE_TEMP_INDEX,
            ffi::SQLITE_CREATE_TEMP_TABLE => Action::SQLITE_CREATE_TEMP_TABLE,
            ffi::SQLITE_CREATE_TEMP_TRIGGER => Action::SQLITE_CREATE_TEMP_TRIGGER,
            ffi::SQLITE_CREATE_TEMP_VIEW => Action::SQLITE_CREATE_TEMP_VIEW,
            ffi::SQLITE_CREATE_TRIGGER => Action::SQLITE_CREATE_TRIGGER,
            ffi::SQLITE_CREATE_VIEW => Action::SQLITE_CREATE_VIEW,
            ffi::SQLITE_DELETE => Action::SQLITE_DELETE,
            ffi::SQLITE_DROP_INDEX => Action::SQLITE_DROP_INDEX,
            ffi::SQLITE_DROP_TABLE => Action::SQLITE_DROP_TABLE,
            ffi::SQLITE_DROP_TEMP_INDEX => Action::SQLITE_DROP_TEMP_INDEX,
            ffi::SQLITE_DROP_TEMP_TABLE => Action::SQLITE_DROP_TEMP_TABLE,
            ffi::SQLITE_DROP_TEMP_TRIGGER => Action::SQLITE_DROP_TEMP_TRIGGER,
            ffi::SQLITE_DROP_TEMP_VIEW => Action::SQLITE_DROP_TEMP_VIEW,
            ffi::SQLITE_DROP_TRIGGER => Action::SQLITE_DROP_TRIGGER,
            ffi::SQLITE_DROP_VIEW => Action::SQLITE_DROP_VIEW,
            ffi::SQLITE_INSERT => Action::SQLITE_INSERT,
            ffi::SQLITE_PRAGMA => Action::SQLITE_PRAGMA,
            ffi::SQLITE_READ => Action::SQLITE_READ,
            ffi::SQLITE_SELECT => Action::SQLITE_SELECT,
            ffi::SQLITE_TRANSACTION => Action::SQLITE_TRANSACTION,
            ffi::SQLITE_UPDATE => Action::SQLITE_UPDATE,
            ffi::SQLITE_ATTACH => Action::SQLITE_ATTACH,
            ffi::SQLITE_DETACH => Action::SQLITE_DETACH,
            ffi::SQLITE_ALTER_TABLE => Action::SQLITE_ALTER_TABLE,
            ffi::SQLITE_REINDEX => Action::SQLITE_REINDEX,
            ffi::SQLITE_ANALYZE => Action::SQLITE_ANALYZE,
            ffi::SQLITE_CREATE_VTABLE => Action::SQLITE_CREATE_VTABLE,
            ffi::SQLITE_DROP_VTABLE => Action::SQLITE_DROP_VTABLE,
            ffi::SQLITE_FUNCTION => Action::SQLITE_FUNCTION,
            ffi::SQLITE_SAVEPOINT => Action::SQLITE_SAVEPOINT,
            ffi::SQLITE_COPY => Action::SQLITE_COPY,
            33 => Action::SQLITE_RECURSIVE,
            _ => Action::UNKNOWN,
        }
    }
}

impl Connection {
    /// Register a callback function to be invoked whenever a transaction is
    /// committed.
    ///
    /// The callback returns `true` to rollback.
    pub fn commit_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut() -> bool + Send + 'static,
    {
        self.db.borrow_mut().commit_hook(hook);
    }

    /// Register a callback function to be invoked whenever a transaction is
    /// committed.
    ///
    /// The callback returns `true` to rollback.
    pub fn rollback_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut() + Send + 'static,
    {
        self.db.borrow_mut().rollback_hook(hook);
    }

    /// Register a callback function to be invoked whenever a row is updated,
    /// inserted or deleted in a rowid table.
    ///
    /// The callback parameters are:
    ///
    /// - the type of database update (SQLITE_INSERT, SQLITE_UPDATE or
    /// SQLITE_DELETE),
    /// - the name of the database ("main", "temp", ...),
    /// - the name of the table that is updated,
    /// - the ROWID of the row that is updated.
    pub fn update_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, i64) + Send + 'static,
    {
        self.db.borrow_mut().update_hook(hook);
    }
}

impl InnerConnection {
    pub fn remove_hooks(&mut self) {
        self.update_hook(None::<fn(Action, &str, &str, i64)>);
        self.commit_hook(None::<fn() -> bool>);
        self.rollback_hook(None::<fn()>);
    }

    fn commit_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut() -> bool + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void) -> c_int
        where
            F: FnMut() -> bool,
        {
            let boxed_hook: *mut F = p_arg as *mut F;
            if (*boxed_hook)() {
                1
            } else {
                0
            }
        }

        // unlike `sqlite3_create_function_v2`, we cannot specify a `xDestroy` with
        // `sqlite3_commit_hook`. so we keep the `xDestroy` function in
        // `InnerConnection.free_boxed_hook`.
        let free_commit_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_commit_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook as *mut _,
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_commit_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_commit_hook {
                free_boxed_hook(previous_hook);
            }
        }
        self.free_commit_hook = free_commit_hook;
    }

    fn rollback_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut() + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void)
        where
            F: FnMut(),
        {
            let boxed_hook: *mut F = p_arg as *mut F;
            (*boxed_hook)();
        }

        let free_rollback_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_rollback_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook as *mut _,
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_rollback_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_rollback_hook {
                free_boxed_hook(previous_hook);
            }
        }
        self.free_rollback_hook = free_rollback_hook;
    }

    fn update_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, i64) + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(
            p_arg: *mut c_void,
            action_code: c_int,
            db_str: *const c_char,
            tbl_str: *const c_char,
            row_id: i64,
        ) where
            F: FnMut(Action, &str, &str, i64),
        {
            use std::ffi::CStr;
            use std::str;

            let boxed_hook: *mut F = p_arg as *mut F;

            let action = Action::from(action_code);
            let db_name = {
                let c_slice = CStr::from_ptr(db_str).to_bytes();
                str::from_utf8_unchecked(c_slice)
            };
            let tbl_name = {
                let c_slice = CStr::from_ptr(tbl_str).to_bytes();
                str::from_utf8_unchecked(c_slice)
            };

            (*boxed_hook)(action, db_name, tbl_name, row_id);
        }

        let free_update_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_update_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook as *mut _,
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_update_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_update_hook {
                free_boxed_hook(previous_hook);
            }
        }
        self.free_update_hook = free_update_hook;
    }
}

fn free_boxed_hook<F>(p: *mut c_void) {
    drop(unsafe { Box::from_raw(p as *mut F) });
}

#[cfg(test)]
mod test {
    use super::Action;
    use std::sync::atomic::{AtomicBool, Ordering};
    use Connection;

    #[test]
    fn test_commit_hook() {
        let db = Connection::open_in_memory().unwrap();

        lazy_static! {
            static ref called: AtomicBool = AtomicBool::new(false);
        }
        db.commit_hook(Some(|| {
            called.store(true, Ordering::Relaxed);
            false
        }));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")
            .unwrap();
        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn test_fn_commit_hook() {
        let db = Connection::open_in_memory().unwrap();

        fn hook() -> bool {
            true
        }

        db.commit_hook(Some(hook));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")
            .unwrap_err();
    }

    #[test]
    fn test_rollback_hook() {
        let db = Connection::open_in_memory().unwrap();

        lazy_static! {
            static ref called: AtomicBool = AtomicBool::new(false);
        }
        db.rollback_hook(Some(|| {
            called.store(true, Ordering::Relaxed);
        }));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); ROLLBACK;")
            .unwrap();
        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn test_update_hook() {
        let db = Connection::open_in_memory().unwrap();

        lazy_static! {
            static ref called: AtomicBool = AtomicBool::new(false);
        }
        db.update_hook(Some(|action, db: &str, tbl: &str, row_id| {
            assert_eq!(Action::SQLITE_INSERT, action);
            assert_eq!("main", db);
            assert_eq!("foo", tbl);
            assert_eq!(1, row_id);
            called.store(true, Ordering::Relaxed);
        }));
        db.execute_batch("CREATE TABLE foo (t TEXT)").unwrap();
        db.execute_batch("INSERT INTO foo VALUES ('lisa')").unwrap();
        assert!(called.load(Ordering::Relaxed));
    }
}
