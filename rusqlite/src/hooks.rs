//! Data Change Notification Callbacks
#![allow(non_camel_case_types)]

use std::mem;
use std::ptr;
use std::os::raw::{c_int, c_char, c_void};

use ffi;

use {Connection, InnerConnection};

// Commit And Rollback Notification Callbacks
// http://sqlite.org/c3ref/commit_hook.html
/*
void *sqlite3_commit_hook(sqlite3*, int(*)(void*), void*);
void *sqlite3_rollback_hook(sqlite3*, void(*)(void *), void*);
*/

/// Authorizer Action Codes
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
            ffi::SQLITE_RECURSIVE => Action::SQLITE_RECURSIVE,
            _ => Action::UNKNOWN,
        }
    }
}

impl Connection {
    pub fn update_hook<F>(&self, hook: Option<F>)
        where F: FnMut(Action, &str, &str, i64)
    {
        self.db.borrow_mut().update_hook(hook);
    }
}

impl InnerConnection {
    // TODO self.update_hook(None) must be called in InnerConnection#close to free any hook
    fn update_hook<F>(&mut self, hook: Option<F>)
        where F: FnMut(Action, &str, &str, i64)
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void,
                                                   action_code: c_int,
                                                   db_str: *const c_char,
                                                   tbl_str: *const c_char,
                                                   row_id: i64)
            where F: FnMut(Action, &str, &str, i64)
        {
            use std::ffi::CStr;
            use std::str;

            let boxed_hook: *mut F = mem::transmute(p_arg);
            assert!(!boxed_hook.is_null(),
                    "Internal error - null function pointer");

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

        let previous_hook = if let Some(hook) = hook {
            let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
            unsafe {
                ffi::sqlite3_update_hook(self.db(),
                                         Some(call_boxed_closure::<F>),
                                         mem::transmute(boxed_hook))
            }
        } else {
            unsafe { ffi::sqlite3_update_hook(self.db(), None, ptr::null_mut()) }
        };
        // TODO Validate: what happens if the previous hook has been set from C ?
        if !previous_hook.is_null() {
            // free_boxed_value
            unsafe {
                let _: Box<F> = Box::from_raw(mem::transmute(previous_hook));
            }
        }
    }
}
