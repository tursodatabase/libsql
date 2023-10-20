use std::path::Path;

use super::hook::{
    InjectorHookCtx, INJECTOR_METHODS, LIBSQL_CONTINUE_REPLICATION, LIBSQL_EXIT_REPLICATION,
};

pub struct FrameInjector<'a> {
    conn: libsql_sys::Connection<'a>,
}

impl<'a> FrameInjector<'a> {
    pub fn new(db_path: &Path, hook_ctx: &'a mut InjectorHookCtx) -> anyhow::Result<Self> {
        let conn = libsql_sys::Connection::open(
            db_path,
            (libsql_sys::ffi::SQLITE_OPEN_READWRITE
                | libsql_sys::ffi::SQLITE_OPEN_CREATE
                | libsql_sys::ffi::SQLITE_OPEN_URI
                | libsql_sys::ffi::SQLITE_OPEN_NOMUTEX) as std::ffi::c_int,
            &INJECTOR_METHODS,
            hook_ctx,
        )
        .map_err(|e| anyhow::anyhow!("Open failed: {e}"))?;

        Ok(Self { conn })
    }

    pub fn step(&self) -> anyhow::Result<bool> {
        unsafe {
            libsql_sys::ffi::sqlite3_exec(
                self.conn.conn,
                "pragma writable_schema=on\0".as_ptr() as *const _,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
        }

        let rc = unsafe {
            libsql_sys::ffi::sqlite3_exec(
                self.conn.conn,
                "create table __dummy__ (dummy);\0".as_ptr() as *const _,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        match rc {
            libsql_sys::ffi::SQLITE_OK => panic!("replication hook was not called"),
            LIBSQL_EXIT_REPLICATION => {
                unsafe {
                    libsql_sys::ffi::sqlite3_exec(
                        self.conn.conn,
                        "pragma writable_schema=reset\0".as_ptr() as *const _,
                        None,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                    );
                }
                Ok(false)
            }
            LIBSQL_CONTINUE_REPLICATION => Ok(true),
            _ => panic!("unexpected error code: {}", rc),
        }
    }
}
