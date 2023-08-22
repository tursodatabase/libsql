#![allow(improper_ctypes)]

pub mod ffi;
pub mod wal_hook;

use std::{ffi::CString, marker::PhantomData, ops::Deref, time::Duration};

pub use crate::wal_hook::WalMethodsHook;
pub use once_cell::sync::Lazy;

use self::{
    ffi::{libsql_wal_methods, libsql_wal_methods_find},
    wal_hook::WalHook,
};

pub fn get_orig_wal_methods() -> anyhow::Result<*mut libsql_wal_methods> {
    let orig: *mut libsql_wal_methods = unsafe { libsql_wal_methods_find(std::ptr::null()) };
    if orig.is_null() {
        anyhow::bail!("no underlying methods");
    }

    Ok(orig)
}

pub struct Connection<'a> {
    conn: rusqlite::Connection,
    _pth: PhantomData<&'a mut ()>,
}

impl Deref for Connection<'_> {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        unsafe {
            let db = self.conn.handle();
            if db.is_null() {
                return;
            }
            let mut stmt = ffi::sqlite3_next_stmt(db, std::ptr::null_mut());
            while !stmt.is_null() {
                let rc = ffi::sqlite3_finalize(stmt);
                if rc != ffi::SQLITE_OK {
                    tracing::error!("Failed to finalize a dangling statement: {rc}")
                }
                stmt = ffi::sqlite3_next_stmt(db, stmt);
            }
        }
    }
}

impl<'a> Connection<'a> {
    /// returns a dummy, in-memory connection. For testing purposes only
    pub fn test(_: &mut ()) -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        Self {
            conn,
            _pth: PhantomData,
        }
    }

    /// Opens a database with the regular wal methods in the directory pointed to by path
    pub fn open<W: WalHook>(
        path: impl AsRef<std::path::Path>,
        flags: rusqlite::OpenFlags,
        // we technically _only_ need to know about W, but requiring a static ref to the wal_hook ensures that
        // it has been instanciated and lives for long enough
        _wal_hook: &'static WalMethodsHook<W>,
        hook_ctx: &'a mut W::Context,
        auto_checkpoint: u32,
    ) -> Result<Self, rusqlite::Error> {
        let path = path.as_ref().join("data");
        tracing::trace!(
            "Opening a connection with regular WAL at {}",
            path.display()
        );

        let conn_str = format!("file:{}?_journal_mode=WAL", path.display());
        let filename = CString::new(conn_str).unwrap();
        let mut db: *mut rusqlite::ffi::sqlite3 = std::ptr::null_mut();

        unsafe {
            // We pass a pointer to the WAL methods data to the database connection. This means
            // that the reference must outlive the connection. This is guaranteed by the marker in
            // the returned connection.
            let mut rc = rusqlite::ffi::libsql_open_v2(
                filename.as_ptr(),
                &mut db as *mut _,
                flags.bits(),
                std::ptr::null_mut(),
                W::name().as_ptr(),
                hook_ctx as *mut _ as *mut _,
            );

            if rc == 0 {
                rc = rusqlite::ffi::sqlite3_wal_autocheckpoint(db, auto_checkpoint as _);
            }

            if rc != 0 {
                rusqlite::ffi::sqlite3_close(db);
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rc),
                    None,
                ));
            }

            assert!(!db.is_null());
        };

        let conn = unsafe { rusqlite::Connection::from_handle_owned(db)? };
        conn.busy_timeout(Duration::from_millis(5000))?;

        Ok(Connection {
            conn,
            _pth: PhantomData,
        })
    }
}
