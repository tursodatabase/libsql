#![allow(improper_ctypes)]

pub mod mwal;

pub mod ffi;
pub mod wal_hook;

use anyhow::ensure;
use rusqlite::Connection;
use std::os::unix::ffi::OsStrExt;

use crate::libsql::{ffi::libsql_wal_methods_register, wal_hook::WalMethodsHook};

use self::{
    ffi::{libsql_wal_methods, libsql_wal_methods_find},
    wal_hook::WalHook,
};

pub struct WalConnection {
    inner: rusqlite::Connection,
}

impl std::ops::Deref for WalConnection {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::Drop for WalConnection {
    fn drop(&mut self) {
        unsafe {
            rusqlite::ffi::sqlite3_close(self.inner.handle());
        }
        let _ = self.inner;
    }
}

extern "C" {
    fn libsql_open(
        filename: *const u8,
        ppdb: *mut *mut rusqlite::ffi::sqlite3,
        flags: std::ffi::c_int,
        vfs: *const u8,
        wal: *const u8,
    ) -> i32;
}

fn get_orig_wal_methods() -> anyhow::Result<*mut libsql_wal_methods> {
    let orig: *mut libsql_wal_methods = unsafe { libsql_wal_methods_find(0) };
    if orig.is_null() {
        anyhow::bail!("no underlying methods");
    }

    Ok(orig)
}

pub(crate) fn open_with_regular_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    wal_hook: impl WalHook + 'static,
) -> anyhow::Result<WalConnection> {
    unsafe {
        let mut pdb: *mut rusqlite::ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut rusqlite::ffi::sqlite3 = &mut pdb;
        let orig = get_orig_wal_methods()?;
        let wrapped = WalMethodsHook::wrap(orig, wal_hook);
        let res = libsql_wal_methods_register(wrapped);
        ensure!(res == 0, "failed to register WAL methods");

        let open_err = libsql_open(
            path.as_ref().as_os_str().as_bytes().as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            WalMethodsHook::METHODS_NAME.as_ptr(),
        );
        assert_eq!(open_err, 0);
        let conn = Connection::from_handle(pdb)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        tracing::trace!(
            "Opening a connection with regular WAL at {}",
            path.as_ref().display()
        );
        Ok(WalConnection { inner: conn })
    }
}
