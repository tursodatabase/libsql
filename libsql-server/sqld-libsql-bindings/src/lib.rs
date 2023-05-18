#![allow(improper_ctypes)]

pub mod ffi;
#[cfg(feature = "mwal_backend")]
pub mod mwal;
pub mod wal_hook;

use std::ops::Deref;

use anyhow::ensure;
use wal_hook::OwnedWalMethods;

use crate::{ffi::libsql_wal_methods_register, wal_hook::WalMethodsHook};

use self::{
    ffi::{libsql_wal_methods, libsql_wal_methods_find},
    wal_hook::WalHook,
};

pub fn get_orig_wal_methods(with_bottomless: bool) -> anyhow::Result<*mut libsql_wal_methods> {
    let orig: *mut libsql_wal_methods = if with_bottomless {
        unsafe { libsql_wal_methods_find("bottomless\0".as_ptr() as *const _) }
    } else {
        unsafe { libsql_wal_methods_find(std::ptr::null()) }
    };
    if orig.is_null() {
        anyhow::bail!("no underlying methods");
    }

    Ok(orig)
}

pub struct Connection {
    // conn must be dropped first, do not reorder.
    conn: rusqlite::Connection,
    #[allow(dead_code)]
    wal_methods: Option<OwnedWalMethods>,
}

impl Deref for Connection {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

// Registering WAL methods may be subject to race with the later call to libsql_wal_methods_find,
// if we overwrite methods with the same name. A short-term solution is to force register+find
// to be atomic.
// FIXME: a proper solution (Marin is working on it) is to be able to pass user data as a pointer
// directly to libsql_open()
static DB_OPENING_MUTEX: once_cell::sync::Lazy<parking_lot::Mutex<()>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(()));

/// Opens a database with the regular wal methods in the directory pointed to by path
pub fn open_with_regular_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    wal_hook: impl WalHook + 'static,
    with_bottomless: bool,
) -> anyhow::Result<Connection> {
    let opening_lock = DB_OPENING_MUTEX.lock();
    let path = path.as_ref().join("data");
    let wal_methods = unsafe {
        let default_methods = get_orig_wal_methods(false)?;
        let maybe_bottomless_methods = get_orig_wal_methods(with_bottomless)?;
        let mut wrapped = WalMethodsHook::wrap(default_methods, maybe_bottomless_methods, wal_hook);
        let res = libsql_wal_methods_register(wrapped.as_ptr());
        ensure!(res == 0, "failed to register WAL methods");
        wrapped
    };
    tracing::trace!(
        "Opening a connection with regular WAL at {}",
        path.display()
    );
    #[cfg(not(feature = "unix-excl-vfs"))]
    let conn = rusqlite::Connection::open_with_flags_and_wal(
        path,
        flags,
        WalMethodsHook::METHODS_NAME_STR,
    )?;
    #[cfg(feature = "unix-excl-vfs")]
    let conn = rusqlite::Connection::open_with_flags_vfs_and_wal(
        path,
        flags,
        "unix-excl",
        WalMethodsHook::METHODS_NAME_STR,
    )?;
    drop(opening_lock);
    conn.pragma_update(None, "journal_mode", "wal")?;

    Ok(Connection {
        conn,
        wal_methods: Some(wal_methods),
    })
}
