#![allow(improper_ctypes)]

pub mod ffi;
#[cfg(feature = "mwal_backend")]
pub mod mwal;
pub mod wal_hook;

pub use wblibsql::{
    libsql_compile_wasm_module, libsql_free_wasm_module, libsql_run_wasm, libsql_wasm_engine_new,
};

use anyhow::ensure;
use rusqlite::Connection;

use crate::libsql::{ffi::libsql_wal_methods_register, wal_hook::WalMethodsHook};

use self::{
    ffi::{libsql_wal_methods, libsql_wal_methods_find},
    wal_hook::WalHook,
};

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
) -> anyhow::Result<Connection> {
    unsafe {
        let orig = get_orig_wal_methods()?;
        let wrapped = WalMethodsHook::wrap(orig, wal_hook);
        let res = libsql_wal_methods_register(wrapped);
        ensure!(res == 0, "failed to register WAL methods");
    }
    tracing::trace!(
        "Opening a connection with regular WAL at {}",
        path.as_ref().display()
    );
    let conn = Connection::open_with_flags_and_wal(path, flags, WalMethodsHook::METHODS_NAME_STR)?;
    conn.pragma_update(None, "journal_mode", "wal")?;
    Ok(conn)
}
