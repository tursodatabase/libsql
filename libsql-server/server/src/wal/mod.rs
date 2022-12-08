#![allow(improper_ctypes)]

#[cfg(feature = "fdb")]
pub mod fdb;

use rusqlite::ffi;
use rusqlite::Connection;
use std::os::unix::ffi::OsStrExt;

#[cfg(not(feature = "fdb"))]
pub struct WalMethods;
#[cfg(feature = "fdb")]
pub use fdb::WalMethods;

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
            ffi::sqlite3_close(self.inner.handle());
        }
        let _ = self.inner;
    }
}

extern "C" {
    fn libsql_open(
        filename: *const u8,
        ppdb: *mut *mut ffi::sqlite3,
        flags: std::ffi::c_int,
        vfs: *const u8,
        wal: *const u8,
    ) -> i32;
}

pub(crate) fn open_with_regular_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
) -> anyhow::Result<WalConnection> {
    unsafe {
        let mut pdb: *mut ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut ffi::sqlite3 = &mut pdb;
        let open_err = libsql_open(
            path.as_ref().as_os_str().as_bytes().as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            std::ptr::null(),
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
