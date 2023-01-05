#![allow(non_snake_case)]

use std::sync::{Arc, Mutex};

pub(crate) fn open_with_virtual_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    vwal_methods: Arc<Mutex<mwal::ffi::libsql_wal_methods>>,
) -> anyhow::Result<super::WalConnection> {
    use std::os::unix::ffi::OsStrExt;
    let mut vwal_methods = vwal_methods.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
    unsafe {
        let mut pdb: *mut rusqlite::ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut rusqlite::ffi::sqlite3 = &mut pdb;
        let register_err = super::ffi::libsql_wal_methods_register(
            &mut *vwal_methods as *const mwal::ffi::libsql_wal_methods as _,
        );
        assert_eq!(register_err, 0);
        let open_err = super::libsql_open(
            path.as_ref().as_os_str().as_bytes().as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            vwal_methods.name,
        );
        assert_eq!(open_err, 0);
        let conn = super::Connection::from_handle(pdb)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        tracing::trace!(
            "Opening a connection with virtual WAL at {}",
            path.as_ref().display()
        );
        Ok(super::WalConnection { inner: conn })
    }
}
