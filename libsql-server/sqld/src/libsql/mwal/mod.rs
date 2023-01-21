#![allow(non_snake_case)]

use std::sync::{Arc, Mutex};

pub(crate) fn open_with_virtual_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    vwal_methods: Arc<Mutex<mwal::ffi::libsql_wal_methods>>,
) -> anyhow::Result<rusqlite::Connection> {
    let mut vwal_methods = vwal_methods.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
    unsafe {
        let register_err = super::ffi::libsql_wal_methods_register(
            &mut *vwal_methods as *const mwal::ffi::libsql_wal_methods as _,
        );
        assert_eq!(register_err, 0);
    }
    tracing::trace!(
        "Opening a connection with virtual WAL at {}",
        path.as_ref().display()
    );
    let conn = rusqlite::Connection::open_with_flags_and_wal(path, flags, unsafe {
        std::ffi::CStr::from_ptr(vwal_methods.name as *const _)
            .to_str()
            .unwrap()
    })?;
    conn.pragma_update(None, "journal_mode", "wal")?;
    Ok(conn)
}
