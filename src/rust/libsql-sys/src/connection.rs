use crate::{WalHook, WalMethodsHook};
use std::ffi::{c_int, CString};
use std::marker::PhantomData;

pub struct Connection<'a> {
    pub conn: *mut crate::ffi::sqlite3,
    _pth: PhantomData<&'a mut ()>,
}

impl<'a> Connection<'a> {
    /// returns a dummy, in-memory connection. For testing purposes only
    pub fn test(_: &mut ()) -> Self {
        let mut conn: *mut crate::ffi::sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            crate::ffi::sqlite3_open(":memory:\0".as_ptr() as *const _, &mut conn as *mut _)
        };
        assert_eq!(rc, 0);
        assert!(!conn.is_null());
        Self {
            conn,
            _pth: PhantomData,
        }
    }

    /// Opens a database with the regular wal methods in the directory pointed to by path
    pub fn open<W: WalHook>(
        path: impl AsRef<std::path::Path>,
        flags: c_int,
        // we technically _only_ need to know about W, but requiring a static ref to the wal_hook ensures that
        // it has been instantiated and lives for long enough
        _wal_hook: &'static WalMethodsHook<W>,
        hook_ctx: &'a mut W::Context,
    ) -> Result<Self, crate::Error> {
        let path = path.as_ref().join("data");
        tracing::trace!(
            "Opening a connection with regular WAL at {}",
            path.display()
        );

        let conn_str = format!("file:{}?_journal_mode=WAL", path.display());
        let filename = CString::new(conn_str).unwrap();
        let mut conn: *mut crate::ffi::sqlite3 = std::ptr::null_mut();

        unsafe {
            // We pass a pointer to the WAL methods data to the database connection. This means
            // that the reference must outlive the connection. This is guaranteed by the marker in
            // the returned connection.
            let rc = crate::ffi::libsql_open_v2(
                filename.as_ptr(),
                &mut conn as *mut _,
                flags,
                std::ptr::null_mut(),
                W::name().as_ptr(),
                hook_ctx as *mut _ as *mut _,
            );

            if rc != 0 {
                crate::ffi::sqlite3_close(conn);
                return Err(crate::Error::LibError(rc));
            }

            assert!(!conn.is_null());
        };

        unsafe {
            crate::ffi::sqlite3_busy_timeout(conn, 5000);
        }

        Ok(Connection {
            conn,
            _pth: PhantomData,
        })
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        if self.conn.is_null() {
            tracing::debug!("Trying to close a null connection");
            return;
        }
        unsafe {
            crate::ffi::sqlite3_close(self.conn as *mut _);
        }
    }
}
