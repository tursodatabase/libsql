use {SqliteResult, Connection};

/// RAII guard temporarily enabling SQLite extensions to be loaded.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, SqliteResult, SqliteLoadExtensionGuard};
/// # use std::path::{Path};
/// fn load_my_extension(conn: &Connection) -> SqliteResult<()> {
///     let _guard = try!(SqliteLoadExtensionGuard::new(conn));
///
///     conn.load_extension(Path::new("my_sqlite_extension"), None)
/// }
/// ```
pub struct SqliteLoadExtensionGuard<'conn> {
    conn: &'conn Connection,
}

impl<'conn> SqliteLoadExtensionGuard<'conn> {
    /// Attempt to enable loading extensions. Loading extensions will be disabled when this
    /// guard goes out of scope. Cannot be meaningfully nested.
    pub fn new(conn: &Connection) -> SqliteResult<SqliteLoadExtensionGuard> {
        conn.load_extension_enable().map(|_| SqliteLoadExtensionGuard { conn: conn })
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for SqliteLoadExtensionGuard<'conn> {
    fn drop(&mut self) {
        self.conn.load_extension_disable();
    }
}
