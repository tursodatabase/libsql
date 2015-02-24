use {SqliteResult, SqliteConnection};

/// RAII guard temporarily enabling SQLite extensions to be loaded.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{SqliteConnection, SqliteResult, SqliteLoadExtensionGuard};
/// # use std::path::{Path};
/// fn load_my_extension(conn: &SqliteConnection) -> SqliteResult<()> {
///     let _guard = try!(SqliteLoadExtensionGuard::new(conn));
///
///     conn.load_extension(Path::new("my_sqlite_extension"), None)
/// }
/// ```
pub struct SqliteLoadExtensionGuard<'conn> {
    conn: &'conn SqliteConnection,
}

impl<'conn> SqliteLoadExtensionGuard<'conn> {
    /// Attempt to enable loading extensions. Loading extensions will be disabled when this
    /// guard goes out of scope. Cannot be meaningfully nested.
    pub fn new(conn: &SqliteConnection) -> SqliteResult<SqliteLoadExtensionGuard> {
        conn.load_extension_enable().map(|_| SqliteLoadExtensionGuard{ conn: conn })
    }
}

#[unsafe_destructor]
#[allow(unused_must_use)]
impl<'conn> Drop for SqliteLoadExtensionGuard<'conn> {
    fn drop(&mut self) {
        self.conn.load_extension_disable();
    }
}
