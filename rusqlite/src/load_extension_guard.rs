use {Connection, Result};

/// Old name for `LoadExtensionGuard`. `SqliteLoadExtensionGuard` is deprecated.
#[deprecated(since = "0.6.0", note = "Use LoadExtensionGuard instead")]
pub type SqliteLoadExtensionGuard<'conn> = LoadExtensionGuard<'conn>;

/// RAII guard temporarily enabling SQLite extensions to be loaded.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, LoadExtensionGuard};
/// # use std::path::{Path};
/// fn load_my_extension(conn: &Connection) -> Result<()> {
///     let _guard = try!(LoadExtensionGuard::new(conn));
///
///     conn.load_extension(Path::new("my_sqlite_extension"), None)
/// }
/// ```
pub struct LoadExtensionGuard<'conn> {
    conn: &'conn Connection,
}

impl<'conn> LoadExtensionGuard<'conn> {
    /// Attempt to enable loading extensions. Loading extensions will be
    /// disabled when this guard goes out of scope. Cannot be meaningfully
    /// nested.
    pub fn new(conn: &Connection) -> Result<LoadExtensionGuard> {
        conn.load_extension_enable()
            .map(|_| LoadExtensionGuard { conn })
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for LoadExtensionGuard<'conn> {
    fn drop(&mut self) {
        self.conn.load_extension_disable();
    }
}
