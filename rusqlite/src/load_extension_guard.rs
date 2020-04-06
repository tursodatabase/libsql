use crate::{Connection, Result};

/// `feature = "load_extension"` RAII guard temporarily enabling SQLite
/// extensions to be loaded.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, LoadExtensionGuard};
/// # use std::path::{Path};
/// fn load_my_extension(conn: &Connection) -> Result<()> {
///     let _guard = LoadExtensionGuard::new(conn)?;
///
///     conn.load_extension(Path::new("my_sqlite_extension"), None)
/// }
/// ```
pub struct LoadExtensionGuard<'conn> {
    conn: &'conn Connection,
}

impl LoadExtensionGuard<'_> {
    /// Attempt to enable loading extensions. Loading extensions will be
    /// disabled when this guard goes out of scope. Cannot be meaningfully
    /// nested.
    pub fn new(conn: &Connection) -> Result<LoadExtensionGuard<'_>> {
        conn.load_extension_enable()
            .map(|_| LoadExtensionGuard { conn })
    }
}

#[allow(unused_must_use)]
impl Drop for LoadExtensionGuard<'_> {
    fn drop(&mut self) {
        self.conn.load_extension_disable();
    }
}
