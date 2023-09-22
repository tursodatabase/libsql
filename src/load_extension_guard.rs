use crate::{Connection, Result};

/// RAII guard temporarily enabling SQLite extensions to be loaded.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result, LoadExtensionGuard};
/// # use std::path::{Path};
/// fn load_my_extension(conn: &Connection) -> Result<()> {
///     unsafe {
///         let _guard = LoadExtensionGuard::new(conn)?;
///         conn.load_extension("trusted/sqlite/extension", None)
///     }
/// }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "load_extension")))]
pub struct LoadExtensionGuard<'conn> {
    conn: &'conn Connection,
}

impl LoadExtensionGuard<'_> {
    /// Attempt to enable loading extensions. Loading extensions will be
    /// disabled when this guard goes out of scope. Cannot be meaningfully
    /// nested.
    ///
    /// # Safety
    ///
    /// You must not run untrusted queries while extension loading is enabled.
    ///
    /// See the safety comment on [`Connection::load_extension_enable`] for more
    /// details.
    #[inline]
    pub unsafe fn new(conn: &Connection) -> Result<LoadExtensionGuard<'_>> {
        conn.load_extension_enable()
            .map(|_| LoadExtensionGuard { conn })
    }
}

#[allow(unused_must_use)]
impl Drop for LoadExtensionGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.conn.load_extension_disable();
    }
}
