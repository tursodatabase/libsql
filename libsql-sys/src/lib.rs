#![allow(clippy::too_many_arguments)]
pub mod ffi {
    //! C ffi for libsql.

    pub use libsql_ffi::*;
    use zerocopy::byteorder::big_endian::{U16 as bu16, U32 as bu32, U64 as bu64};

    /// Patched database header file, in use by libsql
    #[allow(dead_code)] // <- false positive
    #[repr(C)]
    #[derive(Clone, Copy, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes, Debug)]
    pub struct Sqlite3DbHeader {
        /// The header string: "SQLite format 3\000"
        pub header_str: [u8; 16],
        /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
        pub page_size: bu16,
        /// File format write version. 1 for legacy; 2 for WAL.
        pub write_version: u8,
        /// File format write version. 1 for legacy; 2 for WAL.
        pub read_version: u8,
        /// Bytes of unused "reserved" space at the end of each page. Usually 0.
        pub reserved_in_page: u8,
        /// Maximum embedded payload fraction. Must be 64.
        pub max_payload: u8,
        /// Minimum embedded payload fraction. Must be 32.
        pub min_payload: u8,
        /// Leaf payload fraction. Must be 32.
        pub leaf_payload: u8,
        /// File change counter.
        pub change_count: bu32,
        /// Size of the database file in pages. The "in-header database size".
        pub db_size: bu32,
        /// Page number of the first freelist trunk page.
        pub freelist_pno: bu32,
        /// Total number of freelist pages.
        pub freelist_len: bu32,
        /// The schema cookie.
        pub schema_cookie: bu32,
        /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
        pub schema_format_number: bu32,
        /// Default page cache size.
        pub default_cache_size: bu32,
        /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
        pub largest_root: bu32,
        /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
        pub text_encoding: bu32,
        /// The "user version" as read and set by the user_version pragma.
        pub user_version: bu32,
        /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
        pub incremental_vacuum: bu32,
        /// The "Application ID" set by PRAGMA application_id.
        pub app_id: bu32,
        /// Reserved for expansion. Must be zero.
        _reserved: [u8; 12],
        /// The replication index of this database, this is a libsql extension, ignored by sqlite3.
        pub replication_index: bu64,
        /// The version-valid-for number.
        pub version_valid_for: bu32,
        /// SQLITE_VERSION_NUMBER
        pub sqlite_version: bu32,
    }
}

#[cfg(feature = "api")]
pub mod connection;
pub mod error;
pub mod name;
#[cfg(feature = "api")]
pub mod statement;
#[cfg(feature = "api")]
pub mod types;
#[cfg(feature = "api")]
pub mod value;
#[cfg(feature = "wal")]
pub mod wal;

#[cfg(feature = "api")]
pub use connection::Cipher;
#[cfg(feature = "api")]
pub use connection::Connection;
#[cfg(feature = "api")]
pub use connection::EncryptionConfig;
#[cfg(feature = "api")]
pub use error::{Error, Result};
#[cfg(feature = "api")]
pub use statement::{prepare_stmt, Statement};
#[cfg(feature = "api")]
pub use types::*;
#[cfg(feature = "api")]
pub use value::{Value, ValueType};

#[cfg(feature = "rusqlite")]
pub use rusqlite;
