pub mod connection;
pub mod database;
pub mod errors;
pub mod params;
pub mod rows;
pub mod statement;
pub mod transaction;

pub type Result<T> = std::result::Result<T, errors::Error>;

pub use libsql_sys::ffi;
pub use libsql_sys::ValueType;

pub use connection::Connection;
pub use database::Database;
#[cfg(feature = "replication")]
pub use database::Opts;
pub use errors::Error;
pub use params::Params;
pub use params::{params_from_iter, Value, ValueRef};
pub use rows::Row;
pub use rows::Rows;
pub use rows::RowsFuture;
pub use statement::{Column, Statement};
pub use transaction::{Transaction, TransactionBehavior};

/// Return the version of the underlying SQLite library as a number.
pub fn version_number() -> i32 {
    unsafe { ffi::sqlite3_libversion_number() }
}

/// Return the version of the underlying SQLite library as a string.
pub fn version() -> &'static str {
    unsafe {
        std::ffi::CStr::from_ptr(ffi::sqlite3_libversion())
            .to_str()
            .unwrap()
    }
}
