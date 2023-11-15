pub use libsql_ffi as ffi;

#[cfg(feature = "api")]
pub mod connection;
pub mod error;
#[cfg(feature = "api")]
pub mod statement;
#[cfg(feature = "api")]
pub mod types;
#[cfg(feature = "api")]
pub mod value;
#[cfg(feature = "wal")]
pub mod wal;

#[cfg(feature = "api")]
pub use connection::Connection;
#[cfg(feature = "api")]
pub use error::{Error, Result};
#[cfg(feature = "api")]
pub use statement::{prepare_stmt, Statement};
#[cfg(feature = "api")]
pub use types::*;
#[cfg(feature = "api")]
pub use value::{Value, ValueType};
