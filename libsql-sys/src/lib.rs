#[allow(clippy::all)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub mod ffi {
    include!(concat!(
        default_env::default_env!("LIBSQL_SRC_DIR", ".."),
        "/bindings.rs"
    ));
}

pub mod connection;
pub mod error;
pub mod statement;
pub mod types;
pub mod value;
pub mod wal_hook;

pub use connection::Connection;
pub use error::{Error, Result};
pub use statement::{prepare_stmt, Statement};
pub use types::*;
pub use value::{Value, ValueType};
pub use wal_hook::{WalHook, WalMethodsHook};
