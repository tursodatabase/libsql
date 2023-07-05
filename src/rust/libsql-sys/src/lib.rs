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
pub mod types;
pub mod wal_hook;

pub use connection::Connection;
pub use error::{Error, Result};
pub use types::*;
pub use wal_hook::{WalHook, WalMethodsHook};
