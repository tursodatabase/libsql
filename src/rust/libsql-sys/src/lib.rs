#[allow(clippy::all)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub mod ffi {
    include!(concat!(
        default_env::default_env!("LIBSQL_SRC_DIR", ".."),
        "/bindings.rs"
    ));
}

mod connection;
mod error;
mod types;
mod wal_hook;

pub use connection::Connection;
pub use error::{Error, Result};
pub use types::*;
pub use wal_hook::{WalHook, WalMethodsHook};
