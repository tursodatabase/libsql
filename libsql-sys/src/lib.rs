#[allow(clippy::all)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub mod ffi {
    #![allow(non_snake_case, non_camel_case_types)]
    #![cfg_attr(test, allow(deref_nullptr))] // https://github.com/rust-lang/rust-bindgen/issues/2066
    pub use super::error::libsql::*;

    use std::default::Default;
    use std::mem;

    #[must_use]
    pub fn SQLITE_STATIC() -> sqlite3_destructor_type {
        None
    }

    #[must_use]
    pub fn SQLITE_TRANSIENT() -> sqlite3_destructor_type {
        Some(unsafe { mem::transmute(-1_isize) })
    }

    #[allow(clippy::all)]
    mod bindings {
        include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
    }
    pub use bindings::*;

    impl Default for sqlite3_vtab {
        fn default() -> Self {
            unsafe { mem::zeroed() }
        }
    }

    impl Default for sqlite3_vtab_cursor {
        fn default() -> Self {
            unsafe { mem::zeroed() }
        }
    }
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
