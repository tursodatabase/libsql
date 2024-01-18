//! # libSQL API for Rust
//!
//! libSQL is an embeddable SQL database engine based on SQLite.
//! This Rust API is a batteries-included wrapper around the SQLite C API to support transparent replication while retaining compatibility with the SQLite ecosystem, such as the SQL dialect and extensions. If you are building an application in Rust, this is the crate you should use.
//! There are also libSQL language bindings of this Rust crate to other languages such as [JavaScript](), Python, Go, and C.
//!
//! ## Getting Started
//!
//! To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:
//!
//! ```rust,no_run
//! # async fn run() {
//! use libsql::Database;
//!
//! let db = Database::open_in_memory().unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()).await.unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).await.unwrap();
//! # }
//! ```
//!
//! ## Embedded Replicas
//!
//! Embedded replica is libSQL database that's running in your application process, which keeps a local copy of a remote database.
//! They are useful if you want to move data in the memory space of your application for fast access.
//!
//! You can open an embedded read-only replica by using the [`Database::open_with_local_sync`] constructor:
//!
//! ```rust,no_run
//! # async fn run() {
//! use libsql::{Database};
//! use libsql::replication::Frames;
//!
//! let mut db = Database::open_with_local_sync("/tmp/test.db").await.unwrap();
//!
//! let frames = Frames::Vec(vec![]);
//! db.sync_frames(frames).await.unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("SELECT * FROM users", ()).await.unwrap();
//! # }
//! ```
//!
//! ## WASM
//!
//! Due to WASM requiring `!Send` support and the [`Database`] type supporting async and using
//! `async_trait` to abstract between the different database types, we are unable to support WASM
//! via the [`Database`] type. Instead, we have provided simpler parallel types in the `wasm`
//! module that provide access to our remote HTTP protocol in WASM.
//!
//! ## Examples
//!
//! You can find more examples in the [`examples`](https://github.com/tursodatabase/libsql/tree/main/crates/core/examples) directory.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[macro_use]
mod macros;

cfg_core! {
    mod local;

    pub use local::{version, version_number, RowsFuture};
    pub use database::OpenFlags;
}

pub mod params;

cfg_replication! {
    pub mod replication;
}

cfg_core! {
    pub use libsql_sys::ffi;
}

cfg_wasm! {
    pub mod wasm;
}

mod util;

pub mod errors;
pub use errors::Error;

pub use params::params_from_iter;

mod connection;
mod database;

cfg_parser! {
    mod parser;
}

mod rows;
mod statement;
mod transaction;
mod value;

#[cfg(feature = "serde")]
pub mod de;

pub use value::{Value, ValueRef, ValueType};

cfg_hrana! {
    mod hrana;
}

pub use self::{
    connection::Connection,
    database::Database,
    rows::{Column, Row, Rows},
    statement::Statement,
    transaction::{Transaction, TransactionBehavior},
};

/// Convenient alias for `Result` using the `libsql::Error` type.
pub type Result<T> = std::result::Result<T, errors::Error>;
pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
