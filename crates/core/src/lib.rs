//! # LibSQL API for Rust
//!
//! LibSQL is an embeddable SQL database engine based on SQLite.
//! This Rust API is a batteries-included wrapper around the SQLite C API to support transparent replication while retaining compatibility with the SQLite ecosystem, such as the SQL dialect and extensions. If you are building an application in Rust, this is the crate you should use.
//! There are also libSQL language bindings of this Rust crate to other languages such as [JavaScript](), Python, Go, and C.
//!
//! ## Getting Started
//!
//! To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:
//!
//! ```rust,no_run
//! use libsql_core::Database;
//!
//! let db = Database::open(":memory:");
//! let conn = db.connect().unwrap();
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()) .unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).unwrap();
//! ```
//!
//! ## Embedded Replicas
//!
//! Embedded replica is libSQL database that's running in your application process, which keeps a local copy of a remote database.
//! They are useful if you want to move data in the memory space of your application for fast access.
//!
//! You can open an embedded read-only replica by instantiating a [`Replicator`] and setting it up to replicate a remote database:
//!
//! ```rust,no_run
//! use libsql_core::Database;
//! use libsql_replication::{Context, Frame, Frames, Replicator};
//!
//! let db = Database::open("/tmp/test.db");
//! let Context {
//!     mut hook_ctx,
//!     frames_sender,
//!     current_frame_no_notifier,
//!     meta: _,
//! } = Replicator::create_context("data.libsql").unwrap();
//!
//! let mut replicator = libsql_replication::Replicator::new(
//!     "data.libsql",
//!     &mut hook_ctx,
//!     frames_sender,
//!     current_frame_no_notifier,
//! ).unwrap();
//! let frames: Frames = Frames::Vec(vec![]);
//! replicator.sync(frames).unwrap();
//! let conn = db.connect().unwrap();
//! conn.execute("SELECT * FROM users", ()).unwrap();
//! ```
//!
//! ## Examples
//!
//! You can find more examples in the [`examples`](https://github.com/penberg/libsql-experimental/tree/libsql-api/crates/core/examples) directory.

pub mod connection;
pub mod database;
pub mod errors;
pub mod params;
pub mod raw;
pub mod rows;
pub mod statement;

pub type Result<T> = std::result::Result<T, errors::Error>;

pub use connection::Connection;
pub use database::Database;
pub use errors::Error;
pub use params::Params;
pub use params::Value;
pub use rows::Rows;
pub use rows::RowsFuture;
pub use statement::Statement;
