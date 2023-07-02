//! libSQL API with batteries included.
//!
//! Example usage:
//!
//! ```rust
//! let db = libsql::Database::open(":memory:");
//! conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)") .unwrap();
//! conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')").unwrap();
//! ```

pub mod connection;
pub mod database;
pub mod errors;
pub mod rows;
pub mod statement;

pub type Result<T> = std::result::Result<T, errors::Error>;

pub use connection::Connection;
pub use database::Database;
pub use errors::Error;
pub use rows::Rows;
pub use rows::RowsFuture;
pub use statement::Statement;
