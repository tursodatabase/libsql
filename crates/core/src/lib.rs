pub mod connection;
pub mod database;
pub mod errors;

pub type Result<T> = std::result::Result<T, errors::Error>;

pub use connection::Connection;
pub use connection::Rows;
pub use connection::RowsFuture;
pub use database::Database;
