pub mod database;
pub mod connection;
pub mod errors;

pub type Result<T> = std::result::Result<T, errors::Error>;

pub use database::Database;
pub use connection::Connection;
pub use connection::ResultSet;