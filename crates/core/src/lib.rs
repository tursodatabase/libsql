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
