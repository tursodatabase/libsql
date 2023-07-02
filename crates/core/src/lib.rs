pub mod connection;
pub mod errors;

pub use connection::Connection;
pub use connection::ResultSet;

use errors::Error;
use std::ffi::c_int;

type Result<T> = std::result::Result<T, Error>;

pub struct Database {
    pub url: String,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Database {
        Database { url: url.into() }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }
}
