use crate::{Result, connection::Connection};

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