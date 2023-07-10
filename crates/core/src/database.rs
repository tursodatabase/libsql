use crate::{connection::Connection, Result};

// A libSQL database.
pub struct Database {
    pub url: String,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Database {
        let url = url.into();
        if url.starts_with("libsql:") {
            let url = url.replace("libsql:", "http:");
            tracing::info!("Absolutely ignoring libsql URL: {url}");
            let filename = "data.libsql/data".to_string();
            Database::new(filename)
        } else {
            Database::new(url)
        }
    }

    pub fn new(url: String) -> Database {
        Database { url }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }
}
