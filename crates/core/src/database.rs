use crate::{connection::Connection, Result};

use libsql_replication::Replicator;

// A libSQL database.
pub struct Database {
    pub url: String,
    pub replicator: Option<Replicator>,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Database {
        let url = url.into();
        if url.starts_with("libsql:") {
            let url = url.replace("libsql:", "http:");
            let filename = "file:tmp.db".to_string();
            let replicator = Some(Replicator::new(url));
            Database::new(filename, replicator)
        } else {
            Database::new(url, None)
        }
    }

    pub fn new(url: String, replicator: Option<Replicator>) -> Database {
        Database { url, replicator }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(replicator) = &self.replicator {
            replicator.sync();
        }
        Ok(())
    }
}
