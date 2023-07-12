use crate::{connection::Connection, Result};
#[cfg(feature = "replication")]
use libsql_replication::Replicator;
#[cfg(feature = "replication")]
pub use libsql_replication::{Frames, TempSnapshot};

// A libSQL database.
pub struct Database {
    pub url: String,
    #[cfg(feature = "replication")]
    pub replicator: Option<Replicator>,
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
        Database {
            url,
            #[cfg(feature = "replication")]
            replicator: None,
        }
    }

    #[cfg(feature = "replication")]
    pub fn with_replicator(url: impl Into<String>) -> Database {
        let url = url.into();
        let replicator = Some(Replicator::new(&url).unwrap());
        Database { url, replicator }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    #[cfg(feature = "replication")]
    pub fn sync(&mut self, frames: Frames) -> Result<()> {
        if let Some(replicator) = &mut self.replicator {
            replicator
                .sync(frames)
                .map_err(|e| crate::errors::Error::ConnectionFailed(format!("{e}")))
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }
}
