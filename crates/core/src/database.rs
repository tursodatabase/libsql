use crate::{connection::Connection, Error, Result};

use libsql_replication::{replica::Replicator, replication_log::configure_rpc};

// A libSQL database.
pub struct Database {
    pub url: String,
    pub replicator: Option<Replicator>,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Result<Database> {
        let url = url.into();
        // FIXME: in current state of the code, libsql:// address is assumed to be
        // the RPC endpoint for receiving frames from an sqld primary.
        // Primary is the node that runs sqld with a parameter like: --grpc-listen-addr 127.0.0.1:8888
        // It's *not* what users expect, but it makes testing embedded replicas easy, so ¯\_(ツ)_/¯
        let db = if url.starts_with("libsql:") {
            let url = url.replace("libsql:", "http:");
            let filename = "data.libsql/data".to_string();
            std::fs::create_dir("data.libsql").ok();
            std::fs::File::create(filename.as_str())
                .map_err(|e| Error::ConnectionFailed(e.to_string()))?;
            // FIXME: this replicator is blatantly copied from sqld and it does perhaps more
            // than necessary, by also regularly asking for new frames. If we want to control
            // this mechanism by calling sync() ourselves, we need to expose the lower-level API here.
            // This is important, because we *do not* want a Tokio dependency in libsql.
            let (channel, uri) =
                configure_rpc(url).map_err(|e| Error::ConnectionFailed(e.to_string()))?;
            let replicator = Replicator::new("data.libsql".into(), channel, uri, true)
                .map_err(|e| Error::ConnectionFailed(e.to_string()))?;
            Database::new(filename, Some(replicator))
        } else {
            Database::new(url, None)
        };
        Ok(db)
    }

    pub fn new(url: String, replicator: Option<Replicator>) -> Database {
        Database { url, replicator }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }
}
