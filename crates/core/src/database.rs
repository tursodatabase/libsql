use crate::{connection::Connection, errors::Error::ConnectionFailed, Result};
#[cfg(feature = "replication")]
use libsql_replication::Replicator;
#[cfg(feature = "replication")]
pub use libsql_replication::{Frames, TempSnapshot};

#[cfg(feature = "replication")]
pub struct ReplicationContext {
    pub replicator: Replicator,
    pub endpoint: String,
}

#[cfg(feature = "replication")]
pub(crate) enum Sync {
    Frame,
    Http {
        endpoint: String,
        auth_token: String,
    },
}

#[cfg(feature = "replication")]
pub struct Opts {
    pub(crate) sync: Sync,
}

#[cfg(feature = "replication")]
impl Opts {
    pub fn with_sync() -> Opts {
        Opts { sync: Sync::Frame }
    }

    pub fn with_http_sync(endpoint: impl Into<String>, auth_token: impl Into<String>) -> Opts {
        Opts {
            sync: Sync::Http {
                endpoint: endpoint.into(),
                auth_token: auth_token.into(),
            },
        }
    }
}

// A libSQL database.
pub struct Database {
    pub db_path: String,
    #[cfg(feature = "replication")]
    pub replication_ctx: Option<ReplicationContext>,
}

impl Database {
    /// Open a local database file.
    pub fn open<S: Into<String>>(db_path: S) -> Result<Database> {
        let db_path = db_path.into();
        if db_path.starts_with("libsql:") || db_path.starts_with("http:") {
            Err(ConnectionFailed(format!(
                "Unable to open remote database {db_path} with Database::open()"
            )))
        } else {
            Ok(Database::new(db_path))
        }
    }

    #[cfg(feature = "replication")]
    pub async fn open_with_opts(db_path: impl Into<String>, opts: Opts) -> Result<Database> {
        let db_path = db_path.into();
        let mut db = Database::open(&db_path)?;
        let mut replicator =
            Replicator::new(db_path).map_err(|e| ConnectionFailed(format!("{e}")))?;
        if let Sync::Http {
            endpoint,
            auth_token,
        } = opts.sync
        {
            let meta = replicator
                .init_metadata(&endpoint, &auth_token)
                .await
                .map_err(|e| ConnectionFailed(format!("{e}")))?;
            *replicator.meta.lock() = Some(meta);
            db.replication_ctx = Some(ReplicationContext {
                replicator,
                endpoint,
            });
        };

        Ok(db)
    }

    pub fn new(db_path: String) -> Database {
        Database {
            db_path,
            #[cfg(feature = "replication")]
            replication_ctx: None,
        }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    #[cfg(feature = "replication")]
    pub async fn sync(&self) -> Result<usize> {
        if let Some(ctx) = &self.replication_ctx {
            ctx.replicator
                .sync_from_http()
                .await
                .map_err(|e| ConnectionFailed(format!("{e}")))
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    pub fn sync_frames(&self, frames: Frames) -> Result<()> {
        if let Some(ctx) = self.replication_ctx.as_ref() {
            ctx.replicator
                .sync(frames)
                .map_err(|e| ConnectionFailed(format!("{e}")))
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }
}
