use crate::{connection::Connection, errors::Error::ConnectionFailed, Result};
#[cfg(feature = "replication")]
use libsql_replication::Replicator;
#[cfg(feature = "replication")]
pub use libsql_replication::{rpc, Client, Frames, TempSnapshot};

#[cfg(feature = "replication")]
pub struct ReplicationContext {
    pub replicator: Replicator,
    pub client: Option<Client>,
}

#[cfg(feature = "replication")]
pub(crate) enum Sync {
    Frame,
    Rpc { url: String },
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

    pub fn with_rpc_sync(url: impl Into<String>) -> Opts {
        Opts {
            sync: Sync::Rpc { url: url.into() },
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
        let replicator = Replicator::new(db_path).map_err(|e| ConnectionFailed(format!("{e}")))?;
        let client = match opts.sync {
            Sync::Rpc { url } => {
                let (client, meta) = Replicator::connect_to_rpc(
                    rpc::Endpoint::from_shared(url.clone())
                        .map_err(|e| ConnectionFailed(format!("{e}")))?,
                )
                .await
                .map_err(|e| ConnectionFailed(format!("{e}")))?;
                *replicator.meta.lock() = Some(meta);
                Some(client)
            }
            Sync::Frame => None,
        };
        db.replication_ctx = Some(ReplicationContext { replicator, client });
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
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ctx) = &mut self.replication_ctx {
            if let Some(client) = &mut ctx.client {
                ctx.replicator
                    .sync_from_rpc(client)
                    .map_err(|e| ConnectionFailed(format!("{e}")))
            } else {
                Ok(())
            }
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    pub fn sync_frames(&mut self, frames: Frames) -> Result<()> {
        if let Some(ctx) = &mut self.replication_ctx {
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
