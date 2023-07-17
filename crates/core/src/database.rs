use crate::{connection::Connection, errors::Error::ConnectionFailed, Result};
#[cfg(feature = "replication")]
use libsql_replication::Replicator;
#[cfg(feature = "replication")]
pub use libsql_replication::{rpc, Client, Frames, TempSnapshot};

pub struct ReplicationContext {
    pub replicator: Replicator,
    pub client: Client,
}

// A libSQL database.
pub struct Database {
    pub url: String,
    #[cfg(feature = "replication")]
    pub replication_ctx: Option<ReplicationContext>,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Database {
        let url = url.into();
        if url.starts_with("libsql:") || url.starts_with("http:") {
            tracing::warn!("Ignoring {url} in Database::open() and opening a local db");
            let filename = "libsql_tmp.db".to_string();
            Database::new(filename)
        } else {
            Database::new(url)
        }
    }

    pub fn new(url: String) -> Database {
        Database {
            url,
            #[cfg(feature = "replication")]
            replication_ctx: None,
        }
    }

    #[cfg(feature = "replication")]
    pub async fn with_replicator(
        url: impl Into<String>,
        db_path: impl Into<String>,
    ) -> Result<Database> {
        let url = url.into();
        let db_path = db_path.into();
        let mut db = Database::open(&db_path);
        let replicator = Replicator::new(db_path).map_err(|e| ConnectionFailed(format!("{e}")))?;
        let (client, meta) = Replicator::connect_to_rpc(
            rpc::Endpoint::from_shared(url.clone())
                .map_err(|e| ConnectionFailed(format!("{e}")))?,
        )
        .await
        .map_err(|e| ConnectionFailed(format!("{e}")))?;
        *replicator.meta.lock() = Some(meta);
        db.replication_ctx = Some(ReplicationContext { replicator, client });
        Ok(db)
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    #[cfg(feature = "replication")]
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ctx) = &mut self.replication_ctx {
            ctx.replicator
                .sync_from_rpc(&mut ctx.client)
                .map_err(|e| ConnectionFailed(format!("{e}")))
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
