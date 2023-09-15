use std::sync::Once;

use crate::v1::connection::Connection;
use crate::OpenFlags;
use crate::{Error::ConnectionFailed, Result};
#[cfg(feature = "replication")]
use libsql_replication::Replicator;
#[cfg(feature = "replication")]
pub use libsql_replication::{Frames, TempSnapshot};
use libsql_sys::ffi;

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
        // The `libsql://` protocol is an alias for `https://`.
        let endpoint = endpoint.into().replace("libsql://", "https://");
        Opts {
            sync: Sync::Http {
                endpoint,
                auth_token: auth_token.into(),
            },
        }
    }
}

// A libSQL database.
pub struct Database {
    pub db_path: String,
    pub flags: OpenFlags,
    #[cfg(feature = "replication")]
    pub replication_ctx: Option<ReplicationContext>,
}

impl Database {
    /// Open a local database file.
    pub fn open<S: Into<String>>(db_path: S, flags: OpenFlags) -> Result<Database> {
        let db_path = db_path.into();
        if db_path.starts_with("libsql:") || db_path.starts_with("http:") {
            Err(ConnectionFailed(format!(
                "Unable to open remote database {db_path} with Database::open()"
            )))
        } else {
            Ok(Database::new(db_path, flags))
        }
    }

    #[cfg(feature = "replication")]
    pub async fn open_with_opts(db_path: impl Into<String>, opts: Opts) -> Result<Database> {
        let db_path = db_path.into();
        let mut db = Database::open(&db_path, OpenFlags::default())?;

        match opts.sync {
            Sync::Http {
                endpoint,
                auth_token,
            } => {
                let replicator =
                    Replicator::with_http_sync(db_path, endpoint.clone(), auth_token.clone())
                        .map_err(|e| ConnectionFailed(format!("{e}")))?;

                db.replication_ctx = Some(ReplicationContext {
                    replicator,
                    endpoint,
                });
            }
            Sync::Frame => {
                let replicator =
                    Replicator::new(db_path).map_err(|e| ConnectionFailed(format!("{e}")))?;
                db.replication_ctx = Some(ReplicationContext {
                    replicator,
                    endpoint: "".to_string(),
                });
            }
        }

        Ok(db)
    }

    pub fn new(db_path: String, flags: OpenFlags) -> Database {
        static LIBSQL_INIT: Once = Once::new();

        LIBSQL_INIT.call_once(|| {
            // Ensure that we are configured with the correct threading model
            // if this config is not set correctly the entire api is unsafe.
            unsafe {
                assert_eq!(
                    ffi::sqlite3_config(ffi::SQLITE_CONFIG_SERIALIZED as i32),
                    ffi::SQLITE_OK as i32,
                    "libsql was configured with an incorrect threading configuration and
                the api is not safe to use. Please check that no multi-thread options have
                been set. If nothing was configured then please open an issue at:
                https://github.com/libsql/libsql"
                );

                assert_eq!(
                    ffi::sqlite3_initialize(),
                    ffi::SQLITE_OK as i32,
                    "libsql failed to initialize"
                );
            }
        });

        Database {
            db_path,
            flags,
            #[cfg(feature = "replication")]
            replication_ctx: None,
        }
    }

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }

    #[cfg(feature = "replication")]
    pub fn writer(&self) -> Result<Option<libsql_replication::Writer>> {
        if let Some(ctx) = &self.replication_ctx {
            if ctx.endpoint.is_empty() {
                return Ok(None);
            }
            Ok(ctx
                .replicator
                .writer()
                .expect("Unable to get writer")
                .into())
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "replication")]
    pub async fn sync_oneshot(&self) -> Result<usize> {
        if let Some(ctx) = &self.replication_ctx {
            // TODO: Unfortunate that we need to lock then unlock
            // then lock again creating potential race conditions.
            // This for now is fine since DB is the only user
            // of the replicator but we should refactor it such that we
            // can avoid having to do these weird locking patterns.
            if ctx.replicator.meta.lock().is_none() {
                let meta = ctx
                    .replicator
                    .init_metadata()
                    .await
                    .map_err(|e| ConnectionFailed(format!("{e}")))?;

                *ctx.replicator.meta.lock() = Some(meta);
            }

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
    pub async fn sync(&self) -> Result<usize> {
        let mut synced = 0;
        loop {
            let n = self.sync_oneshot().await?;
            tracing::trace!("Synced {n} frames");
            if n == 0 {
                break;
            } else {
                synced += n;
            }
        }
        Ok(synced)
    }

    #[cfg(feature = "replication")]
    pub fn sync_frames(&self, frames: Frames) -> Result<usize> {
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
