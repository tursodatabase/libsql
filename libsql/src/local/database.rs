use std::sync::Once;

cfg_replication!(
    use crate::database::EncryptionConfig;
    use libsql_replication::frame::FrameNo;

    use crate::replication::client::Client;
    use crate::replication::local_client::LocalClient;
    use crate::replication::remote_client::RemoteClient;
    use crate::replication::EmbeddedReplicator;
    pub use crate::replication::Frames;

    pub struct ReplicationContext {
        pub(crate) replicator: EmbeddedReplicator,
        client: Option<Client>,
        read_your_writes: bool,
    }
);

use crate::{database::OpenFlags, local::connection::Connection};
use crate::{Error::ConnectionFailed, Result};
use libsql_sys::ffi;

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

        if db_path.starts_with("libsql:")
            || db_path.starts_with("http:")
            || db_path.starts_with("https:")
        {
            Err(ConnectionFailed(format!(
                "Unable to open local database {db_path} with Database::open()"
            )))
        } else {
            Ok(Database::new(db_path, flags))
        }
    }

    #[cfg(feature = "replication")]
    pub async fn open_http_sync(
        connector: crate::util::ConnectorService,
        db_path: String,
        endpoint: String,
        auth_token: String,
        encryption_config: Option<EncryptionConfig>,
        sync_interval: Option<std::time::Duration>,
    ) -> Result<Database> {
        Self::open_http_sync_internal(
            connector,
            db_path,
            endpoint,
            auth_token,
            None,
            false,
            encryption_config,
            sync_interval,
            None,
        )
        .await
    }

    #[cfg(feature = "replication")]
    #[doc(hidden)]
    pub async fn open_http_sync_internal(
        connector: crate::util::ConnectorService,
        db_path: String,
        endpoint: String,
        auth_token: String,
        version: Option<String>,
        read_your_writes: bool,
        encryption_config: Option<EncryptionConfig>,
        sync_interval: Option<std::time::Duration>,
        http_request_callback: Option<crate::util::HttpRequestCallback>,
    ) -> Result<Database> {
        use std::path::PathBuf;

        use crate::util::coerce_url_scheme;

        let mut db = Database::open(&db_path, OpenFlags::default())?;

        let endpoint = coerce_url_scheme(endpoint);
        let remote = crate::replication::client::Client::new(
            connector,
            endpoint.as_str().try_into().unwrap(),
            auth_token,
            version.as_deref(),
            http_request_callback,
        )
        .unwrap();
        let path = PathBuf::from(db_path);
        let client = RemoteClient::new(remote.clone(), &path)
            .await
            .map_err(|e| crate::errors::Error::ConnectionFailed(e.to_string()))?;

        let replicator =
            EmbeddedReplicator::with_remote(client, path, 1000, encryption_config, sync_interval)
                .await?;

        db.replication_ctx = Some(ReplicationContext {
            replicator,
            client: Some(remote),
            read_your_writes,
        });

        Ok(db)
    }

    #[cfg(feature = "replication")]
    pub async fn open_local_sync(
        db_path: impl Into<String>,
        flags: OpenFlags,
        encryption_config: Option<EncryptionConfig>,
    ) -> Result<Database> {
        use std::path::PathBuf;

        let db_path = db_path.into();
        let mut db = Database::open(&db_path, flags)?;

        let path = PathBuf::from(db_path);
        let client = LocalClient::new(&path).await.unwrap();

        let replicator =
            EmbeddedReplicator::with_local(client, path, 1000, encryption_config).await?;

        db.replication_ctx = Some(ReplicationContext {
            replicator,
            client: None,
            read_your_writes: false,
        });

        Ok(db)
    }

    #[cfg(feature = "replication")]
    pub async fn open_local_sync_remote_writes(
        connector: crate::util::ConnectorService,
        db_path: impl Into<String>,
        endpoint: String,
        auth_token: String,
        version: Option<String>,
        flags: OpenFlags,
        encryption_config: Option<EncryptionConfig>,
        http_request_callback: Option<crate::util::HttpRequestCallback>,
    ) -> Result<Database> {
        use std::path::PathBuf;

        let db_path = db_path.into();
        let mut db = Database::open(&db_path, flags)?;

        use crate::util::coerce_url_scheme;

        let endpoint = coerce_url_scheme(endpoint);
        let remote = crate::replication::client::Client::new(
            connector,
            endpoint.as_str().try_into().unwrap(),
            auth_token,
            version.as_deref(),
            http_request_callback,
        )
        .unwrap();

        let path = PathBuf::from(db_path);
        let client = LocalClient::new(&path).await.unwrap();

        let replicator =
            EmbeddedReplicator::with_local(client, path, 1000, encryption_config).await?;

        db.replication_ctx = Some(ReplicationContext {
            replicator,
            client: Some(remote),
            read_your_writes: false,
        });

        Ok(db)
    }

    pub fn new(db_path: String, flags: OpenFlags) -> Database {
        static LIBSQL_INIT: Once = Once::new();

        LIBSQL_INIT.call_once(|| {
            // Ensure that we are configured with the correct threading model
            // if this config is not set correctly the entire api is unsafe.
            unsafe {
                assert_eq!(
                    ffi::sqlite3_config(ffi::SQLITE_CONFIG_SERIALIZED),
                    ffi::SQLITE_OK,
                    "libsql was configured with an incorrect threading configuration and
                    the api is not safe to use. Please check that no multi-thread options have
                    been set. If nothing was configured then please open an issue at:
                    https://github.com/libsql/libsql"
                );

                assert_eq!(
                    ffi::sqlite3_initialize(),
                    ffi::SQLITE_OK,
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
    pub(crate) fn writer(&self) -> Result<Option<crate::replication::Writer>> {
        use crate::replication::Writer;
        if let Some(ReplicationContext {
            client: Some(ref client),
            replicator,
            read_your_writes,
        }) = &self.replication_ctx
        {
            Ok(Some(Writer {
                client: client.clone(),
                replicator: if *read_your_writes {
                    Some(replicator.clone())
                } else {
                    None
                },
            }))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "replication")]
    /// Perform a sync step, returning the new replication index, or None, if the nothing was
    /// replicated yet
    pub async fn sync_oneshot(&self) -> Result<Option<FrameNo>> {
        if let Some(ref ctx) = self.replication_ctx {
            ctx.replicator.sync_oneshot().await
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    /// Sync until caught up with primary
    // FIXME: there is no guarantee this ever returns!
    pub async fn sync(&self) -> Result<Option<FrameNo>> {
        let mut previous_fno = None;
        loop {
            let new_fno = self.sync_oneshot().await?;
            tracing::trace!("New commited fno: {new_fno:?}");
            if new_fno == previous_fno {
                break;
            } else {
                previous_fno = new_fno;
            }
        }
        Ok(previous_fno)
    }

    #[cfg(feature = "replication")]
    pub async fn sync_frames(&self, frames: Frames) -> Result<Option<FrameNo>> {
        if let Some(ref ctx) = self.replication_ctx {
            ctx.replicator.sync_frames(frames).await
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    pub async fn flush_replicator(&self) -> Result<Option<FrameNo>> {
        if let Some(ref ctx) = self.replication_ctx {
            ctx.replicator.flush().await
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    pub async fn replication_index(&self) -> Result<Option<FrameNo>> {
        if let Some(ref ctx) = self.replication_ctx {
            Ok(ctx.replicator.committed_frame_no().await)
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.db_path
    }
}
