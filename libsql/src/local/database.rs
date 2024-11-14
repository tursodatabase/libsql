use std::sync::Once;

cfg_replication!(
    use http::uri::InvalidUri;
    use crate::database::{EncryptionConfig, FrameNo};

    use crate::replication::client::Client;
    use crate::replication::local_client::LocalClient;
    use crate::replication::remote_client::RemoteClient;
    use crate::replication::EmbeddedReplicator;
    pub use crate::replication::Frames;
    pub use crate::replication::SyncUsageStats;

    pub struct ReplicationContext {
        pub(crate) replicator: EmbeddedReplicator,
        client: Option<Client>,
        read_your_writes: bool,
    }
);

cfg_sync! {
    use crate::sync::SyncContext;
}

use crate::{database::OpenFlags, local::connection::Connection};
use crate::{Error::ConnectionFailed, Result};
use libsql_sys::ffi;

// A libSQL database.
pub struct Database {
    pub db_path: String,
    pub flags: OpenFlags,
    #[cfg(feature = "replication")]
    pub replication_ctx: Option<ReplicationContext>,
    #[cfg(feature = "sync")]
    pub sync_ctx: Option<SyncContext>,
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
        namespace: Option<String>,
    ) -> Result<Database> {
        use std::path::PathBuf;

        use crate::util::coerce_url_scheme;

        let mut db = Database::open(&db_path, OpenFlags::default())?;

        let endpoint = coerce_url_scheme(endpoint);
        let remote = crate::replication::client::Client::new(
            connector.clone(),
            endpoint
                .as_str()
                .try_into()
                .map_err(|e: InvalidUri| crate::Error::Replication(e.into()))?,
            auth_token.clone(),
            version.as_deref(),
            http_request_callback.clone(),
            namespace,
        )
        .map_err(|e| crate::Error::Replication(e.into()))?;
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

    #[cfg(feature = "sync")]
    #[doc(hidden)]
    pub async fn open_local_with_offline_writes(
        db_path: impl Into<String>,
        flags: OpenFlags,
        endpoint: String,
        auth_token: String,
    ) -> Result<Database> {
        let db_path = db_path.into();
        let endpoint = if endpoint.starts_with("libsql:") {
            endpoint.replace("libsql:", "https:")
        } else {
            endpoint
        };
        let mut db = Database::open(&db_path, flags)?;
        db.sync_ctx = Some(SyncContext::new(endpoint, Some(auth_token)));
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
        let client = LocalClient::new(&path)
            .await
            .map_err(|e| crate::Error::Replication(e.into()))?;

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
            endpoint
                .as_str()
                .try_into()
                .map_err(|e: InvalidUri| crate::Error::Replication(e.into()))?,
            auth_token,
            version.as_deref(),
            http_request_callback,
            None,
        )
        .map_err(|e| crate::Error::Replication(e.into()))?;

        let path = PathBuf::from(db_path);
        let client = LocalClient::new(&path)
            .await
            .map_err(|e| crate::Error::Replication(e.into()))?;

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
            #[cfg(feature = "sync")]
            sync_ctx: None,
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
    pub async fn sync_oneshot(&self) -> Result<crate::database::Replicated> {
        if let Some(ctx) = &self.replication_ctx {
            ctx.replicator.sync_oneshot().await
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    /// Sync with primary
    pub async fn sync(&self) -> Result<crate::database::Replicated> {
        Ok(self.sync_oneshot().await?)
    }

    #[cfg(feature = "replication")]
    /// Return detailed logs about bytes synced with primary
    pub async fn get_sync_usage_stats(&self) -> Result<SyncUsageStats> {
        if let Some(ctx) = &self.replication_ctx {
            let sync_stats = ctx.replicator.get_sync_usage_stats().await?;
            Ok(sync_stats)
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
    }

    #[cfg(feature = "replication")]
    /// Sync with primary at least to a given replication index
    pub async fn sync_until(&self, replication_index: FrameNo) -> Result<crate::database::Replicated> {
        if let Some(ctx) = &self.replication_ctx {
            let mut frame_no: Option<FrameNo> = ctx.replicator.committed_frame_no().await;
            let mut frames_synced: usize = 0;
            while frame_no.unwrap_or(0) < replication_index {
                let res = ctx.replicator.sync_oneshot().await?;
                frame_no = res.frame_no();
                frames_synced += res.frames_synced();
            }
            Ok(crate::database::Replicated {
                frame_no,
                frames_synced,
            })
        } else {
            Err(crate::errors::Error::Misuse(
                "No replicator available. Use Database::with_replicator() to enable replication"
                    .to_string(),
            ))
        }
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

    #[cfg(feature = "sync")]
    /// Push WAL frames to remote.
    pub async fn push(&self) -> Result<crate::database::Replicated> {
        let sync_ctx = self.sync_ctx.as_ref().unwrap();
        let conn = self.connect()?;

        let page_size = {
            let rows = conn.query("PRAGMA page_size", crate::params::Params::None)?.unwrap();
            let row = rows.next()?.unwrap();
            let page_size = row.get::<u32>(0)?;
            page_size
        };

        let mut max_frame_no: std::os::raw::c_uint = 0;
        unsafe { libsql_sys::ffi::libsql_wal_frame_count(conn.handle(), &mut max_frame_no) };
        
        let generation = 1; // TODO: Probe from WAL.
        let start_frame_no = sync_ctx.durable_frame_num + 1;
        let end_frame_no = max_frame_no;

        let mut frame_no = start_frame_no;
        while frame_no <= end_frame_no {
            // The server returns its maximum frame number. To avoid resending
            // frames the server already knows about, we need to update the
            // frame number to the one returned by the server.
            let max_frame_no = self.push_one_frame(&conn, &sync_ctx, generation, frame_no, page_size).await?;
            if max_frame_no > frame_no {
                frame_no = max_frame_no;
            }
            frame_no += 1;
        }

        let frame_count = end_frame_no - start_frame_no + 1;
        Ok(crate::database::Replicated{
            frame_no: None,
            frames_synced: frame_count as usize,
        })
    }

    #[cfg(feature = "sync")]
    async fn push_one_frame(&self, conn: &Connection, sync_ctx: &SyncContext, generation: u32, frame_no: u32, page_size: u32) -> Result<u32> {
        let frame_size: usize = 24+page_size as usize;
        let frame = vec![0; frame_size];
        let rc = unsafe {
            libsql_sys::ffi::libsql_wal_get_frame(conn.handle(), frame_no, frame.as_ptr() as *mut _, frame_size as u32)
        };
        if rc != 0 {
            return Err(crate::errors::Error::SqliteFailure(rc as std::ffi::c_int, format!("Failed to get frame: {}", frame_no)));
        }
        let uri = format!("{}/sync/{}/{}/{}", sync_ctx.sync_url, generation, frame_no, frame_no+1);
        let max_frame_no = self.push_with_retry(uri, &sync_ctx.auth_token, frame.to_vec(), sync_ctx.max_retries).await?;
        Ok(max_frame_no)
    }

    #[cfg(feature = "sync")]
    async fn push_with_retry(&self, uri: String, auth_token: &Option<String>, frame: Vec<u8>, max_retries: usize) -> Result<u32> {
        let mut nr_retries = 0;
        loop {
            let client = reqwest::Client::new();
            let mut builder = client.post(uri.to_owned());
            match auth_token {   
                Some(ref auth_token) => {
                    builder = builder.header("Authorization", format!("Bearer {}", auth_token.to_owned()));
                }
                None => {}
            }
            let res = builder.body(frame.to_vec()).send().await.unwrap();
            if res.status().is_success() {
                let resp = res.json::<serde_json::Value>().await.unwrap();
                let max_frame_no = resp.get("max_frame_no").unwrap().as_u64().unwrap();
                return Ok(max_frame_no as u32);
            }
            if nr_retries > max_retries {
                return Err(crate::errors::Error::ConnectionFailed(format!("Failed to push frame: {}", res.status())));
            }
            let delay = std::time::Duration::from_millis(100 * (1 << nr_retries));
            tokio::time::sleep(delay).await;
            nr_retries += 1;
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.db_path
    }
}
