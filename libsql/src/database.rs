#![allow(deprecated)]

mod builder;

pub use builder::Builder;

#[cfg(feature = "core")]
pub use libsql_sys::{Cipher, EncryptionConfig};

use std::fmt;

use crate::{Connection, Result};

cfg_core! {
    bitflags::bitflags! {
        /// Flags that can be passed to libsql to open a database in specific
        /// modes.
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(C)]
        pub struct OpenFlags: ::std::os::raw::c_int {
            const SQLITE_OPEN_READ_ONLY = libsql_sys::ffi::SQLITE_OPEN_READONLY;
            const SQLITE_OPEN_READ_WRITE = libsql_sys::ffi::SQLITE_OPEN_READWRITE;
            const SQLITE_OPEN_CREATE = libsql_sys::ffi::SQLITE_OPEN_CREATE;
        }
    }

    impl Default for OpenFlags {
        #[inline]
        fn default() -> OpenFlags {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        }
    }
}

enum DbType {
    #[cfg(feature = "core")]
    Memory,
    #[cfg(feature = "core")]
    File {
        path: String,
        flags: OpenFlags,
        encryption_config: Option<EncryptionConfig>,
    },
    #[cfg(feature = "replication")]
    Sync {
        db: crate::local::Database,
        encryption_config: Option<EncryptionConfig>,
    },
    #[cfg(feature = "remote")]
    Remote {
        url: String,
        auth_token: String,
        connector: crate::util::ConnectorService,
        version: Option<String>,
    },
}

impl fmt::Debug for DbType {
    #[allow(unreachable_patterns)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "core")]
            Self::Memory => write!(f, "Memory"),
            #[cfg(feature = "core")]
            Self::File { .. } => write!(f, "File"),
            #[cfg(feature = "replication")]
            Self::Sync { .. } => write!(f, "Sync"),
            #[cfg(feature = "remote")]
            Self::Remote { .. } => write!(f, "Remote"),
            _ => write!(f, "no database type set"),
        }
    }
}

/// A struct that knows how to build [`Connection`]'s, this type does
/// not do much work until the [`Database::connect`] fn is called.
pub struct Database {
    db_type: DbType,
}

cfg_core! {
    impl Database {
        /// Open an in-memory libsql database.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_in_memory() -> Result<Self> {
            Ok(Database {
                db_type: DbType::Memory,
            })
        }

        /// Open a file backed libsql database.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open(db_path: impl Into<String>) -> Result<Database> {
            Database::open_with_flags(db_path, OpenFlags::default())
        }

        /// Open a file backed libsql database with flags.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_with_flags(db_path: impl Into<String>, flags: OpenFlags) -> Result<Database> {
            Ok(Database {
                db_type: DbType::File {
                    path: db_path.into(),
                    flags,
                    encryption_config: None,
                },
            })
        }
    }
}

cfg_replication! {
    use crate::Error;
    use libsql_replication::frame::FrameNo;


    impl Database {
        /// Open a local database file with the ability to sync from snapshots from local filesystem.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_local_sync(
            db_path: impl Into<String>,
            encryption_config: Option<EncryptionConfig>
        ) -> Result<Database> {
            let db = crate::local::Database::open_local_sync(
                db_path,
                OpenFlags::default(),
                encryption_config.clone()
            ).await?;

            Ok(Database {
                db_type: DbType::Sync { db, encryption_config },
            })
        }


        /// Open a local database file with the ability to sync from snapshots from local filesystem
        /// and forward writes to the provided endpoint.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_local_sync_remote_writes(
            db_path: impl Into<String>,
            endpoint: String,
            auth_token: String,
            encryption_config: Option<EncryptionConfig>,
        ) -> Result<Database> {
            let https = connector()?;

            Self::open_with_local_sync_remote_writes_connector(
                db_path,
                endpoint,
                auth_token,
                https,
                encryption_config
            ).await
        }

        /// Open a local database file with the ability to sync from snapshots from local filesystem
        /// and forward writes to the provided endpoint and a custom http connector.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_local_sync_remote_writes_connector<C>(
            db_path: impl Into<String>,
            endpoint: String,
            auth_token: String,
            connector: C,
            encryption_config: Option<EncryptionConfig>,
        ) -> Result<Database>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            use tower::ServiceExt;

            let svc = connector
                .map_err(|e| e.into())
                .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);

            let svc = crate::util::ConnectorService::new(svc);

            let db = crate::local::Database::open_local_sync_remote_writes(
                svc,
                db_path.into(),
                endpoint,
                auth_token,
                None,
                OpenFlags::default(),
                encryption_config.clone(),
                None
            ).await?;

            Ok(Database {
                db_type: DbType::Sync { db, encryption_config },
            })
        }

        /// Open a local database file with the ability to sync from a remote database.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_remote_sync(
            db_path: impl Into<String>,
            url: impl Into<String>,
            token: impl Into<String>,
            encryption_config: Option<EncryptionConfig>,
        ) -> Result<Database> {
            let https = connector()?;

            Self::open_with_remote_sync_connector(db_path, url, token, https, false, encryption_config).await
        }

        /// Open a local database file with the ability to sync from a remote database
        /// in consistent mode.
        ///
        /// Consistent mode means that when a write happens it will not complete until
        /// that write is visible in the local db.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_remote_sync_consistent(
            db_path: impl Into<String>,
            url: impl Into<String>,
            token: impl Into<String>,
            encryption_config: Option<EncryptionConfig>,
        ) -> Result<Database> {
            let https = connector()?;

            Self::open_with_remote_sync_connector(db_path, url, token, https, true, encryption_config).await
        }

        /// Connect an embedded replica to a remote primary with a custom
        /// http connector.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub async fn open_with_remote_sync_connector<C>(
            db_path: impl Into<String>,
            url: impl Into<String>,
            token: impl Into<String>,
            connector: C,
            read_your_writes: bool,
            encryption_config: Option<EncryptionConfig>,
        ) -> Result<Database>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            Self::open_with_remote_sync_connector_internal(
                db_path,
                url,
                token,
                connector,
                None,
                read_your_writes,
                encryption_config,
                None
            ).await
        }

        #[doc(hidden)]
        pub async fn open_with_remote_sync_internal(
            db_path: impl Into<String>,
            url: impl Into<String>,
            token: impl Into<String>,
            version: Option<String>,
            read_your_writes: bool,
            encryption_config: Option<EncryptionConfig>,
            sync_interval: Option<std::time::Duration>,
        ) -> Result<Database> {
            let https = connector()?;

            Self::open_with_remote_sync_connector_internal(
                db_path,
                url,
                token,
                https,
                version,
                read_your_writes,
                encryption_config,
                sync_interval
            ).await
        }

        #[doc(hidden)]
        async fn open_with_remote_sync_connector_internal<C>(
            db_path: impl Into<String>,
            url: impl Into<String>,
            token: impl Into<String>,
            connector: C,
            version: Option<String>,
            read_your_writes: bool,
            encryption_config: Option<EncryptionConfig>,
            sync_interval: Option<std::time::Duration>,
        ) -> Result<Database>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            use tower::ServiceExt;

            let svc = connector
                .map_err(|e| e.into())
                .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);

            let svc = crate::util::ConnectorService::new(svc);

            let db = crate::local::Database::open_http_sync_internal(
                svc,
                db_path.into(),
                url.into(),
                token.into(),
                version,
                read_your_writes,
                encryption_config.clone(),
                sync_interval,
                None
            ).await?;

            Ok(Database {
                db_type: DbType::Sync { db, encryption_config },
            })
        }


        /// Sync database from remote, and returns the committed frame_no after syncing, if
        /// applicable.
        pub async fn sync(&self) -> Result<Option<FrameNo>> {
            if let DbType::Sync { db, encryption_config: _ } = &self.db_type {
                db.sync().await
            } else {
                Err(Error::SyncNotSupported(format!("{:?}", self.db_type)))
            }
        }

        /// Apply a set of frames to the database and returns the committed frame_no after syncing, if
        /// applicable.
        pub async fn sync_frames(&self, frames: crate::replication::Frames) -> Result<Option<FrameNo>> {
            if let DbType::Sync { db, encryption_config: _ } = &self.db_type {
                db.sync_frames(frames).await
            } else {
                Err(Error::SyncNotSupported(format!("{:?}", self.db_type)))
            }
        }

        /// Force buffered replication frames to be applied, and return the current commit frame_no
        /// if applicable.
        pub async fn flush_replicator(&self) -> Result<Option<FrameNo>> {
            if let DbType::Sync { db, encryption_config: _ } = &self.db_type {
                db.flush_replicator().await
            } else {
                Err(Error::SyncNotSupported(format!("{:?}", self.db_type)))
            }
        }

        /// Returns the database currently committed replication index
        pub async fn replication_index(&self) -> Result<Option<FrameNo>> {
            if let DbType::Sync { db, encryption_config: _ } = &self.db_type {
                db.replication_index().await
            } else {
                Err(Error::SyncNotSupported(format!("{:?}", self.db_type)))
            }
        }

        /// Freeze this embedded replica and convert it into a regular
        /// non-embedded replica database.
        ///
        /// # Error
        ///
        /// Returns `FreezeNotSupported` if the database is not configured in
        /// embedded replica mode.
        pub fn freeze(self) -> Result<Database> {
           match self.db_type {
               DbType::Sync { db, .. } => {
                   let path = db.path().to_string();
                   Ok(Database {
                       db_type: DbType::File { path, flags: OpenFlags::default(), encryption_config: None}
                   })
               }
               t => Err(Error::FreezeNotSupported(format!("{:?}", t)))
           }
        }
    }
}

impl Database {}

cfg_remote! {
    impl Database {
        /// Open a remote based HTTP database using libsql's hrana protocol.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_remote(url: impl Into<String>, auth_token: impl Into<String>) -> Result<Self> {
            let https = connector()?;

            Self::open_remote_with_connector_internal(url, auth_token, https, None)
        }

        #[doc(hidden)]
        pub fn open_remote_internal(
            url: impl Into<String>,
            auth_token: impl Into<String>,
            version: impl Into<String>,
        ) -> Result<Self> {
            let https = connector()?;

            Self::open_remote_with_connector_internal(url, auth_token, https, Some(version.into()))
        }

        /// Connect to a remote libsql using libsql's hrana protocol with a custom connector.
        #[deprecated = "Use the new `Builder` to construct `Database`"]
        pub fn open_remote_with_connector<C>(
            url: impl Into<String>,
            auth_token: impl Into<String>,
            connector: C,
        ) -> Result<Self>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            Self::open_remote_with_connector_internal(url, auth_token, connector, None)
        }

        #[doc(hidden)]
        fn open_remote_with_connector_internal<C>(
            url: impl Into<String>,
            auth_token: impl Into<String>,
            connector: C,
            version: Option<String>
        ) -> Result<Self>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            use tower::ServiceExt;

            let svc = connector
                .map_err(|e| e.into())
                .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);
            Ok(Database {
                db_type: DbType::Remote {
                    url: url.into(),
                    auth_token: auth_token.into(),
                    connector: crate::util::ConnectorService::new(svc),
                    version,
                },
            })
        }
    }
}

impl Database {
    /// Connect to the database this can mean a few things depending on how it was constructed:
    ///
    /// - When constructed with `open`/`open_with_flags`/`open_in_memory` this will call into the
    ///     libsql C ffi and create a connection to the libsql database.
    /// - When constructed with `open_remote` and friends it will not call any C ffi and will
    ///     lazily create a HTTP connection to the provided endpoint.
    /// - When constructed with `open_with_remote_sync_` and friends it will attempt to perform a
    ///     handshake with the remote server and will attempt to replicate the remote database
    ///     locally.
    #[allow(unreachable_patterns)]
    pub fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            #[cfg(feature = "core")]
            DbType::Memory => {
                use crate::local::impls::LibsqlConnection;

                let db = crate::local::Database::open(":memory:", OpenFlags::default())?;
                let conn = db.connect()?;

                let conn = std::sync::Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            #[cfg(feature = "core")]
            DbType::File {
                path,
                flags,
                encryption_config,
            } => {
                use crate::local::impls::LibsqlConnection;

                let db = crate::local::Database::open(path, *flags)?;
                let conn = db.connect()?;

                if !cfg!(feature = "encryption") && encryption_config.is_some() {
                    return Err(crate::Error::Misuse(
                        "Encryption is not enabled: enable the `encryption` feature in order to enable encryption-at-rest".to_string(),
                    ));
                }

                #[cfg(feature = "encryption")]
                if let Some(cfg) = encryption_config {
                    if unsafe {
                        libsql_sys::connection::set_encryption_cipher(conn.raw, cfg.cipher_id())
                    } == -1
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption cipher".to_string(),
                        ));
                    }
                    if unsafe {
                        libsql_sys::connection::set_encryption_key(conn.raw, &cfg.encryption_key)
                    } != crate::ffi::SQLITE_OK
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption key".to_string(),
                        ));
                    }
                }

                let conn = std::sync::Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            #[cfg(feature = "replication")]
            DbType::Sync {
                db,
                encryption_config,
            } => {
                use crate::local::impls::LibsqlConnection;

                let conn = db.connect()?;

                if !cfg!(feature = "encryption") && encryption_config.is_some() {
                    return Err(crate::Error::Misuse(
                        "Encryption is not enabled: enable the `encryption` feature in order to enable encryption-at-rest".to_string(),
                    ));
                }
                #[cfg(feature = "encryption")]
                if let Some(cfg) = encryption_config {
                    if unsafe {
                        libsql_sys::connection::set_encryption_cipher(conn.raw, cfg.cipher_id())
                    } == -1
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption cipher".to_string(),
                        ));
                    }
                    if unsafe {
                        libsql_sys::connection::set_encryption_key(conn.raw, &cfg.encryption_key)
                    } != crate::ffi::SQLITE_OK
                    {
                        return Err(crate::Error::Misuse(
                            "failed to set encryption key".to_string(),
                        ));
                    }
                }

                let local = LibsqlConnection { conn };
                let writer = local.conn.writer().cloned();
                let remote = crate::replication::RemoteConnection::new(local, writer);
                let conn = std::sync::Arc::new(remote);

                Ok(Connection { conn })
            }

            #[cfg(feature = "remote")]
            DbType::Remote {
                url,
                auth_token,
                connector,
                version,
            } => {
                let conn = std::sync::Arc::new(
                    crate::hrana::connection::HttpConnection::new_with_connector(
                        url,
                        auth_token,
                        connector.clone(),
                        version.as_ref().map(|s| s.as_str()),
                    ),
                );

                Ok(Connection { conn })
            }

            _ => unreachable!("no database type set"),
        }
    }
}

#[cfg(any(feature = "replication", feature = "remote"))]
fn connector() -> Result<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>> {
    let mut http = hyper::client::HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);

    Ok(hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(crate::Error::InvalidTlsConfiguration)?
        .https_or_http()
        .enable_http1()
        .wrap_connector(http))
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").finish()
    }
}
