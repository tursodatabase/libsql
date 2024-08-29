cfg_core! {
    use crate::EncryptionConfig;
}
use crate::{Database, Result};

use super::DbType;

/// A builder for [`Database`]. This struct can be used to build
/// all variants of [`Database`]. These variants include:
///
/// - `new_local`/`Local` which means a `Database` that is just a local libsql database
///     it does no networking and does not connect to any remote database.
/// - `new_remote_replica`/`RemoteReplica` creates an embedded replica database that will be able
///     to sync from the remote url and delegate writes to the remote primary.
/// - `new_local_replica`/`LocalReplica` creates an embedded replica similar to the remote version
///     except you must use `Database::sync_frames` to sync with the remote. This version also
///     includes the ability to delegate writes to a remote primary.
/// - `new_remote`/`Remote` creates a database that does not create anything locally but will
///     instead run all queries on the remote database. This is essentially the pure HTTP api.
///
/// # Note
///
/// Embedded replica's require a clean database (no database file) or a previously synced database or else it will
/// throw an error to prevent any misuse. To work around this error a user can delete the database
/// and let it resync and create the wal_index metadata file.
pub struct Builder<T = ()> {
    inner: T,
}

impl Builder<()> {
    cfg_core! {
        /// Create a new local database.
        pub fn new_local(path: impl AsRef<std::path::Path>) -> Builder<Local> {
            Builder {
                inner: Local {
                    path: path.as_ref().to_path_buf(),
                    flags: crate::OpenFlags::default(),
                    encryption_config: None,
                },
            }
        }
    }

    cfg_replication! {
        /// Create a new remote embedded replica.
        pub fn new_remote_replica(
            path: impl AsRef<std::path::Path>,
            url: String,
            auth_token: String,
        ) -> Builder<RemoteReplica> {
            Builder {
                inner: RemoteReplica {
                    path: path.as_ref().to_path_buf(),
                    remote: Remote {
                        url,
                        auth_token,
                        connector: None,
                        version: None,
                    },
                    encryption_config: None,
                    read_your_writes: true,
                    sync_interval: None,
                    http_request_callback: None,
                    namespace: None
                },
            }
        }

        /// Create a new local replica.
        pub fn new_local_replica(path: impl AsRef<std::path::Path>) -> Builder<LocalReplica> {
            Builder {
                inner: LocalReplica {
                    path: path.as_ref().to_path_buf(),
                    flags: crate::OpenFlags::default(),
                    remote: None,
                    encryption_config: None,
                    http_request_callback: None
                },
            }
        }
    }

    cfg_remote! {
        /// Create a new remote database.
        pub fn new_remote(url: String, auth_token: String) -> Builder<Remote> {
            Builder {
                inner: Remote {
                    url,
                    auth_token,
                    connector: None,
                    version: None,
                },
            }
        }
    }
}

cfg_replication_or_remote! {
    /// Remote configuration type used in [`Builder`].
    pub struct Remote {
        url: String,
        auth_token: String,
        connector: Option<crate::util::ConnectorService>,
        version: Option<String>,
    }
}

cfg_core! {
    /// Local database configuration type in [`Builder`].
    pub struct Local {
        path: std::path::PathBuf,
        flags: crate::OpenFlags,
        encryption_config: Option<EncryptionConfig>,
    }

    impl Builder<Local> {
        /// Set [`OpenFlags`] for this database.
        pub fn flags(mut self, flags: crate::OpenFlags) -> Builder<Local> {
            self.inner.flags = flags;
            self
        }

        /// Set an encryption config that will encrypt the local database.
        pub fn encryption_config(
            mut self,
            encryption_config: EncryptionConfig,
        ) -> Builder<Local> {
            self.inner.encryption_config = Some(encryption_config);
            self
        }

        /// Build the local database.
        pub async fn build(self) -> Result<Database> {
            let db = if self.inner.path == std::path::Path::new(":memory:") {
                let db = crate::local::Database::open(":memory:", crate::OpenFlags::default())?;
                Database {
                    db_type: DbType::Memory { db } ,
                    max_write_replication_index: Default::default(),
                }
            } else {
                let path = self
                    .inner
                    .path
                    .to_str()
                    .ok_or(crate::Error::InvalidUTF8Path)?
                    .to_owned();

                Database {
                    db_type: DbType::File {
                        path,
                        flags: self.inner.flags,
                        encryption_config: self.inner.encryption_config,
                    },
                    max_write_replication_index: Default::default(),
                }
            };

            Ok(db)
        }
    }
}

cfg_replication! {
    /// Remote replica configuration type in [`Builder`].
    pub struct RemoteReplica {
        path: std::path::PathBuf,
        remote: Remote,
        encryption_config: Option<EncryptionConfig>,
        read_your_writes: bool,
        sync_interval: Option<std::time::Duration>,
        http_request_callback: Option<crate::util::HttpRequestCallback>,
        namespace: Option<String>,
    }

    /// Local replica configuration type in [`Builder`].
    pub struct LocalReplica {
        path: std::path::PathBuf,
        flags: crate::OpenFlags,
        remote: Option<Remote>,
        encryption_config: Option<EncryptionConfig>,
        http_request_callback: Option<crate::util::HttpRequestCallback>,
    }

    impl Builder<RemoteReplica> {
        /// Provide a custom http connector that will be used to create http connections.
        pub fn connector<C>(mut self, connector: C) -> Builder<RemoteReplica>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            self.inner.remote = self.inner.remote.connector(connector);
            self
        }

        /// Set an encryption key that will encrypt the local database.
        pub fn encryption_config(
            mut self,
            encryption_config: EncryptionConfig,
        ) -> Builder<RemoteReplica> {
            self.inner.encryption_config = Some(encryption_config.into());
            self
        }

        /// Set weather you want writes to be visible locally before the write query returns. This
        /// means that you will be able to read your own writes if this is set to `true`.
        ///
        /// # Default
        ///
        /// This defaults to `true`.
        pub fn read_your_writes(mut self, read_your_writes: bool) -> Builder<RemoteReplica> {
            self.inner.read_your_writes = read_your_writes;
            self
        }

        /// Set the duration at which the replicator will automatically call `sync` in the
        /// background. The sync will continue for the duration that the resulted `Database`
        /// type is alive for, once it is dropped the background task will get dropped and stop.
        pub fn sync_interval(mut self, duration: std::time::Duration) -> Builder<RemoteReplica> {
            self.inner.sync_interval = Some(duration);
            self
        }

        pub fn http_request_callback<F>(mut self, f: F) -> Builder<RemoteReplica>
        where
            F: Fn(&mut http::Request<()>) + Send + Sync + 'static
        {
            self.inner.http_request_callback = Some(std::sync::Arc::new(f));
            self

        }

        /// Set the namespace that will be communicated to remote replica in the http header.
        pub fn namespace(mut self, namespace: impl Into<String>) -> Builder<RemoteReplica>
        {
            self.inner.namespace = Some(namespace.into());
            self
        }

        #[doc(hidden)]
        pub fn version(mut self, version: String) -> Builder<RemoteReplica> {
            self.inner.remote = self.inner.remote.version(version);
            self
        }

        /// Build the remote embedded replica database.
        pub async fn build(self) -> Result<Database> {
            let RemoteReplica {
                path,
                remote:
                    Remote {
                        url,
                        auth_token,
                        connector,
                        version,
                    },
                encryption_config,
                read_your_writes,
                sync_interval,
                http_request_callback,
                namespace
            } = self.inner;

            let connector = if let Some(connector) = connector {
                connector
            } else {
                let https = super::connector()?;
                use tower::ServiceExt;

                let svc = https
                    .map_err(|e| e.into())
                    .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);

                crate::util::ConnectorService::new(svc)
            };

            let path = path.to_str().ok_or(crate::Error::InvalidUTF8Path)?.to_owned();

            let db = crate::local::Database::open_http_sync_internal(
                connector,
                path,
                url,
                auth_token,
                version,
                read_your_writes,
                encryption_config.clone(),
                sync_interval,
                http_request_callback,
                namespace,
            )
            .await?;

            Ok(Database {
                db_type: DbType::Sync { db, encryption_config },
                max_write_replication_index: Default::default(),
            })
        }
    }

    impl Builder<LocalReplica> {
        /// Set [`OpenFlags`] for this database.
        pub fn flags(mut self, flags: crate::OpenFlags) -> Builder<LocalReplica> {
            self.inner.flags = flags;
            self
        }

        pub fn http_request_callback<F>(mut self, f: F) -> Builder<LocalReplica>
        where
            F: Fn(&mut http::Request<()>) + Send + Sync + 'static
        {
            self.inner.http_request_callback = Some(std::sync::Arc::new(f));
            self

        }

        /// Build the local embedded replica database.
        pub async fn build(self) -> Result<Database> {
            let LocalReplica {
                path,
                flags,
                remote,
                encryption_config,
                http_request_callback
            } = self.inner;

            let path = path.to_str().ok_or(crate::Error::InvalidUTF8Path)?.to_owned();

            let db = if let Some(Remote {
                url,
                auth_token,
                connector,
                version,
            }) = remote
            {
                let connector = if let Some(connector) = connector {
                    connector
                } else {
                    let https = super::connector()?;
                    use tower::ServiceExt;

                    let svc = https
                        .map_err(|e| e.into())
                        .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);

                    crate::util::ConnectorService::new(svc)
                };

                crate::local::Database::open_local_sync_remote_writes(
                    connector,
                    path,
                    url,
                    auth_token,
                    version,
                    flags,
                    encryption_config.clone(),
                    http_request_callback,
                )
                .await?
            } else {
                crate::local::Database::open_local_sync(path, flags, encryption_config.clone()).await?
            };

            Ok(Database {
                db_type: DbType::Sync { db, encryption_config },
                max_write_replication_index: Default::default(),
            })
        }
    }
}

cfg_remote! {
    impl Builder<Remote> {
        /// Provide a custom http connector that will be used to create http connections.
        pub fn connector<C>(mut self, connector: C) -> Builder<Remote>
        where
            C: tower::Service<http::Uri> + Send + Clone + Sync + 'static,
            C::Response: crate::util::Socket,
            C::Future: Send + 'static,
            C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        {
            self.inner = self.inner.connector(connector);
            self
        }

        #[doc(hidden)]
        pub fn version(mut self, version: String) -> Builder<Remote> {
            self.inner = self.inner.version(version);
            self
        }

        /// Build the remote database client.
        pub async fn build(self) -> Result<Database> {
            let Remote {
                url,
                auth_token,
                connector,
                version,
            } = self.inner;

            let connector = if let Some(connector) = connector {
                connector
            } else {
                let https = super::connector()?;
                use tower::ServiceExt;

                let svc = https
                    .map_err(|e| e.into())
                    .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);

                crate::util::ConnectorService::new(svc)
            };

            Ok(Database {
                db_type: DbType::Remote {
                    url,
                    auth_token,
                    connector,
                    version,
                },
                max_write_replication_index: Default::default(),
            })
        }
    }
}

cfg_replication_or_remote! {
    impl Remote {
        fn connector<C>(mut self, connector: C) -> Remote
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

            self.connector = Some(svc);
            self
        }

        fn version(mut self, version: String) -> Remote {
            self.version = Some(version);
            self
        }
    }
}
