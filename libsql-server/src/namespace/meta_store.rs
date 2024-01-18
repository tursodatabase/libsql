use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::CompressionKind;
use libsql_replication::rpc::metadata;
use libsql_sys::wal::{
    wrapper::{WalWrapper, WrappedWal},
    Sqlite3Wal, Sqlite3WalManager,
};
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};

use crate::connection::config::DatabaseConfig;
use crate::{
    config::MetaStoreConfig, connection::libsql::open_conn_active_checkpoint, error::Error, Result,
};

use super::NamespaceName;

type ChangeMsg = (NamespaceName, Arc<DatabaseConfig>);
type WalManager = WalWrapper<Option<BottomlessWalWrapper>, Sqlite3WalManager>;
type Connection = libsql_sys::Connection<WrappedWal<Option<BottomlessWalWrapper>, Sqlite3Wal>>;

pub struct MetaStore {
    changes_tx: mpsc::Sender<ChangeMsg>,
    inner: Arc<Mutex<MetaStoreInner>>,
}

#[derive(Clone, Debug)]
pub struct MetaStoreHandle {
    namespace: NamespaceName,
    inner: HandleState,
}

#[derive(Debug, Clone)]
enum HandleState {
    Internal(Arc<Mutex<Arc<DatabaseConfig>>>),
    External(mpsc::Sender<ChangeMsg>, Receiver<InnerConfig>),
}

#[derive(Debug, Default, Clone)]
struct InnerConfig {
    /// Version of this config _per_ each running process of sqld, this means
    /// that this version is not stored between restarts and is only used to track
    /// config changes during the lifetime of the sqld process.
    version: usize,
    config: Arc<DatabaseConfig>,
}

struct MetaStoreInner {
    // TODO(lucio): Use a concurrent hashmap so we don't block connection creation
    // when we are updating the config. The config si already synced via the watch
    // channel.
    configs: HashMap<NamespaceName, Sender<InnerConfig>>,
    conn: Connection,
    wal_manager: WalManager,
}

/// Handles config change updates by inserting them into the database and in-memory
/// cache of configs.
fn process(msg: ChangeMsg, inner: Arc<Mutex<MetaStoreInner>>) -> Result<()> {
    let (namespace, config) = msg;

    let config_encoded = metadata::DatabaseConfig::from(&*config).encode_to_vec();

    let inner = &mut inner.lock();

    inner.conn.execute(
        "INSERT OR REPLACE INTO namespace_configs (namespace, config) VALUES (?1, ?2)",
        rusqlite::params![namespace.to_string(), config_encoded],
    )?;

    let configs = &mut inner.configs;

    if let Some(config_watch) = configs.get_mut(&namespace) {
        let new_version = config_watch.borrow().version.wrapping_add(1);

        config_watch.send_modify(|c| {
            *c = InnerConfig {
                version: new_version,
                config,
            };
        });
    } else {
        let (tx, _) = watch::channel(InnerConfig { version: 0, config });
        configs.insert(namespace, tx);
    }

    Ok(())
}

#[tracing::instrument(skip(db))]
fn restore(db: &Connection) -> Result<HashMap<NamespaceName, Sender<InnerConfig>>> {
    tracing::info!("restoring meta store");

    db.execute(
        "CREATE TABLE IF NOT EXISTS namespace_configs (
            namespace TEXT NOT NULL PRIMARY KEY,
            config BLOB NOT NULL
        )
        ",
        (),
    )?;

    let mut stmt = db.prepare("SELECT namespace, config FROM namespace_configs")?;

    let rows = stmt.query(())?.mapped(|r| {
        let ns = r.get::<_, String>(0)?;
        let config = r.get::<_, Vec<u8>>(1)?;

        Ok((ns, config))
    });

    let mut configs = HashMap::new();

    for row in rows {
        match row {
            Ok((k, v)) => {
                let ns = match NamespaceName::from_string(k) {
                    Ok(ns) => ns,
                    Err(e) => {
                        tracing::warn!("unable to convert namespace name: {}", e);
                        continue;
                    }
                };

                let config = match metadata::DatabaseConfig::decode(&v[..]) {
                    Ok(c) => Arc::new(DatabaseConfig::from(&c)),
                    Err(e) => {
                        tracing::warn!("unable to convert config: {}", e);
                        continue;
                    }
                };

                // We don't store the version in the sqlitedb due to the session token
                // changed each time we start the primary, this will cause the replica to
                // handshake again and get the latest config.
                let (tx, _) = watch::channel(InnerConfig { version: 0, config });

                configs.insert(ns, tx);
            }

            Err(e) => {
                tracing::error!("meta store restore failed: {}", e);
                return Err(Error::from(e));
            }
        }
    }

    tracing::info!("meta store restore completed");

    Ok(configs)
}

impl MetaStore {
    #[tracing::instrument(skip(config, base_path))]
    pub async fn new(config: Option<MetaStoreConfig>, base_path: &Path) -> Result<Self> {
        let db_path = base_path.join("metastore");
        tokio::fs::create_dir_all(&db_path).await?;
        let replicator = match config {
            Some(config) => {
                let options = bottomless::replicator::Options {
                    create_bucket_if_not_exists: true,
                    verify_crc: true,
                    use_compression: CompressionKind::None,
                    aws_endpoint: Some(config.bucket_endpoint),
                    access_key_id: Some(config.access_key_id),
                    secret_access_key: Some(config.secret_access_key),
                    region: Some(config.region),
                    db_id: Some(config.backup_id),
                    bucket_name: config.bucket_name,
                    max_frames_per_batch: 10_000,
                    max_batch_interval: config.backup_interval,
                    s3_upload_max_parallelism: 32,
                    restore_transaction_page_swap_after: 1000,
                    restore_transaction_cache_fpath: ".bottomless.restore".into(),
                    s3_max_retries: 10,
                };
                let mut replicator = bottomless::replicator::Replicator::with_options(
                    db_path.join("data").to_str().unwrap(),
                    options,
                )
                .await?;
                let (action, _did_recover) = replicator.restore(None, None).await?;
                // TODO: this logic should probably be moved to bottomless.
                match action {
                    bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
                        replicator.new_generation();
                        if let Some(_handle) = replicator.snapshot_main_db_file().await? {
                            tracing::trace!(
                                "got snapshot handle after restore with generation upgrade"
                            );
                        }
                        // Restoration process only leaves the local WAL file if it was
                        // detected to be newer than its remote counterpart.
                        replicator.maybe_replicate_wal().await?
                    }
                    bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
                        replicator.set_generation(gen);
                    }
                }

                Some(replicator)
            }
            None => None,
        };

        let wal_manager = WalWrapper::new(
            replicator.map(BottomlessWalWrapper::new),
            Sqlite3WalManager::default(),
        );
        let conn = open_conn_active_checkpoint(&db_path, wal_manager.clone(), None, 1000, None)?;

        let configs = restore(&conn)?;

        let (changes_tx, mut changes_rx) = mpsc::channel(256);

        let inner = Arc::new(Mutex::new(MetaStoreInner {
            configs,
            conn,
            wal_manager,
        }));

        tokio::spawn({
            let inner = inner.clone();
            async move {
                while let Some(msg) = changes_rx.recv().await {
                    let inner = inner.clone();
                    let jh = tokio::task::spawn_blocking(move || process(msg, inner));

                    if let Err(e) = jh.await {
                        tracing::error!("error processing metastore update: {}", e);
                    }
                }
            }
        });

        Ok(Self { changes_tx, inner })
    }

    pub fn handle(&self, namespace: NamespaceName) -> MetaStoreHandle {
        tracing::debug!("getting meta store handle");
        let change_tx = self.changes_tx.clone();

        let lock = &mut self.inner.lock().configs;
        let sender = lock.entry(namespace.clone()).or_insert_with(|| {
            // TODO(lucio): if no entry exists we need to ensure we send the update to
            // the bg channel.
            let (tx, _) = watch::channel(InnerConfig::default());
            tx
        });

        let rx = sender.subscribe();

        tracing::debug!("meta handle subscribed");

        MetaStoreHandle {
            namespace,
            inner: HandleState::External(change_tx, rx),
        }
    }

    pub fn remove(&self, namespace: NamespaceName) -> Result<Option<Arc<DatabaseConfig>>> {
        tracing::debug!("removing namespace `{}` from meta store", namespace);

        let mut guard = self.inner.lock();
        guard.conn.execute(
            "DELETE FROM namespace_configs WHERE namespace = ?",
            [namespace.as_str()],
        )?;
        if let Some(sender) = guard.configs.remove(&namespace) {
            tracing::debug!("removed namespace `{}` from meta store", namespace);
            let config = sender.borrow().clone();
            Ok(Some(config.config))
        } else {
            tracing::trace!("namespace `{}` not found in meta store", namespace);
            Ok(None)
        }
    }

    // TODO: we need to either make sure that the metastore is restored
    // before we start accepting connections or we need to contact bottomless
    // here to check if a namespace exists. Preferably the former.
    pub fn exists(&self, namespace: &NamespaceName) -> bool {
        self.inner.lock().configs.contains_key(namespace)
    }

    pub(crate) async fn shutdown(&self) -> crate::Result<()> {
        let replicator = self
            .inner
            .lock()
            .wal_manager
            .wrapper()
            .as_ref()
            .and_then(|b| b.shutdown());

        if let Some(mut replicator) = replicator {
            tracing::info!("Started meta store backup");
            replicator.shutdown_gracefully().await?;
            tracing::info!("meta store backed up");
        }

        Ok(())
    }
}

impl MetaStoreHandle {
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::internal()
    }

    #[cfg(test)]
    pub fn load(db_path: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        use std::{fs, io};

        let config_path = db_path.as_ref().join("config.json");

        let config = match fs::read(config_path) {
            Ok(data) => {
                let c = metadata::DatabaseConfig::decode(&data[..])?;
                DatabaseConfig::from(&c)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => DatabaseConfig::default(),
            Err(err) => return Err(Error::IOError(err)),
        };

        Ok(Self {
            namespace: NamespaceName("testmetastore".into()),
            inner: HandleState::Internal(Arc::new(Mutex::new(Arc::new(config)))),
        })
    }

    pub fn internal() -> Self {
        MetaStoreHandle {
            namespace: NamespaceName("testmetastore".into()),
            inner: HandleState::Internal(Arc::new(Mutex::new(Arc::new(DatabaseConfig::default())))),
        }
    }

    pub fn get(&self) -> Arc<DatabaseConfig> {
        match &self.inner {
            HandleState::Internal(config) => config.lock().clone(),
            HandleState::External(_, config) => config.borrow().clone().config,
        }
    }

    pub fn version(&self) -> usize {
        match &self.inner {
            HandleState::Internal(_) => 0,
            HandleState::External(_, config) => config.borrow().version,
        }
    }

    pub async fn store(&self, new_config: impl Into<Arc<DatabaseConfig>>) -> Result<()> {
        match &self.inner {
            HandleState::Internal(config) => {
                *config.lock() = new_config.into();
            }
            HandleState::External(changes_tx, config) => {
                let new_config = new_config.into();
                tracing::debug!(?new_config, "storing new namespace config");
                let mut c = config.clone();
                let changed = c.changed();

                changes_tx
                    .send((self.namespace.clone(), new_config))
                    .await
                    .map_err(|e| Error::MetaStoreUpdateFailure(e.into()))?;

                changed
                    .await
                    .map_err(|e| Error::MetaStoreUpdateFailure(e.into()))?;

                tracing::debug!("done storing new namespace config");
            }
        };

        Ok(())
    }
}
