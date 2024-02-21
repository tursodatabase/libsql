#![allow(clippy::mutable_key_type)]
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, fs::read_dir};

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::CompressionKind;
use libsql_replication::rpc::metadata;
use libsql_sys::wal::{
    wrapper::{WalWrapper, WrappedWal},
    Sqlite3Wal, Sqlite3WalManager,
};
use parking_lot::Mutex;
use prost::Message;
use rusqlite::params;
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};

use crate::connection::config::DatabaseConfig;
use crate::namespace::multi_db::{LastUpdateStatus, MultiDbUpdate, UpdateStatus};
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

impl MetaStoreInner {
    async fn new(base_path: &Path, mut config: MetaStoreConfig) -> Result<Self> {
        let db_path = base_path.join("metastore");
        tokio::fs::create_dir_all(&db_path).await?;
        let replicator = match config.bottomless.take() {
            Some(config) => {
                let options = bottomless::replicator::Options {
                    create_bucket_if_not_exists: true,
                    verify_crc: true,
                    use_compression: CompressionKind::None,
                    encryption_config: None,
                    aws_endpoint: Some(config.bucket_endpoint),
                    access_key_id: Some(config.access_key_id),
                    secret_access_key: Some(config.secret_access_key),
                    region: Some(config.region),
                    db_id: Some(config.backup_id),
                    bucket_name: config.bucket_name,
                    max_frames_per_batch: 10_000,
                    max_batch_interval: config.backup_interval,
                    s3_upload_max_parallelism: 32,
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
            replicator.map(|b| BottomlessWalWrapper::new(Arc::new(std::sync::Mutex::new(Some(b))))),
            Sqlite3WalManager::default(),
        );
        let conn = open_conn_active_checkpoint(&db_path, wal_manager.clone(), None, 1000, None)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS multi_db_update(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shared_schema TEXT NOT NULL,
                script TEXT NOT NULL)",
            (),
        )?;
        conn.execute(
            "CRATE TABLE IF NOT EXISTS multi_db_update_progress(
                id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                status INTEGER,
                err_msg TEXT,
                PRIMARY KEY(id, namespace),
                FOREIGN KEY(id) REFERENCES libsql_jobs(id))
            ",
            (),
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS namespace_configs (
                namespace TEXT NOT NULL PRIMARY KEY,
                config BLOB NOT NULL
            )
            ",
            (),
        )?;

        let mut this = MetaStoreInner {
            configs: Default::default(),
            conn,
            wal_manager,
        };

        if config.allow_recover_from_fs {
            this.maybe_recover_from_fs(base_path)?;
        }

        this.restore()?;

        Ok(this)
    }

    fn maybe_recover_from_fs(&mut self, base_path: &Path) -> Result<()> {
        let count = self
            .conn
            .query_row("SELECT count(*) FROM namespace_configs", (), |row| {
                row.get::<_, u64>(0)
            })?;

        let txn = self.conn.transaction()?;
        // nothing in the meta store, check fs
        let dbs_dir_path = base_path.join("dbs");
        if count == 0 && dbs_dir_path.try_exists()? {
            tracing::info!("Recovering metastore from filesystem...");
            let db_dir = read_dir(&dbs_dir_path)?;
            for entry in db_dir {
                let entry = entry?;
                if !entry.path().is_dir() {
                    continue;
                }
                let config_path = entry.path().join("config.json");
                let name =
                    NamespaceName::from_string(entry.file_name().to_str().unwrap().to_string())?;
                let config = if config_path.try_exists()? {
                    let config_bytes = std::fs::read(&config_path)?;
                    serde_json::from_slice(&config_bytes)?
                } else {
                    DatabaseConfig::default()
                };
                let config_encoded = metadata::DatabaseConfig::from(&config).encode_to_vec();
                tracing::info!("Recovered namespace config: `{name}`");
                txn.execute(
                    "INSERT INTO namespace_configs VALUES (?1, ?2)",
                    (name.as_str(), &config_encoded),
                )?;
            }
        }

        txn.commit()?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn restore(&mut self) -> Result<()> {
        tracing::info!("restoring meta store");

        let mut stmt = self
            .conn
            .prepare("SELECT namespace, config FROM namespace_configs")?;

        let rows = stmt.query(())?.mapped(|r| {
            let ns = r.get::<_, String>(0)?;
            let config = r.get::<_, Vec<u8>>(1)?;

            Ok((ns, config))
        });

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

                    self.configs.insert(ns, tx);
                }

                Err(e) => {
                    tracing::error!("meta store restore failed: {}", e);
                    return Err(Error::from(e));
                }
            }
        }

        tracing::info!("meta store restore completed");

        Ok(())
    }
}

/// Handles config change updates by inserting them into the database and in-memory
/// cache of configs.
fn process(msg: ChangeMsg, inner: Arc<Mutex<MetaStoreInner>>) -> Result<()> {
    let (namespace, config) = msg;

    let config_encoded = metadata::DatabaseConfig::from(&*config).encode_to_vec();

    let inner = &mut inner.lock();

    inner.conn.execute(
        "INSERT OR REPLACE INTO namespace_configs (namespace, config) VALUES (?1, ?2)",
        rusqlite::params![namespace.as_str(), config_encoded],
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

impl MetaStore {
    #[tracing::instrument(skip(config, base_path))]
    pub async fn new(config: MetaStoreConfig, base_path: &Path) -> Result<Self> {
        let (changes_tx, mut changes_rx) = mpsc::channel(256);
        let inner = Arc::new(Mutex::new(MetaStoreInner::new(base_path, config).await?));

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

impl MultiDbUpdate {
    pub fn restore_last(
        shared_schema: &NamespaceName,
        meta_store: &mut MetaStore,
    ) -> Result<Option<Self>> {
        let id = {
            let mut inner = meta_store.inner.lock();

            let tx = inner.conn.transaction()?;
            let last_job = Self::last_update_status(&tx, shared_schema.as_str())?;
            match last_job {
                LastUpdateStatus::NeedsRetry(id) => Some(id),
                LastUpdateStatus::None | LastUpdateStatus::Completed(_) => None,
            }
        };
        match id {
            None => Ok(None),
            Some(id) => Self::restore(id, shared_schema, meta_store).map(Some),
        }
    }

    pub fn restore(
        id: i64,
        shared_schema: &NamespaceName,
        meta_store: &mut MetaStore,
    ) -> Result<Self> {
        let mut inner = meta_store.inner.lock();

        let tx = inner.conn.transaction()?;
        let sql = tx.query_row(
            "SELECT script FROM multi_db_update WHERE id = ?",
            params![id],
            |row| row.get(0),
        )?;
        let mut stmt = tx.prepare(
            r#"SELECT namespace FROM multi_db_update_progress WHERE status != 0 AND id = ?"#,
        )?;
        let mut rows = stmt.query(params![&id])?;
        let mut namespaces = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            let ns = NamespaceName::from_string(row.get(0)?)?;
            if &ns == shared_schema {
                // shared schema db should be evaluated first
                namespaces.insert(0, (ns, UpdateStatus::Pending));
            } else {
                namespaces.push((ns, UpdateStatus::Pending));
            }
        }

        Ok(MultiDbUpdate::new(id, sql, namespaces))
    }

    /// Prepare new multi-db update.
    ///
    /// # Errors
    ///
    /// If there's already an unfinished SQL script waiting to be executed, this method will
    /// return [crate::Error::SharedSchemaRetryRequired] with ID of an update that should be
    /// retried first. In that case use [MultiDbUpdate::retry] instead of prepare.
    pub fn prepare(
        sql: String,
        shared_schema: &NamespaceName,
        meta_store: &mut MetaStore,
    ) -> Result<Self> {
        let mut inner = meta_store.inner.lock();

        let tx = inner.conn.transaction()?;
        let last_job = Self::last_update_status(&tx, shared_schema.as_str())?;
        match last_job {
            LastUpdateStatus::NeedsRetry(id) => {
                // another job was in progress and not finished yet
                return Err(crate::Error::SharedSchemaRetryRequired(id));
            }
            LastUpdateStatus::None | LastUpdateStatus::Completed(_) => {
                //TODO: what if previous script is equal to current one?
            }
        }

        tx.execute("INSERT INTO multi_db_update(script) VALUES (?)", [&sql])?;
        let id = tx.last_insert_rowid();
        // insert shared schema db
        tx.execute(
            r#"INSERT INTO multi_db_update_progress(id, namespace) VALUES(?,?)"#,
            params![&id, shared_schema.as_str()],
        )?;
        // insert databases linking shared schema db
        tx.execute(
            r#"INSERT INTO multi_db_update_progress(id, namespace) 
            SELECT ? as id, namespace shared_schema_links"#,
            [&id],
        )?;
        let mut namespaces = Vec::new();
        {
            let mut stmt = tx.prepare(
                r#"SELECT namespace FROM multi_db_update_progress WHERE status != 0 AND id = ?"#,
            )?;
            let mut rows = stmt.query(params![&id])?;
            while let Ok(Some(row)) = rows.next() {
                let ns = NamespaceName::from_string(row.get(0)?)?;
                if &ns == shared_schema {
                    // shared schema db should be evaluated first
                    namespaces.insert(0, (ns, UpdateStatus::Pending));
                } else {
                    namespaces.push((ns, UpdateStatus::Pending));
                }
            }
        }
        tx.commit()?;

        Ok(MultiDbUpdate::new(id, sql, namespaces))
    }
}
