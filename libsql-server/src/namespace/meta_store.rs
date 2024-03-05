#![allow(clippy::mutable_key_type)]
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, fs::read_dir};

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::CompressionKind;
use futures_core::Future;
use libsql_replication::rpc::metadata;
use libsql_sys::wal::{
    wrapper::{WalWrapper, WrappedWal},
    Sqlite3Wal, Sqlite3WalManager,
};
use parking_lot::Mutex;
use prost::Message;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};

use crate::config::BottomlessConfig;
use crate::connection::config::DatabaseConfig;
use crate::{
    config::MetaStoreConfig, connection::libsql::open_conn_active_checkpoint, error::Error, Result,
};

use super::NamespaceName;

type ChangeMsg = (
    NamespaceName,
    Arc<DatabaseConfig>,
    oneshot::Sender<Result<()>>,
);
type MetaStoreWalManager = WalWrapper<Option<BottomlessWalWrapper>, Sqlite3WalManager>;
pub type MetaStoreConnection =
    libsql_sys::Connection<WrappedWal<Option<BottomlessWalWrapper>, Sqlite3Wal>>;

#[derive(Clone)]
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
    conn: MetaStoreConnection,
    wal_manager: MetaStoreWalManager,
}

fn setup_connection(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute("PRAGMA foreign_keys=ON", ())?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS namespace_configs (
            namespace TEXT NOT NULL PRIMARY KEY,
            config BLOB NOT NULL
        )
        ",
        (),
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS shared_schema_links (
            shared_schema_name TEXT NOT NULL,
            namespace TEXT NOT NULL,
            PRIMARY KEY (shared_schema_name, namespace),
            FOREIGN KEY (shared_schema_name) REFERENCES namespace_configs (namespace) ON DELETE RESTRICT ON UPDATE RESTRICT,
            FOREIGN KEY (namespace) REFERENCES namespace_configs (namespace) ON DELETE RESTRICT ON UPDATE RESTRICT
        )
        ",
        (),
    )?;

    Ok(())
}

pub async fn metastore_connection_maker(
    config: Option<BottomlessConfig>,
    base_path: &Path,
) -> crate::Result<(
    impl Fn() -> crate::Result<MetaStoreConnection>,
    MetaStoreWalManager,
)> {
    let db_path = base_path.join("metastore");
    tokio::fs::create_dir_all(&db_path).await?;
    let replicator = match config {
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
                    replicator.new_generation().await;
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

    let maker = {
        let wal_manager = wal_manager.clone();
        move || {
            let conn =
                open_conn_active_checkpoint(&db_path, wal_manager.clone(), None, 1000, None)?;
            Ok(conn)
        }
    };

    Ok((maker, wal_manager))
}

impl MetaStoreInner {
    async fn new(
        base_path: &Path,
        conn: MetaStoreConnection,
        wal_manager: MetaStoreWalManager,
        config: MetaStoreConfig,
    ) -> Result<Self> {
        setup_connection(&conn)?;
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

    /// Create a migration task, and returns the jobn id
    fn register_schema_migration_task(
        &mut self,
        schema: &NamespaceName,
        migration: &Program,
    ) -> Result<i64> {
        let txn = self.conn.transaction()?;

        // get the config for the schema and validate that it's actually a schema
        let mut stmt =
            txn.prepare("SELECT namespace, config FROM namespace_configs where namespace = ?")?;
        let mut rows = stmt.query([schema.as_str()])?;
        let Some(row) = rows.next()? else {
            todo!("no such schema")
        };
        let config_bytes = row.get_ref(1)?.as_blob().unwrap();
        let config = DatabaseConfig::from(&metadata::DatabaseConfig::decode(config_bytes)?);
        if !config.is_shared_schema {
            todo!("not a shared schema table");
        }

        drop(rows);

        stmt.finalize()?;

        let migration_serialized = serde_json::to_string(&migration).unwrap();
        txn.execute(
            "INSERT INTO migration_jobs (schema, migration, status) VALUES (?, ?, ?)",
            (
                schema.as_str(),
                &migration_serialized,
                MigrationJobStatus::WaitingDryRun as u64,
            ),
        )?;
        let job_id = txn.last_insert_rowid();

        txn.execute(
            "
            INSERT INTO
                migration_job_pending_tasks (job_id, target_namespace, status)
            SELECT job_id, namespace, status
                FROM shared_schema_links 
                CROSS JOIN (SELECT ? as job_id, ? as status)
            WHERE shared_schema_name = ?",
            (
                job_id,
                MigrationTaskStatus::Enqueued as u64,
                schema.as_ref(),
            ),
        )?;

        txn.commit()?;

        Ok(job_id)
    }

    /// returns a batch of tasks for job_id that are in the passed status
    fn get_next_pending_migration_tasks_batch(
        &mut self,
        job_id: i64,
        status: MigrationTaskStatus,
        limit: usize,
    ) -> crate::Result<Vec<MigrationTask>> {
        let txn = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let tasks = txn
            .prepare(
                "SELECT task_id, target_namespace, status, job_id 
                FROM migration_job_pending_tasks 
                WHERE job_id = ? AND status = ? AND task_id NOT IN (select * from enqueued_tasks)
                LIMIT ?",
            )?
            .query_map((job_id, status as u64, limit), |row| {
                let task_id = row.get::<_, i64>(0)?;
                let namespace = NamespaceName::from_string(row.get::<_, String>(1)?).unwrap();
                let status = MigrationTaskStatus::from_int(row.get::<_, u64>(2)?);
                let job_id = row.get::<_, i64>(3)?;
                Ok(MigrationTask {
                    namespace,
                    status,
                    job_id,
                    task_id,
                })
            })?
            .map(|r| r.map_err(Into::into))
            .collect::<crate::Result<Vec<_>>>()?;

        for task in tasks.iter() {
            txn.execute("INSERT INTO enqueued_tasks VALUES (?)", [task.task_id])?;
        }

        txn.commit()?;
        Ok(tasks)
    }

    fn update_task_status(
        &mut self,
        task: MigrationTask,
        error: Option<String>,
    ) -> crate::Result<()> {
        assert!(error.is_none() || task.status.is_failure());
        let txn = self.conn.transaction()?;
        txn.execute(
            "UPDATE migration_job_pending_tasks SET status = ?, error = ? WHERE task_id = ?",
            (task.status as u64, error, task.task_id),
        )?;
        txn.commit()?;
        Ok(())
    }

    /// Attempt to set the job to DryRunSuccess.
    /// Checks that:
    /// - current state is WaitinForDryRun
    /// - all tasks are DryRunSuccess
    fn job_step_dry_run_success(&self, mut job: MigrationJob) -> crate::Result<MigrationJob> {
        let row_changed = self.conn.execute("
            WITH tasks AS (SELECT * FROM migration_job_pending_tasks WHERE job_id = ?1)
                UPDATE migration_jobs 
                SET status = ?2
                WHERE job_id = ?1
                    AND status = ?3
                    AND (SELECT count(*) from tasks) = (SELECT count(*) FROM tasks WHERE status = ?4)",
            (
                job.job_id,
                MigrationJobStatus::DryRunSuccess as u64,
                MigrationJobStatus::WaitingDryRun as u64,
                MigrationTaskStatus::DryRunSuccess as u64))?;

        if row_changed == 0 {
            return Ok(job);
        }

        job.status = MigrationJobStatus::DryRunSuccess;

        Ok(job)
    }

    fn update_job_status(&self, job_id: i64, status: MigrationJobStatus) -> crate::Result<()> {
        self.conn.execute(
            "UPDATE migration_jobs SET status = ? WHERE job_id = ?",
            (status as u64, job_id),
        )?;
        Ok(())
    }

    fn get_migration_details(
        &mut self,
        schema: NamespaceName,
        job_id: u64,
    ) -> crate::Result<MigrationDetails> {
        let status = self.conn.query_row(
            "SELECT status
            FROM migration_jobs
            WHERE schema = ? AND job_id = ?",
            params![schema.as_str(), job_id],
            |r| {
                let status: Option<u64> = r.get(0)?;
                Ok(status.map(MigrationJobStatus::from_int))
            },
        )?;
        let mut stmt = self.conn.prepare(
            "SELECT target_namespace, status, error
            FROM migration_job_pending_tasks
            WHERE job_id = ?",
        )?;
        let rows = stmt.query([job_id])?.mapped(|r| {
            let target_namespace = r.get(0)?;
            let status: Option<u64> = r.get(1)?;
            let error: Option<String> = r.get(2)?;
            Ok(MigrationJobProgress {
                namespace: target_namespace,
                status: status.map(MigrationJobStatus::from_int),
                error,
            })
        });
        let mut progress = Vec::new();
        for row in rows {
            progress.push(row?);
        }
        Ok(MigrationDetails {
            job_id,
            status,
            progress,
        })
    }

    fn get_migrations_summary(&mut self, schema: NamespaceName) -> crate::Result<MigrationSummary> {
        let schema_version: i64 = self
            .conn
            .query_row("PRAGMA schema_version;", (), |r| r.get(0))?;
        let mut stmt = self.conn.prepare(
            "SELECT job_id, status
            FROM migration_jobs
            WHERE schema = ?
            ORDER BY job_id DESC",
        )?;
        let rows = stmt.query([schema.as_str()])?.mapped(|r| {
            let status: Option<u64> = r.get(1)?;
            Ok(MigrationJobSummary {
                job_id: r.get(0)?,
                status: status.map(MigrationJobStatus::from_int),
            })
        });
        let mut migrations = Vec::new();
        for row in rows {
            migrations.push(row?);
        }
        Ok(MigrationSummary {
            schema_version,
            migrations,
        })
    }

    fn get_next_pending_migration_job(&mut self) -> crate::Result<Option<MigrationJob>> {
        let mut job = self
            .conn
            .query_row(
                "SELECT job_id, status, migration, schema
            FROM migration_jobs
            WHERE status != ? AND status != ?
            LIMIT 1",
                (
                    MigrationJobStatus::RunSuccess as u64,
                    MigrationJobStatus::RunFailure as u64,
                ),
                |row| {
                    let job_id = row.get::<_, i64>(0)?;
                    let status = MigrationJobStatus::from_int(row.get::<_, u64>(1)?);
                    let migration = serde_json::from_str(row.get_ref(2)?.as_str()?).unwrap();
                    let schema = NamespaceName::from_string(row.get::<_, String>(3)?).unwrap();
                    Ok(MigrationJob {
                        schema,
                        job_id,
                        status,
                        migration,
                        progress: Default::default(),
                    })
                },
            )
            .optional()?;

        if let Some(ref mut job) = job {
            self.conn
                .prepare(
                    "
                SELECT status, count(*)
                FROM migration_job_pending_tasks 
                WHERE job_id = ?
                GROUP BY status",
                )?
                .query_map([job.job_id], |row| {
                    job.progress[row.get::<_, usize>(0)?] = row.get::<_, usize>(1)?;
                    Ok(())
                })?
                .collect::<Result<(), rusqlite::Error>>()?;
        }

        Ok(job)
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
fn process(msg: ChangeMsg, inner: Arc<Mutex<MetaStoreInner>>) {
    let (namespace, config, ret_chan) = msg;

    let mut inner = inner.lock();
    let ret = try_process(&mut *inner, &namespace, &config);

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

    let _ = ret_chan.send(ret);
}

fn try_process(
    inner: &mut MetaStoreInner,
    namespace: &NamespaceName,
    config: &DatabaseConfig,
) -> Result<()> {
    let config_encoded = metadata::DatabaseConfig::from(&*config).encode_to_vec();

    if let Some(schema) = config.shared_schema_name.as_ref() {
        let tx = inner.conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO namespace_configs (namespace, config) VALUES (?1, ?2)",
            rusqlite::params![namespace.as_str(), config_encoded],
        )?;
        tx.execute(
            "DELETE FROM shared_schema_links WHERE namespace = ?",
            rusqlite::params![namespace.as_str()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO shared_schema_links (shared_schema_name, namespace) VALUES (?1, ?2)",
            rusqlite::params![schema.as_str(), namespace.as_str()],
        )?;
        tx.commit()?;
    } else {
        inner.conn.execute(
            "INSERT OR REPLACE INTO namespace_configs (namespace, config) VALUES (?1, ?2)",
            rusqlite::params![namespace.as_str(), config_encoded],
        )?;
    }

    Ok(())
}

impl MetaStore {
    #[tracing::instrument(skip(config, base_path, conn, wal_manager))]
    pub async fn new(
        config: MetaStoreConfig,
        base_path: &Path,
        conn: MetaStoreConnection,
        wal_manager: MetaStoreWalManager,
    ) -> Result<Self> {
        let (changes_tx, mut changes_rx) = mpsc::channel(256);
        let inner = Arc::new(Mutex::new(
            MetaStoreInner::new(base_path, conn, wal_manager, config).await?,
        ));

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
        let r = if let Some(sender) = guard.configs.get(&namespace) {
            tracing::debug!("removed namespace `{}` from meta store", namespace);
            let config = sender.borrow().clone();
            let tx = guard.conn.transaction()?;
            if let Some(ref shared_schema) = config.config.shared_schema_name {
                tx.execute(
                    "DELETE FROM shared_schema_links WHERE shared_schema_name = ? AND namespace = ?",
                    (shared_schema.as_str(), namespace.as_str()),
                )?;
            }
            tx.execute(
                "DELETE FROM namespace_configs WHERE namespace = ?",
                [namespace.as_str()],
            )?;
            tx.commit()?;
            Ok(Some(config.config))
        } else {
            tracing::trace!("namespace `{}` not found in meta store", namespace);
            Ok(None)
        };
        guard.configs.remove(&namespace);
        r
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

    pub async fn get_migrations_summary(
        &self,
        schema: NamespaceName,
    ) -> crate::Result<MigrationSummary> {
        let inner = self.inner.clone();
        let summary =
            tokio::task::spawn_blocking(move || inner.lock().get_migrations_summary(schema))
                .await
                .unwrap()?;
        Ok(summary)
    }

    pub async fn get_migration_details(
        &self,
        schema: NamespaceName,
        job_id: u64,
    ) -> crate::Result<MigrationDetails> {
        let inner = self.inner.clone();
        let summary =
            tokio::task::spawn_blocking(move || inner.lock().get_migration_details(schema, job_id))
                .await
                .unwrap()?;
        Ok(summary)
    }

    pub async fn register_schema_migration(
        &self,
        schema: NamespaceName,
        migration: Arc<Program>,
    ) -> crate::Result<i64> {
        let inner = self.inner.clone();
        let job_id = tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .register_schema_migration_task(&schema, &migration)
        })
        .await
        .unwrap()?;

        Ok(job_id)
    }

    pub(crate) async fn job_step_dry_run_success(
        &self,
        job: MigrationJob,
    ) -> crate::Result<MigrationJob> {
        let inner = self.inner.clone();

        let job = tokio::task::spawn_blocking(move || inner.lock().job_step_dry_run_success(job))
            .await
            .unwrap()?;

        Ok(job)
    }

    pub(crate) async fn update_task_status(
        &self,
        task: MigrationTask,
        error: Option<String>,
    ) -> crate::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.lock().update_task_status(task, error))
            .await
            .unwrap()?;

        Ok(())
    }

    pub(crate) async fn get_next_pending_job(&self) -> crate::Result<Option<MigrationJob>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.lock().get_next_pending_migration_job())
            .await
            .unwrap()
    }

    pub(crate) async fn get_next_pending_migration_tasks_batch(
        &self,
        job_id: i64,
        status: MigrationTaskStatus,
        limit: usize,
    ) -> crate::Result<Vec<MigrationTask>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .get_next_pending_migration_tasks_batch(job_id, status, limit)
        })
        .await
        .unwrap()
    }

    pub(crate) async fn update_job_status(
        &self,
        job_id: i64,
        status: MigrationJobStatus,
    ) -> crate::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.lock().update_job_status(job_id, status))
            .await
            .unwrap()
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
            namespace: NamespaceName::new_unchecked("testmetastore"),
            inner: HandleState::Internal(Arc::new(Mutex::new(Arc::new(config)))),
        })
    }

    pub fn internal() -> Self {
        MetaStoreHandle {
            namespace: NamespaceName::new_unchecked("testmetastore"),
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

    pub fn changed(&self) -> impl Future<Output = ()> {
        let mut rcv = match &self.inner {
            HandleState::Internal(_) => panic!("can't wait for change on internal handle"),
            HandleState::External(_, rcv) => rcv.clone(),
        };
        // ack the current value.
        rcv.borrow_and_update();
        async move {
            let _ = rcv.changed().await;
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
                // ack the current value.
                c.borrow_and_update();
                let changed = c.changed();

                let (snd, rcv) = oneshot::channel();
                changes_tx
                    .send((self.namespace.clone(), new_config, snd))
                    .await
                    .map_err(|e| Error::MetaStoreUpdateFailure(e.into()))?;

                rcv.await??;
                changed
                    .await
                    .map_err(|e| Error::MetaStoreUpdateFailure(e.into()))?;

                tracing::debug!("done storing new namespace config");
            }
        };

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationSummary {
    pub schema_version: i64,
    pub migrations: Vec<MigrationJobSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationJobSummary {
    pub job_id: u64,
    pub status: Option<MigrationJobStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationDetails {
    pub job_id: u64,
    pub status: Option<MigrationJobStatus>,
    pub progress: Vec<MigrationJobProgress>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationJobProgress {
    pub namespace: String,
    pub status: Option<MigrationJobStatus>,
    pub error: Option<String>,
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use tempfile::tempdir;

    use super::*;

    async fn register_schema(meta_store: &MetaStore, schema: &'static str) {
        meta_store
            .handle(schema.into())
            .store(DatabaseConfig {
                is_shared_schema: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    async fn register_shared(meta_store: &MetaStore, name: &'static str, schema: &'static str) {
        meta_store
            .handle(name.into())
            .store(DatabaseConfig {
                shared_schema_name: Some(schema.into()),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn enqueue_migration_job() {
        let tmp = tempdir().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path())
            .await
            .unwrap();
        // create 2 shared schema tables
        register_schema(&meta_store, "schema1").await;
        register_schema(&meta_store, "schema2").await;

        // create namespaces
        register_shared(&meta_store, "ns1", "schema1").await;
        register_shared(&meta_store, "ns2", "schema2").await;
        register_shared(&meta_store, "ns3", "schema1").await;

        let mut lock = meta_store.inner.lock();
        // create a migration task
        lock.register_schema_migration_task(
            &"schema1".into(),
            &Program::seq(&["select * from test"]),
        )
        .unwrap();
        let mut stmt = lock.conn.prepare("select * from migration_jobs").unwrap();
        assert_debug_snapshot!(stmt.query(()).unwrap().next().unwrap().unwrap());
        stmt.finalize().unwrap();

        let mut stmt = lock
            .conn
            .prepare("select * from migration_job_pending_tasks")
            .unwrap();
        let mut rows = stmt.query(()).unwrap();
        assert_debug_snapshot!(rows.next().unwrap().unwrap());
        assert_debug_snapshot!(rows.next().unwrap().unwrap());
        assert!(rows.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn schema_doesnt_exist() {
        let tmp = tempdir().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path())
            .await
            .unwrap();
        // FIXME: the actual error reported here is a shitty constraint error, we should make the
        // necessary checks beforehand, and return a nice error message.
        assert!(meta_store
            .handle("ns1".into())
            .store(DatabaseConfig {
                shared_schema_name: Some("schema1".into()),
                ..Default::default()
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pending_job() {
        let tmp = tempdir().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path())
            .await
            .unwrap();

        register_schema(&meta_store, "schema1").await;
        register_shared(&meta_store, "ns1", "schema1").await;
        register_shared(&meta_store, "ns2", "schema1").await;
        register_shared(&meta_store, "ns3", "schema1").await;

        let job_id = meta_store
            .register_schema_migration(
                "schema1".into(),
                Program::seq(&["create table test (x)"]).into(),
            )
            .await
            .unwrap();

        assert_debug_snapshot!(meta_store.get_next_pending_job().await.unwrap().unwrap());

        let mut tasks = meta_store
            .get_next_pending_migration_tasks_batch(job_id, MigrationTaskStatus::Enqueued, 10)
            .await
            .unwrap();
        assert_debug_snapshot!(tasks);

        let mut task = tasks.pop().unwrap();
        task.status = MigrationTaskStatus::Success;
        meta_store.update_task_status(task, None).await.unwrap();

        assert_debug_snapshot!(meta_store.get_next_pending_job().await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn step_job_dry_run_success() {
        let tmp = tempdir().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path())
            .await
            .unwrap();

        register_schema(&meta_store, "schema1").await;
        register_shared(&meta_store, "ns1", "schema1").await;
        register_shared(&meta_store, "ns2", "schema1").await;
        register_shared(&meta_store, "ns3", "schema1").await;
        meta_store
            .register_schema_migration(
                "schema1".into(),
                Program::seq(&["create table test (x)"]).into(),
            )
            .await
            .unwrap();

        let job = meta_store.get_next_pending_job().await.unwrap().unwrap();
        let job = meta_store.job_step_dry_run_success(job).await.unwrap();

        // the job status wasn't updated: there are still tasks that need dry run
        assert_eq!(job.status, MigrationJobStatus::WaitingDryRun);

        let tasks = meta_store
            .get_next_pending_migration_tasks_batch(job.job_id, MigrationTaskStatus::Enqueued, 10)
            .await
            .unwrap();
        for mut task in tasks {
            task.status = MigrationTaskStatus::DryRunSuccess;
            meta_store.update_task_status(task, None).await.unwrap();
        }

        let job = meta_store.job_step_dry_run_success(job).await.unwrap();
        assert_eq!(job.status, MigrationJobStatus::DryRunSuccess);
    }
}
