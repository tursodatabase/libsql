#![allow(dead_code)]

use std::{collections::HashMap, path::Path, sync::Arc};

use crate::{connection::config::DatabaseConfig, error::Error, Result};
use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};

use super::NamespaceName;

type ChangeMsg = (NamespaceName, Arc<DatabaseConfig>);

#[derive(Debug)]
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
    External(mpsc::Sender<ChangeMsg>, Receiver<Arc<DatabaseConfig>>),
}

#[derive(Debug)]
struct MetaStoreInner {
    // TODO(lucio): Use a concurrent hashmap so we don't block connection creation
    // when we are updating the config. The config si already synced via the watch
    // channel.
    configs: HashMap<NamespaceName, Sender<Arc<DatabaseConfig>>>,
    db: Connection,
}

/// Handles config change updates by inserting them into the database and in-memory
/// cache of configs.
fn process(msg: ChangeMsg, inner: Arc<Mutex<MetaStoreInner>>) -> Result<()> {
    let (namespace, config) = msg;

    let config_encoded = serde_json::to_vec(&config)?;

    let inner = &mut inner.lock();

    inner.db.execute(
        "INSERT OR REPLACE INTO namespace_configs (namespace, config) VALUES (?1, ?2)",
        rusqlite::params![namespace.to_string(), config_encoded],
    )?;

    let configs = &mut inner.configs;

    if let Some(config_watch) = configs.get_mut(&namespace) {
        config_watch.send_modify(|c| {
            *c = config;
        });
    } else {
        let (tx, _) = watch::channel(config);
        configs.insert(namespace, tx);
    }

    Ok(())
}

#[tracing::instrument(skip(db))]
fn restore(db: &Connection) -> Result<HashMap<NamespaceName, Sender<Arc<DatabaseConfig>>>> {
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

                let config = match serde_json::from_slice::<DatabaseConfig>(&v[..]) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("unable to convert config: {}", e);
                        continue;
                    }
                };

                let (tx, _) = watch::channel(Arc::new(config));

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
    pub async fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let db = Connection::open(base_path)?;

        let configs = restore(&db)?;

        let (changes_tx, mut changes_rx) = mpsc::channel(256);

        let inner = Arc::new(Mutex::new(MetaStoreInner { configs, db }));

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
            let (tx, _) = watch::channel(Arc::new(DatabaseConfig::default()));
            tx
        });

        let rx = sender.subscribe();

        tracing::debug!("meta handle subscribed");

        MetaStoreHandle {
            namespace,
            inner: HandleState::External(change_tx, rx),
        }
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

        let config = match fs::read(&config_path) {
            Ok(data) => serde_json::from_slice(&data)?,
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
            HandleState::External(_, config) => config.borrow().clone(),
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
