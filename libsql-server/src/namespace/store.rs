use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_lock::RwLock;
use chrono::NaiveDateTime;
use futures::TryFutureExt;
use moka::future::Cache;
use once_cell::sync::OnceCell;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

use crate::auth::Authenticated;
use crate::connection::config::DatabaseConfig;
use crate::error::Error;
use crate::metrics::NAMESPACE_LOAD_LATENCY;
use crate::namespace::{NamespaceBottomlessDbId, NamespaceBottomlessDbIdInit, NamespaceName};
use crate::stats::Stats;

use super::meta_store::{MetaStore, MetaStoreHandle};
use super::schema_lock::SchemaLocksRegistry;
use super::{Namespace, NamespaceConfig, ResetCb, ResetOp, ResolveNamespacePathFn, RestoreOption};

type NamespaceEntry = Arc<RwLock<Option<Namespace>>>;

/// Stores and manage a set of namespaces.
pub struct NamespaceStore {
    pub inner: Arc<NamespaceStoreInner>,
}

impl Clone for NamespaceStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct NamespaceStoreInner {
    store: Cache<NamespaceName, NamespaceEntry>,
    metadata: MetaStore,
    allow_lazy_creation: bool,
    has_shutdown: AtomicBool,
    snapshot_at_shutdown: bool,
    pub config: NamespaceConfig,
    schema_locks: SchemaLocksRegistry,
}

impl NamespaceStore {
    pub async fn new(
        allow_lazy_creation: bool,
        snapshot_at_shutdown: bool,
        max_active_namespaces: usize,
        config: NamespaceConfig,
        metadata: MetaStore,
    ) -> crate::Result<Self> {
        tracing::trace!("Max active namespaces: {max_active_namespaces}");
        let store = Cache::<NamespaceName, NamespaceEntry>::builder()
            .async_eviction_listener(move |name, ns, cause| {
                tracing::debug!("evicting namespace `{name}` asynchronously: {cause:?}");
                // TODO(sarna): not clear if we should snapshot-on-evict...
                // On the one hand, better to do so, because we have no idea
                // for how long we're evicting a namespace.
                // On the other, if there's lots of cache pressure, snapshotting
                // very often will kill the machine's I/O.
                Box::pin(async move {
                    tracing::info!("namespace `{name}` deallocated");
                    // shutdown namespace
                    if let Some(ns) = ns.write().await.take() {
                        if let Err(e) = ns.shutdown(snapshot_at_shutdown).await {
                            tracing::error!("error deallocating `{name}`: {e}")
                        }
                    }
                })
            })
            .max_capacity(max_active_namespaces as u64)
            .time_to_idle(Duration::from_secs(86400))
            .build();

        Ok(Self {
            inner: Arc::new(NamespaceStoreInner {
                store,
                metadata,
                allow_lazy_creation,
                has_shutdown: AtomicBool::new(false),
                snapshot_at_shutdown,
                config,
                schema_locks: Default::default(),
            }),
        })
    }

    pub fn exists(&self, namespace: &NamespaceName) -> bool {
        self.inner.metadata.exists(namespace)
    }

    pub async fn destroy(&self, namespace: NamespaceName, prune_all: bool) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }

        // destroy on-disk database and backups
        // FIXME: this is blocking
        let db_config = self
            .inner
            .metadata
            .remove(namespace.clone())?
            .ok_or_else(|| crate::Error::NamespaceDoesntExist(namespace.to_string()))?;

        let mut bottomless_db_id_init = NamespaceBottomlessDbIdInit::FetchFromConfig;
        if let Some(ns) = self.inner.store.remove(&namespace).await {
            // deallocate in-memory resources
            if let Some(ns) = ns.write().await.take() {
                bottomless_db_id_init = NamespaceBottomlessDbIdInit::Provided(
                    NamespaceBottomlessDbId::from_config(&ns.db_config_store.get()),
                );
                ns.destroy().await?;
            }
        }

        Namespace::cleanup(
            &self.inner.config,
            &namespace,
            &db_config,
            prune_all,
            bottomless_db_id_init,
        )
        .await?;

        tracing::info!("destroyed namespace: {namespace}");

        Ok(())
    }

    pub async fn reset(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
    ) -> anyhow::Result<()> {
        // The process for reseting is as follow:
        // - get a lock on the namespace entry, if the entry exists, then it's a lock on the entry,
        // if it doesn't exist, insert an empty entry and take a lock on it
        // - destroy the old namespace
        // - create a new namespace and insert it in the held lock
        let entry = self
            .inner
            .store
            .get_with(namespace.clone(), async { Default::default() })
            .await;
        let mut lock = entry.write().await;
        if let Some(ns) = lock.take() {
            ns.destroy().await?;
        }

        let handle = self.inner.metadata.handle(namespace.clone());
        // destroy on-disk database
        Namespace::cleanup(
            &self.inner.config,
            &namespace,
            &handle.get(),
            false,
            NamespaceBottomlessDbIdInit::FetchFromConfig,
        )
        .await?;
        let ns = Namespace::from_config(
            &self.inner.config,
            handle,
            restore_option,
            &namespace,
            self.make_reset_cb(),
            self.resolve_attach_fn(),
        )
        .await?;

        lock.replace(ns);

        Ok(())
    }

    // This is only called on replica
    fn make_reset_cb(&self) -> ResetCb {
        let this = self.clone();
        Box::new(move |op| {
            let this = this.clone();
            tokio::spawn(async move {
                match op {
                    ResetOp::Reset(ns) => {
                        tracing::info!("received reset signal for: {ns}");
                        if let Err(e) = this.reset(ns.clone(), RestoreOption::Latest).await {
                            tracing::error!("error resetting namespace `{ns}`: {e}");
                        }
                    }
                    ResetOp::Destroy(ns) => {
                        if let Err(e) = this.destroy(ns.clone(), false).await {
                            tracing::error!("error destroying namesace `{ns}`: {e}",);
                        }
                    }
                }
            });
        })
    }

    pub async fn fork(
        &self,
        from: NamespaceName,
        to: NamespaceName,
        to_config: DatabaseConfig,
        timestamp: Option<NaiveDateTime>,
    ) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }

        // check that the source namespace exists
        if !self.inner.metadata.exists(&from) {
            return Err(crate::error::Error::NamespaceDoesntExist(from.to_string()));
        }

        let to_entry = self
            .inner
            .store
            .get_with(to.clone(), async { Default::default() })
            .await;
        let mut to_lock = to_entry.write().await;
        if to_lock.is_some() {
            return Err(crate::error::Error::NamespaceAlreadyExist(to.to_string()));
        }

        // FIXME: we could potentially delete the namespace while trying to fork it
        if !self.inner.metadata.exists(&from) {
            return Err(crate::Error::NamespaceDoesntExist(from.to_string()));
        }

        let from_config = self.inner.metadata.handle(from.clone());
        let from_entry = self
            .load_namespace(&from, from_config.clone(), RestoreOption::Latest)
            .await?;
        let from_lock = from_entry.read().await;
        let Some(from_ns) = &*from_lock else {
            return Err(crate::error::Error::NamespaceDoesntExist(from.to_string()));
        };

        struct Bomb {
            store: MetaStore,
            ns: NamespaceName,
            should_delete: bool,
        }

        impl Drop for Bomb {
            fn drop(&mut self) {
                if self.should_delete {
                    if let Err(e) = self.store.remove(self.ns.clone()) {
                        tracing::error!("failed to clean handle while forking: {e}");
                    }
                }
            }
        }

        let mut bomb = Bomb {
            store: self.inner.metadata.clone(),
            ns: to.clone(),
            should_delete: true,
        };

        let handle = self.inner.metadata.handle(to.clone());
        handle
            .store_and_maybe_flush(Some(to_config.into()), false)
            .await?;
        let to_ns = Namespace::fork(
            &self.inner.config,
            from_ns,
            from_config,
            to.clone(),
            handle.clone(),
            timestamp,
            self.resolve_attach_fn(),
        )
        .await?;

        to_lock.replace(to_ns);
        handle.flush().await?;
        // defuse
        bomb.should_delete = false;

        Ok(())
    }

    pub async fn with_authenticated<Fun, R>(
        &self,
        namespace: NamespaceName,
        auth: Authenticated,
        f: Fun,
    ) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace) -> R + 'static,
    {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        if !auth.is_namespace_authorized(&namespace) {
            return Err(Error::NamespaceDoesntExist(namespace.to_string()));
        }

        self.with(namespace, f).await
    }

    pub async fn with<Fun, R>(&self, namespace: NamespaceName, f: Fun) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace) -> R + 'static,
    {
        if namespace != NamespaceName::default()
            && !self.inner.metadata.exists(&namespace)
            && !self.inner.allow_lazy_creation
        {
            return Err(Error::NamespaceDoesntExist(namespace.to_string()));
        }

        let f = {
            let name = namespace.clone();
            move |ns: NamespaceEntry| async move {
                let lock = ns.read().await;
                match &*lock {
                    Some(ns) => Ok(f(ns)),
                    // the namespace was taken out of the entry
                    None => Err(Error::NamespaceDoesntExist(name.to_string())),
                }
            }
        };

        let handle = self.inner.metadata.handle(namespace.to_owned());
        f(self
            .load_namespace(&namespace, handle, RestoreOption::Latest)
            .await?)
        .await
    }

    fn resolve_attach_fn(&self) -> ResolveNamespacePathFn {
        static FN: OnceCell<ResolveNamespacePathFn> = OnceCell::new();
        FN.get_or_init(|| {
            Arc::new({
                let store = self.clone();
                move |ns: &NamespaceName| {
                    tokio::runtime::Handle::current()
                        .block_on(store.with(ns.clone(), |ns| ns.path.clone()))
                }
            })
        })
        .clone()
    }

    async fn load_namespace(
        &self,
        namespace: &NamespaceName,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
    ) -> crate::Result<NamespaceEntry> {
        let init = {
            let namespace = namespace.clone();
            async move {
                let ns = Namespace::from_config(
                    &self.inner.config,
                    db_config,
                    restore_option,
                    &namespace,
                    self.make_reset_cb(),
                    self.resolve_attach_fn(),
                )
                .await?;
                tracing::info!("loaded namespace: `{namespace}`");

                Ok(Some(ns))
            }
        };

        let before_load = Instant::now();
        let ns = self
            .inner
            .store
            .try_get_with(
                namespace.clone(),
                init.map_ok(|ns| Arc::new(RwLock::new(ns))),
            )
            .await?;
        NAMESPACE_LOAD_LATENCY.record(before_load.elapsed());

        Ok(ns)
    }

    pub async fn create(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
        db_config: DatabaseConfig,
    ) -> crate::Result<()> {
        if let Some(shared_schema_name) = &db_config.shared_schema_name {
            // we hold a lock for the duration of the namespace creation
            let _lock = self
                .inner
                .schema_locks
                .acquire_shared(shared_schema_name.clone())
                .await;
            return self
                .fork(shared_schema_name.clone(), namespace, db_config, None)
                .await;
        };

        // With namespaces disabled, the default namespace can be auto-created,
        // otherwise it's an error.
        // FIXME: move the default namespace check out of this function.
        if self.inner.allow_lazy_creation || namespace == NamespaceName::default() {
            tracing::trace!("auto-creating the namespace");
        } else if self.inner.metadata.exists(&namespace) {
            return Err(Error::NamespaceAlreadyExist(namespace.to_string()));
        }

        let db_config = Arc::new(db_config);
        let handle = self.inner.metadata.handle(namespace.clone());
        handle.store(db_config).await?;
        self.load_namespace(&namespace, handle, restore_option)
            .await?;

        Ok(())
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        let mut set = JoinSet::new();
        self.inner.has_shutdown.store(true, Ordering::Relaxed);

        for (_name, entry) in self.inner.store.iter() {
            let snapshow_at_shutdown = self.inner.snapshot_at_shutdown;
            let mut lock = entry.write().await;
            if let Some(ns) = lock.take() {
                set.spawn(async move {
                    ns.shutdown(snapshow_at_shutdown).await?;
                    Ok::<_, anyhow::Error>(())
                });
            }
        }

        while let Some(_) = set.join_next().await.transpose()?.transpose()? {}

        self.inner.metadata.shutdown().await?;
        self.inner.store.invalidate_all();
        self.inner.store.run_pending_tasks().await;
        Ok(())
    }

    pub(crate) async fn stats(&self, namespace: NamespaceName) -> crate::Result<Arc<Stats>> {
        self.with(namespace, |ns| ns.stats.clone()).await
    }

    pub(crate) async fn config_store(
        &self,
        namespace: NamespaceName,
    ) -> crate::Result<MetaStoreHandle> {
        self.with(namespace, |ns| ns.db_config_store.clone()).await
    }

    pub(crate) fn meta_store(&self) -> &MetaStore {
        &self.inner.metadata
    }

    pub(crate) fn schema_locks(&self) -> &SchemaLocksRegistry {
        &self.inner.schema_locks
    }
}
