use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDateTime;
use futures::Future;
use libsql_sys::EncryptionConfig;
use tokio::sync::Semaphore;

use crate::connection::config::DatabaseConfig;
use crate::replication::script_backup_manager::ScriptBackupManager;
use crate::StatsSender;

use super::broadcasters::BroadcasterHandle;
use super::meta_store::MetaStoreHandle;
use super::{
    Namespace, NamespaceBottomlessDbIdInit, NamespaceName, NamespaceStore, ResetCb,
    ResolveNamespacePathFn, RestoreOption,
};

pub mod fork;
mod helpers;
mod libsql_primary;
mod libsql_replica;
mod libsql_schema;
mod primary;
mod replica;
mod schema;

pub use libsql_primary::LibsqlPrimaryConfigurator;
pub use libsql_replica::LibsqlReplicaConfigurator;
pub use libsql_schema::LibsqlSchemaConfigurator;
pub use primary::PrimaryConfigurator;
pub use replica::ReplicaConfigurator;
pub use schema::SchemaConfigurator;

#[derive(Clone, Debug)]
pub struct BaseNamespaceConfig {
    pub(crate) base_path: Arc<Path>,
    pub(crate) extensions: Arc<[PathBuf]>,
    pub(crate) stats_sender: StatsSender,
    pub(crate) max_response_size: u64,
    pub(crate) max_total_response_size: u64,
    pub(crate) max_concurrent_connections: Arc<Semaphore>,
    pub(crate) max_concurrent_requests: u64,
    pub(crate) encryption_config: Option<EncryptionConfig>,
}

#[derive(Clone)]
pub struct PrimaryConfig {
    pub(crate) max_log_size: u64,
    pub(crate) max_log_duration: Option<Duration>,
    pub(crate) bottomless_replication: Option<bottomless::replicator::Options>,
    pub(crate) scripted_backup: Option<ScriptBackupManager>,
    pub(crate) checkpoint_interval: Option<Duration>,
}

pub type DynConfigurator = dyn ConfigureNamespace + Send + Sync + 'static;

pub(crate) struct NamespaceConfigurators {
    replica_configurator: Option<Box<DynConfigurator>>,
    primary_configurator: Option<Box<DynConfigurator>>,
    schema_configurator: Option<Box<DynConfigurator>>,
}

impl Default for NamespaceConfigurators {
    fn default() -> Self {
        Self::empty()
    }
}

impl NamespaceConfigurators {
    pub fn empty() -> Self {
        Self {
            replica_configurator: None,
            primary_configurator: None,
            schema_configurator: None,
        }
    }

    pub fn with_primary(
        &mut self,
        c: impl ConfigureNamespace + Send + Sync + 'static,
    ) -> &mut Self {
        self.primary_configurator = Some(Box::new(c));
        self
    }

    pub fn with_replica(
        &mut self,
        c: impl ConfigureNamespace + Send + Sync + 'static,
    ) -> &mut Self {
        self.replica_configurator = Some(Box::new(c));
        self
    }

    pub fn with_schema(&mut self, c: impl ConfigureNamespace + Send + Sync + 'static) -> &mut Self {
        self.schema_configurator = Some(Box::new(c));
        self
    }

    pub fn configure_schema(&self) -> crate::Result<&DynConfigurator> {
        self.schema_configurator.as_deref().ok_or_else(|| todo!())
    }

    pub fn configure_primary(&self) -> crate::Result<&DynConfigurator> {
        self.primary_configurator.as_deref().ok_or_else(|| todo!())
    }

    pub fn configure_replica(&self) -> crate::Result<&DynConfigurator> {
        self.replica_configurator.as_deref().ok_or_else(|| todo!())
    }
}

pub trait ConfigureNamespace {
    fn setup<'a>(
        &'a self,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>>;

    fn cleanup<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        db_config: &'a DatabaseConfig,
        prune_all: bool,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>>;

    fn fork<'a>(
        &'a self,
        from_ns: &'a Namespace,
        from_config: MetaStoreHandle,
        to_ns: NamespaceName,
        to_config: MetaStoreHandle,
        timestamp: Option<NaiveDateTime>,
        store: NamespaceStore,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>>;
}
