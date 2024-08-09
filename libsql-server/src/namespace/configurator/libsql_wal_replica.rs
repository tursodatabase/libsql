use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::prelude::NaiveDateTime;
use hyper::Uri;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::storage::NoStorage;
use tokio::task::JoinSet;
use tonic::transport::Channel;

use crate::connection::config::DatabaseConfig;
use crate::connection::connection_manager::InnerWalManager;
use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::MakeConnection;
use crate::database::{Database, ReplicaDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::configurator::helpers::make_stats;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceBottomlessDbIdInit, NamespaceName, NamespaceStore, ResetCb,
    ResolveNamespacePathFn, RestoreOption,
};
use crate::DEFAULT_AUTO_CHECKPOINT;

use super::{BaseNamespaceConfig, ConfigureNamespace};

pub struct LibsqlWalReplicaConfigurator {
    base: BaseNamespaceConfig,
    registry: Arc<WalRegistry<StdIO, NoStorage>>,
    uri: Uri,
    channel: Channel,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
}

impl ConfigureNamespace for LibsqlWalReplicaConfigurator {
    fn setup<'a>(
        &'a self,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        todo!()
        // Box::pin(async move {
        //     tracing::debug!("creating replica namespace");
        //     let db_path = self.base.base_path.join("dbs").join(name.as_str());
        //     let channel = self.channel.clone();
        //     let uri = self.uri.clone();
        //
        //     let rpc_client = ReplicationLogClient::with_origin(channel.clone(), uri.clone());
        //     // TODO! setup replication
        //
        //     let mut join_set = JoinSet::new();
        //     let namespace = name.clone();
        //
        //     let stats = make_stats(
        //         &db_path,
        //         &mut join_set,
        //         db_config.clone(),
        //         self.base.stats_sender.clone(),
        //         name.clone(),
        //         applied_frame_no_receiver.clone(),
        //     )
        //     .await?;
        //
        //     let connection_maker = MakeWriteProxyConn::new(
        //         db_path.clone(),
        //         self.base.extensions.clone(),
        //         channel.clone(),
        //         uri.clone(),
        //         stats.clone(),
        //         broadcaster,
        //         db_config.clone(),
        //         applied_frame_no_receiver,
        //         self.base.max_response_size,
        //         self.base.max_total_response_size,
        //         primary_current_replication_index,
        //         None,
        //         resolve_attach_path,
        //         self.make_wal_manager.clone(),
        //     )
        //     .await?
        //     .throttled(
        //         self.base.max_concurrent_connections.clone(),
        //         Some(DB_CREATE_TIMEOUT),
        //         self.base.max_total_response_size,
        //         self.base.max_concurrent_requests,
        //     );
        //
        //     Ok(Namespace {
        //         tasks: join_set,
        //         db: Database::Replica(ReplicaDatabase {
        //             connection_maker: Arc::new(connection_maker),
        //         }),
        //         name: name.clone(),
        //         stats,
        //         db_config_store: db_config,
        //         path: db_path.into(),
        //     })
        // })
    }

    fn cleanup<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        _db_config: &DatabaseConfig,
        _prune_all: bool,
        _bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let ns_path = self.base.base_path.join("dbs").join(namespace.as_str());
            if ns_path.try_exists()? {
                tracing::debug!("removing database directory: {}", ns_path.display());
                tokio::fs::remove_dir_all(ns_path).await?;
            }
            Ok(())
        })
    }

    fn fork<'a>(
        &'a self,
        _from_ns: &'a Namespace,
        _from_config: MetaStoreHandle,
        _to_ns: NamespaceName,
        _to_config: MetaStoreHandle,
        _timestamp: Option<chrono::prelude::NaiveDateTime>,
        _store: NamespaceStore,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(std::future::ready(Err(crate::Error::Fork(
            super::fork::ForkError::ForkReplica,
        ))))
    }
}
