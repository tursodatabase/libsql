#![allow(clippy::type_complexity, clippy::too_many_arguments)]

use std::alloc::Layout;
use std::ffi::c_void;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Weak};

use crate::connection::{Connection, MakeConnection};
use crate::database::DatabaseKind;
use crate::error::Error;
use crate::migration::maybe_migrate;
use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
use crate::net::Accept;
use crate::pager::{make_pager, PAGER_CACHE_SIZE};
use crate::rpc::proxy::rpc::proxy_server::Proxy;
use crate::rpc::proxy::ProxyService;
use crate::rpc::replica_proxy::ReplicaProxyService;
use crate::rpc::replication::libsql_replicator::LibsqlReplicationService;
use crate::rpc::replication::replication_log::rpc::replication_log_server::ReplicationLog;
use crate::rpc::replication::replication_log::ReplicationLogService;
use crate::rpc::replication::replication_log_proxy::ReplicationLogProxyService;
use crate::rpc::run_rpc_server;
use crate::schema::Scheduler;
use crate::stats::Stats;
use anyhow::Context as AnyhowContext;
use auth::Auth;
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use config::{
    AdminApiConfig, DbConfig, HeartbeatConfig, RpcClientConfig, RpcServerConfig, UserApiConfig,
};
use futures::future::ready;
use futures::Future;
use http::user::UserApi;
use hyper::client::HttpConnector;
use hyper::Uri;
use hyper_rustls::HttpsConnector;
use libsql_replication::rpc::replication::BoxReplicationService;
#[cfg(feature = "durable-wal")]
use libsql_storage::{DurableWalManager, LockManager};
use libsql_sys::wal::either::Either;
#[cfg(not(feature = "durable-wal"))]
use libsql_sys::wal::either::Either as EitherWAL;
#[cfg(feature = "durable-wal")]
use libsql_sys::wal::either::Either3 as EitherWAL;
use libsql_sys::wal::Sqlite3WalManager;
use libsql_wal::checkpointer::LibsqlCheckpointer;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::segment::sealed::SealedSegment;
use libsql_wal::storage::async_storage::{AsyncStorage, AsyncStorageInitConfig};
use libsql_wal::storage::backend::s3::S3Backend;
use libsql_wal::storage::NoStorage;
use namespace::meta_store::MetaStoreHandle;
use namespace::NamespaceName;
use net::Connector;
use once_cell::sync::Lazy;
use rusqlite::ffi::SQLITE_CONFIG_MALLOC;
use rusqlite::ffi::{sqlite3_config, SQLITE_CONFIG_PCACHE2};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_stream::StreamExt as _;
use tonic::transport::Channel;
use url::Url;
use utils::services::idle_shutdown::IdleShutdownKicker;

use self::bottomless_migrate::bottomless_migrate;
use self::config::MetaStoreConfig;
use self::connection::connection_manager::InnerWalManager;
use self::connection::MakeThrottledConnection;
use self::namespace::configurator::{
    BaseNamespaceConfig, LibsqlPrimaryConfigurator, LibsqlReplicaConfigurator,
    LibsqlSchemaConfigurator, NamespaceConfigurators, PrimaryConfig, PrimaryConfigurator,
    ReplicaConfigurator, SchemaConfigurator,
};
use self::namespace::NamespaceStore;
use self::net::AddrIncoming;
use self::replication::script_backup_manager::{CommandHandler, ScriptBackupManager};
use self::schema::SchedulerHandle;

pub mod admin_shell;
pub mod auth;
mod broadcaster;
pub mod config;
pub mod connection;
pub mod net;
pub mod rpc;
pub mod version;

pub use hrana::proto as hrana_proto;

mod bottomless_migrate;
mod database;
mod error;
mod h2c;
mod heartbeat;
mod hrana;
mod http;
mod metrics;
mod migration;
mod namespace;
mod pager;
mod query;
mod query_analysis;
mod query_result_builder;
mod replication;
mod schema;
mod stats;
#[cfg(test)]
mod test;
mod utils;
pub mod wal_toolkit;

const DB_CREATE_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_AUTO_CHECKPOINT: u32 = 1000;
const LIBSQL_PAGE_SIZE: u64 = 4096;

pub(crate) static BLOCKING_RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(50_000)
        .enable_all()
        .build()
        .unwrap()
});

type Result<T, E = Error> = std::result::Result<T, E>;
type StatsSender = mpsc::Sender<(NamespaceName, MetaStoreHandle, Weak<Stats>)>;
type MakeReplicationSvc = Box<
    dyn Fn(
            NamespaceStore,
            Option<Auth>,
            Option<IdleShutdownKicker>,
            bool,
            bool,
        ) -> BoxReplicationService
        + Send
        + 'static,
>;

// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: rheaper::Allocator<mimalloc::MiMalloc> =
    rheaper::Allocator::from_allocator(mimalloc::MiMalloc);

#[derive(clap::ValueEnum, PartialEq, Clone, Copy, Debug)]
pub enum CustomWAL {
    LibsqlWal,
    #[cfg(feature = "durable-wal")]
    DurableWal,
}

pub struct Server<C = HttpConnector, A = AddrIncoming, D = HttpsConnector<HttpConnector>> {
    pub path: Arc<Path>,
    pub db_config: DbConfig,
    pub user_api_config: UserApiConfig<A>,
    pub admin_api_config: Option<AdminApiConfig<A, D>>,
    pub rpc_server_config: Option<RpcServerConfig<A>>,
    pub rpc_client_config: Option<RpcClientConfig<C>>,
    pub idle_shutdown_timeout: Option<Duration>,
    pub initial_idle_shutdown_timeout: Option<Duration>,
    pub disable_default_namespace: bool,
    pub heartbeat_config: Option<HeartbeatConfig>,
    pub disable_namespaces: bool,
    pub shutdown: Arc<Notify>,
    pub max_active_namespaces: usize,
    pub meta_store_config: MetaStoreConfig,
    pub max_concurrent_connections: usize,
    pub shutdown_timeout: std::time::Duration,
    pub use_custom_wal: Option<CustomWAL>,
    pub storage_server_address: String,
    pub connector: Option<D>,
    pub migrate_bottomless: bool,
    pub enable_deadlock_monitor: bool,
    pub should_sync_from_storage: bool,
    pub force_load_wals: bool,
    pub sync_conccurency: usize,
    pub set_log_level: Option<Box<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync + 'static>>,
}

impl<C, A, D> Default for Server<C, A, D> {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data.sqld").into(),
            db_config: Default::default(),
            user_api_config: Default::default(),
            admin_api_config: Default::default(),
            rpc_server_config: Default::default(),
            rpc_client_config: Default::default(),
            idle_shutdown_timeout: Default::default(),
            initial_idle_shutdown_timeout: Default::default(),
            disable_default_namespace: false,
            heartbeat_config: Default::default(),
            disable_namespaces: true,
            shutdown: Default::default(),
            max_active_namespaces: 100,
            meta_store_config: Default::default(),
            max_concurrent_connections: 128,
            shutdown_timeout: Duration::from_secs(30),
            use_custom_wal: None,
            storage_server_address: Default::default(),
            connector: None,
            migrate_bottomless: false,
            enable_deadlock_monitor: false,
            should_sync_from_storage: false,
            force_load_wals: false,
            sync_conccurency: 8,
            set_log_level: None,
        }
    }
}

struct Services<A, P, S, C> {
    namespace_store: NamespaceStore,
    idle_shutdown_kicker: Option<IdleShutdownKicker>,
    proxy_service: P,
    replication_service: S,
    user_api_config: UserApiConfig<A>,
    admin_api_config: Option<AdminApiConfig<A, C>>,
    disable_namespaces: bool,
    disable_default_namespace: bool,
    db_config: DbConfig,
    user_auth_strategy: Auth,
    pub set_log_level: Option<Box<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync + 'static>>,
}

struct TaskManager {
    join_set: JoinSet<anyhow::Result<()>>,
    shutdown: Arc<Notify>,
}

impl TaskManager {
    /// pass a shutdown notifier to the task. The task must shutdown upon receiving a signal
    pub fn spawn_with_shutdown_notify<F, Fut>(&mut self, f: F)
    where
        F: FnOnce(Arc<Notify>) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let fut = f(self.shutdown.clone());
        self.join_set.spawn(fut);
    }

    pub fn spawn_until_shutdown<F>(&mut self, fut: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.spawn_until_shutdown_with_teardown(fut, ready(Ok(())))
    }

    /// run the passed future until shutdown is called, then call the passed teardown future
    #[track_caller]
    pub fn spawn_until_shutdown_with_teardown<F, T>(&mut self, fut: F, teardown: T)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
        T: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let shutdown = self.shutdown.clone();
        self.join_set.spawn(async move {
            tokio::select! {
                _ = shutdown.notified() => {
                    let ret = teardown.await;
                    if let Err(ref e) = ret {
                        let caller = std::panic::Location::caller();
                        tracing::error!(caller = caller.to_string(), "task teardown returned an error: {e}");
                    }
                    ret
                },
                ret = fut => ret
            }
        });
    }

    fn new() -> Self {
        Self {
            join_set: JoinSet::new(),
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.shutdown.notify_waiters();
        while let Some(ret) = self.join_set.join_next().await {
            ret??
        }

        Ok(())
    }

    pub async fn join_next(&mut self) -> anyhow::Result<()> {
        if let Some(ret) = self.join_set.join_next().await {
            ret??;
        }
        Ok(())
    }
}

impl<A, P, S, C> Services<A, P, S, C>
where
    A: crate::net::Accept,
    P: Proxy,
    S: ReplicationLog,
    C: Connector,
{
    fn configure(mut self, task_manager: &mut TaskManager) {
        let user_http = UserApi {
            http_acceptor: self.user_api_config.http_acceptor,
            hrana_ws_acceptor: self.user_api_config.hrana_ws_acceptor,
            user_auth_strategy: self.user_auth_strategy,
            namespaces: self.namespace_store.clone(),
            idle_shutdown_kicker: self.idle_shutdown_kicker.clone(),
            proxy_service: self.proxy_service,
            replication_service: self.replication_service,
            disable_default_namespace: self.disable_default_namespace,
            disable_namespaces: self.disable_namespaces,
            max_response_size: self.db_config.max_response_size,
            enable_console: self.user_api_config.enable_http_console,
            self_url: self.user_api_config.self_url,
            primary_url: self.user_api_config.primary_url,
        };

        let user_http_service = user_http.configure(task_manager);

        if let Some(AdminApiConfig {
            acceptor,
            connector,
            disable_metrics,
            auth_key,
        }) = self.admin_api_config
        {
            task_manager.spawn_with_shutdown_notify(|shutdown| {
                http::admin::run(
                    acceptor,
                    user_http_service,
                    self.namespace_store,
                    connector,
                    disable_metrics,
                    shutdown,
                    auth_key.map(Into::into),
                    self.set_log_level.take(),
                )
            });
        }
    }
}

pub type SqldStorage =
    Either<AsyncStorage<S3Backend<StdIO>, SealedSegment<std::fs::File>>, NoStorage>;

#[tracing::instrument(skip(connection_maker))]
async fn run_periodic_checkpoint<C>(
    connection_maker: Arc<MakeThrottledConnection<C>>,
    period: Duration,
    namespace_name: NamespaceName,
) -> anyhow::Result<()>
where
    C: MakeConnection,
{
    use tokio::time::{interval, sleep, Instant, MissedTickBehavior};

    const RETRY_INTERVAL: Duration = Duration::from_secs(60);
    tracing::info!("setting checkpoint interval to {:?}", period);
    let mut interval = interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut retry: Option<Duration> = None;
    loop {
        if let Some(retry) = retry.take() {
            if retry.is_zero() {
                tracing::warn!("database was not set in WAL journal mode");
                return Ok(());
            }
            sleep(retry).await;
        } else {
            interval.tick().await;
        }
        retry = match connection_maker.untracked().await {
            Ok(conn) => {
                if let Err(e) = conn.vacuum_if_needed().await {
                    tracing::warn!("vacuum failed: {}", e);
                }
                tracing::info!("database checkpoint starts");
                let start = Instant::now();
                match conn.checkpoint().await {
                    Ok(_) => {
                        let elapsed = Instant::now() - start;
                        if elapsed >= Duration::from_secs(10) {
                            tracing::warn!("database checkpoint finished (took: {:?})", elapsed);
                        } else {
                            tracing::info!("database checkpoint finished (took: {:?})", elapsed);
                        }
                        None
                    }
                    Err(err) => {
                        tracing::warn!("failed to execute checkpoint: {}", err);
                        Some(RETRY_INTERVAL)
                    }
                }
            }
            Err(err) => {
                tracing::warn!("couldn't connect: {}", err);
                Some(RETRY_INTERVAL)
            }
        }
    }
}

fn init_version_file(db_path: &Path) -> anyhow::Result<()> {
    // try to detect the presence of the data file at the root of db_path. If it's there, it's a
    // pre-0.18.0 database and needs to be migrated
    if db_path.join("data").exists() {
        return Ok(());
    }

    let version_path = db_path.join(".version");
    if !version_path.exists() {
        std::fs::create_dir_all(db_path)?;
        std::fs::write(version_path, env!("CARGO_PKG_VERSION"))?;
    }

    Ok(())
}

/// The deadlock watcher monitors the main tokio runtime for deadlock by sending Ping to a task
/// within it, and waiting for pongs. If the runtime fails to respond in due time, the watcher
/// exits the process.
fn install_deadlock_monitor() {
    // this is a very generous deadline for the main runtime to respond
    const PONG_DEADLINE: Duration = Duration::from_secs(5);

    struct Ping;
    struct Pong;

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            loop {
                let (snd, ret) = tokio::sync::oneshot::channel();
                sender.try_send((snd, Ping)).unwrap();
                match tokio::time::timeout(PONG_DEADLINE, ret).await {
                    Ok(Ok(Pong)) => (),
                    Err(_) => {
                        tracing::error!(
                            "main runtime failed to respond within deadlines, deadlock detected"
                        );
                        // std::process::exit(1);
                    }
                    _ => (),
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    });

    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Some((ret, Ping)) => {
                    let _ = ret.send(Pong);
                }
                None => break,
            }
        }

        tracing::warn!("deadlock monitor exited")
    });
}

impl<C, A, D> Server<C, A, D>
where
    C: Connector,
    A: Accept,
    D: Connector,
{
    /// Setup sqlite global environment
    fn init_sqlite_globals(&self) {
        if let Some(soft_limit_mb) = self.db_config.soft_heap_limit_mb {
            tracing::warn!("Setting soft heap limit to {soft_limit_mb}MiB");
            unsafe {
                libsql_sys::ffi::sqlite3_soft_heap_limit64(soft_limit_mb as i64 * 1024 * 1024)
            };
        }
        if let Some(hard_limit_mb) = self.db_config.hard_heap_limit_mb {
            tracing::warn!("Setting hard heap limit to {hard_limit_mb}MiB");
            unsafe {
                libsql_sys::ffi::sqlite3_hard_heap_limit64(hard_limit_mb as i64 * 1024 * 1024)
            };
        }
    }

    fn spawn_monitoring_tasks(
        &self,
        task_manager: &mut TaskManager,
        stats_receiver: mpsc::Receiver<(NamespaceName, MetaStoreHandle, Weak<Stats>)>,
    ) -> anyhow::Result<()> {
        match self.heartbeat_config {
            Some(ref config) => {
                tracing::info!(
                    "Server sending heartbeat to URL {} every {:?}",
                    config.heartbeat_url.as_deref().unwrap_or("<not supplied>"),
                    config.heartbeat_period,
                );

                task_manager.spawn_until_shutdown({
                    let heartbeat_auth = config.heartbeat_auth.clone();
                    let heartbeat_period = config.heartbeat_period;
                    let heartbeat_url = if let Some(url) = &config.heartbeat_url {
                        Some(Url::from_str(url).context("invalid heartbeat URL")?)
                    } else {
                        None
                    };
                    async move {
                        heartbeat::server_heartbeat(
                            heartbeat_url,
                            heartbeat_auth,
                            heartbeat_period,
                            stats_receiver,
                        )
                        .await;
                        Ok(())
                    }
                });

                // join_set.spawn(run_storage_monitor(self.path.clone(), stats));
            }
            None => {
                tracing::warn!("No server heartbeat configured")
            }
        }

        Ok(())
    }

    fn make_services<P: Proxy, L: ReplicationLog>(
        mut self,
        namespace_store: NamespaceStore,
        idle_shutdown_kicker: Option<IdleShutdownKicker>,
        proxy_service: P,
        replication_service: L,
        user_auth_strategy: Auth,
    ) -> Services<A, P, L, D> {
        Services {
            namespace_store,
            idle_shutdown_kicker,
            proxy_service,
            replication_service,
            user_api_config: self.user_api_config,
            admin_api_config: self.admin_api_config,
            disable_namespaces: self.disable_namespaces,
            disable_default_namespace: self.disable_default_namespace,
            db_config: self.db_config,
            user_auth_strategy,
            set_log_level: self.set_log_level.take(),
        }
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
        static INIT: std::sync::Once = std::sync::Once::new();
        let mut task_manager = TaskManager::new();

        if self.enable_deadlock_monitor {
            install_deadlock_monitor();
            tracing::info!("deadlock monitor installed");
        }

        if std::env::var("LIBSQL_SQLITE_MIMALLOC").is_ok() {
            setup_sqlite_alloc();
        }

        INIT.call_once(|| {
            if let Ok(size) = std::env::var("LIBSQL_EXPERIMENTAL_PAGER") {
                let size = size.parse().unwrap();
                PAGER_CACHE_SIZE.store(size, std::sync::atomic::Ordering::SeqCst);
                unsafe {
                    let rc = sqlite3_config(SQLITE_CONFIG_PCACHE2, &make_pager());
                    if rc != 0 {
                        // necessary because in some tests there is race between client and server
                        // to initialize global state.
                        tracing::error!(
                            "failed to setup sqld pager, using sqlite3 default instead"
                        );
                    }
                }
            }
        });

        init_version_file(&self.path)?;
        maybe_migrate(&self.path)?;
        self.init_sqlite_globals();
        let idle_shutdown_kicker = self.setup_shutdown();

        let extensions = self.db_config.validate_extensions()?;
        let user_auth_strategy = self.user_api_config.auth_strategy.clone();

        let scripted_backup = match self.db_config.snapshot_exec {
            Some(ref command) => {
                let (scripted_backup, script_backup_task) =
                    ScriptBackupManager::new(&self.path, CommandHandler::new(command.to_string()))
                        .await?;
                task_manager.spawn_until_shutdown(script_backup_task.run());
                Some(scripted_backup)
            }
            None => None,
        };

        let db_kind = match self.rpc_client_config {
            Some(_) => DatabaseKind::Replica,
            _ => DatabaseKind::Primary,
        };

        let client_config = self.get_client_config().await?;
        let (scheduler_sender, scheduler_receiver) = mpsc::channel(128);
        let (stats_sender, stats_receiver) = mpsc::channel(1024);

        let base_config = BaseNamespaceConfig {
            base_path: self.path.clone(),
            extensions,
            stats_sender,
            max_response_size: self.db_config.max_response_size,
            max_total_response_size: self.db_config.max_total_response_size,
            max_concurrent_connections: Arc::new(Semaphore::new(self.max_concurrent_connections)),
            max_concurrent_requests: self.db_config.max_concurrent_requests,
            encryption_config: self.db_config.encryption_config.clone(),
        };

        let (metastore_conn_maker, meta_store_wal_manager) =
            metastore_connection_maker(self.meta_store_config.bottomless.clone(), &self.path)
                .await?;
        let meta_conn = metastore_conn_maker()?;
        let meta_store = MetaStore::new(
            self.meta_store_config.clone(),
            &self.path,
            meta_conn,
            meta_store_wal_manager,
            db_kind,
        )
        .await?;

        let (configurators, make_replication_svc) = self
            .make_configurators_and_replication_svc(
                base_config,
                client_config.clone(),
                &mut task_manager,
                scheduler_sender.into(),
                scripted_backup,
                meta_store.clone(),
            )
            .await?;

        let namespace_store: NamespaceStore = NamespaceStore::new(
            db_kind.is_replica(),
            self.db_config.snapshot_at_shutdown,
            self.max_active_namespaces,
            meta_store,
            configurators,
            db_kind,
        )
        .await?;

        self.spawn_monitoring_tasks(&mut task_manager, stats_receiver)?;

        // if namespaces are enabled, then bottomless must have set DB ID
        if !self.disable_namespaces {
            if let Some(bottomless) = &self.db_config.bottomless_replication {
                if bottomless.db_id.is_none() {
                    anyhow::bail!("bottomless replication with namespaces requires a DB ID");
                }
            }
        }

        // configure rpc server
        if let Some(config) = self.rpc_server_config.take() {
            let proxy_service =
                ProxyService::new(namespace_store.clone(), None, self.disable_namespaces);
            // Garbage collect proxy clients every 30 seconds
            task_manager.spawn_until_shutdown({
                let clients = proxy_service.clients();
                async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        rpc::proxy::garbage_collect(&mut *clients.write().await).await;
                    }
                }
            });

            let replication_service = make_replication_svc(
                namespace_store.clone(),
                Some(user_auth_strategy.clone()),
                idle_shutdown_kicker.clone(),
                false,
                true,
            );

            task_manager.spawn_until_shutdown(run_rpc_server(
                proxy_service,
                config.acceptor,
                config.tls_config,
                idle_shutdown_kicker.clone(),
                replication_service, // internal replicaton service
            ));
        }

        let shutdown_timeout = self.shutdown_timeout.clone();
        let shutdown = self.shutdown.clone();
        let service_shutdown = Arc::new(Notify::new());
        // setup user-facing rpc services
        match db_kind {
            DatabaseKind::Primary => {
                // The migration scheduler is only useful on the primary
                let meta_conn = metastore_conn_maker()?;
                let scheduler = Scheduler::new(namespace_store.clone(), meta_conn).await?;
                task_manager.spawn_until_shutdown(async move {
                    scheduler.run(scheduler_receiver).await;
                    Ok(())
                });

                if self.disable_namespaces {
                    namespace_store
                        .create(
                            NamespaceName::default(),
                            namespace::RestoreOption::Latest,
                            Default::default(),
                        )
                        .await?;
                }

                let replication_svc = make_replication_svc(
                    namespace_store.clone(),
                    Some(user_auth_strategy.clone()),
                    idle_shutdown_kicker.clone(),
                    true,
                    false, // external replication service
                );

                let proxy_svc = ProxyService::new(
                    namespace_store.clone(),
                    Some(user_auth_strategy.clone()),
                    self.disable_namespaces,
                );

                // Garbage collect proxy clients every 30 seconds
                task_manager.spawn_until_shutdown({
                    let clients = proxy_svc.clients();
                    async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            rpc::proxy::garbage_collect(&mut *clients.write().await).await;
                        }
                    }
                });

                self.make_services(
                    namespace_store.clone(),
                    idle_shutdown_kicker,
                    proxy_svc,
                    replication_svc,
                    user_auth_strategy.clone(),
                )
                .configure(&mut task_manager);
            }
            DatabaseKind::Replica => {
                let (channel, uri) = client_config.clone().unwrap();
                let replication_svc = ReplicationLogProxyService::new(channel.clone(), uri.clone());
                let proxy_svc = ReplicaProxyService::new(
                    channel,
                    uri,
                    namespace_store.clone(),
                    user_auth_strategy.clone(),
                    self.disable_namespaces,
                );

                self.make_services(
                    namespace_store.clone(),
                    idle_shutdown_kicker,
                    proxy_svc,
                    replication_svc,
                    user_auth_strategy,
                )
                .configure(&mut task_manager);
            }
        };

        tokio::select! {
            _ = shutdown.notified() => {
                let shutdown = async {
                    namespace_store.shutdown().await?;
                    task_manager.shutdown().await?;
                    // join_set.shutdown().await;
                    service_shutdown.notify_waiters();

                    Ok::<_, crate::Error>(())
                };

                match tokio::time::timeout(shutdown_timeout, shutdown).await {
                    Ok(Ok(())) =>  {
                        tracing::info!("sqld was shutdown gracefully. Bye!");
                    }
                    Ok(Err(e)) => {
                        tracing::error!("failed to shutdown gracefully: {}", e);
                        std::process::exit(1);
                    },
                    Err(_) => {
                        tracing::error!("shutdown timeout hit, forcefully shutting down");
                        std::process::exit(1);
                    },

                }
            }
            res = task_manager.join_next() => {
                res?;
            },
            else => (),
        }

        Ok(())
    }

    async fn make_configurators_and_replication_svc(
        &self,
        base_config: BaseNamespaceConfig,
        client_config: Option<(Channel, Uri)>,
        task_manager: &mut TaskManager,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
        meta_store: MetaStore,
    ) -> anyhow::Result<(NamespaceConfigurators, MakeReplicationSvc)> {
        let wal_path = base_config.base_path.join("wals");
        let enable_libsql_wal_test = {
            let is_primary = self.rpc_server_config.is_some();
            let is_libsql_wal_test = std::env::var("LIBSQL_WAL_TEST").is_ok();
            is_primary && is_libsql_wal_test
        };
        let use_libsql_wal =
            self.use_custom_wal == Some(CustomWAL::LibsqlWal) || enable_libsql_wal_test;
        if !use_libsql_wal {
            if wal_path.try_exists()? {
                anyhow::bail!("database was previously setup to use libsql-wal");
            }
        }

        #[cfg(feature = "durable-wal")]
        if let Some(CustomWAL::DurableWal) = self.use_custom_wal {
            if self.db_config.bottomless_replication.is_some() {
                anyhow::bail!("bottomless not supported with durable WAL");
            }
        }

        match self.use_custom_wal {
            Some(CustomWAL::LibsqlWal) => {
                self.libsql_wal_configurators(
                    base_config,
                    client_config,
                    task_manager,
                    migration_scheduler_handle,
                    scripted_backup,
                    wal_path,
                    meta_store,
                )
                .await
            }
            #[cfg(feature = "durable-wal")]
            Some(CustomWAL::DurableWal) => self.durable_wal_configurators(
                base_config,
                client_config,
                migration_scheduler_handle,
                scripted_backup,
            ),
            None => {
                self.legacy_configurators(
                    base_config,
                    client_config,
                    migration_scheduler_handle,
                    scripted_backup,
                )
                .await
            }
        }
    }

    async fn libsql_wal_configurators(
        &self,
        base_config: BaseNamespaceConfig,
        client_config: Option<(Channel, Uri)>,
        task_manager: &mut TaskManager,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
        wal_path: PathBuf,
        meta_store: MetaStore,
    ) -> anyhow::Result<(NamespaceConfigurators, MakeReplicationSvc)> {
        tracing::info!("using libsql wal");
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let storage: Arc<_> = if let Some(ref opt) = self.db_config.bottomless_replication {
            if client_config.is_some() {
                anyhow::bail!("bottomless cannot be enabled on replicas");
            }

            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

            let http_client = HyperClientBuilder::new().build(self.connector.clone().unwrap());
            let mut builder = config.into_builder();
            builder.set_http_client(Some(http_client));
            builder.set_endpoint_url(opt.aws_endpoint.clone());
            builder.set_retry_config(RetryConfig::standard().with_max_attempts(10).into());
            builder.set_region(Region::new(
                opt.region.clone().expect("expected aws region"),
            ));
            let cred = Credentials::new(
                opt.access_key_id.as_ref().unwrap(),
                opt.secret_access_key.as_ref().unwrap(),
                None,
                None,
                "Static",
            );
            builder.set_credentials_provider(Some(SharedCredentialsProvider::new(cred)));
            let config = builder.build();
            let backend = S3Backend::from_sdk_config(
                config,
                opt.bucket_name.clone(),
                opt.db_id.clone().expect("expected db id"),
            )
            .await?;
            let config = AsyncStorageInitConfig {
                backend: Arc::new(backend),
                max_in_flight_jobs: 16,
            };
            let (storage, storage_loop) = AsyncStorage::new(config).await;

            task_manager.spawn_with_shutdown_notify(|_| async move {
                storage_loop.run().await;
                Ok(())
            });

            Either::A(storage)
        } else {
            Either::B(NoStorage)
        }
        .into();

        let primary_config = PrimaryConfig {
            max_log_size: self.db_config.max_log_size,
            max_log_duration: self.db_config.max_log_duration.map(Duration::from_secs_f32),
            bottomless_replication: self.db_config.bottomless_replication.clone(),
            scripted_backup,
            checkpoint_interval: self.db_config.checkpoint_interval,
        };

        // perform migration before creating the actual registry creation
        let did_migrate = self
            .maybe_migrate_bottomless(meta_store.clone(), &base_config, &primary_config)
            .await?;

        if self.rpc_server_config.is_some() && matches!(*storage, Either::B(_)) {
            anyhow::bail!("replication without bottomless not supported yet");
        }

        let registry = Arc::new(WalRegistry::new(wal_path, storage, sender)?);
        let checkpointer = LibsqlCheckpointer::new(registry.clone(), receiver, 8);
        task_manager.spawn_with_shutdown_notify(|_| async move {
            checkpointer.run().await;
            Ok(())
        });

        // If we performed a migration from bottomless to libsql-wal earlier, then we need to
        // forecefully load all the wals, to trigger segment storage with the actual storage. This
        // is because migration didn't actually send anything to storage, but just created the
        // segments.
        if did_migrate || self.should_sync_from_storage || self.force_load_wals {
            // eagerly load all namespaces, then call sync_all on the registry
            // TODO: do conccurently
            let dbs_path = base_config.base_path.join("dbs");
            let stream = meta_store.namespaces();
            tokio::pin!(stream);
            while let Some(conf) = stream.next().await {
                let registry = registry.clone();
                let namespace = conf.namespace().clone();
                let path = dbs_path.join(namespace.as_str());
                tokio::fs::create_dir_all(&path).await?;
                tokio::task::spawn_blocking(move || {
                    registry.open(&path.join("data"), &namespace.into())
                })
                .await
                .unwrap()?;
            }

            if self.should_sync_from_storage {
                registry.sync_all(self.sync_conccurency).await?;
            }
        }

        let namespace_resolver = Arc::new(|path: &Path| {
            NamespaceName::from_string(
                path.parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
            )
            .unwrap()
            .into()
        });

        task_manager.spawn_with_shutdown_notify(|shutdown| {
            let registry = registry.clone();
            async move {
                shutdown.notified().await;
                registry.shutdown().await?;
                Ok(())
            }
        });

        let make_replication_svc = Box::new({
            let registry = registry.clone();
            let disable_namespaces = self.disable_namespaces;
            move |store, user_auth, _, _, _| -> BoxReplicationService {
                Box::new(LibsqlReplicationService::new(
                    registry.clone(),
                    store,
                    user_auth,
                    disable_namespaces,
                ))
            }
        });
        let mut configurators = NamespaceConfigurators::empty();

        match client_config {
            // configure replica
            Some((channel, uri)) => {
                let replica_configurator = LibsqlReplicaConfigurator::new(
                    base_config,
                    registry.clone(),
                    uri,
                    channel,
                    namespace_resolver,
                );
                configurators.with_replica(replica_configurator);
            }
            // configure primary
            None => {
                let primary_configurator = LibsqlPrimaryConfigurator::new(
                    base_config.clone(),
                    primary_config.clone(),
                    registry.clone(),
                    namespace_resolver.clone(),
                );

                let schema_configurator = LibsqlSchemaConfigurator::new(
                    base_config,
                    primary_config,
                    migration_scheduler_handle,
                    registry,
                    namespace_resolver,
                );

                configurators.with_primary(primary_configurator);
                configurators.with_schema(schema_configurator);
            }
        }

        Ok((configurators, make_replication_svc))
    }

    #[cfg(feature = "durable-wal")]
    fn durable_wal_configurators(
        &self,
        base_config: BaseNamespaceConfig,
        client_config: Option<(Channel, Uri)>,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
    ) -> anyhow::Result<(NamespaceConfigurators, MakeReplicationSvc)> {
        tracing::info!("using durable wal");
        let lock_manager = Arc::new(std::sync::Mutex::new(LockManager::new()));
        let namespace_resolver = |path: &Path| {
            NamespaceName::from_string(
                path.parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
            )
            .unwrap()
            .into()
        };
        let wal = DurableWalManager::new(
            lock_manager,
            namespace_resolver,
            self.storage_server_address.clone(),
        );
        let make_wal_manager = Arc::new(move || EitherWAL::C(wal.clone()));
        let configurators = self.configurators_common(
            base_config,
            client_config,
            make_wal_manager,
            migration_scheduler_handle,
            scripted_backup,
        )?;

        let make_replication_svc = Box::new({
            let disable_namespaces = self.disable_namespaces;
            move |store,
                  client_auth,
                  idle_shutdown,
                  collect_stats,
                  is_internal|
                  -> BoxReplicationService {
                Box::new(ReplicationLogService::new(
                    store,
                    idle_shutdown,
                    client_auth,
                    disable_namespaces,
                    collect_stats,
                    is_internal,
                ))
            }
        });

        Ok((configurators, make_replication_svc))
    }

    async fn legacy_configurators(
        &self,
        base_config: BaseNamespaceConfig,
        client_config: Option<(Channel, Uri)>,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
    ) -> anyhow::Result<(NamespaceConfigurators, MakeReplicationSvc)> {
        let make_wal_manager = Arc::new(|| EitherWAL::A(Sqlite3WalManager::default()));
        let configurators = self.configurators_common(
            base_config,
            client_config,
            make_wal_manager,
            migration_scheduler_handle,
            scripted_backup,
        )?;

        let make_replication_svc = Box::new({
            let disable_namespaces = self.disable_namespaces;
            move |store,
                  client_auth,
                  idle_shutdown,
                  collect_stats,
                  is_internal|
                  -> BoxReplicationService {
                Box::new(ReplicationLogService::new(
                    store,
                    idle_shutdown,
                    client_auth,
                    disable_namespaces,
                    collect_stats,
                    is_internal,
                ))
            }
        });

        Ok((configurators, make_replication_svc))
    }

    fn configurators_common(
        &self,
        base_config: BaseNamespaceConfig,
        client_config: Option<(Channel, Uri)>,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
    ) -> anyhow::Result<NamespaceConfigurators> {
        let mut configurators = NamespaceConfigurators::empty();
        match client_config {
            // replica mode
            Some((channel, uri)) => {
                let replica_configurator =
                    ReplicaConfigurator::new(base_config, channel, uri, make_wal_manager);
                configurators.with_replica(replica_configurator);
            }
            // primary mode
            None => self.configure_primary_common(
                base_config,
                &mut configurators,
                make_wal_manager,
                migration_scheduler_handle,
                scripted_backup,
            ),
        }

        Ok(configurators)
    }

    fn configure_primary_common(
        &self,
        base_config: BaseNamespaceConfig,
        configurators: &mut NamespaceConfigurators,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
        migration_scheduler_handle: SchedulerHandle,
        scripted_backup: Option<ScriptBackupManager>,
    ) {
        let primary_config = PrimaryConfig {
            max_log_size: self.db_config.max_log_size,
            max_log_duration: self.db_config.max_log_duration.map(Duration::from_secs_f32),
            bottomless_replication: self.db_config.bottomless_replication.clone(),
            scripted_backup,
            checkpoint_interval: self.db_config.checkpoint_interval,
        };

        let primary_configurator = PrimaryConfigurator::new(
            base_config.clone(),
            primary_config.clone(),
            make_wal_manager.clone(),
        );

        let schema_configurator = SchemaConfigurator::new(
            base_config.clone(),
            primary_config,
            make_wal_manager.clone(),
            migration_scheduler_handle,
        );

        configurators.with_schema(schema_configurator);
        configurators.with_primary(primary_configurator);
    }

    fn setup_shutdown(&self) -> Option<IdleShutdownKicker> {
        let shutdown_notify = self.shutdown.clone();
        self.idle_shutdown_timeout.map(|d| {
            IdleShutdownKicker::new(d, self.initial_idle_shutdown_timeout, shutdown_notify)
        })
    }

    async fn get_client_config(&self) -> anyhow::Result<Option<(Channel, hyper::Uri)>> {
        match self.rpc_client_config {
            Some(ref config) => Ok(Some(config.configure().await?)),
            None => Ok(None),
        }
    }

    /// perform migration from bottomless_wal to libsql_wal if necessary. This only happens if
    /// all:
    /// - bottomless is enabled
    /// - this is a primary
    /// - we are operating in libsql-wal mode
    /// - migrate_bottomless flag is raised
    /// - there hasn't been a previous successfull migration (wals directory is either absent,
    /// or emtpy)
    /// returns whether the migration was performed
    async fn maybe_migrate_bottomless(
        &self,
        meta_store: MetaStore,
        base_config: &BaseNamespaceConfig,
        primary_config: &PrimaryConfig,
    ) -> anyhow::Result<bool> {
        let is_primary = self.rpc_client_config.is_none();
        if self.migrate_bottomless && is_primary {
            let is_previous_migration_successful = self.check_previous_migration_success()?;
            let is_libsql_wal = matches!(self.use_custom_wal, Some(CustomWAL::LibsqlWal));
            let is_bottomless_enabled = self.db_config.bottomless_replication.is_some();
            let should_attempt_migration =
                is_bottomless_enabled && !is_previous_migration_successful && is_libsql_wal;

            if should_attempt_migration {
                bottomless_migrate(meta_store, base_config.clone(), primary_config.clone()).await?;
                return Ok(true);
            } else {
                // the wals directory is present and so is the _dbs. This means that a crash occured
                // before we could remove it. clean it up now. see code in `bottomless_migrate.rs`
                let tmp_dbs_path = base_config.base_path.join("_dbs");
                if tmp_dbs_path.try_exists()? {
                    tracing::info!("removed dangling `_dbs` folder");
                    tokio::fs::remove_dir_all(&tmp_dbs_path).await?;
                }

                tracing::info!("bottomless already migrated, skipping...");
            }
        }

        Ok(false)
    }

    fn check_previous_migration_success(&self) -> anyhow::Result<bool> {
        let wals_path = self.path.join("wals");
        if !wals_path.try_exists()? {
            return Ok(false);
        }

        let dir = std::fs::read_dir(&wals_path)?;

        // wals dir exist and is not empty
        Ok(dir.count() != 0)
    }
}

/// Setup sqlite to use the same allocator as sqld.
/// the size of the allocation is stored as a usize before the returned pointer. A i32 would be
/// sufficient, but we need the returned pointer to be aligned to 8
fn setup_sqlite_alloc() {
    use std::alloc::GlobalAlloc;

    unsafe extern "C" fn malloc(size: i32) -> *mut c_void {
        let size_total = size as usize + size_of::<usize>();
        let layout = Layout::from_size_align(size_total, align_of::<usize>()).unwrap();
        let ptr = GLOBAL.alloc(layout);

        if ptr.is_null() {
            return std::ptr::null_mut();
        }

        *(ptr as *mut usize) = size as usize;
        ptr.offset(size_of::<usize>() as _) as *mut _
    }

    unsafe extern "C" fn free(ptr: *mut c_void) {
        let orig_ptr = ptr.offset(-(size_of::<usize>() as isize));
        let size = *(orig_ptr as *mut usize);
        let layout = Layout::from_size_align(size as usize, align_of::<usize>()).unwrap();
        GLOBAL.dealloc(orig_ptr as *mut _, layout);
    }

    unsafe extern "C" fn realloc(ptr: *mut c_void, new_size: i32) -> *mut c_void {
        let orig_ptr = ptr.offset(-(size_of::<usize>() as isize));
        let orig_size = *(orig_ptr as *mut usize);
        let layout =
            Layout::from_size_align(orig_size + size_of::<usize>(), align_of::<usize>()).unwrap();
        let new_ptr = GLOBAL.realloc(
            orig_ptr as *mut _,
            layout,
            new_size as usize + size_of::<usize>(),
        );

        if ptr.is_null() {
            return std::ptr::null_mut();
        }

        *(new_ptr as *mut usize) = new_size as usize;
        new_ptr.offset(size_of::<usize>() as _) as *mut _
    }

    unsafe extern "C" fn size(ptr: *mut c_void) -> i32 {
        let orig_ptr = ptr.offset(-(size_of::<usize>() as isize));
        *(orig_ptr as *mut usize) as i32
    }

    unsafe extern "C" fn init(_: *mut c_void) -> i32 {
        0
    }

    unsafe extern "C" fn shutdown(_: *mut c_void) {}

    unsafe extern "C" fn roundup(n: i32) -> i32 {
        (n as usize).next_multiple_of(align_of::<usize>()) as i32
    }

    let mem = rusqlite::ffi::sqlite3_mem_methods {
        xMalloc: Some(malloc),
        xFree: Some(free),
        xRealloc: Some(realloc),
        xSize: Some(size),
        xRoundup: Some(roundup),
        xInit: Some(init),
        xShutdown: Some(shutdown),
        pAppData: std::ptr::null_mut(),
    };

    unsafe {
        sqlite3_config(SQLITE_CONFIG_MALLOC, &mem as *const _);
    }
}
