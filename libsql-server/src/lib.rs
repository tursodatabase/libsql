#![allow(clippy::type_complexity, clippy::too_many_arguments)]

use std::alloc::Layout;
use std::ffi::c_void;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::pin::Pin;
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
use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLog;
use crate::rpc::replication_log::ReplicationLogService;
use crate::rpc::replication_log_proxy::ReplicationLogProxyService;
use crate::rpc::run_rpc_server;
use crate::schema::Scheduler;
use crate::stats::Stats;
use anyhow::Context as AnyhowContext;
use auth::Auth;
use config::{
    AdminApiConfig, DbConfig, HeartbeatConfig, RpcClientConfig, RpcServerConfig, UserApiConfig,
};
use futures::future::ready;
use futures::Future;
use http::user::UserApi;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
#[cfg(feature = "durable-wal")]
use libsql_storage::{DurableWalManager, LockManager};
#[cfg(not(feature = "durable-wal"))]
use libsql_sys::wal::either::Either as EitherWAL;
#[cfg(feature = "durable-wal")]
use libsql_sys::wal::either::Either3 as EitherWAL;
use libsql_sys::wal::Sqlite3WalManager;
use libsql_wal::registry::WalRegistry;
use libsql_wal::storage::NoStorage;
use libsql_wal::wal::LibsqlWalManager;
use namespace::meta_store::MetaStoreHandle;
use namespace::{NamespaceConfig, NamespaceName};
use net::Connector;
use once_cell::sync::Lazy;
use rusqlite::ffi::SQLITE_CONFIG_MALLOC;
use rusqlite::ffi::{sqlite3_config, SQLITE_CONFIG_PCACHE2};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Duration;
use url::Url;
use utils::services::idle_shutdown::IdleShutdownKicker;

use self::config::MetaStoreConfig;
use self::connection::connection_manager::InnerWalManager;
use self::namespace::NamespaceStore;
use self::net::AddrIncoming;
use self::replication::script_backup_manager::{CommandHandler, ScriptBackupManager};

pub mod auth;
mod broadcaster;
pub mod config;
pub mod connection;
pub mod net;
pub mod rpc;
pub mod version;

pub use hrana::proto as hrana_proto;

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
    path: Arc<Path>,
    shutdown: Arc<Notify>,
}

impl<A, P, S, C> Services<A, P, S, C>
where
    A: crate::net::Accept,
    P: Proxy,
    S: ReplicationLog,
    C: Connector,
{
    fn configure(self, join_set: &mut JoinSet<anyhow::Result<()>>) {
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
            path: self.path.clone(),
            shutdown: self.shutdown.clone(),
        };

        let user_http_service = user_http.configure(join_set);

        if let Some(AdminApiConfig {
            acceptor,
            connector,
            disable_metrics,
        }) = self.admin_api_config
        {
            let shutdown = self.shutdown.clone();
            join_set.spawn(http::admin::run(
                acceptor,
                user_http_service,
                self.namespace_store,
                connector,
                disable_metrics,
                shutdown,
            ));
        }
    }
}

#[tracing::instrument(skip(connection_maker))]
async fn run_periodic_checkpoint<C>(
    connection_maker: Arc<C>,
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
        retry = match connection_maker.create().await {
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
        join_set: &mut JoinSet<anyhow::Result<()>>,
        stats_receiver: mpsc::Receiver<(NamespaceName, MetaStoreHandle, Weak<Stats>)>,
    ) -> anyhow::Result<()> {
        match self.heartbeat_config {
            Some(ref config) => {
                tracing::info!(
                    "Server sending heartbeat to URL {} every {:?}",
                    config.heartbeat_url.as_deref().unwrap_or("<not supplied>"),
                    config.heartbeat_period,
                );
                join_set.spawn({
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
        self,
        namespace_store: NamespaceStore,
        idle_shutdown_kicker: Option<IdleShutdownKicker>,
        proxy_service: P,
        replication_service: L,
        user_auth_strategy: Auth,
        shutdown: Arc<Notify>,
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
            path: self.path.clone(),
            shutdown,
        }
    }

    pub async fn start(mut self) -> anyhow::Result<()> {
        static INIT: std::sync::Once = std::sync::Once::new();
        let mut join_set = JoinSet::new();

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

        let service_shutdown = Arc::new(Notify::new());
        let db_kind = if self.rpc_client_config.is_some() {
            DatabaseKind::Replica
        } else {
            DatabaseKind::Primary
        };

        let scripted_backup = match self.db_config.snapshot_exec {
            Some(ref command) => {
                let (scripted_backup, script_backup_task) =
                    ScriptBackupManager::new(&self.path, CommandHandler::new(command.to_string()))
                        .await?;
                join_set.spawn(script_backup_task.run());
                Some(scripted_backup)
            }
            None => None,
        };

        let (channel, uri) = match self.rpc_client_config {
            Some(ref config) => {
                let (channel, uri) = config.configure().await?;
                (Some(channel), Some(uri))
            }
            None => (None, None),
        };

        let (scheduler_sender, scheduler_receiver) = mpsc::channel(128);

        let (stats_sender, stats_receiver) = mpsc::channel(1024);

        // chose the wal backend
        let (make_wal_manager, registry_shutdown) = self.configure_wal_manager()?;

        let ns_config = NamespaceConfig {
            db_kind,
            base_path: self.path.clone(),
            max_log_size: self.db_config.max_log_size,
            max_log_duration: self.db_config.max_log_duration.map(Duration::from_secs_f32),
            bottomless_replication: self.db_config.bottomless_replication.clone(),
            extensions,
            stats_sender: stats_sender.clone(),
            max_response_size: self.db_config.max_response_size,
            max_total_response_size: self.db_config.max_total_response_size,
            checkpoint_interval: self.db_config.checkpoint_interval,
            encryption_config: self.db_config.encryption_config.clone(),
            max_concurrent_connections: Arc::new(Semaphore::new(self.max_concurrent_connections)),
            scripted_backup,
            max_concurrent_requests: self.db_config.max_concurrent_requests,
            channel: channel.clone(),
            uri: uri.clone(),
            migration_scheduler: scheduler_sender.into(),
            make_wal_manager,
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
        )
        .await?;
        let namespace_store: NamespaceStore = NamespaceStore::new(
            db_kind.is_replica(),
            self.db_config.snapshot_at_shutdown,
            self.max_active_namespaces,
            ns_config,
            meta_store,
        )
        .await?;

        let meta_conn = metastore_conn_maker()?;
        let scheduler = Scheduler::new(namespace_store.clone(), meta_conn).await?;

        join_set.spawn(async move {
            scheduler.run(scheduler_receiver).await;
            Ok(())
        });

        self.spawn_monitoring_tasks(&mut join_set, stats_receiver)?;

        // eagerly load the default namespace when namespaces are disabled
        if self.disable_namespaces && db_kind.is_primary() {
            namespace_store
                .create(
                    NamespaceName::default(),
                    namespace::RestoreOption::Latest,
                    Default::default(),
                )
                .await?;
        }

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
            join_set.spawn({
                let clients = proxy_service.clients();
                async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        rpc::proxy::garbage_collect(&mut *clients.write().await).await;
                    }
                }
            });
            join_set.spawn(run_rpc_server(
                proxy_service,
                config.acceptor,
                config.tls_config,
                idle_shutdown_kicker.clone(),
                namespace_store.clone(),
                self.disable_namespaces,
            ));
        }

        let shutdown_timeout = self.shutdown_timeout.clone();
        let shutdown = self.shutdown.clone();
        // setup user-facing rpc services
        match db_kind {
            DatabaseKind::Primary => {
                let replication_svc = ReplicationLogService::new(
                    namespace_store.clone(),
                    idle_shutdown_kicker.clone(),
                    Some(user_auth_strategy.clone()),
                    self.disable_namespaces,
                    true,
                );

                let proxy_svc = ProxyService::new(
                    namespace_store.clone(),
                    Some(user_auth_strategy.clone()),
                    self.disable_namespaces,
                );

                // Garbage collect proxy clients every 30 seconds
                join_set.spawn({
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
                    service_shutdown.clone(),
                )
                .configure(&mut join_set);
            }
            DatabaseKind::Replica => {
                let replication_svc =
                    ReplicationLogProxyService::new(channel.clone().unwrap(), uri.clone().unwrap());
                let proxy_svc = ReplicaProxyService::new(
                    channel.clone().unwrap(),
                    uri.clone().unwrap(),
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
                    service_shutdown.clone(),
                )
                .configure(&mut join_set);
            }
        };

        tokio::select! {
            _ = shutdown.notified() => {
                let shutdown = async {
                    join_set.shutdown().await;
                    service_shutdown.notify_waiters();
                    namespace_store.shutdown().await?;
                    registry_shutdown.await?;

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
            Some(res) = join_set.join_next() => {
                res??;
            },
            else => (),
        }

        Ok(())
    }

    fn setup_shutdown(&self) -> Option<IdleShutdownKicker> {
        let shutdown_notify = self.shutdown.clone();
        self.idle_shutdown_timeout.map(|d| {
            IdleShutdownKicker::new(d, self.initial_idle_shutdown_timeout, shutdown_notify)
        })
    }

    fn configure_wal_manager(
        &self,
    ) -> anyhow::Result<(
        Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
        Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Sync + 'static>>,
    )> {
        let wal_path = self.path.join("wals");
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

        if self.use_custom_wal.is_some() {
            if self.db_config.bottomless_replication.is_some() {
                anyhow::bail!("bottomless not supported with custom WAL");
            }
            if self.rpc_client_config.is_some() {
                anyhow::bail!("custom WAL not supported in replica mode");
            }
        }

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

        match self.use_custom_wal {
            Some(CustomWAL::LibsqlWal) => {
                let registry = Arc::new(WalRegistry::new(wal_path, NoStorage)?);

                let wal = LibsqlWalManager::new(registry.clone(), Arc::new(namespace_resolver));
                let shutdown_notify = self.shutdown.clone();
                let shutdown_fut = Box::pin(async move {
                    shutdown_notify.notified().await;
                    tokio::task::spawn_blocking(move || registry.shutdown())
                        .await
                        .unwrap()?;
                    Ok(())
                });

                tracing::info!("using libsql wal");
                Ok((Arc::new(move || EitherWAL::B(wal.clone())), shutdown_fut))
            }
            #[cfg(feature = "durable-wal")]
            Some(CustomWAL::DurableWal) => {
                tracing::info!("using durable wal");
                let lock_manager = Arc::new(std::sync::Mutex::new(LockManager::new()));
                let wal = DurableWalManager::new(
                    lock_manager,
                    namespace_resolver,
                    self.storage_server_address.clone(),
                );
                Ok((
                    Arc::new(move || EitherWAL::C(wal.clone())),
                    Box::pin(ready(Ok(()))),
                ))
            }
            None => {
                tracing::info!("using sqlite3 wal");
                Ok((
                    Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
                    Box::pin(ready(Ok(()))),
                ))
            }
        }
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
