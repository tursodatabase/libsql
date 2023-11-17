#![allow(clippy::type_complexity, clippy::too_many_arguments)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Weak};

use crate::auth::Auth;
use crate::connection::{Connection, MakeConnection};
use crate::error::Error;
use crate::metrics::DIRTY_STARTUP;
use crate::migration::maybe_migrate;
use crate::net::Accept;
use crate::rpc::proxy::rpc::proxy_server::Proxy;
use crate::rpc::proxy::ProxyService;
use crate::rpc::replica_proxy::ReplicaProxyService;
use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLog;
use crate::rpc::replication_log::ReplicationLogService;
use crate::rpc::replication_log_proxy::ReplicationLogProxyService;
use crate::rpc::run_rpc_server;
use crate::stats::Stats;
use anyhow::Context as AnyhowContext;
use config::{
    AdminApiConfig, DbConfig, HeartbeatConfig, RpcClientConfig, RpcServerConfig, UserApiConfig,
};
use http::user::UserApi;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use namespace::{
    MakeNamespace, NamespaceBottomlessDbId, NamespaceName, NamespaceStore, PrimaryNamespaceConfig,
    PrimaryNamespaceMaker, ReplicaNamespaceConfig, ReplicaNamespaceMaker,
};
use net::Connector;
use once_cell::sync::Lazy;
use replication::NamespacedSnapshotCallback;
pub use sqld_libsql_bindings as libsql_bindings;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tokio::time::Duration;
use url::Url;
use utils::services::idle_shutdown::IdleShutdownKicker;

use self::net::AddrIncoming;

pub mod config;
pub mod connection;
pub mod net;
pub mod rpc;
pub mod version;

pub use hrana::proto as hrana_proto;

mod auth;
mod database;
mod error;
mod h2c;
mod heartbeat;
mod hrana;
mod http;
mod metrics;
mod migration;
mod namespace;
mod query;
mod query_analysis;
mod query_result_builder;
mod replication;
mod stats;
#[cfg(test)]
mod test;
mod utils;

const MAX_CONCURRENT_DBS: usize = 128;
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
type StatsSender = mpsc::Sender<(NamespaceName, Weak<Stats>)>;

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
        }
    }
}

struct Services<M: MakeNamespace, A, P, S, C> {
    namespaces: NamespaceStore<M>,
    idle_shutdown_kicker: Option<IdleShutdownKicker>,
    proxy_service: P,
    replication_service: S,
    user_api_config: UserApiConfig<A>,
    admin_api_config: Option<AdminApiConfig<A, C>>,
    disable_namespaces: bool,
    disable_default_namespace: bool,
    db_config: DbConfig,
    auth: Arc<Auth>,
    path: Arc<Path>,
    shutdown: Arc<Notify>,
}

impl<M, A, P, S, C> Services<M, A, P, S, C>
where
    M: MakeNamespace,
    A: crate::net::Accept,
    P: Proxy,
    S: ReplicationLog,
    C: Connector,
{
    fn configure(self, join_set: &mut JoinSet<anyhow::Result<()>>) {
        let user_http = UserApi {
            http_acceptor: self.user_api_config.http_acceptor,
            hrana_ws_acceptor: self.user_api_config.hrana_ws_acceptor,
            auth: self.auth,
            namespaces: self.namespaces.clone(),
            idle_shutdown_kicker: self.idle_shutdown_kicker.clone(),
            proxy_service: self.proxy_service,
            replication_service: self.replication_service,
            disable_default_namespace: self.disable_default_namespace,
            disable_namespaces: self.disable_namespaces,
            max_response_size: self.db_config.max_response_size,
            enable_console: self.user_api_config.enable_http_console,
            self_url: self.user_api_config.self_url,
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
                self.namespaces,
                connector,
                disable_metrics,
                shutdown,
            ));
        }
    }
}

async fn run_periodic_checkpoint<C>(
    connection_maker: Arc<C>,
    period: Duration,
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

fn sentinel_file_path(path: &Path) -> PathBuf {
    path.join(".sentinel")
}

/// initialize the sentinel file. This file is created at the beginning of the process, and is
/// deleted at the end, on a clean exit. If the file is present when we start the process, this
/// means that the database was not shutdown properly, and might need repair. This function return
/// `true` if the database is dirty and needs repair.
fn init_sentinel_file(path: &Path) -> anyhow::Result<bool> {
    let path = sentinel_file_path(path);
    if path.try_exists()? {
        DIRTY_STARTUP.increment(1);
        tracing::warn!(
            "sentinel file found: sqld was not shutdown gracefully, namespaces will be recovered."
        );
        return Ok(true);
    }

    std::fs::File::create(path)?;

    Ok(false)
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
        if self.db_config.bottomless_replication.is_some() {
            bottomless::static_init::register_bottomless_methods();
        }

        if let Some(soft_limit_mb) = self.db_config.soft_heap_limit_mb {
            tracing::warn!("Setting soft heap limit to {soft_limit_mb}MiB");
            unsafe {
                sqld_libsql_bindings::ffi::sqlite3_soft_heap_limit64(
                    soft_limit_mb as i64 * 1024 * 1024,
                )
            };
        }
        if let Some(hard_limit_mb) = self.db_config.hard_heap_limit_mb {
            tracing::warn!("Setting hard heap limit to {hard_limit_mb}MiB");
            unsafe {
                sqld_libsql_bindings::ffi::sqlite3_hard_heap_limit64(
                    hard_limit_mb as i64 * 1024 * 1024,
                )
            };
        }
    }

    pub fn make_snapshot_callback(&self) -> NamespacedSnapshotCallback {
        let snapshot_exec = self.db_config.snapshot_exec.clone();
        Arc::new(move |snapshot_file: &Path, namespace: &NamespaceName| {
            if let Some(exec) = snapshot_exec.as_ref() {
                let status = Command::new(exec)
                    .arg(snapshot_file)
                    .arg(namespace.as_str())
                    .status()?;
                anyhow::ensure!(
                    status.success(),
                    "Snapshot exec process failed with status {status}"
                );
            }
            Ok(())
        })
    }

    fn spawn_monitoring_tasks(
        &self,
        join_set: &mut JoinSet<anyhow::Result<()>>,
        stats_receiver: mpsc::Receiver<(NamespaceName, Weak<Stats>)>,
        namespaces: NamespaceStore<impl MakeNamespace>,
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
                        Some(Url::from_str(&url).context("invalid heartbeat URL")?)
                    } else {
                        None
                    };
                    async move {
                        heartbeat::server_heartbeat(
                            heartbeat_url,
                            heartbeat_auth,
                            heartbeat_period,
                            stats_receiver,
                            namespaces,
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

    pub async fn start(mut self) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();

        init_version_file(&self.path)?;
        maybe_migrate(&self.path)?;
        self.init_sqlite_globals();
        let db_is_dirty = init_sentinel_file(&self.path)?;
        let idle_shutdown_kicker = self.setup_shutdown();

        let snapshot_callback = self.make_snapshot_callback();
        let auth = self.user_api_config.get_auth().map(Arc::new)?;
        let extensions = self.db_config.validate_extensions()?;
        let namespace_store_shutdown_fut: Pin<Box<dyn Future<Output = Result<()>> + Send>>;

        match self.rpc_client_config {
            Some(rpc_config) => {
                let (stats_sender, stats_receiver) = mpsc::channel(8);
                let replica = Replica {
                    rpc_config,
                    stats_sender,
                    extensions,
                    db_config: self.db_config.clone(),
                    base_path: self.path.clone(),
                    auth: auth.clone(),
                };
                let (namespaces, proxy_service, replication_service) = replica.configure().await?;
                self.rpc_client_config = None;
                self.spawn_monitoring_tasks(&mut join_set, stats_receiver, namespaces.clone())?;
                namespace_store_shutdown_fut = {
                    let namespaces = namespaces.clone();
                    Box::pin(async move { namespaces.shutdown().await })
                };

                let services = Services {
                    namespaces,
                    idle_shutdown_kicker,
                    proxy_service,
                    replication_service,
                    user_api_config: self.user_api_config,
                    admin_api_config: self.admin_api_config,
                    disable_namespaces: self.disable_namespaces,
                    disable_default_namespace: self.disable_default_namespace,
                    db_config: self.db_config,
                    auth,
                    path: self.path.clone(),
                    shutdown: self.shutdown.clone(),
                };

                services.configure(&mut join_set);
            }
            None => {
                let (stats_sender, stats_receiver) = mpsc::channel(8);
                let primary = Primary {
                    rpc_config: self.rpc_server_config,
                    db_config: self.db_config.clone(),
                    idle_shutdown_kicker: idle_shutdown_kicker.clone(),
                    stats_sender,
                    db_is_dirty,
                    snapshot_callback,
                    extensions,
                    base_path: self.path.clone(),
                    disable_namespaces: self.disable_namespaces,
                    join_set: &mut join_set,
                    auth: auth.clone(),
                };
                let (namespaces, proxy_service, replication_service) = primary.configure().await?;
                self.rpc_server_config = None;
                self.spawn_monitoring_tasks(&mut join_set, stats_receiver, namespaces.clone())?;
                namespace_store_shutdown_fut = {
                    let namespaces = namespaces.clone();
                    Box::pin(async move { namespaces.shutdown().await })
                };

                let services = Services {
                    namespaces,
                    idle_shutdown_kicker,
                    proxy_service,
                    replication_service,
                    user_api_config: self.user_api_config,
                    admin_api_config: self.admin_api_config,
                    disable_namespaces: self.disable_namespaces,
                    disable_default_namespace: self.disable_default_namespace,
                    db_config: self.db_config,
                    auth,
                    path: self.path.clone(),
                    shutdown: self.shutdown.clone(),
                };

                services.configure(&mut join_set);
            }
        }

        tokio::select! {
            _ = self.shutdown.notified() => {
                join_set.shutdown().await;
                namespace_store_shutdown_fut.await?;
                // clean shutdown, remove sentinel file
                std::fs::remove_file(sentinel_file_path(&self.path))?;
                tracing::info!("sqld was shutdown gracefully. Bye!");
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
}

struct Primary<'a, A> {
    rpc_config: Option<RpcServerConfig<A>>,
    db_config: DbConfig,
    idle_shutdown_kicker: Option<IdleShutdownKicker>,
    stats_sender: StatsSender,
    db_is_dirty: bool,
    snapshot_callback: NamespacedSnapshotCallback,
    extensions: Arc<[PathBuf]>,
    base_path: Arc<Path>,
    disable_namespaces: bool,
    auth: Arc<Auth>,
    join_set: &'a mut JoinSet<anyhow::Result<()>>,
}

impl<A> Primary<'_, A>
where
    A: Accept,
{
    async fn configure(
        mut self,
    ) -> anyhow::Result<(
        NamespaceStore<PrimaryNamespaceMaker>,
        ProxyService,
        ReplicationLogService,
    )> {
        let conf = PrimaryNamespaceConfig {
            base_path: self.base_path,
            max_log_size: self.db_config.max_log_size,
            db_is_dirty: self.db_is_dirty,
            max_log_duration: self.db_config.max_log_duration.map(Duration::from_secs_f32),
            snapshot_callback: self.snapshot_callback,
            bottomless_replication: self.db_config.bottomless_replication,
            extensions: self.extensions,
            stats_sender: self.stats_sender.clone(),
            max_response_size: self.db_config.max_response_size,
            max_total_response_size: self.db_config.max_total_response_size,
            checkpoint_interval: self.db_config.checkpoint_interval,
            disable_namespace: self.disable_namespaces,
        };
        let factory = PrimaryNamespaceMaker::new(conf);
        let namespaces = NamespaceStore::new(factory, false);

        // eagerly load the default namespace when namespaces are disabled
        if self.disable_namespaces {
            namespaces
                .create(
                    NamespaceName::default(),
                    namespace::RestoreOption::Latest,
                    NamespaceBottomlessDbId::NotProvided,
                )
                .await?;
        }

        if let Some(config) = self.rpc_config.take() {
            let proxy_service =
                ProxyService::new(namespaces.clone(), None, self.disable_namespaces);
            // Garbage collect proxy clients every 30 seconds
            self.join_set.spawn({
                let clients = proxy_service.clients();
                async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        rpc::proxy::garbage_collect(&mut *clients.write().await).await;
                    }
                }
            });
            self.join_set.spawn(run_rpc_server(
                proxy_service,
                config.acceptor,
                config.tls_config,
                self.idle_shutdown_kicker.clone(),
                namespaces.clone(),
                self.disable_namespaces,
            ));
        }

        let logger_service = ReplicationLogService::new(
            namespaces.clone(),
            self.idle_shutdown_kicker,
            Some(self.auth.clone()),
            self.disable_namespaces,
        );

        let proxy_service =
            ProxyService::new(namespaces.clone(), Some(self.auth), self.disable_namespaces);
        // Garbage collect proxy clients every 30 seconds
        self.join_set.spawn({
            let clients = proxy_service.clients();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    rpc::proxy::garbage_collect(&mut *clients.write().await).await;
                }
            }
        });
        Ok((namespaces, proxy_service, logger_service))
    }
}

struct Replica<C> {
    rpc_config: RpcClientConfig<C>,
    stats_sender: StatsSender,
    extensions: Arc<[PathBuf]>,
    db_config: DbConfig,
    base_path: Arc<Path>,
    auth: Arc<Auth>,
}

impl<C: Connector> Replica<C> {
    async fn configure(
        self,
    ) -> anyhow::Result<(
        NamespaceStore<impl MakeNamespace>,
        impl Proxy,
        impl ReplicationLog,
    )> {
        let (channel, uri) = self.rpc_config.configure().await?;

        let conf = ReplicaNamespaceConfig {
            channel: channel.clone(),
            uri: uri.clone(),
            extensions: self.extensions.clone(),
            stats_sender: self.stats_sender.clone(),
            base_path: self.base_path,
            max_response_size: self.db_config.max_response_size,
            max_total_response_size: self.db_config.max_total_response_size,
        };
        let factory = ReplicaNamespaceMaker::new(conf);
        let namespaces = NamespaceStore::new(factory, true);
        let replication_service = ReplicationLogProxyService::new(channel.clone(), uri.clone());
        let proxy_service = ReplicaProxyService::new(channel, uri, self.auth.clone());

        Ok((namespaces, proxy_service, replication_service))
    }
}
