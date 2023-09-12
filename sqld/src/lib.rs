#![allow(clippy::type_complexity, clippy::too_many_arguments)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as AnyhowContext;
use bytes::Bytes;
use config::{
    AdminApiConfig, DbConfig, HeartbeatConfig, RpcClientConfig, RpcServerConfig, UserApiConfig,
};
use futures::never::Never;
use http::UserApi;
use hyper::client::HttpConnector;
use libsql::wal_hook::TRANSPARENT_METHODS;
use namespace::{
    MakeNamespace, NamespaceStore, PrimaryNamespaceConfig, PrimaryNamespaceMaker,
    ReplicaNamespaceConfig, ReplicaNamespaceMaker,
};
use net::Connector;
use replication::NamespacedSnapshotCallback;
use rpc::proxy::rpc::proxy_server::Proxy;
use rpc::proxy::ProxyService;
use rpc::replica_proxy::ReplicaProxyService;
use rpc::replication_log::rpc::replication_log_server::ReplicationLog;
use rpc::replication_log::ReplicationLogService;
use rpc::replication_log_proxy::ReplicationLogProxyService;
use rpc::run_rpc_server;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, Instant, MissedTickBehavior};
use utils::services::idle_shutdown::IdleShutdownKicker;

use crate::auth::Auth;
use crate::connection::config::DatabaseConfigStore;
use crate::connection::libsql::open_db;
use crate::error::Error;
use crate::migration::maybe_migrate;
use crate::net::Accept;
use crate::net::AddrIncoming;
use crate::stats::Stats;

pub use sqld_libsql_bindings as libsql;

pub mod config;
pub mod connection;
pub mod net;
pub mod rpc;
pub mod version;

mod admin_api;
mod auth;
mod database;
mod error;
mod h2c;
mod heartbeat;
mod hrana;
mod http;
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
const DEFAULT_NAMESPACE_NAME: &str = "default";
const DEFAULT_AUTO_CHECKPOINT: u32 = 1000;

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Server<C = HttpConnector, A = AddrIncoming> {
    pub path: Arc<Path>,
    pub db_config: DbConfig,
    pub user_api_config: UserApiConfig<A>,
    pub admin_api_config: Option<AdminApiConfig<A>>,
    pub rpc_server_config: Option<RpcServerConfig<A>>,
    pub rpc_client_config: Option<RpcClientConfig<C>>,
    pub idle_shutdown_timeout: Option<Duration>,
    pub initial_idle_shutdown_timeout: Option<Duration>,
    pub disable_default_namespace: bool,
    pub heartbeat_config: Option<HeartbeatConfig>,
    pub disable_namespaces: bool,
    pub shutdown: Arc<Notify>,
}

struct Services<M: MakeNamespace, A, P, S> {
    namespaces: NamespaceStore<M>,
    idle_shutdown_kicker: Option<IdleShutdownKicker>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
    proxy_service: P,
    replication_service: S,
    user_api_config: UserApiConfig<A>,
    admin_api_config: Option<AdminApiConfig<A>>,
    disable_namespaces: bool,
    disable_default_namespace: bool,
    db_config: DbConfig,
    auth: Arc<Auth>,
}

impl<M, A, P, S> Services<M, A, P, S>
where
    M: MakeNamespace,
    A: crate::net::Accept,
    P: Proxy,
    S: ReplicationLog,
{
    fn configure(self, join_set: &mut JoinSet<anyhow::Result<()>>) {
        let user_http = UserApi {
            http_acceptor: self.user_api_config.http_acceptor,
            hrana_ws_acceptor: self.user_api_config.hrana_ws_acceptor,
            auth: self.auth,
            namespaces: self.namespaces.clone(),
            idle_shutdown_kicker: self.idle_shutdown_kicker.clone(),
            stats: self.stats.clone(),
            proxy_service: self.proxy_service,
            replication_service: self.replication_service,
            disable_default_namespace: self.disable_default_namespace,
            disable_namespaces: self.disable_namespaces,
            max_response_size: self.db_config.max_response_size,
            enable_console: self.user_api_config.enable_http_console,
            self_url: self.user_api_config.self_url,
        };

        user_http.configure(join_set);

        if let Some(AdminApiConfig { acceptor }) = self.admin_api_config {
            join_set.spawn(admin_api::run_admin_api(
                acceptor,
                self.db_config_store,
                self.namespaces,
            ));
        }
    }
}

// Periodically check the storage used by the database and save it in the Stats structure.
// TODO: Once we have a separate fiber that does WAL checkpoints, running this routine
// right after checkpointing is exactly where it should be done.
async fn run_storage_monitor(db_path: Arc<Path>, stats: Stats) -> anyhow::Result<()> {
    let (_drop_guard, exit_notify) = std::sync::mpsc::channel::<Never>();
    let _ = tokio::task::spawn_blocking(move || {
        let duration = tokio::time::Duration::from_secs(60);
        loop {
            // because closing the last connection interferes with opening a new one, we lazily
            // initialize a connection here, and keep it alive for the entirety of the program. If we
            // fail to open it, we wait for `duration` and try again later.
            let ctx = &mut ();
            // We can safely open db with DEFAULT_AUTO_CHECKPOINT, since monitor is read-only: it 
            // won't produce new updates, frames or generate checkpoints.
            let maybe_conn = match open_db(&db_path, &TRANSPARENT_METHODS, ctx, Some(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY), DEFAULT_AUTO_CHECKPOINT) {
                Ok(conn) => Some(conn),
                Err(e) => {
                    tracing::warn!("failed to open connection for storager monitor: {e}, trying again in {duration:?}");
                    None
                },
            };

            loop {
                if let Some(ref conn) = maybe_conn {
                    if let Ok(storage_bytes_used) =
                        conn.query_row("select sum(pgsize) from dbstat;", [], |row| {
                            row.get::<usize, u64>(0)
                        })
                    {
                        stats.set_storage_bytes_used(storage_bytes_used);
                    }
                }

                match exit_notify.recv_timeout(duration) {
                    Ok(_) => unreachable!(),
                    Err(RecvTimeoutError::Disconnected) => return,
                    Err(RecvTimeoutError::Timeout) => (),

                }
                if maybe_conn.is_none() {
                    break
                }
            }
        }
    }).await;

    Ok(())
}

async fn run_checkpoint_cron(db_path: Arc<Path>, period: Duration) -> anyhow::Result<()> {
    const RETRY_INTERVAL: Duration = Duration::from_secs(60);
    let data_path = db_path.join("data");
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
        let data_path = data_path.clone();
        retry = tokio::task::spawn_blocking(move || match rusqlite::Connection::open(&data_path) {
            Ok(conn) => unsafe {
                let start = Instant::now();
                let mut num_checkpointed: std::ffi::c_int = 0;
                let rc = rusqlite::ffi::sqlite3_wal_checkpoint_v2(
                    conn.handle(),
                    std::ptr::null(),
                    libsql::ffi::SQLITE_CHECKPOINT_TRUNCATE,
                    &mut num_checkpointed as *mut _,
                    std::ptr::null_mut(),
                );
                if rc == 0 {
                    if num_checkpointed == -1 {
                        return Some(Duration::default());
                    } else {
                        let elapsed = Instant::now() - start;
                        tracing::info!("database checkpoint (took: {:?})", elapsed);
                    }
                    None
                } else {
                    tracing::warn!("failed to execute checkpoint - error code: {}", rc);
                    Some(RETRY_INTERVAL)
                }
            },
            Err(err) => {
                tracing::warn!("couldn't connect to '{:?}': {}", data_path, err);
                Some(RETRY_INTERVAL)
            }
        })
        .await?;
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

impl<C, A> Server<C, A>
where
    C: Connector,
    A: Accept,
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
        Arc::new(move |snapshot_file: &Path, namespace: &Bytes| {
            if let Some(exec) = snapshot_exec.as_ref() {
                let ns = std::str::from_utf8(namespace)?;
                let status = Command::new(exec).arg(snapshot_file).arg(ns).status()?;
                anyhow::ensure!(
                    status.success(),
                    "Snapshot exec process failed with status {status}"
                );
            }
            Ok(())
        })
    }

    fn spawn_monitoring_tasks(&self, join_set: &mut JoinSet<anyhow::Result<()>>, stats: Stats) {
        match self.heartbeat_config {
            Some(ref config) => {
                tracing::info!(
                    "Server sending heartbeat to URL {} every {:?}",
                    config.heartbeat_url,
                    config.heartbeat_period,
                );
                join_set.spawn({
                    let heartbeat_url = config.heartbeat_url.clone();
                    let heartbeat_auth = config.heartbeat_auth.clone();
                    let heartbeat_period = config.heartbeat_period;
                    let stats = stats.clone();
                    async move {
                        heartbeat::server_heartbeat(
                            heartbeat_url,
                            heartbeat_auth,
                            heartbeat_period,
                            stats,
                        )
                        .await;
                        Ok(())
                    }
                });

                join_set.spawn(run_storage_monitor(self.path.clone(), stats));
            }
            None => {
                tracing::warn!("No server heartbeat configured")
            }
        }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();

        init_version_file(&self.path)?;
        maybe_migrate(&self.path)?;
        let stats = Stats::new(&self.path)?;
        self.spawn_monitoring_tasks(&mut join_set, stats.clone());
        self.init_sqlite_globals();
        let db_is_dirty = init_sentinel_file(&self.path)?;
        let idle_shutdown_kicker = self.setup_shutdown();

        if let Some(interval) = self.db_config.checkpoint_interval {
            if self.db_config.bottomless_replication.is_some() {
                join_set.spawn(run_checkpoint_cron(self.path.clone(), interval));
            }
        }

        let db_config_store = Arc::new(
            DatabaseConfigStore::load(&self.path).context("Could not load database config")?,
        );
        let snapshot_callback = self.make_snapshot_callback();
        let auth = self.user_api_config.get_auth()?.into();
        let extensions = self.db_config.validate_extensions()?;

        match self.rpc_client_config {
            Some(rpc_config) => {
                let replica = Replica {
                    rpc_config,
                    stats: stats.clone(),
                    db_config_store: db_config_store.clone(),
                    extensions,
                    db_config: self.db_config.clone(),
                    base_path: self.path.clone(),
                };
                let (namespaces, proxy_service, replication_service) = replica.configure().await?;
                let services = Services {
                    namespaces,
                    idle_shutdown_kicker,
                    stats,
                    db_config_store,
                    proxy_service,
                    replication_service,
                    user_api_config: self.user_api_config,
                    admin_api_config: self.admin_api_config,
                    disable_namespaces: self.disable_namespaces,
                    disable_default_namespace: self.disable_default_namespace,
                    db_config: self.db_config,
                    auth,
                };

                services.configure(&mut join_set);
            }
            None => {
                let primary = Primary {
                    rpc_config: self.rpc_server_config,
                    db_config: self.db_config.clone(),
                    idle_shutdown_kicker: idle_shutdown_kicker.clone(),
                    stats: stats.clone(),
                    db_config_store: db_config_store.clone(),
                    db_is_dirty,
                    snapshot_callback,
                    extensions,
                    base_path: self.path.clone(),
                    disable_namespaces: self.disable_namespaces,
                    join_set: &mut join_set,
                    auth: auth.clone(),
                };
                let (namespaces, proxy_service, replication_service) = primary.configure().await?;

                let services = Services {
                    namespaces,
                    idle_shutdown_kicker,
                    stats,
                    db_config_store,
                    proxy_service,
                    replication_service,
                    user_api_config: self.user_api_config,
                    admin_api_config: self.admin_api_config,
                    disable_namespaces: self.disable_namespaces,
                    disable_default_namespace: self.disable_default_namespace,
                    db_config: self.db_config,
                    auth,
                };

                services.configure(&mut join_set);
            }
        }

        tokio::select! {
            _ = self.shutdown.notified() => {
                join_set.shutdown().await;
                // clean shutdown, remove sentinel file
                std::fs::remove_file(sentinel_file_path(&self.path))?;
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
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
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
            stats: self.stats,
            config_store: self.db_config_store,
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
                    DEFAULT_NAMESPACE_NAME.into(),
                    namespace::RestoreOption::Latest,
                )
                .await?;
        }

        if let Some(config) = self.rpc_config.take() {
            self.join_set.spawn(run_rpc_server(
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

        Ok((namespaces, proxy_service, logger_service))
    }
}

struct Replica<C> {
    rpc_config: RpcClientConfig<C>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
    extensions: Arc<[PathBuf]>,
    db_config: DbConfig,
    base_path: Arc<Path>,
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
            stats: self.stats.clone(),
            config_store: self.db_config_store.clone(),
            base_path: self.base_path,
            max_response_size: self.db_config.max_response_size,
            max_total_response_size: self.db_config.max_total_response_size,
        };
        let factory = ReplicaNamespaceMaker::new(conf);
        let namespaces = NamespaceStore::new(factory, true);
        let replication_service = ReplicationLogProxyService::new(channel.clone(), uri.clone());
        let proxy_service = ReplicaProxyService::new(channel, uri);

        Ok((namespaces, proxy_service, replication_service))
    }
}
