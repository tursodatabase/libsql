#![allow(clippy::type_complexity, clippy::too_many_arguments)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as AnyhowContext;
use bytes::Bytes;
use enclose::enclose;
use futures::never::Never;
use libsql::wal_hook::TRANSPARENT_METHODS;
use namespace::{
    MakeNamespace, NamespaceStore, PrimaryNamespaceConfig, PrimaryNamespaceMaker,
    ReplicaNamespaceConfig, ReplicaNamespaceMaker,
};
use replication::{NamespacedSnapshotCallback, ReplicationLogger};
use rpc::proxy::rpc::proxy_server::Proxy;
use rpc::proxy::ProxyService;
use rpc::replica_proxy::ReplicaProxyService;
use rpc::replication_log::rpc::replication_log_server::ReplicationLog;
use rpc::replication_log::ReplicationLogService;
use rpc::replication_log_proxy::ReplicationLogProxyService;
use rpc::run_rpc_server;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use utils::services::idle_shutdown::IdleShutdownLayer;

use self::connection::config::DatabaseConfigStore;
use self::connection::libsql::open_db;
use crate::auth::Auth;
use crate::error::Error;
use crate::migration::maybe_migrate;
use crate::stats::Stats;

use sha256::try_digest;
use tokio::time::{interval, sleep, Instant, MissedTickBehavior};

use crate::namespace::RestoreOption;
pub use sqld_libsql_bindings as libsql;

mod admin_api;
mod auth;
pub mod connection;
mod database;
mod error;
mod heartbeat;
mod hrana;
mod http;
mod migration;
mod namespace;
mod query;
mod query_analysis;
mod query_result_builder;
mod replication;
pub mod rpc;
mod stats;
#[cfg(test)]
mod test;
mod utils;
pub mod version;

const MAX_CONCURRENT_DBS: usize = 128;
const DB_CREATE_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_NAMESPACE_NAME: &str = "default";
const DEFAULT_AUTO_CHECKPOINT: u32 = 1000;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum Backend {
    Libsql,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Config {
    pub db_path: PathBuf,
    pub extensions_path: Option<PathBuf>,
    pub http_addr: Option<SocketAddr>,
    pub enable_http_console: bool,
    pub http_auth: Option<String>,
    pub http_self_url: Option<String>,
    pub hrana_addr: Option<SocketAddr>,
    pub admin_addr: Option<SocketAddr>,
    pub auth_jwt_key: Option<String>,
    pub backend: Backend,
    pub writer_rpc_addr: Option<String>,
    pub writer_rpc_tls: bool,
    pub writer_rpc_cert: Option<PathBuf>,
    pub writer_rpc_key: Option<PathBuf>,
    pub writer_rpc_ca_cert: Option<PathBuf>,
    pub rpc_server_addr: Option<SocketAddr>,
    pub rpc_server_tls: bool,
    pub rpc_server_cert: Option<PathBuf>,
    pub rpc_server_key: Option<PathBuf>,
    pub rpc_server_ca_cert: Option<PathBuf>,
    pub bottomless_replication: Option<bottomless::replicator::Options>,
    pub idle_shutdown_timeout: Option<Duration>,
    pub initial_idle_shutdown_timeout: Option<Duration>,
    pub max_log_size: u64,
    pub max_log_duration: Option<f32>,
    pub heartbeat_url: Option<String>,
    pub heartbeat_auth: Option<String>,
    pub heartbeat_period: Duration,
    pub soft_heap_limit_mb: Option<usize>,
    pub hard_heap_limit_mb: Option<usize>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    pub snapshot_exec: Option<String>,
    pub disable_default_namespace: bool,
    pub disable_namespaces: bool,
    pub checkpoint_interval: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_path: "data.sqld".into(),
            extensions_path: None,
            http_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)),
            enable_http_console: false,
            http_auth: None,
            http_self_url: None,
            hrana_addr: None,
            admin_addr: None,
            auth_jwt_key: None,
            backend: Backend::Libsql,
            writer_rpc_addr: None,
            writer_rpc_tls: false,
            writer_rpc_cert: None,
            writer_rpc_key: None,
            writer_rpc_ca_cert: None,
            rpc_server_addr: None,
            rpc_server_tls: false,
            rpc_server_cert: None,
            rpc_server_key: None,
            rpc_server_ca_cert: None,
            bottomless_replication: None,
            idle_shutdown_timeout: None,
            initial_idle_shutdown_timeout: None,
            max_log_size: 200,
            max_log_duration: None,
            heartbeat_url: None,
            heartbeat_auth: None,
            heartbeat_period: Duration::from_secs(30),
            soft_heap_limit_mb: None,
            hard_heap_limit_mb: None,
            max_response_size: 10 * 1024 * 1024,       // 10MiB
            max_total_response_size: 32 * 1024 * 1024, // 32MiB
            snapshot_exec: None,
            disable_default_namespace: false,
            disable_namespaces: true,
            checkpoint_interval: None,
        }
    }
}

async fn run_service<F, S, P>(
    namespaces: Arc<NamespaceStore<F>>,
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
    proxy_service: P,
    replication_service: S,
) -> anyhow::Result<()>
where
    F: MakeNamespace,
    S: ReplicationLog,
    P: Proxy,
{
    let auth = get_auth(config)?;

    let (hrana_accept_tx, hrana_accept_rx) = mpsc::channel(8);
    let (hrana_upgrade_tx, hrana_upgrade_rx) = mpsc::channel(8);

    if config.http_addr.is_some() || config.hrana_addr.is_some() {
        let namespaces = namespaces.clone();
        let auth = auth.clone();
        let idle_kicker = idle_shutdown_layer.clone().map(|isl| isl.into_kicker());
        let disable_default_namespace = config.disable_default_namespace;
        let disable_namespaces = config.disable_namespaces;
        let max_response_size = config.max_response_size;

        join_set.spawn(async move {
            hrana::ws::serve(
                auth,
                idle_kicker,
                max_response_size,
                hrana_accept_rx,
                hrana_upgrade_rx,
                namespaces,
                disable_default_namespace,
                disable_namespaces,
            )
            .await
            .context("Hrana server failed")
        });
    }

    if let Some(addr) = config.http_addr {
        let hrana_http_srv = Arc::new(hrana::http::Server::new(config.http_self_url.clone()));
        join_set.spawn(http::run_http(
            addr,
            auth,
            namespaces.clone(),
            hrana_upgrade_tx,
            hrana_http_srv.clone(),
            config.enable_http_console,
            idle_shutdown_layer,
            stats.clone(),
            proxy_service,
            replication_service,
            config.disable_default_namespace,
            config.disable_namespaces,
        ));
        join_set.spawn(async move {
            hrana_http_srv.run_expire().await;
            Ok(())
        });
    }

    if let Some(addr) = config.hrana_addr {
        join_set.spawn(async move {
            hrana::ws::listen(addr, hrana_accept_tx)
                .await
                .context("Hrana listener failed")
        });
    }

    if let Some(addr) = config.admin_addr {
        join_set.spawn(admin_api::run_admin_api(addr, db_config_store, namespaces));
    }

    match &config.heartbeat_url {
        Some(heartbeat_url) => {
            let heartbeat_period = config.heartbeat_period;
            tracing::info!(
                "Server sending heartbeat to URL {} every {:?}",
                heartbeat_url,
                heartbeat_period,
            );
            let heartbeat_url = heartbeat_url.clone();
            let heartbeat_auth = config.heartbeat_auth.clone();
            join_set.spawn(async move {
                heartbeat::server_heartbeat(
                    heartbeat_url,
                    heartbeat_auth,
                    heartbeat_period,
                    stats.clone(),
                )
                .await;
                Ok(())
            });
        }
        None => {
            tracing::warn!("No server heartbeat configured")
        }
    }

    Ok(())
}

fn get_auth(config: &Config) -> anyhow::Result<Arc<Auth>> {
    let mut auth = Auth::default();

    if let Some(arg) = config.http_auth.as_deref() {
        if let Some(param) = auth::parse_http_basic_auth_arg(arg)? {
            auth.http_basic = Some(param);
            tracing::info!("Using legacy HTTP basic authentication");
        }
    }

    if let Some(jwt_key) = config.auth_jwt_key.as_deref() {
        let jwt_key = auth::parse_jwt_key(jwt_key).context("Could not parse JWT decoding key")?;
        auth.jwt_key = Some(jwt_key);
        tracing::info!("Using JWT-based authentication");
    }

    auth.disabled = auth.http_basic.is_none() && auth.jwt_key.is_none();
    if auth.disabled {
        tracing::warn!("No authentication specified, the server will not require authentication")
    }

    Ok(Arc::new(auth))
}

fn configure_rpc(config: &Config) -> anyhow::Result<(Channel, tonic::transport::Uri)> {
    let mut endpoint = Channel::from_shared(config.writer_rpc_addr.clone().unwrap())?;
    if config.writer_rpc_tls {
        let cert_pem = std::fs::read_to_string(config.writer_rpc_cert.clone().unwrap())?;
        let key_pem = std::fs::read_to_string(config.writer_rpc_key.clone().unwrap())?;
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

        let ca_cert_pem = std::fs::read_to_string(config.writer_rpc_ca_cert.clone().unwrap())?;
        let ca_cert = tonic::transport::Certificate::from_pem(ca_cert_pem);

        let tls_config = tonic::transport::ClientTlsConfig::new()
            .identity(identity)
            .ca_certificate(ca_cert)
            .domain_name("sqld");
        endpoint = endpoint.tls_config(tls_config)?;
    }

    let channel = endpoint.connect_lazy();
    let uri = tonic::transport::Uri::from_maybe_shared(config.writer_rpc_addr.clone().unwrap())?;

    Ok((channel, uri))
}

pub enum ResetOp {
    Reset(Bytes),
    Destroy(Bytes),
}

async fn start_replica(
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
) -> anyhow::Result<()> {
    let (channel, uri) = configure_rpc(config)?;
    let extensions = validate_extensions(config.extensions_path.clone())?;
    let (hard_reset_snd, mut hard_reset_rcv) = mpsc::channel(1);
    let conf = ReplicaNamespaceConfig {
        base_path: config.db_path.to_owned(),
        channel: channel.clone(),
        uri: uri.clone(),
        extensions,
        stats: stats.clone(),
        config_store: db_config_store.clone(),
        max_response_size: config.max_response_size,
        max_total_response_size: config.max_total_response_size,
        hard_reset: hard_reset_snd,
    };
    let factory = ReplicaNamespaceMaker::new(conf);
    let namespaces = Arc::new(NamespaceStore::new(factory, true));

    // start the hard reset monitor
    join_set.spawn({
        let namespaces = namespaces.clone();
        async move {
            while let Some(op) = hard_reset_rcv.recv().await {
                match op {
                    ResetOp::Reset(ns) => {
                        tracing::warn!(
                            "received reset signal for: {:?}",
                            std::str::from_utf8(&ns).ok()
                        );
                        namespaces.reset(ns, RestoreOption::Latest).await?;
                    }
                    ResetOp::Destroy(ns) => {
                        namespaces.destroy(ns).await?;
                    }
                }
            }

            Ok(())
        }
    });

    let replication_service = ReplicationLogProxyService::new(channel.clone(), uri.clone());
    let proxy_service = ReplicaProxyService::new(channel, uri);

    run_service(
        namespaces,
        config,
        join_set,
        idle_shutdown_layer,
        stats,
        db_config_store,
        proxy_service,
        replication_service,
    )
    .await?;

    Ok(())
}

fn check_fresh_db(path: &Path) -> bool {
    !path.join("wallog").exists()
}

fn validate_extensions(extensions_path: Option<PathBuf>) -> anyhow::Result<Vec<PathBuf>> {
    let mut valid_extensions = vec![];
    if let Some(ext_dir) = extensions_path {
        let extensions_list = ext_dir.join("trusted.lst");

        let file_contents = std::fs::read_to_string(&extensions_list)
            .with_context(|| format!("can't read {}", &extensions_list.display()))?;

        let extensions = file_contents.lines().filter(|c| !c.is_empty());

        for line in extensions {
            let mut ext_info = line.trim().split_ascii_whitespace();

            let ext_sha = ext_info.next().ok_or_else(|| {
                anyhow::anyhow!("invalid line on {}: {}", &extensions_list.display(), line)
            })?;
            let ext_fname = ext_info.next().ok_or_else(|| {
                anyhow::anyhow!("invalid line on {}: {}", &extensions_list.display(), line)
            })?;

            anyhow::ensure!(
                ext_info.next().is_none(),
                "extension list seem to contain a filename with whitespaces. Rejected"
            );

            let extension_full_path = ext_dir.join(ext_fname);
            let digest = try_digest(extension_full_path.as_path()).with_context(|| {
                format!(
                    "Failed to get sha256 digest, while trying to read {}",
                    extension_full_path.display()
                )
            })?;

            anyhow::ensure!(
                digest == ext_sha,
                "sha256 differs for {}. Got {}",
                ext_fname,
                digest
            );
            valid_extensions.push(extension_full_path);
        }
    }
    Ok(valid_extensions)
}

pub async fn init_bottomless_replicator(
    path: impl AsRef<std::path::Path>,
    options: bottomless::replicator::Options,
    restore_option: &RestoreOption,
) -> anyhow::Result<(bottomless::replicator::Replicator, bool)> {
    tracing::debug!("Initializing bottomless replication");
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid db path"))?
        .to_owned();
    let mut replicator = bottomless::replicator::Replicator::with_options(path, options).await?;
    let mut did_recover = false;

    let (generation, timestamp) = match restore_option {
        RestoreOption::Latest | RestoreOption::Dump(_) => (None, None),
        RestoreOption::Generation(generation) => (Some(*generation), None),
        RestoreOption::PointInTime(timestamp) => (None, Some(*timestamp)),
    };

    match replicator.restore(generation, timestamp).await? {
        bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
            replicator.new_generation();
            if let Some(handle) = replicator.snapshot_main_db_file(None).await? {
                handle.await?;
                did_recover = true;
            }
            // Restoration process only leaves the local WAL file if it was
            // detected to be newer than its remote counterpart.
            replicator.maybe_replicate_wal().await?
        }
        bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
            replicator.set_generation(gen);
        }
    }

    Ok((replicator, did_recover))
}

async fn start_primary(
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    config_store: Arc<DatabaseConfigStore>,
    db_is_dirty: bool,
    snapshot_callback: NamespacedSnapshotCallback,
) -> anyhow::Result<()> {
    let extensions = validate_extensions(config.extensions_path.clone())?;
    let conf = PrimaryNamespaceConfig {
        base_path: config.db_path.to_owned(),
        max_log_size: config.max_log_size,
        db_is_dirty,
        max_log_duration: config.max_log_duration.map(Duration::from_secs_f32),
        snapshot_callback,
        bottomless_replication: config.bottomless_replication.clone(),
        extensions,
        stats: stats.clone(),
        config_store: config_store.clone(),
        max_response_size: config.max_response_size,
        max_total_response_size: config.max_total_response_size,
        checkpoint_interval: config.checkpoint_interval,
        disable_namespace: config.disable_namespaces,
    };
    let factory = PrimaryNamespaceMaker::new(conf);
    let namespaces = Arc::new(NamespaceStore::new(factory, false));

    if config.disable_namespaces {
        // eagerly load the default namespace
        namespaces
            .create(DEFAULT_NAMESPACE_NAME.into(), RestoreOption::Latest)
            .await?;
    }

    if let Some(ref addr) = config.rpc_server_addr {
        join_set.spawn(run_rpc_server(
            *addr,
            config.rpc_server_tls,
            config.rpc_server_cert.clone(),
            config.rpc_server_key.clone(),
            config.rpc_server_ca_cert.clone(),
            idle_shutdown_layer.clone(),
            namespaces.clone(),
            config.disable_namespaces,
        ));
    }

    let auth = get_auth(config)?;

    let logger_service = ReplicationLogService::new(
        namespaces.clone(),
        idle_shutdown_layer.clone(),
        Some(auth.clone()),
        config.disable_namespaces,
    );

    let proxy_service =
        ProxyService::new(namespaces.clone(), Some(auth), config.disable_namespaces);

    run_service(
        namespaces.clone(),
        config,
        join_set,
        idle_shutdown_layer,
        stats,
        config_store,
        proxy_service,
        logger_service,
    )
    .await?;

    Ok(())
}

async fn run_periodic_compactions(logger: Arc<ReplicationLogger>) -> anyhow::Result<()> {
    // calling `ReplicationLogger::maybe_compact()` is cheap if the compaction does not actually
    // take place, so we can affort to poll it very often for simplicity
    let mut interval = tokio::time::interval(Duration::from_millis(1000));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let handle = tokio::task::spawn_blocking(enclose! {(logger) move || {
            logger.maybe_compact()
        }});
        handle
            .await
            .expect("Compaction task crashed")
            .context("Compaction failed")?;
    }
}

// Periodically check the storage used by the database and save it in the Stats structure.
// TODO: Once we have a separate fiber that does WAL checkpoints, running this routine
// right after checkpointing is exactly where it should be done.
async fn run_storage_monitor(db_path: PathBuf, stats: Stats) -> anyhow::Result<()> {
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

async fn run_checkpoint_cron(db_path: PathBuf, period: Duration) -> anyhow::Result<()> {
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

pub async fn run_server(config: Config) -> anyhow::Result<()> {
    tracing::trace!("Backend: {:?}", config.backend);

    init_version_file(&config.db_path)?;
    maybe_migrate(&config.db_path)?;

    if config.bottomless_replication.is_some() {
        bottomless::static_init::register_bottomless_methods();
    }

    if let Some(soft_limit_mb) = config.soft_heap_limit_mb {
        tracing::warn!("Setting soft heap limit to {soft_limit_mb}MiB");
        unsafe {
            sqld_libsql_bindings::ffi::sqlite3_soft_heap_limit64(soft_limit_mb as i64 * 1024 * 1024)
        };
    }
    if let Some(hard_limit_mb) = config.hard_heap_limit_mb {
        tracing::warn!("Setting hard heap limit to {hard_limit_mb}MiB");
        unsafe {
            sqld_libsql_bindings::ffi::sqlite3_hard_heap_limit64(hard_limit_mb as i64 * 1024 * 1024)
        };
    }

    loop {
        if !config.db_path.exists() {
            std::fs::create_dir_all(&config.db_path)?;
        }
        let mut join_set = JoinSet::new();

        let (shutdown_sender, mut shutdown_receiver) = tokio::sync::mpsc::channel::<()>(1);

        join_set.spawn({
            let shutdown_sender = shutdown_sender.clone();
            async move {
                loop {
                    tokio::signal::ctrl_c()
                        .await
                        .expect("failed to listen to CTRL-C");
                    tracing::info!(
                        "received CTRL-C, shutting down gracefully... This may take some time"
                    );
                    shutdown_sender
                        .send(())
                        .await
                        .expect("failed to shutdown gracefully");
                }
            }
        });

        let db_is_dirty = init_sentinel_file(&config.db_path)?;

        let snapshot_exec = config.snapshot_exec.clone();
        let snapshot_callback = Arc::new(move |snapshot_file: &Path, namespace: &Bytes| {
            if let Some(exec) = snapshot_exec.as_ref() {
                let ns = std::str::from_utf8(namespace)?;
                let status = Command::new(exec).arg(snapshot_file).arg(ns).status()?;
                anyhow::ensure!(
                    status.success(),
                    "Snapshot exec process failed with status {status}"
                );
            }
            Ok(())
        });

        let idle_shutdown_layer = config.idle_shutdown_timeout.map(|d| {
            IdleShutdownLayer::new(
                d,
                config.initial_idle_shutdown_timeout,
                shutdown_sender.clone(),
            )
        });

        let stats = Stats::new(&config.db_path)?;

        let db_config_store = Arc::new(
            DatabaseConfigStore::load(&config.db_path).context("Could not load database config")?,
        );

        match config.writer_rpc_addr {
            Some(_) => {
                start_replica(
                    &config,
                    &mut join_set,
                    idle_shutdown_layer,
                    stats.clone(),
                    db_config_store,
                )
                .await?
            }
            None => {
                start_primary(
                    &config,
                    &mut join_set,
                    idle_shutdown_layer,
                    stats.clone(),
                    db_config_store,
                    db_is_dirty,
                    snapshot_callback,
                )
                .await?
            }
        }

        if config.heartbeat_url.is_some() {
            join_set.spawn(run_storage_monitor(config.db_path.clone(), stats));
        }

        if let Some(interval) = config.checkpoint_interval {
            if config.bottomless_replication.is_some() {
                join_set.spawn(run_checkpoint_cron(config.db_path.clone(), interval));
            }
        }

        loop {
            tokio::select! {
                _ = shutdown_receiver.recv() => {
                    join_set.shutdown().await;
                    // clean shutdown, remove sentinel file
                    std::fs::remove_file(sentinel_file_path(&config.db_path))?;
                    return Ok(())
                }
                Some(res) = join_set.join_next() => {
                    res??;
                },
                else => return Ok(()),
            }
        }
    }
}
