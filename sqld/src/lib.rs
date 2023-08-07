use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as AnyhowContext;
use enclose::enclose;
use futures::never::Never;
use libsql::wal_hook::TRANSPARENT_METHODS;
use once_cell::sync::Lazy;
use rpc::run_rpc_server;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tonic::transport::Channel;
use utils::services::idle_shutdown::IdleShutdownLayer;

use self::database::config::DatabaseConfigStore;
use self::database::dump::loader::DumpLoader;
use self::database::factory::DbFactory;
use self::database::libsql::{open_db, LibSqlDbFactory};
use self::database::write_proxy::WriteProxyDbFactory;
use self::database::Database;
use self::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use self::replication::{ReplicationLogger, SnapshotCallback};
use crate::auth::Auth;
use crate::error::Error;
use crate::replication::replica::Replicator;
use crate::stats::Stats;

use sha256::try_digest;

pub use sqld_libsql_bindings as libsql;

mod admin_api;
mod auth;
pub mod database;
mod error;
mod heartbeat;
mod hrana;
mod http;
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

const MAX_CONCCURENT_DBS: usize = 32;
const DB_CREATE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum Backend {
    Libsql,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Trigger a hard database reset. This cause the database to be wiped, freshly restarted
/// This is used for replicas that are left in an unrecoverabe state and should restart from a
/// fresh state.
///
/// /!\ use with caution.
pub(crate) static HARD_RESET: Lazy<Arc<Notify>> = Lazy::new(|| Arc::new(Notify::new()));

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
    pub load_from_dump: Option<PathBuf>,
    pub max_log_size: u64,
    pub max_log_duration: Option<f32>,
    pub heartbeat_url: Option<String>,
    pub heartbeat_auth: Option<String>,
    pub heartbeat_period: Duration,
    pub soft_heap_limit_mb: Option<usize>,
    pub hard_heap_limit_mb: Option<usize>,
    pub allow_replica_overwrite: bool,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    pub snapshot_exec: Option<String>,
    pub http_replication_addr: Option<SocketAddr>,
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
            load_from_dump: None,
            max_log_size: 200,
            max_log_duration: None,
            heartbeat_url: None,
            heartbeat_auth: None,
            heartbeat_period: Duration::from_secs(30),
            soft_heap_limit_mb: None,
            hard_heap_limit_mb: None,
            allow_replica_overwrite: false,
            max_response_size: 10 * 1024 * 1024,       // 10MiB
            max_total_response_size: 32 * 1024 * 1024, // 32MiB
            snapshot_exec: None,
            http_replication_addr: None,
        }
    }
}

async fn run_service<D: Database>(
    db_factory: Arc<dyn DbFactory<Db = D>>,
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
    logger: Option<Arc<ReplicationLogger>>,
) -> anyhow::Result<()> {
    let auth = get_auth(config)?;

    let (hrana_accept_tx, hrana_accept_rx) = mpsc::channel(8);
    let (hrana_upgrade_tx, hrana_upgrade_rx) = mpsc::channel(8);

    if config.http_addr.is_some() || config.hrana_addr.is_some() {
        let db_factory = db_factory.clone();
        let auth = auth.clone();
        let idle_kicker = idle_shutdown_layer.clone().map(|isl| isl.into_kicker());
        join_set.spawn(async move {
            hrana::ws::serve(
                db_factory,
                auth,
                idle_kicker,
                hrana_accept_rx,
                hrana_upgrade_rx,
            )
            .await
            .context("Hrana server failed")
        });
    }

    if let Some(addr) = config.http_addr {
        let hrana_http_srv = Arc::new(hrana::http::Server::new(
            db_factory.clone(),
            config.http_self_url.clone(),
        ));
        join_set.spawn(http::run_http(
            addr,
            auth,
            db_factory,
            hrana_upgrade_tx,
            hrana_http_srv.clone(),
            config.enable_http_console,
            idle_shutdown_layer,
            stats.clone(),
            logger,
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
        join_set.spawn(admin_api::run_admin_api(addr, db_config_store));
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

/// nukes current DB and start anew
async fn hard_reset(
    config: &Config,
    mut join_set: JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    tracing::error!("received hard-reset command: reseting replica.");

    tracing::info!("Shutting down all services...");
    join_set.shutdown().await;
    tracing::info!("All services have been shut down.");

    let db_path = &config.db_path;
    tokio::fs::remove_dir_all(db_path).await?;

    Ok(())
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

async fn start_replica(
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
) -> anyhow::Result<()> {
    let (channel, uri) = configure_rpc(config)?;
    let replicator = Replicator::new(
        config.db_path.clone(),
        channel.clone(),
        uri.clone(),
        config.allow_replica_overwrite,
    )?;
    let applied_frame_no_receiver = replicator.current_frame_no_notifier.clone();

    join_set.spawn(replicator.run());

    let valid_extensions = validate_extensions(config.extensions_path.clone())?;

    let factory = WriteProxyDbFactory::new(
        config.db_path.clone(),
        valid_extensions,
        channel,
        uri,
        stats.clone(),
        db_config_store.clone(),
        applied_frame_no_receiver,
        config.max_response_size,
        config.max_total_response_size,
    )
    .throttled(
        MAX_CONCCURENT_DBS,
        Some(DB_CREATE_TIMEOUT),
        config.max_total_response_size,
    );

    run_service(
        Arc::new(factory),
        config,
        join_set,
        idle_shutdown_layer,
        stats,
        db_config_store,
        None,
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
) -> anyhow::Result<bottomless::replicator::Replicator> {
    tracing::debug!("Initializing bottomless replication");
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid db path"))?
        .to_owned();
    let mut replicator = bottomless::replicator::Replicator::with_options(path, options).await?;

    match replicator.restore(None, None).await? {
        bottomless::replicator::RestoreAction::None => (),
        bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
            replicator.new_generation();
            replicator.snapshot_main_db_file().await?;
            // Restoration process only leaves the local WAL file if it was
            // detected to be newer than its remote counterpart.
            replicator.maybe_replicate_wal().await?
        }
        bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
            replicator.set_generation(gen);
        }
    }

    Ok(replicator)
}

async fn start_primary(
    config: &Config,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    db_config_store: Arc<DatabaseConfigStore>,
    db_is_dirty: bool,
    snapshot_callback: SnapshotCallback,
) -> anyhow::Result<()> {
    let is_fresh_db = check_fresh_db(&config.db_path);
    let logger = Arc::new(ReplicationLogger::open(
        &config.db_path,
        config.max_log_size,
        config.max_log_duration.map(Duration::from_secs_f32),
        db_is_dirty,
        snapshot_callback,
    )?);

    join_set.spawn(run_periodic_compactions(logger.clone()));

    let bottomless_replicator = if let Some(options) = &config.bottomless_replication {
        Some(Arc::new(std::sync::Mutex::new(
            init_bottomless_replicator(config.db_path.join("data"), options.clone()).await?,
        )))
    } else {
        None
    };

    // load dump is necessary
    let dump_loader = DumpLoader::new(
        config.db_path.clone(),
        logger.clone(),
        bottomless_replicator.clone(),
    )
    .await?;
    if let Some(ref path) = config.load_from_dump {
        if !is_fresh_db {
            anyhow::bail!("cannot load from a dump if a database already exists.\nIf you're sure you want to load from a dump, delete your database folder at `{}`", config.db_path.display());
        }
        dump_loader.load_dump(path.into()).await?;
    }

    let valid_extensions = validate_extensions(config.extensions_path.clone())?;

    let db_factory: Arc<_> = LibSqlDbFactory::new(
        config.db_path.clone(),
        &REPLICATION_METHODS,
        {
            let logger = logger.clone();
            let bottomless_replicator = bottomless_replicator.clone();
            move || ReplicationLoggerHookCtx::new(logger.clone(), bottomless_replicator.clone())
        },
        stats.clone(),
        db_config_store.clone(),
        valid_extensions,
        config.max_response_size,
        config.max_total_response_size,
    )
    .await?
    .throttled(
        MAX_CONCCURENT_DBS,
        Some(DB_CREATE_TIMEOUT),
        config.max_total_response_size,
    )
    .into();

    if let Some(ref addr) = config.rpc_server_addr {
        join_set.spawn(run_rpc_server(
            *addr,
            config.rpc_server_tls,
            config.rpc_server_cert.clone(),
            config.rpc_server_key.clone(),
            config.rpc_server_ca_cert.clone(),
            db_factory.clone(),
            logger.clone(),
            idle_shutdown_layer.clone(),
        ));
    }

    if let Some(ref addr) = config.http_replication_addr {
        // FIXME: let's bring it back once I figure out how Axum works
        // let auth = get_auth(config)?;
        join_set.spawn(replication::http::run(*addr, logger.clone()));
    }

    run_service(
        db_factory,
        config,
        join_set,
        idle_shutdown_layer,
        stats,
        db_config_store,
        Some(logger),
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
            let maybe_conn = match open_db(&db_path, &TRANSPARENT_METHODS, ctx, Some(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)) {
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

pub async fn run_server(config: Config) -> anyhow::Result<()> {
    tracing::trace!("Backend: {:?}", config.backend);

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
        let snapshot_callback: SnapshotCallback = Box::new(move |snapshot_file| {
            if let Some(exec) = snapshot_exec.as_ref() {
                let status = Command::new(exec).arg(snapshot_file).status()?;
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

        let reset = HARD_RESET.clone();
        loop {
            tokio::select! {
                _ = reset.notified() => {
                    hard_reset(&config, join_set).await?;
                    break;
                },
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
