use std::env;
use std::fs::OpenOptions;
use std::io::{stdout, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use bytesize::ByteSize;
use clap::Parser;
use hyper::client::HttpConnector;
use libsql_server::auth::{parse_http_basic_auth_arg, parse_jwt_key, user_auth_strategies, Auth};
// use mimalloc::MiMalloc;
use tokio::sync::Notify;
use tokio::time::Duration;
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use libsql_server::config::{
    AdminApiConfig, BottomlessConfig, DbConfig, HeartbeatConfig, MetaStoreConfig, RpcClientConfig,
    RpcServerConfig, TlsConfig, UserApiConfig,
};
use libsql_server::net::AddrIncoming;
use libsql_server::Server;
use libsql_server::{connection::dump::exporter::export_dump, version::Version};
use libsql_sys::{Cipher, EncryptionConfig};

// Use system allocator for now, seems like we are getting too much fragmentation.
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

/// SQL daemon
#[derive(Debug, Parser)]
#[command(name = "sqld")]
#[command(about = "SQL daemon", version = Version::default(), long_about = None)]
struct Cli {
    #[clap(long, short, default_value = "data.sqld", env = "SQLD_DB_PATH")]
    db_path: PathBuf,

    /// The directory path where trusted extensions can be loaded from.
    /// If not present, extension loading is disabled.
    /// If present, the directory is expected to have a trusted.lst file containing the sha256 and name of each extension, one per line. Example:
    ///
    /// 99890762817735984843bf5cf02a4b2ea648018fd05f04df6f9ce7f976841510  math.dylib
    #[clap(long, short)]
    extensions_path: Option<PathBuf>,

    #[clap(long, default_value = "127.0.0.1:8080", env = "SQLD_HTTP_LISTEN_ADDR")]
    http_listen_addr: SocketAddr,
    #[clap(long)]
    enable_http_console: bool,

    /// Address and port for the legacy, Web-Socket-only Hrana server.
    #[clap(long, short = 'l', env = "SQLD_HRANA_LISTEN_ADDR")]
    hrana_listen_addr: Option<SocketAddr>,

    /// The address and port for the admin HTTP API.
    #[clap(long, env = "SQLD_ADMIN_LISTEN_ADDR")]
    admin_listen_addr: Option<SocketAddr>,

    /// Path to a file with a JWT decoding key used to authenticate clients in the Hrana and HTTP
    /// APIs. The key is either a PKCS#8-encoded Ed25519 public key in PEM, or just plain bytes of
    /// the Ed25519 public key in URL-safe base64.
    ///
    /// You can also pass the key directly in the env variable SQLD_AUTH_JWT_KEY.
    #[clap(long, env = "SQLD_AUTH_JWT_KEY_FILE")]
    auth_jwt_key_file: Option<PathBuf>,
    /// Specifies legacy HTTP basic authentication. The argument must be in format "basic:$PARAM",
    /// where $PARAM is base64-encoded string "$USERNAME:$PASSWORD".
    #[clap(long, env = "SQLD_HTTP_AUTH")]
    http_auth: Option<String>,
    /// URL that points to the HTTP API of this server. If set, this is used to implement "sticky
    /// sessions" in Hrana over HTTP.
    #[clap(long, env = "SQLD_HTTP_SELF_URL")]
    http_self_url: Option<String>,

    /// The address and port the inter-node RPC protocol listens to. Example: `0.0.0.0:5001`.
    #[clap(
        long,
        conflicts_with = "primary_grpc_url",
        env = "SQLD_GRPC_LISTEN_ADDR"
    )]
    grpc_listen_addr: Option<SocketAddr>,
    #[clap(
        long,
        requires = "grpc_cert_file",
        requires = "grpc_key_file",
        requires = "grpc_ca_cert_file"
    )]
    grpc_tls: bool,
    #[clap(long)]
    grpc_cert_file: Option<PathBuf>,
    #[clap(long)]
    grpc_key_file: Option<PathBuf>,
    #[clap(long)]
    grpc_ca_cert_file: Option<PathBuf>,

    /// The gRPC URL of the primary node to connect to for writes. Example: `http://localhost:5001`.
    #[clap(long, env = "SQLD_PRIMARY_GRPC_URL")]
    primary_grpc_url: Option<String>,
    #[clap(
        long,
        requires = "primary_grpc_cert_file",
        requires = "primary_grpc_key_file",
        requires = "primary_grpc_ca_cert_file"
    )]
    primary_grpc_tls: bool,
    #[clap(long)]
    primary_grpc_cert_file: Option<PathBuf>,
    #[clap(long)]
    primary_grpc_key_file: Option<PathBuf>,
    #[clap(long)]
    primary_grpc_ca_cert_file: Option<PathBuf>,

    /// Don't display welcome message
    #[clap(long)]
    no_welcome: bool,
    #[clap(long, env = "SQLD_ENABLE_BOTTOMLESS_REPLICATION")]
    enable_bottomless_replication: bool,
    /// The duration, in second, after which to shutdown the server if no request have been
    /// received.
    /// By default, the server doesn't shutdown when idle.
    #[clap(long, env = "SQLD_IDLE_SHUTDOWN_TIMEOUT_S")]
    idle_shutdown_timeout_s: Option<u64>,

    /// Like idle_shutdown_timeout_s but used only once after the server is started.
    /// After that server either is shut down because it does not receive any requests
    /// or idle_shutdown_timeout_s is used moving forward.
    #[clap(long, env = "SQLD_INITIAL_IDLE_SHUTDOWN_TIMEOUT_S")]
    initial_idle_shutdown_timeout_s: Option<u64>,

    /// Maximum size the replication log is allowed to grow (in MB).
    /// defaults to 200MB.
    #[clap(long, env = "SQLD_MAX_LOG_SIZE", default_value = "200")]
    max_log_size: u64,
    /// Maximum duration before the replication log is compacted (in seconds).
    /// By default, the log is compacted only if it grows above the limit specified with
    /// `--max-log-size`.
    #[clap(long, env = "SQLD_MAX_LOG_DURATION")]
    max_log_duration: Option<f32>,

    #[clap(subcommand)]
    utils: Option<UtilsSubcommands>,

    /// The URL to send a server heartbeat `POST` request to.
    /// By default, the server doesn't send a heartbeat.
    #[clap(long, env = "SQLD_HEARTBEAT_URL")]
    heartbeat_url: Option<String>,

    /// The HTTP "Authornization" header to include in the a server heartbeat
    /// `POST` request.
    /// By default, the server doesn't send a heartbeat.
    #[clap(long, env = "SQLD_HEARTBEAT_AUTH")]
    heartbeat_auth: Option<String>,

    /// The heartbeat time period in seconds.
    /// By default, the the period is 30 seconds.
    #[clap(long, env = "SQLD_HEARTBEAT_PERIOD_S", default_value = "30")]
    heartbeat_period_s: u64,

    /// Soft heap size limit in mebibytes - libSQL will try to not go over this limit with memory usage.
    #[clap(long, env = "SQLD_SOFT_HEAP_LIMIT_MB")]
    soft_heap_limit_mb: Option<usize>,

    /// Hard heap size limit in mebibytes - libSQL will bail out with SQLITE_NOMEM error
    /// if it goes over this limit with memory usage.
    #[clap(long, env = "SQLD_HARD_HEAP_LIMIT_MB")]
    hard_heap_limit_mb: Option<usize>,

    /// Set the maximum size for a response. e.g 5KB, 10MB...
    #[clap(long, env = "SQLD_MAX_RESPONSE_SIZE", default_value = "10MB")]
    max_response_size: ByteSize,

    /// Set the maximum size for all responses. e.g 5KB, 10MB...
    #[clap(long, env = "SQLD_MAX_TOTAL_RESPONSE_SIZE", default_value = "32MB")]
    max_total_response_size: ByteSize,

    /// Set a command to execute when a snapshot file is generated.
    #[clap(long, env = "SQLD_SNAPSHOT_EXEC")]
    snapshot_exec: Option<String>,

    /// Interval in seconds, in which WAL checkpoint is being called.
    /// By default, the interval is 1 hour.
    #[clap(long, env = "SQLD_CHECKPOINT_INTERVAL_S")]
    checkpoint_interval_s: Option<u64>,

    /// By default, all request for which a namespace can't be determined fallaback to the default
    /// namespace `default`. This flag disables that.
    #[clap(long)]
    disable_default_namespace: bool,

    /// Enable the namespaces features. Namespaces are disabled by default, and all requests target
    /// the default namespace.
    #[clap(long)]
    enable_namespaces: bool,

    /// Enable snapshot at shutdown
    #[clap(long)]
    snapshot_at_shutdown: bool,

    /// Max active namespaces kept in-memory
    #[clap(long, env = "SQLD_MAX_ACTIVE_NAMESPACES", default_value = "100")]
    max_active_namespaces: usize,

    /// Enable backup for the metadata store
    #[clap(long, env = "SQLD_BACKUP_META_STORE")]
    backup_meta_store: bool,
    /// S3 access key ID for the meta store backup
    #[clap(long, env = "SQLD_META_STORE_ACCESS_KEY_ID")]
    meta_store_access_key_id: Option<String>,
    /// S3 secret access key for the meta store backup
    #[clap(long, env = "SQLD_META_STORE_SECRET_ACCESS")]
    meta_store_secret_access_key: Option<String>,
    /// S3 region for the metastore backup
    #[clap(long, env = "SQLD_META_STORE_REGION")]
    meta_store_region: Option<String>,
    /// Id for the meta store backup
    #[clap(long, env = "SQLD_META_STORE_BACKUP_ID")]
    meta_store_backup_id: Option<String>,
    /// S3 bucket name for the meta store backup
    #[clap(long, env = "SQLD_META_STORE_BUCKET_NAME")]
    meta_store_bucket_name: Option<String>,
    /// Interval at which to perform backups of the meta store
    #[clap(long, env = "SQLD_META_STORE_BACKUP_INTERVAL_S")]
    meta_store_backup_interval_s: Option<usize>,
    /// S3 endpoint for the meta store backups
    #[clap(long, env = "SQLD_META_STORE_BUCKET_ENDPOINT")]
    meta_store_bucket_endpoint: Option<String>,

    /// encryption_key for encryption at rest
    #[clap(long, env = "SQLD_ENCRYPTION_KEY")]
    encryption_key: Option<bytes::Bytes>,

    #[clap(long, default_value = "128", env = "SQLD_MAX_CONCURRENT_CONNECTIONS")]
    max_concurrent_connections: usize,
    // max number of concurrent requests across all connections
    #[clap(long, default_value = "128", env = "SQLD_MAX_CONCURRENT_REQUESTS")]
    max_concurrent_requests: u64,

    /// Allow meta store to recover config from filesystem from older version, if meta store is
    /// empty on startup
    #[clap(long, env = "SQLD_ALLOW_METASTORE_RECOVERY")]
    allow_metastore_recovery: bool,

    /// Shutdown timeout duration in seconds, defaults to 30 seconds.
    #[clap(long, env = "SQLD_SHUTDOWN_TIMEOUT")]
    shutdown_timeout: Option<u64>,
}

#[derive(clap::Subcommand, Debug)]
enum UtilsSubcommands {
    Dump {
        #[clap(long)]
        /// Path at which to write the dump
        path: Option<PathBuf>,
        #[clap(long)]
        namespace: String,
    },
}

impl Cli {
    #[rustfmt::skip]
    fn print_welcome_message(&self) {
        // no welcome :'(
        if self.no_welcome { return }

        eprintln!(r#"           _     _ "#);
        eprintln!(r#" ___  __ _| | __| |"#);
        eprintln!(r#"/ __|/ _` | |/ _` |"#);
        eprintln!(r#"\__ \ (_| | | (_| |"#);
        eprintln!(r#"|___/\__, |_|\__,_|"#);
        eprintln!(r#"        |_|        "#);

        eprintln!();
        eprintln!("Welcome to sqld!");
        eprintln!();
        eprintln!("version: {}", env!("CARGO_PKG_VERSION"));
        if env!("VERGEN_GIT_SHA") != "VERGEN_IDEMPOTENT_OUTPUT" {
            eprintln!("commit SHA: {}", env!("VERGEN_GIT_SHA"));
        }
        eprintln!("build date: {}", env!("VERGEN_BUILD_DATE"));
        eprintln!();
        eprintln!("This software is in BETA version.");
        eprintln!("If you encounter any bug, please open an issue at https://github.com/tursodatabase/libsql/issues");
        eprintln!();

        eprintln!("config:");

        eprint!("\t- mode: ");
        match (&self.grpc_listen_addr, &self.primary_grpc_url) {
            (None, None) => eprintln!("standalone"),
            (Some(addr), None) => eprintln!("primary ({addr})"),
            (None, Some(url)) => eprintln!("replica (primary at {url})"),
            _ => unreachable!("invalid configuration!"),
        };
        eprintln!("\t- database path: {}", self.db_path.display());
        let extensions_str = self.extensions_path.clone().map_or("<disabled>".to_string(), |x| x.display().to_string());
        eprintln!("\t- extensions path: {extensions_str}");
        eprintln!("\t- listening for HTTP requests on: {}", self.http_listen_addr);
        eprintln!("\t- grpc_tls: {}", if self.grpc_tls { "yes" } else { "no" });
        #[cfg(feature = "encryption")]
        eprintln!("\t- encryption at rest: {}", if self.encryption_key.is_some() { "enabled" } else { "disabled" });
    }
}

fn perform_dump(dump_path: Option<&Path>, db_path: &Path) -> anyhow::Result<()> {
    let out: Box<dyn Write> = match dump_path {
        Some(path) => {
            let f = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .with_context(|| format!("file `{}` already exists", path.display()))?;
            Box::new(f)
        }
        None => Box::new(stdout()),
    };
    let conn = if cfg!(feature = "unix-excl-vfs") {
        rusqlite::Connection::open_with_flags_and_vfs(
            db_path.join("data"),
            rusqlite::OpenFlags::default(),
            "unix-excl",
        )
    } else {
        rusqlite::Connection::open(db_path.join("data"))
    }?;

    export_dump(conn, out)?;

    Ok(())
}

#[cfg(feature = "debug-tools")]
fn enable_libsql_logging() {
    use std::ffi::c_int;
    use std::sync::Once;
    static ONCE: Once = Once::new();

    fn libsql_log(code: c_int, msg: &str) {
        tracing::error!("sqlite error {code}: {msg}");
    }

    ONCE.call_once(|| unsafe {
        rusqlite::trace::config_log(Some(libsql_log)).unwrap();
    });
}

fn make_db_config(config: &Cli) -> anyhow::Result<DbConfig> {
    let encryption_config = config.encryption_key.as_ref().map(|key| EncryptionConfig {
        cipher: Cipher::Aes256Cbc,
        encryption_key: key.clone(),
    });
    let mut bottomless_replication = config
        .enable_bottomless_replication
        .then(bottomless::replicator::Options::from_env)
        .transpose()?;
    // Inherit encryption key for bottomless from the db config, if not specified.
    if let Some(ref mut bottomless_replication) = bottomless_replication {
        if bottomless_replication.encryption_config.is_none() {
            bottomless_replication.encryption_config = encryption_config.clone();
        }
    }
    Ok(DbConfig {
        extensions_path: config.extensions_path.clone().map(Into::into),
        bottomless_replication,
        max_log_size: config.max_log_size,
        max_log_duration: config.max_log_duration,
        soft_heap_limit_mb: config.soft_heap_limit_mb,
        hard_heap_limit_mb: config.hard_heap_limit_mb,
        max_response_size: config.max_response_size.as_u64(),
        max_total_response_size: config.max_total_response_size.as_u64(),
        snapshot_exec: config.snapshot_exec.clone(),
        checkpoint_interval: config.checkpoint_interval_s.map(Duration::from_secs),
        snapshot_at_shutdown: config.snapshot_at_shutdown,
        encryption_config: encryption_config.clone(),
        max_concurrent_requests: config.max_concurrent_requests,
    })
}

async fn make_user_auth_strategy(config: &Cli) -> anyhow::Result<Auth> {
    if let Some(http_auth) = config.http_auth.as_deref() {
        tracing::info!("Using legacy HTTP basic authentication");

        let credential =
            parse_http_basic_auth_arg(http_auth)?.expect("Invalid HTTP Basic configuration");

        return Ok(Auth::new(user_auth_strategies::HttpBasic::new(
            credential.into(),
        )));
    }

    let auth_jwt_key = if let Some(ref file_path) = config.auth_jwt_key_file {
        let data = tokio::fs::read_to_string(file_path)
            .await
            .context("Could not read file with JWT key")?;
        Some(data)
    } else {
        match env::var("SQLD_AUTH_JWT_KEY") {
            Ok(key) => Some(key),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                bail!("Env variable SQLD_AUTH_JWT_KEY does not contain a valid Unicode value")
            }
        }
    };

    if let Some(jwt_key) = auth_jwt_key.as_deref() {
        let jwt_key: jsonwebtoken::DecodingKey =
            parse_jwt_key(jwt_key).context("Could not parse JWT decoding key")?;
        tracing::info!("Using JWT-based authentication");
        return Ok(Auth::new(user_auth_strategies::Jwt::new(jwt_key)));
    }

    Ok(Auth::new(user_auth_strategies::Disabled::new()))
}

async fn make_user_api_config(config: &Cli) -> anyhow::Result<UserApiConfig> {
    let http_acceptor =
        AddrIncoming::new(tokio::net::TcpListener::bind(config.http_listen_addr).await?);
    tracing::info!(
        "listening for incoming user HTTP connection on {}",
        config.http_listen_addr
    );

    let hrana_ws_acceptor = match config.hrana_listen_addr {
        Some(addr) => {
            let incoming = AddrIncoming::new(tokio::net::TcpListener::bind(addr).await?);

            tracing::info!(
                "listening for incoming user hrana websocket connection on {}",
                addr
            );

            Some(incoming)
        }
        None => None,
    };

    let auth_strategy = make_user_auth_strategy(&config).await?;

    Ok(UserApiConfig {
        http_acceptor: Some(http_acceptor),
        hrana_ws_acceptor,
        enable_http_console: config.enable_http_console,
        self_url: config.http_self_url.clone(),
        auth_strategy,
    })
}

async fn make_admin_api_config(config: &Cli) -> anyhow::Result<Option<AdminApiConfig>> {
    match config.admin_listen_addr {
        Some(addr) => {
            let acceptor = AddrIncoming::new(tokio::net::TcpListener::bind(addr).await?);

            tracing::info!("listening for incoming adming HTTP connection on {}", addr);
            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .build();

            Ok(Some(AdminApiConfig {
                acceptor,
                connector,
                disable_metrics: false,
            }))
        }
        None => Ok(None),
    }
}

async fn make_rpc_server_config(config: &Cli) -> anyhow::Result<Option<RpcServerConfig>> {
    match config.grpc_listen_addr {
        Some(addr) => {
            let acceptor = AddrIncoming::new(tokio::net::TcpListener::bind(addr).await?);

            tracing::info!("listening for incoming gRPC connection on {}", addr);

            let tls_config = if config.grpc_tls {
                Some(TlsConfig {
                    cert: config
                        .grpc_cert_file
                        .clone()
                        .context("server tls is enabled but cert file is missing")?,
                    key: config
                        .grpc_key_file
                        .clone()
                        .context("server tls is enabled but key file is missing")?,
                    ca_cert: config
                        .grpc_ca_cert_file
                        .clone()
                        .context("server tls is enabled but ca_cert file is missing")?,
                })
            } else {
                None
            };

            Ok(Some(RpcServerConfig {
                acceptor,
                tls_config,
            }))
        }
        None => Ok(None),
    }
}

async fn make_rpc_client_config(config: &Cli) -> anyhow::Result<Option<RpcClientConfig>> {
    match config.primary_grpc_url {
        Some(ref url) => {
            let mut connector = HttpConnector::new();
            connector.enforce_http(false);
            connector.set_nodelay(true);
            let tls_config = if config.primary_grpc_tls {
                Some(TlsConfig {
                    cert: config
                        .primary_grpc_cert_file
                        .clone()
                        .context("client tls is enabled but cert file is missing")?,
                    key: config
                        .primary_grpc_key_file
                        .clone()
                        .context("client tls is enabled but key file is missing")?,
                    ca_cert: config
                        .primary_grpc_ca_cert_file
                        .clone()
                        .context("client tls is enabled but ca_cert file is missing")?,
                })
            } else {
                None
            };

            Ok(Some(RpcClientConfig {
                remote_url: url.clone(),
                connector,
                tls_config,
            }))
        }
        None => Ok(None),
    }
}

fn make_hearbeat_config(config: &Cli) -> Option<HeartbeatConfig> {
    Some(HeartbeatConfig {
        heartbeat_url: config.heartbeat_url.clone(),
        heartbeat_period: Duration::from_secs(config.heartbeat_period_s),
        heartbeat_auth: config.heartbeat_auth.clone(),
    })
}

async fn shutdown_signal() -> Result<&'static str> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut int = signal(SignalKind::interrupt())?;
    let mut term = signal(SignalKind::terminate())?;

    let signal = tokio::select! {
        _ = int.recv() => "SIGINT",
        _ = term.recv() => "SIGTERM",
    };

    Ok(signal)
}

fn make_meta_store_config(config: &Cli) -> anyhow::Result<MetaStoreConfig> {
    let bottomless = if config.backup_meta_store {
        Some(BottomlessConfig {
            access_key_id: config
                .meta_store_access_key_id
                .clone()
                .context("missing meta store bucket access key id")?,
            secret_access_key: config
                .meta_store_secret_access_key
                .clone()
                .context("missing meta store bucket secret access key")?,
            region: config
                .meta_store_region
                .clone()
                .context("missing meta store bucket region")?,
            backup_id: config
                .meta_store_backup_id
                .clone()
                .context("missing meta store backup id")?,
            bucket_name: config
                .meta_store_bucket_name
                .clone()
                .context("missing meta store bucket name")?,
            backup_interval: Duration::from_secs(
                config
                    .meta_store_backup_interval_s
                    .context("missing meta store backup internal")? as _,
            ),
            bucket_endpoint: config
                .meta_store_bucket_endpoint
                .clone()
                .context("missing meta store bucket name")?,
        })
    } else {
        None
    };

    Ok(MetaStoreConfig {
        bottomless,
        allow_recover_from_fs: config.allow_metastore_recovery,
    })
}

async fn build_server(config: &Cli) -> anyhow::Result<Server> {
    let db_config = make_db_config(config)?;
    let user_api_config = make_user_api_config(config).await?;
    let admin_api_config = make_admin_api_config(config).await?;
    let rpc_server_config = make_rpc_server_config(config).await?;
    let rpc_client_config = make_rpc_client_config(config).await?;
    let heartbeat_config = make_hearbeat_config(config);
    let meta_store_config = make_meta_store_config(config)?;

    let shutdown = Arc::new(Notify::new());
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            loop {
                let signal = shutdown_signal()
                    .await
                    .expect("Failed to registry shutdown signals");

                tracing::info!(
                    "Got {} shutdown signal, gracefully shutting down...this may take some time",
                    signal
                );

                shutdown.notify_waiters();
            }
        }
    });

    Ok(Server {
        path: config.db_path.clone().into(),
        db_config,
        user_api_config,
        admin_api_config,
        rpc_server_config,
        rpc_client_config,
        heartbeat_config,
        idle_shutdown_timeout: config.idle_shutdown_timeout_s.map(Duration::from_secs),
        initial_idle_shutdown_timeout: config
            .initial_idle_shutdown_timeout_s
            .map(Duration::from_secs),
        disable_default_namespace: config.disable_default_namespace,
        disable_namespaces: !config.enable_namespaces,
        shutdown,
        max_active_namespaces: config.max_active_namespaces,
        meta_store_config,
        max_concurrent_connections: config.max_concurrent_connections,
        shutdown_timeout: config
            .shutdown_timeout
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30)),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let registry = tracing_subscriber::registry();

    #[cfg(feature = "debug-tools")]
    let registry = registry.with(console_subscriber::spawn());

    #[cfg(feature = "debug-tools")]
    enable_libsql_logging();

    registry
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
        )
        .init();

    let args = Cli::parse();

    match args.utils {
        Some(UtilsSubcommands::Dump { path, namespace }) => {
            if let Some(ref path) = path {
                eprintln!(
                    "Dumping database {} to {}",
                    args.db_path.display(),
                    path.display()
                );
            }
            let db_path = args.db_path.join("dbs").join(&namespace);
            if !db_path.exists() {
                bail!("no database for namespace `{namespace}`");
            }

            perform_dump(path.as_deref(), &db_path)
        }
        None => {
            args.print_welcome_message();
            let server = build_server(&args).await?;
            server.start().await?;

            Ok(())
        }
    }
}
