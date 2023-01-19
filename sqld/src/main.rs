use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use sqld::Config;

/// SQL daemon
#[derive(Debug, Parser)]
#[command(name = "sqld")]
#[command(about = "SQL daemon", long_about = None)]
struct Cli {
    #[clap(long, short, default_value = "iku.db", env = "SQLD_DB_PATH")]
    db_path: PathBuf,
    /// The address and port the PostgreSQL server listens to.
    #[clap(long, short, env = "SQLD_PG_LISTEN_ADDR")]
    pg_listen_addr: Option<SocketAddr>,
    /// The address and port the PostgreSQL over WebSocket server listens to.
    #[clap(long, short, env = "SQLD_WS_LISTEN_ADDR")]
    ws_listen_addr: Option<SocketAddr>,

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

    #[clap(
        long,
        short,
        value_enum,
        default_value = "libsql",
        env = "SQLD_WS_LISTEN_ADDR"
    )]
    backend: sqld::Backend,
    // The url to connect with mWAL backend, based on mvSQLite
    #[cfg(feature = "mwal_backend")]
    #[clap(long, short, env = "SQLD_MWAL_ADDR")]
    mwal_addr: Option<String>,

    #[clap(long, default_value = "127.0.0.1:8080", env = "SQLD_HTTP_LISTEN_ADDR")]
    http_listen_addr: SocketAddr,
    #[clap(long, env = "SQLD_HTTP_AUTH")]
    http_auth: Option<String>,
    #[clap(long)]
    enable_http_console: bool,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        Self {
            db_path: cli.db_path,
            tcp_addr: cli.pg_listen_addr,
            ws_addr: cli.ws_listen_addr,
            http_addr: Some(cli.http_listen_addr),
            http_auth: cli.http_auth,
            enable_http_console: cli.enable_http_console,
            backend: cli.backend,
            writer_rpc_addr: cli.primary_grpc_url,
            writer_rpc_tls: cli.primary_grpc_tls,
            writer_rpc_cert: cli.primary_grpc_cert_file,
            writer_rpc_key: cli.primary_grpc_key_file,
            writer_rpc_ca_cert: cli.primary_grpc_ca_cert_file,
            rpc_server_addr: cli.grpc_listen_addr,
            rpc_server_tls: cli.grpc_tls,
            rpc_server_cert: cli.grpc_cert_file,
            rpc_server_key: cli.grpc_key_file,
            rpc_server_ca_cert: cli.grpc_ca_cert_file,
            #[cfg(feature = "mwal_backend")]
            mwal_addr: cli.mwal_addr,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Cli::parse();

    #[cfg(feature = "mwal_backend")]
    match (&args.backend, args.mwal_addr.is_some()) {
        (sqld::Backend::Mwal, false) => {
            anyhow::bail!("--mwal-addr parameter must be present with mwal backend")
        }
        (backend, true) if backend != &sqld::Backend::Mwal => {
            anyhow::bail!(
                "--mwal-addr parameter conflicts with backend {:?}",
                args.backend
            )
        }
        _ => (),
    }

    sqld::run_server(args.into()).await?;

    Ok(())
}
