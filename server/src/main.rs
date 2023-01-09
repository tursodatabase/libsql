use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;

/// SQL daemon
#[derive(Debug, Parser)]
#[command(name = "sqld")]
#[command(about = "SQL daemon", long_about = None)]
struct Cli {
    #[clap(long, short, default_value = "iku.db")]
    db_path: PathBuf,
    /// The address and port the PostgreSQL server listens to.
    #[clap(long, short, default_value = "127.0.0.1:5000")]
    pg_listen_addr: SocketAddr,
    /// The address and port the PostgreSQL over WebSocket server listens to.
    #[clap(long, short)]
    ws_listen_addr: Option<SocketAddr>,
    /// The address and port the inter-node RPC protocol listens to. Example: `0.0.0.0:5001`.
    #[clap(long, conflicts_with = "primary_grpc_url")]
    grpc_listen_addr: Option<SocketAddr>,
    /// The gRPC URL of the primary node to connect to for writes. Example: `http://localhost:5001`.
    #[clap(long)]
    primary_grpc_url: Option<String>,
    #[clap(long, short, value_enum, default_value = "libsql")]
    backend: sqld::Backend,
    // The url to connect with mWAL backend, based on mvSQLite
    #[cfg(feature = "mwal_backend")]
    #[clap(long, short)]
    mwal_addr: Option<String>,

    #[clap(long)]
    http_addr: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
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

    sqld::run_server(
        args.db_path,
        args.pg_listen_addr,
        args.ws_listen_addr,
        args.http_addr,
        args.backend,
        #[cfg(feature = "mwal_backend")]
        args.mwal_addr,
        args.primary_grpc_url,
        args.grpc_listen_addr,
    )
    .await?;

    Ok(())
}
