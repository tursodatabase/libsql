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
    #[clap(long, short)]
    fdb_config_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    // This is how foundationdb crate recommends its initialization,
    // along with dropping `network` manually before the program ends:
    // https://docs.rs/foundationdb/0.7.0/foundationdb/fn.boot.html
    #[cfg(feature = "fdb")]
    let network = unsafe { foundationdb::boot() };

    sqld::run_server(
        args.db_path,
        args.pg_listen_addr,
        args.ws_listen_addr,
        args.fdb_config_path,
        args.primary_grpc_url,
        args.grpc_listen_addr,
    )
    .await?;

    #[cfg(feature = "fdb")]
    drop(network);
    Ok(())
}
