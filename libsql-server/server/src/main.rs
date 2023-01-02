use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

/// ChiselEdge CLI
#[derive(Debug, Parser)]
#[command(name = "edge")]
#[command(about = "ChiselEdge CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start a ChiselEdge server.
    Serve {
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
    },
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

    match args.command {
        Commands::Serve {
            db_path,
            pg_listen_addr,
            ws_listen_addr,
            grpc_listen_addr,
            primary_grpc_url,
            fdb_config_path,
        } => {
            server::run_server(
                db_path,
                pg_listen_addr,
                ws_listen_addr,
                fdb_config_path,
                primary_grpc_url,
                grpc_listen_addr,
            )
            .await?;
        }
    }

    #[cfg(feature = "fdb")]
    drop(network);
    Ok(())
}
