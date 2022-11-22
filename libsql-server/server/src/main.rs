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
        #[clap(long, short, default_value = "127.0.0.1:5000")]
        serve_addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();
    match args.command {
        Commands::Serve {
            db_path,
            serve_addr,
        } => {
            server::run_server(&db_path, serve_addr).await?;
        }
    }

    Ok(())
}
