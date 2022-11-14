mod coordinator;
mod messages;
mod server;
mod shell;
mod sql_parser;
mod types;

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
    Serve,
    /// Start a ChiselEdge shell.
    Shell,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Serve => {
            server::start()?;
        }
        Commands::Shell => {
            shell::start()?;
        }
    }
    Ok(())
}
