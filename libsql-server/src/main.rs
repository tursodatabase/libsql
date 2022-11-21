mod job;
mod messages;
mod net;
mod scheduler;
mod server;
mod shell;
mod statements;
mod worker_pool;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use sqlite::OpenFlags;
use worker_pool::WorkerPool;

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
        #[clap(default_value = "iku.db")]
        db_path: PathBuf,
    },
    /// Start a ChiselEdge shell.
    Shell,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();
    match args.command {
        Commands::Serve { db_path } => {
            let (pool, pool_sender) = WorkerPool::new(0, move || {
                sqlite::Connection::open_with_flags(
                    &db_path,
                    OpenFlags::new()
                        .set_create()
                        .set_no_mutex()
                        .set_read_write(),
                )
                .unwrap()
            })?;
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

            let scheduler = scheduler::Scheduler::new(pool_sender, receiver)?;
            let shandle = tokio::spawn(scheduler.start());
            server::start("127.0.0.1:5000", sender).await?;
            shandle.await?;
            pool.join().await;
        }
        Commands::Shell => {
            shell::start("127.0.0.1:5000").await?;
        }
    }
    Ok(())
}
