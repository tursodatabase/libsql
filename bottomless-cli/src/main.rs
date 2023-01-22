use anyhow::Result;
use clap::{Parser, Subcommand};

mod replicator_extras;
use replicator_extras::Replicator;

#[derive(Debug, Parser)]
#[command(name = "bottomless-cli")]
#[command(about = "Bottomless CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[clap(long, short)]
    endpoint: Option<String>,
    #[clap(long, short)]
    bucket: Option<String>,
    #[clap(long, short)]
    database: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[clap(about = "List available generations")]
    Ls {
        #[clap(long, short, long_help = "List details about single generation")]
        generation: Option<uuid::Uuid>,
        #[clap(
            long,
            short,
            conflicts_with = "generation",
            long_help = "List only <limit> newest generations"
        )]
        limit: Option<u64>,
        #[clap(
            long,
            conflicts_with = "generation",
            long_help = "List only generations older than given date"
        )]
        older_than: Option<chrono::NaiveDate>,
        #[clap(
            long,
            conflicts_with = "generation",
            long_help = "List only generations newer than given date"
        )]
        newer_than: Option<chrono::NaiveDate>,
        #[clap(
            long,
            short,
            long_help = "Print detailed information on each generation"
        )]
        verbose: bool,
    },
    #[clap(about = "Restore the database")]
    Restore {
        #[clap(
            long,
            short,
            long_help = "Generation to restore from.\nSkip this parameter to restore from the newest generation."
        )]
        generation: Option<uuid::Uuid>,
    },
    #[clap(about = "Remove given generation from remote storage")]
    Rm {
        #[clap(long, short)]
        generation: Option<uuid::Uuid>,
        #[clap(
            long,
            conflicts_with = "generation",
            long_help = "Remove generations older than given date"
        )]
        older_than: Option<chrono::NaiveDate>,
        #[clap(long, short)]
        verbose: bool,
    },
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();
    let options = Cli::parse();

    if let Some(ep) = options.endpoint {
        std::env::set_var("LIBSQL_BOTTOMLESS_ENDPOINT", ep)
    }

    if let Some(bucket) = options.bucket {
        std::env::set_var("LIBSQL_BOTTOMLESS_BUCKET", bucket)
    }

    let mut client = Replicator::new().await?;

    let database = match options.database {
        Some(db) => db,
        None => {
            match client.detect_db().await {
                Some(db) => db,
                None => {
                    println!("Could not autodetect the database. Please pass it explicitly with -d option");
                    return Ok(());
                }
            }
        }
    };
    tracing::info!("Database: {}", database);

    client.register_db(database);

    match options.command {
        Commands::Ls {
            generation,
            limit,
            older_than,
            newer_than,
            verbose,
        } => match generation {
            Some(gen) => client.list_generation(gen).await?,
            None => {
                client
                    .list_generations(limit, older_than, newer_than, verbose)
                    .await?
            }
        },
        Commands::Restore { generation } => {
            match generation {
                Some(gen) => client.restore_from(gen).await?,
                None => client.restore().await?,
            };
        }
        Commands::Rm {
            generation,
            older_than,
            verbose,
        } => match (generation, older_than) {
            (None, Some(older_than)) => client.remove_many(older_than, verbose).await?,
            (Some(generation), None) => client.remove(generation, verbose).await?,
            (Some(_), Some(_)) => unreachable!(),
            (None, None) => println!(
                "rm command cannot be run without parameters; see -h or --help for details"
            ),
        },
    };
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1)
    }
}
