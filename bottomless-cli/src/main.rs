use anyhow::{anyhow, Result};
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::NaiveDateTime;
use clap::{Parser, Subcommand};
use libsql_sys::{connection::NO_AUTOCHECKPOINT, wal::Sqlite3WalManager};
use rusqlite::params;
use std::path::PathBuf;

mod replicator_extras;
use crate::replicator_extras::detect_db;
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
    #[clap(long, short)]
    namespace: Option<String>,
    #[clap(long)]
    encryption_key: Option<Bytes>,
    #[clap(long)]
    db_name: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[clap(about = "Copy bottomless generation locally")]
    Copy {
        #[clap(long, short, long_help = "Generation to copy (latest by default)")]
        generation: Option<uuid::Uuid>,
        #[clap(long, short, long_help = "Target local directory")]
        to_dir: String,
    },
    #[clap(about = "Create new generation from database")]
    Create {
        #[clap(long, short, long_help = "Path to the source database file")]
        source_db_path: String,
    },
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
        #[clap(
            long,
            short,
            conflicts_with = "generation",
            long_help = "UTC timestamp which is an upper bound for the transactions to be restored."
        )]
        utc_time: Option<NaiveDateTime>,
        #[clap(long, short, conflicts_with_all = ["generation", "utc_time"], long_help = "Restore from a local directory")]
        from_dir: Option<PathBuf>,
    },
    #[clap(about = "Verify integrity of the database")]
    Verify {
        #[clap(
            long,
            short,
            long_help = "Generation to verify.\nSkip this parameter to verify the newest generation."
        )]
        generation: Option<uuid::Uuid>,
        #[clap(
            long,
            short,
            conflicts_with = "generation",
            long_help = "UTC timestamp which is an upper bound for the transactions to be verified."
        )]
        utc_time: Option<NaiveDateTime>,
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
    #[clap(about = "Generate and upload a snapshot for a given generation or timestamp")]
    Snapshot {
        #[clap(long, short)]
        generation: Option<uuid::Uuid>,
    },
}

async fn detect_database(options: &Cli, namespace: &str) -> Result<(String, String)> {
    let database = match options.database.clone() {
        Some(db) => db,
        None => {
            let client = Client::from_conf({
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
                if let Some(endpoint) = options.endpoint.clone() {
                    loader = loader.endpoint_url(endpoint);
                }
                aws_sdk_s3::config::Builder::from(&loader.load().await)
                    .force_path_style(true)
                    .build()
            });
            let bucket = options.bucket.as_deref().unwrap_or("bottomless");
            match detect_db(&client, bucket, namespace).await {
                Some(db) => db,
                None => {
                    return Err(anyhow!("Could not autodetect the database. Please pass it explicitly with -d option"));
                }
            }
        }
    };
    let database_dir = database + "/dbs/" + namespace.strip_prefix("ns-").unwrap();
    let database = database_dir.clone() + "/data";
    tracing::info!("Database: '{}' (namespace: {})", database, namespace);
    return Ok((database, database_dir));
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut options = Cli::parse();

    if let Commands::Restore {
        generation: _,
        utc_time: _,
        from_dir: Some(from_dir),
    } = options.command
    {
        let database = match &options.database {
            Some(database) => database,
            None => {
                println!("Please pass the database name with -d option");
                return Ok(());
            }
        };
        println!("trying to restore from {}", from_dir.display());
        let mut db_file = tokio::fs::File::create(database).await?;
        let (page_size, checksum) = match Replicator::get_local_metadata(&from_dir).await {
            Ok(Some((page_size, checksum))) => (page_size, checksum),
            Ok(None) => {
                println!("No local metadata found, continuing anyway");
                (4096, (0, 0))
            }
            Err(e) => {
                println!("Failed to get local metadata: {e}, continuing anyway");
                (4096, (0, 0))
            }
        };
        println!(
            "Local metadata: page_size={page_size}, checksum={:X}-{:X}",
            checksum.0, checksum.1
        );
        Replicator::restore_from_local_snapshot(&from_dir, &mut db_file).await?;
        println!("Restored local snapshot to {}", database);
        let applied_frames = Replicator::apply_wal_from_local_generation(
            &from_dir,
            &mut db_file,
            page_size,
            checksum,
        )
        .await?;
        println!("Applied {applied_frames} frames from local generation");
        if let Err(e) = verify_db(&PathBuf::from(database)) {
            println!("Verification failed: {e}");
            std::process::exit(1)
        }
        println!("Verification: ok");
        return Ok(());
    }

    if let Some(ep) = options.endpoint.as_deref() {
        std::env::set_var("LIBSQL_BOTTOMLESS_ENDPOINT", ep)
    } else {
        options.endpoint = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT").ok();
    }

    if let Some(bucket) = options.bucket.as_deref() {
        std::env::set_var("LIBSQL_BOTTOMLESS_BUCKET", bucket)
    } else {
        options.bucket = std::env::var("LIBSQL_BOTTOMLESS_BUCKET").ok();
    }

    if let Some(ns) = options.namespace.as_deref() {
        if !ns.starts_with("ns-") {
            println!("Namespace should start with 'ns-'");
            std::process::exit(1)
        }
    }
    if let Some(encryption_key) = options.encryption_key.as_ref() {
        std::env::set_var(
            "LIBSQL_BOTTOMLESS_ENCRYPTION_KEY",
            std::str::from_utf8(encryption_key)?,
        );
    }
    let namespace_init = std::env::var("LIBSQL_BOTTOMLESS_DATABASE_ID").unwrap_or(String::new());
    if options.db_name.is_some() && options.namespace.is_some() {
        return Err(anyhow!(
            "only one of the arguments --db-name or --namespace is expected to be set"
        ));
    }
    if let Some(ref db_name) = options.db_name {
        if namespace_init != "" {
            std::env::set_var(
                "LIBSQL_BOTTOMLESS_DATABASE_ID",
                format!("ns-{}:{}", &namespace_init, db_name),
            );
        } else {
            return Err(anyhow!(
                "db_name can be set only if LIBSQL_BOTTOMLESS_DATABASE_ID env var has namespace ID"
            ));
        }
    } else {
        let namespace = options.namespace.as_deref().unwrap_or("ns-default");
        std::env::set_var("LIBSQL_BOTTOMLESS_DATABASE_ID", namespace);
    }
    let namespace = std::env::var("LIBSQL_BOTTOMLESS_DATABASE_ID").unwrap();
    if namespace_init != namespace {
        tracing::info!(
            "LIBSQL_BOTTOMLESS_DATABASE_ID env var were updated: '{}' -> '{}'",
            namespace_init,
            namespace
        );
    }
    match options.command {
        Commands::Create { ref source_db_path } => {
            let mut client =
                Replicator::new(detect_database(&options, &namespace).await?.0).await?;

            let db_path = PathBuf::from(client.db_path.clone());
            let db_dir = db_path.parent().unwrap();
            if db_dir.exists() {
                return Err(anyhow!("directory for fresh generation must be empty"));
            }
            if options.namespace.is_none() {
                return Err(anyhow!("namespace must be specified explicitly"));
            }
            std::fs::create_dir_all(db_dir)?;
            tracing::info!(
                "created temporary directory for fresh generation: {}",
                db_dir.to_str().unwrap()
            );
            let options = bottomless::replicator::Options::from_env()?;
            if options.encryption_config.is_some() {
                return Err(anyhow!("creation from encrypted DB is not supported"));
            }
            let connection = libsql_sys::Connection::open(
                format!("file:{}?mode=ro", source_db_path),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                Sqlite3WalManager::new(),
                NO_AUTOCHECKPOINT,
                None,
            )?;
            tracing::info!(
                "read to VACUUM source database file {} from read-only connection to the DB {}",
                &source_db_path,
                &client.db_path
            );
            let _ = connection.execute("VACUUM INTO ?", params![&client.db_path])?;
            let _ = client.new_generation().await;
            tracing::info!("set generation {} for replicator", client.generation()?);
            client.snapshot_main_db_file(true).await?;
            client.wait_until_snapshotted().await?;
            println!("snapshot uploaded for generation: {}", client.generation()?);
            return Ok(());
        }
        Commands::Copy { generation, to_dir } => {
            let temp = std::env::temp_dir().join("bottomless-copy-temp-dir");
            let mut client = Replicator::new(temp.display().to_string()).await?;
            client.copy(generation, to_dir).await?;
        }
        Commands::Ls {
            generation,
            limit,
            older_than,
            newer_than,
            verbose,
        } => {
            let temp = std::env::temp_dir().join("bottomless-ls-temp-dir");
            let client = Replicator::new(temp.display().to_string()).await?;
            match generation {
                Some(gen) => client.list_generation(gen).await?,
                None => {
                    client
                        .list_generations(limit, older_than, newer_than, verbose)
                        .await?
                }
            }
        }
        Commands::Restore {
            generation,
            utc_time,
            ..
        } => {
            let (database, database_dir) = detect_database(&options, &namespace).await?;
            let mut client = Replicator::new(database.clone()).await?;
            tokio::fs::create_dir_all(&database_dir).await?;
            client.restore(generation, utc_time).await?;
            let db_path = PathBuf::from(&database);
            if let Err(e) = verify_db(&db_path) {
                println!("Verification failed: {e}");
                std::process::exit(1)
            }
            println!("Verification: ok");
        }
        Commands::Verify {
            generation,
            utc_time,
        } => {
            let temp: PathBuf = std::env::temp_dir().join("bottomless-verify-temp-dir");
            let mut client = Replicator::new(temp.display().to_string()).await?;
            let _ = tokio::fs::remove_file(&temp).await;
            tracing::info!(
                "ready to restore DB from generation '{}'",
                &generation
                    .map(|x| x.to_string())
                    .unwrap_or(String::from(""))
            );
            client.restore(generation, utc_time).await?;
            let size = tokio::fs::metadata(&temp).await?.len();
            println!("Snapshot size: {size}");
            let result = verify_db(&temp);
            let _ = tokio::fs::remove_file(&temp).await;
            if let Err(e) = result {
                println!("Verification failed: {e}");
                std::process::exit(1)
            }
            println!("Verification: ok");
        }
        Commands::Rm {
            generation,
            older_than,
            verbose,
        } => {
            let (database, _) = detect_database(&options, &namespace).await?;
            let client = Replicator::new(database.clone()).await?;

            match (generation, older_than) {
                (None, Some(older_than)) => client.remove_many(older_than, verbose).await?,
                (Some(generation), None) => client.remove(generation, verbose).await?,
                (Some(_), Some(_)) => unreachable!(),
                (None, None) => println!(
                    "rm command cannot be run without parameters; see -h or --help for details"
                ),
            }
        }
        Commands::Snapshot { generation } => {
            let (database, database_dir) = detect_database(&options, &namespace).await?;
            let mut client = Replicator::new(database.clone()).await?;

            tokio::fs::create_dir_all(&database_dir).await?;
            let generation = if let Some(gen) = generation {
                gen
            } else if let Some(gen) = client.latest_generation_before(None).await {
                gen
            } else {
                println!("no generation to snapshot found; nothing to do");
                return Ok(());
            };
            // snapshots mark the state of the DB at the beginning of the generation, therefore
            // snapshot at generation N is a final state of database at generation N-1. This can
            // be later used for fast restore: restore from generation N = snapshot + all WAL frames
            // from that generation.
            let parent = if let Some(parent) = client.get_dependency(&generation).await? {
                parent
            } else {
                println!("cannot create a snapshot at the beginning of the generation {}: parent generation not found", generation);
                return Ok(());
            };
            client.restore(Some(parent.clone()), None).await?;
            println!(
                "restored database at the start of generation {}: preparing snapshot...",
                generation
            );
            let db_path = PathBuf::from(&database);
            if let Err(e) = verify_db(&db_path) {
                println!("Verification failed: {e}");
                std::process::exit(1)
            } else {
                println!("verification succeeded");
                client.set_generation(generation.clone());
                client.snapshot_main_db_file(true).await?;
                client.wait_until_snapshotted().await?;
                println!("snapshot uploaded for generation: {}", generation);
                tokio::fs::remove_dir_all(&database_dir).await?;
            }
        }
    };
    Ok(())
}

fn verify_db(path: &PathBuf) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let mut rows = stmt.query(())?;
    let result: String = rows.next()?.unwrap().get(0)?;
    if result == "ok" {
        Ok(())
    } else {
        anyhow::bail!("{result}")
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1)
    }
}
