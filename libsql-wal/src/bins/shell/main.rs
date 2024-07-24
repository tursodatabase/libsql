use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::SharedCredentialsProvider;
use clap::{Parser, ValueEnum};
use libsql_wal::checkpointer::LibsqlCheckpointer;
use tokio::task::{block_in_place, JoinSet};

use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::OpenFlags;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::segment::sealed::SealedSegment;
use libsql_wal::storage::async_storage::{AsyncStorage, AsyncStorageInitConfig};
use libsql_wal::storage::backend::s3::S3Backend;
use libsql_wal::storage::Storage;
use libsql_wal::wal::LibsqlWalManager;

#[derive(Debug, clap::Parser)]
struct Cli {
    #[command(flatten)]
    s3_args: S3Args,
    #[arg(long, short = 'n')]
    namespace: String,
    #[command(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, clap::Args)]
#[group(
    required = false,
    multiple = true,
    requires_all = [
    "s3_url",
    "s3_access_key",
    "s3_access_key_id",
    "s3_bucket",
    "cluster_id",
    ])]
struct S3Args {
    #[arg(long, requires = "S3Args")]
    enable_s3: bool,
    #[arg(long)]
    cluster_id: Option<String>,
    #[arg(long)]
    s3_url: Option<String>,
    #[arg(long)]
    s3_access_key: Option<String>,
    #[arg(long)]
    s3_access_key_id: Option<String>,
    #[arg(long)]
    s3_bucket: Option<String>,
    #[arg(long)]
    s3_region_id: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum RestoreOptions {
    Latest,
}

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    Shell {
        #[arg(long, short = 'p')]
        db_path: PathBuf,
    },
    Infos,
    Restore {
        #[arg(long)]
        from: RestoreOptions,
        #[arg(long, short)]
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli: Cli = Cli::parse();
    let mut join_set = JoinSet::new();

    if cli.s3_args.enable_s3 {
        let storage = setup_s3_storage(&cli, &mut join_set).await;
        handle(&cli, storage, &mut join_set).await;
    } else {
        todo!()
    }

    while join_set.join_next().await.is_some() {}
}

async fn handle<S>(cli: &Cli, storage: S, join_set: &mut JoinSet<()>)
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
{
    match &cli.subcommand {
        Subcommand::Shell { db_path } => {
            let (sender, receiver) = tokio::sync::mpsc::channel(64);
            let registry = Arc::new(WalRegistry::new(db_path.clone(), storage, sender).unwrap());
            let checkpointer = LibsqlCheckpointer::new(registry.clone(), receiver, 64);
            join_set.spawn(checkpointer.run());
            run_shell(
                registry,
                &db_path,
                NamespaceName::from_string(cli.namespace.clone()),
            )
            .await;
        }
        Subcommand::Infos => handle_infos(&cli.namespace, storage).await,
        Subcommand::Restore { from, path } => {
            let namespace = NamespaceName::from_string(cli.namespace.clone());
            handle_restore(&namespace, storage, *from, path).await
        }
    }
}

async fn handle_restore<S>(
    namespace: &NamespaceName,
    storage: S,
    _from: RestoreOptions,
    db_path: &Path,
) where
    S: Storage,
{
    let options = libsql_wal::storage::RestoreOptions::Latest;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(db_path)
        .unwrap();
    storage
        .restore(file, &namespace, options, None)
        .await
        .unwrap();
}

async fn handle_infos<S>(namespace: &str, storage: S)
where
    S: Storage,
{
    let namespace = NamespaceName::from_string(namespace.to_owned());
    let durable = storage.durable_frame_no(&namespace, None).await;
    println!("namespace: {namespace}");
    println!("max durable frame: {durable}");
}

async fn run_shell<S>(registry: Arc<WalRegistry<StdIO, S>>, db_path: &Path, namespace: NamespaceName)
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
{
    let db_path = db_path.join("dbs").join(namespace.as_str());
    tokio::fs::create_dir_all(&db_path).await.unwrap();
    let resolver = move |path: &Path| {
        NamespaceName::from_string(
            path.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        )
    };
    let wal_manager = LibsqlWalManager::new(registry.clone(), Arc::new(resolver));
    std::fs::create_dir_all(&db_path).unwrap();
    let path = db_path.join("data");
    let conn = block_in_place(|| {
        libsql_sys::Connection::open(
            path,
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            wal_manager.clone(),
            100000,
            None,
        )
    })
    .unwrap();

    loop {
        match inquire::Text::new(">").prompt() {
            Ok(q) => {
                if q.trim().starts_with(".") {
                    if handle_builtin(&q, &registry, &namespace).await {
                        break;
                    }
                    continue;
                }

                match block_in_place(|| conn.prepare(&q)) {
                    Ok(mut stmt) => {
                        match block_in_place(|| {
                            stmt.query_map((), |row| {
                                println!("{row:?}");
                                Ok(())
                            })
                        }) {
                            Ok(rows) => block_in_place(|| {
                                rows.for_each(|_| ());
                            }),
                            Err(e) => {
                                println!("error: {e}");
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        println!("error: {e}");
                        continue;
                    }
                }
            }
            Err(_) => {
                println!("invalid input")
            }
        }
    }

    drop(conn);

    registry.shutdown().unwrap();
}

async fn handle_builtin<S>(
    q: &str,
    registry: &WalRegistry<StdIO, S>,
    namespace: &NamespaceName,
) -> bool {
    match q {
        ".quit" => return true,
        ".seal_current" => match registry.get_async(namespace).await {
            Some(shared) => {
                shared.seal_current().unwrap();
            }
            None => {
                println!("wal not yet openned");
            }
        },
        unknown => println!("unknown command: `{unknown}`"),
    }
    false
}

async fn setup_s3_storage(
    cli: &Cli,
    join_set: &mut JoinSet<()>,
) -> AsyncStorage<S3Backend<StdIO>, SealedSegment<std::fs::File>> {
    let cred = Credentials::new(
        cli.s3_args.s3_access_key_id.as_ref().unwrap(),
        cli.s3_args.s3_access_key.as_ref().unwrap(),
        None,
        None,
        "",
    );
    let config = aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(
            cli.s3_args.s3_region_id.as_ref().unwrap().to_string(),
        ))
        .credentials_provider(SharedCredentialsProvider::new(cred))
        .endpoint_url(cli.s3_args.s3_url.as_ref().unwrap())
        .build();
    let backend = Arc::new(
        S3Backend::from_sdk_config(
            config.clone(),
            cli.s3_args.s3_bucket.as_ref().unwrap().to_string(),
            cli.s3_args.cluster_id.as_ref().unwrap().to_string(),
        )
        .await
        .unwrap(),
    );
    let config = AsyncStorageInitConfig {
        backend: backend.clone(),
        max_in_flight_jobs: 16,
    };
    let (storage, storage_loop) = AsyncStorage::new(config).await;

    join_set.spawn(async move {
        storage_loop.run().await;
    });

    storage
}
