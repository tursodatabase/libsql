use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::SharedCredentialsProvider;
use clap::Parser;
use libsql_wal::storage::backend::Backend;
use tokio::task::{block_in_place, JoinSet};

use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::{OpenFlags, OptionalExtension};
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::segment::sealed::SealedSegment;
use libsql_wal::storage::async_storage::{AsyncStorage, AsyncStorageInitConfig};
use libsql_wal::storage::backend::s3::{S3Backend, S3Config};
use libsql_wal::storage::Storage;
use libsql_wal::wal::LibsqlWalManager;

#[derive(Debug, clap::Parser)]
struct Cli {
    #[arg(long, short = 'p')]
    db_path: PathBuf,
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

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    Shell,
    Infos,
}

#[tokio::main]
async fn main() {
    let cli: Cli = Cli::parse();
    let mut join_set = JoinSet::new();

    if cli.s3_args.enable_s3 {
        let registry = setup_s3_registry(
            &cli.db_path,
            &cli.s3_args.s3_bucket.as_ref().unwrap(),
            &cli.s3_args.cluster_id.as_ref().unwrap(),
            &cli.s3_args.s3_url.as_ref().unwrap(),
            &cli.s3_args.s3_region_id.as_ref().unwrap(),
            &cli.s3_args.s3_access_key_id.as_ref().unwrap(),
            &cli.s3_args.s3_access_key.as_ref().unwrap(),
            &mut join_set,
        )
        .await;

        handle(registry, &cli).await;
    } else {
        todo!()
    }

    while join_set.join_next().await.is_some() {}
}

async fn handle<S, B>(env: Env<S, B>, cli: &Cli)
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
    B: Backend,
{
    match cli.subcommand {
        Subcommand::Shell => {
            let path = cli.db_path.join("dbs").join(&cli.namespace);
            run_shell(
                env.registry,
                &path,
                NamespaceName::from_string(cli.namespace.clone()),
            )
            .await
        }
        Subcommand::Infos => handle_infos(&cli.namespace, env).await,
    }
}

async fn handle_infos<B, S>(namespace: &str, env: Env<S, B>)
where
    B: Backend,
{
    let namespace = NamespaceName::from_string(namespace.to_owned());
    let meta = env
        .backend
        .meta(&env.backend.default_config(), namespace.clone())
        .await
        .unwrap();
    println!("namespace: {namespace}");
    println!("max durable frame: {}", meta.max_frame_no);
}

async fn run_shell<S>(registry: WalRegistry<StdIO, S>, db_path: &Path, namespace: NamespaceName)
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
{
    let registry = Arc::new(registry);
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

                if let Err(e) = block_in_place(|| {
                    conn.query_row(&q, (), |row| {
                        println!("{row:?}");
                        Ok(())
                    })
                    .optional()
                }) {
                    println!("error: {e}");
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

struct Env<S, B: Backend> {
    registry: WalRegistry<StdIO, S>,
    backend: Arc<B>,
}

async fn setup_s3_registry(
    db_path: &Path,
    bucket_name: &str,
    cluster_id: &str,
    url: &str,
    region_id: &str,
    access_key_id: &str,
    secret_access_key: &str,
    join_set: &mut JoinSet<()>,
) -> Env<AsyncStorage<S3Config, SealedSegment<std::fs::File>>, S3Backend<StdIO>> {
    let cred = Credentials::new(access_key_id, secret_access_key, None, None, "");
    let config = aws_config::SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region_id.to_string()))
        .credentials_provider(SharedCredentialsProvider::new(cred))
        .endpoint_url(url)
        .build();
    let backend = Arc::new(
        S3Backend::from_sdk_config(
            config.clone(),
            bucket_name.to_string(),
            cluster_id.to_string(),
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
    let path = db_path.join("wals");
    let registry = WalRegistry::new(path, storage).unwrap();
    Env { registry, backend }
}
