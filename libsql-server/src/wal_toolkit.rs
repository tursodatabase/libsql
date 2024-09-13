use std::path::{Path, PathBuf};

use anyhow::Context as _;
use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use chrono::DateTime;
use libsql_wal::io::StdIO;
use libsql_wal::storage::backend::s3::S3Backend;
use libsql_wal::storage::compaction::strategy::identity::IdentityStrategy;
use libsql_wal::storage::compaction::strategy::log_strategy::LogReductionStrategy;
use libsql_wal::storage::compaction::strategy::PartitionStrategy;
use libsql_wal::storage::compaction::Compactor;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum CompactStrategy {
    Logarithmic,
    CompactAll,
}

#[derive(Debug, clap::Subcommand)]
pub enum WalToolkit {
    /// Register namespaces to monitor
    Monitor { namespace: String },
    /// Analyze segments for a namespaces
    Analyze {
        /// list all segments
        #[clap(long)]
        list_all: bool,
        namespace: String,
    },
    /// Compact segments into bigger segments
    Compact {
        /// compaction strategy
        #[clap(long, short)]
        strategy: CompactStrategy,
        /// prints the compaction plan, but doesn't perform it.
        #[clap(long)]
        dry_run: bool,
        namespace: String,
    },
    /// Sync namespace metadata from remote storage
    Sync {
        /// When performing a full sync, all the segment space is scanned again. By default, only
        /// segments with frame_no greated that the last frame_no are retrieved.
        #[clap(long)]
        full: bool,
        /// unless this is specified, all monitored namespaces are synced
        namespace: Option<String>,
    },
    /// Restore namespace
    Restore {
        #[clap(long)]
        verify: bool,
        namespace: String,
        out: PathBuf,
    },
}

impl WalToolkit {
    pub async fn run(&self, compact_path: &Path, s3_args: &S3Args) -> anyhow::Result<()> {
        let backend = setup_storage(s3_args).await?;
        tokio::fs::create_dir_all(compact_path).await?;
        let mut compactor = Compactor::new(backend.into(), compact_path)?;
        match self {
            Self::Monitor { namespace } => {
                let namespace = libsql_sys::name::NamespaceName::from_string(namespace.to_string());
                compactor.monitor(&namespace).await?;
                println!("monitoring {namespace}");
            }
            Self::Analyze {
                namespace,
                list_all,
            } => {
                let namespace = libsql_sys::name::NamespaceName::from_string(namespace.to_string());
                let analysis = compactor.analyze(&namespace)?;
                println!("stats for {namespace}:");
                println!("- segment count: {}", analysis.segment_count());
                println!("- last frame_no: {}", analysis.last_frame_no());
                let set = analysis.shortest_restore_path();
                println!("- shortest restore path len: {}", set.len());
                if let Some((first, last)) = compactor.get_segment_range(&namespace)? {
                    println!(
                        "- oldest segment: {}-{} ({})",
                        first.key.start_frame_no, first.key.end_frame_no, DateTime::from_timestamp_millis(first.key.timestamp as _).unwrap()
                    );
                    println!(
                        "- most recent segment: {}-{} ({})",
                        last.key.start_frame_no, last.key.end_frame_no, DateTime::from_timestamp_millis(last.key.timestamp as _).unwrap()
                    );
                }

                if *list_all {
                    println!("segments:");
                    compactor.list_all(&namespace, |info| {
                        println!(
                            "- {}-{} ({})",
                            info.key.start_frame_no, info.key.end_frame_no, DateTime::from_timestamp_millis(info.key.timestamp as _).unwrap()
                        );
                    })?;
                }
            }
            Self::Compact {
                strategy,
                dry_run,
                namespace,
            } => {
                let namespace = libsql_sys::name::NamespaceName::from_string(namespace.to_string());
                let analysis = compactor.analyze(&namespace)?;
                let strat: Box<dyn PartitionStrategy> = match strategy {
                    CompactStrategy::Logarithmic => Box::new(LogReductionStrategy),
                    CompactStrategy::CompactAll => Box::new(IdentityStrategy),
                };
                let set = analysis.shortest_restore_path();
                let partition = strat.partition(&set);

                println!("initial shortest restore path len: {}", set.len());
                println!("compacting into {} segments", partition.len());
                for set in partition.iter() {
                    println!("- {:?}", set.range().unwrap());
                }
                if *dry_run {
                    println!("dry run: stopping");
                } else {
                    println!("performing compaction");
                    let part_len = partition.len();
                    for (idx, set) in partition.into_iter().enumerate() {
                        let Some((start, end)) = set.range() else {
                            continue;
                        };
                        println!("compacting {start}-{end} ({}/{})", idx + 1, part_len);
                        // TODO: we can compact in conccurently
                        compactor.compact(set).await?;
                    }
                }
            }
            Self::Sync { full, namespace } => match namespace {
                Some(_ns) => {
                    todo!()
                }
                None if *full => {
                    compactor.sync_full().await?;
                    println!("all monitored namespace fully up to date.");
                }
                _ => todo!(),
            },
            Self::Restore {
                namespace,
                out,
                verify,
            } => {
                let namespace = libsql_sys::name::NamespaceName::from_string(namespace.to_string());
                let analysis = compactor.analyze(&namespace)?;
                let set = analysis.shortest_restore_path();
                compactor.restore(set, &out).await?;
                if *verify {
                    let conn = libsql_sys::rusqlite::Connection::open(&out)?;
                    conn.pragma_query(None, "integrity_check", |r| {
                        println!("{r:?}");
                        Ok(())
                    })?;
                }
            }
        }

        Ok(())
    }
}

async fn setup_storage(opt: &S3Args) -> anyhow::Result<S3Backend<StdIO>> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    let mut builder = config.into_builder();
    builder.set_endpoint_url(opt.s3_url.clone());
    builder.set_retry_config(RetryConfig::standard().with_max_attempts(10).into());
    builder.set_region(Region::new(
        opt.s3_region_id.clone().expect("expected aws region"),
    ));
    let cred = Credentials::new(
        opt.s3_access_key_id.as_ref().unwrap(),
        opt.s3_access_key.as_ref().unwrap(),
        None,
        None,
        "Static",
    );
    builder.set_credentials_provider(Some(SharedCredentialsProvider::new(cred)));
    let config = builder.build();
    let backend = S3Backend::from_sdk_config(
        config,
        opt.s3_bucket.clone().context("missing bucket id")?,
        opt.cluster_id.clone().context("missing cluster id")?,
    )
    .await?;

    Ok(backend)
}

#[derive(Debug, clap::Args)]
pub struct S3Args {
    #[arg(long, requires = "S3Args")]
    enable_s3: bool,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_DATABASE_ID")]
    cluster_id: Option<String>,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_ENDPOINT")]
    s3_url: Option<String>,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY")]
    s3_access_key: Option<String>,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION")]
    s3_region_id: Option<String>,
}
