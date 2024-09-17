use std::path::{Path, PathBuf};

use anyhow::Context as _;
use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use chrono::DateTime;
use hashbrown::HashSet;
use libsql_sys::name::NamespaceName;
use libsql_wal::io::StdIO;
use libsql_wal::storage::backend::s3::S3Backend;
use libsql_wal::storage::backend::Backend;
use libsql_wal::storage::compaction::strategy::identity::IdentityStrategy;
use libsql_wal::storage::compaction::strategy::log_strategy::LogReductionStrategy;
use libsql_wal::storage::compaction::strategy::PartitionStrategy;
use libsql_wal::storage::compaction::Compactor;
use rusqlite::OpenFlags;

#[derive(Clone, Debug, clap::ValueEnum, Copy)]
pub enum CompactStrategy {
    Logarithmic,
    CompactAll,
}

#[derive(Debug, clap::Subcommand)]
pub enum WalToolkit {
    /// Register namespaces to monitor
    Monitor {
        /// list monitored namespaces
        #[clap(long, short)]
        list: bool,
        /// Monitor the passed namespace
        #[clap(long, short)]
        add: Option<String>,
        /// Unmonitor the passed namespace
        #[clap(long, short)]
        delete: Option<String>,
        /// Sync namespaces from a sqld meta-store
        #[clap(long)]
        from_db: Option<PathBuf>,
    },
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
        /// only compact if it takes more than `threshold` segments to restore
        #[clap(long, short, default_value = "1")]
        threshold: usize,
        /// namespace to compact, otherwise, all namespaces are compacted
        namespace: Option<String>,
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
            Self::Monitor {
                add,
                list,
                from_db,
                delete,
            } => {
                handle_monitor(
                    *list,
                    &mut compactor,
                    add.as_deref(),
                    delete.as_deref(),
                    from_db.as_deref(),
                )
                .await?;
            }
            Self::Analyze {
                namespace,
                list_all,
            } => {
                handle_analyze(namespace, &compactor, *list_all)?;
            }
            Self::Compact {
                strategy,
                dry_run,
                namespace,
                threshold,
            } => {
                handle_compact(
                    namespace.as_deref(),
                    &mut compactor,
                    *threshold,
                    *strategy,
                    *dry_run,
                )
                .await?
            }
            Self::Sync { full, namespace } => {
                handle_sync(namespace.as_deref(), &mut compactor, full).await?
            }
            Self::Restore {
                namespace,
                out,
                verify,
            } => {
                handle_restore(namespace, compactor, out, *verify).await?;
            }
        }

        Ok(())
    }
}

async fn handle_restore(
    namespace: &str,
    compactor: Compactor<S3Backend<StdIO>>,
    out: &Path,
    verify: bool,
) -> Result<(), anyhow::Error> {
    let namespace = NamespaceName::from_string(namespace.to_string());
    let analysis = compactor.analyze(&namespace)?;
    let set = analysis.shortest_restore_path();
    compactor.restore(set, &out).await?;
    Ok(if verify {
        let conn = libsql_sys::rusqlite::Connection::open(&out)?;
        conn.pragma_query(None, "integrity_check", |r| {
            println!("{r:?}");
            Ok(())
        })?;
    })
}

async fn handle_sync(
    namespace: Option<&str>,
    compactor: &mut Compactor<S3Backend<StdIO>>,
    full: &bool,
) -> Result<(), anyhow::Error> {
    Ok(match namespace {
        Some(ns) => {
            let namespace = NamespaceName::from_string(ns.to_string());
            compactor.sync_one(&namespace, *full).await?;
            println!("`{namespace}` fully up to date.");
        }
        None => {
            compactor.sync_all(*full).await?;
            println!("all monitored namespace fully up to date.");
        }
    })
}

async fn handle_compact(
    namespace: Option<&str>,
    compactor: &mut Compactor<S3Backend<StdIO>>,
    threshold: usize,
    strategy: CompactStrategy,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    Ok(match namespace {
        Some(namespace) => {
            let namespace = NamespaceName::from_string(namespace.to_string());
            compact_namespace(compactor, &namespace, threshold, strategy, dry_run).await?;
        }
        None => {
            let mut out = Vec::new();
            compactor.list_monitored_namespaces(|ns| {
                out.push(ns);
            })?;

            for ns in &out {
                compact_namespace(compactor, ns, threshold, strategy, dry_run).await?;
            }
        }
    })
}

fn handle_analyze(
    namespace: &str,
    compactor: &Compactor<S3Backend<StdIO>>,
    list_all: bool,
) -> Result<(), anyhow::Error> {
    let namespace = NamespaceName::from_string(namespace.to_string());
    let analysis = compactor.analyze(&namespace)?;
    println!("stats for {namespace}:");
    println!("- segment count: {}", analysis.segment_count());
    println!("- last frame_no: {}", analysis.last_frame_no());
    let set = analysis.shortest_restore_path();
    println!("- shortest restore path len: {}", set.len());
    if let Some((first, last)) = compactor.get_segment_range(&namespace)? {
        println!(
            "- oldest segment: {}-{} ({})",
            first.key.start_frame_no,
            first.key.end_frame_no,
            DateTime::from_timestamp_millis(first.key.timestamp as _).unwrap()
        );
        println!(
            "- most recent segment: {}-{} ({})",
            last.key.start_frame_no,
            last.key.end_frame_no,
            DateTime::from_timestamp_millis(last.key.timestamp as _).unwrap()
        );
    }
    Ok(if list_all {
        println!("segments:");
        compactor.list_all_segments(&namespace, |info| {
            println!(
                "- {}-{} ({})",
                info.key.start_frame_no,
                info.key.end_frame_no,
                DateTime::from_timestamp_millis(info.key.timestamp as _).unwrap()
            );
        })?;
    })
}

async fn handle_monitor(
    list: bool,
    compactor: &mut Compactor<S3Backend<StdIO>>,
    add: Option<&str>,
    delete: Option<&str>,
    from_db: Option<&Path>,
) -> Result<(), anyhow::Error> {
    if list {
        compactor.list_monitored_namespaces(|ns| {
            println!("{ns}");
        })?;
    } else if let Some(namespace) = add {
        let namespace = NamespaceName::from_string(namespace.to_string());
        compactor.monitor(&namespace).await?;
        println!("monitoring {namespace}");
    }
    Ok(if let Some(namespace) = delete {
        let namespace = NamespaceName::from_string(namespace.to_string());
        compactor.unmonitor(&namespace)?;
        println!("{namespace} is unmonitored");
    } else if let Some(path) = from_db {
        let metastore_path = path.join("metastore").join("data");
        let conn = rusqlite::Connection::open_with_flags(
            metastore_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut stmt = conn.prepare("SELECT namespace FROM namespace_configs")?;
        let metastore_namespaces = stmt
            .query(())?
            .mapped(|r| Ok(NamespaceName::from_string(r.get(0)?)))
            .collect::<Result<HashSet<_>, _>>()?;

        let mut monitored_namespace = HashSet::new();
        compactor.list_monitored_namespaces(|n| {
            monitored_namespace.insert(n);
        })?;

        let to_remove = monitored_namespace.difference(&metastore_namespaces);
        for ns in to_remove {
            println!("- {ns}");
            compactor.unmonitor(ns)?;
        }

        let to_add = metastore_namespaces.difference(&monitored_namespace);
        for ns in to_add {
            println!("+ {ns}");
            compactor.monitor(&ns).await?;
        }
    })
}

async fn compact_namespace<B: Backend>(
    compactor: &mut Compactor<B>,
    namespace: &NamespaceName,
    threshold: usize,
    strategy: CompactStrategy,
    dry_run: bool,
) -> anyhow::Result<()> {
    let analysis = compactor.analyze(&namespace)?;
    let strat: Box<dyn PartitionStrategy> = match strategy {
        CompactStrategy::Logarithmic => Box::new(LogReductionStrategy),
        CompactStrategy::CompactAll => Box::new(IdentityStrategy),
    };
    let set = analysis.shortest_restore_path();
    if set.len() <= threshold {
        println!(
            "skipping {namespace}: shortest restore path is {}, and threshold is {threshold}",
            set.len()
        );
        return Ok(());
    }
    let partition = strat.partition(&set);

    println!("compacting {namespace}:");
    println!("-> initial shortest restore path len: {}", set.len());
    println!("-> compacting into {} segments", partition.len());
    for set in partition.iter() {
        println!("\t- {:?}", set.range().unwrap());
    }

    if dry_run {
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

            // sync back the new segments
            compactor.sync_one(&namespace, false).await?;
        }
    }

    Ok(())
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
