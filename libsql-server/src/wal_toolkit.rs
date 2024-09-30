use std::path::{Path, PathBuf};

use anyhow::Context as _;
use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use chrono::DateTime;
use hashbrown::HashSet;
use indicatif::ProgressStyle;
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
pub enum WalToolkitCommand {
    Monitor(MonitorCommand),
    Analyze(AnalyzeCommand),
    Compact(CompactCommand),
    Sync(SyncCommand),
    Restore(RestoreCommand),
}

impl WalToolkitCommand {
    pub async fn exec(&self, compact_path: &Path, s3_args: &S3Args) -> anyhow::Result<()> {
        let backend = setup_storage(s3_args).await?;
        tokio::fs::create_dir_all(compact_path).await?;
        let mut compactor = Compactor::new(backend.into(), compact_path)?;
        match self {
            Self::Monitor(cmd) => cmd.exec(&mut compactor).await?,
            Self::Analyze(cmd) => cmd.exec(&compactor).await?,
            Self::Compact(cmd) => cmd.exec(&mut compactor).await?,
            Self::Sync(cmd) => cmd.exec(&mut compactor).await?,
            Self::Restore(cmd) => cmd.exec(&compactor).await?,
        }

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
/// Restore namespace
pub struct RestoreCommand {
    #[clap(long)]
    pub verify: bool,
    pub namespace: String,
    pub out: PathBuf,
}

fn make_progress_fn() -> impl FnMut(u32, u32) {
    let bar = indicatif::ProgressBar::new(0);
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:30} {percent_precise}% eta: {eta}")
            .unwrap()
            .progress_chars("##-"),
    );

    move |current, total| {
        bar.set_length(total as u64);
        bar.set_position(current as u64);
    }
}

impl RestoreCommand {
    async fn exec(&self, compactor: &Compactor<S3Backend<StdIO>>) -> Result<(), anyhow::Error> {
        let namespace = NamespaceName::from_string(self.namespace.to_string());
        let analysis = compactor.analyze(&namespace)?;
        let set = analysis.shortest_restore_path();
        compactor
            .restore(set, &self.out, make_progress_fn())
            .await?;
        if self.verify {
            let conn = libsql_sys::rusqlite::Connection::open(&self.out)?;
            conn.pragma_query(None, "integrity_check", |r| {
                println!("{r:?}");
                Ok(())
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, clap::Args)]
/// Sync namespace metadata from remote storage
pub struct SyncCommand {
    /// When performing a full sync, all the segment space is scanned again. By default, only
    /// segments with frame_no greated that the last frame_no are retrieved.
    #[clap(long)]
    full: bool,
    /// unless this is specified, all monitored namespaces are synced
    namespace: Option<String>,
}

impl SyncCommand {
    async fn exec(&self, compactor: &mut Compactor<S3Backend<StdIO>>) -> Result<(), anyhow::Error> {
        match self.namespace {
            Some(ref ns) => {
                let namespace = NamespaceName::from_string(ns.to_string());
                compactor.sync_one(&namespace, self.full).await?;
                println!("`{namespace}` fully up to date.");
            }
            None => {
                compactor.sync_all(self.full).await?;
                println!("all monitored namespace fully up to date.");
            }
        }

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
/// Compact segments into bigger segments
pub struct CompactCommand {
    /// compaction strategy
    #[clap(long, short)]
    pub strategy: CompactStrategy,
    /// prints the compaction plan, but doesn't perform it.
    #[clap(long)]
    pub dry_run: bool,
    /// only compact if it takes more than `threshold` segments to restore
    #[clap(long, short, default_value = "1")]
    pub threshold: usize,
    /// whether to display a progress bar
    #[clap(long, short)]
    pub progress: bool,
    /// namespace to compact, otherwise, all namespaces are compacted
    pub namespace: Option<String>,
    #[clap(requires = "namespace")]
    /// compact to given path instead of sending to backend
    pub out: Option<PathBuf>,
}

impl CompactCommand {
    async fn exec(&self, compactor: &mut Compactor<S3Backend<StdIO>>) -> Result<(), anyhow::Error> {
        match self.namespace {
            Some(ref namespace) => {
                let namespace = NamespaceName::from_string(namespace.to_string());
                self.compact_namespace(compactor, &namespace).await?;
            }
            None => {
                let mut out = Vec::new();
                compactor.list_monitored_namespaces(|ns| {
                    out.push(ns);
                })?;

                for ns in &out {
                    self.compact_namespace(compactor, ns).await?;
                }
            }
        }
        Ok(())
    }

    async fn compact_namespace<B: Backend>(
        &self,
        compactor: &mut Compactor<B>,
        namespace: &NamespaceName,
    ) -> anyhow::Result<()> {
        let analysis = compactor.analyze(&namespace)?;
        let strat: Box<dyn PartitionStrategy> = match self.strategy {
            CompactStrategy::Logarithmic => Box::new(LogReductionStrategy),
            CompactStrategy::CompactAll => Box::new(IdentityStrategy),
        };
        let set = analysis.shortest_restore_path();
        if set.len() <= self.threshold {
            println!(
                "skipping {namespace}: shortest restore path is {}, and threshold is {}",
                set.len(),
                self.threshold,
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

        if self.dry_run {
            println!("dry run: stopping");
        } else {
            println!("performing compaction");
            let part_len = partition.len();
            for (idx, set) in partition.into_iter().enumerate() {
                let Some((start, end)) = set.range() else {
                    continue;
                };
                println!("compacting {start}-{end} ({}/{})", idx + 1, part_len);
                if self.progress {
                    compactor
                        .compact(set, self.out.as_deref(), make_progress_fn())
                        .await?;
                } else {
                    compactor
                        .compact(set, self.out.as_deref(), |_, _| ())
                        .await?;
                }

                // sync back the new segments
                compactor.sync_one(&namespace, false).await?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
/// Analyze segments for a namespaces
pub struct AnalyzeCommand {
    /// list all segments
    #[clap(long)]
    pub list_all: bool,
    pub namespace: String,
}

impl AnalyzeCommand {
    async fn exec(&self, compactor: &Compactor<S3Backend<StdIO>>) -> Result<(), anyhow::Error> {
        let namespace = NamespaceName::from_string(self.namespace.to_string());
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

        if self.list_all {
            println!("segments:");
            compactor.list_all_segments(&namespace, |info| {
                println!(
                    "- {}-{} ({})",
                    info.key.start_frame_no,
                    info.key.end_frame_no,
                    DateTime::from_timestamp_millis(info.key.timestamp as _).unwrap()
                );
            })?;
        }

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
/// Register namespaces to monitor
pub struct MonitorCommand {
    /// list monitored namespaces
    #[clap(long, short)]
    pub list: bool,
    /// Monitor the passed namespace
    #[clap(long, short)]
    pub add: Option<String>,
    /// Unmonitor the passed namespace
    #[clap(long, short)]
    pub delete: Option<String>,
    /// Sync namespaces from a sqld meta-store
    #[clap(long)]
    pub from_db: Option<PathBuf>,
}

impl MonitorCommand {
    async fn exec(&self, compactor: &mut Compactor<S3Backend<StdIO>>) -> Result<(), anyhow::Error> {
        if self.list {
            compactor.list_monitored_namespaces(|ns| {
                println!("{ns}");
            })?;
        } else if let Some(ref namespace) = self.add {
            let namespace = NamespaceName::from_string(namespace.to_string());
            compactor.monitor(&namespace).await?;
            println!("monitoring {namespace}");
        }

        if let Some(ref namespace) = self.delete {
            let namespace = NamespaceName::from_string(namespace.to_string());
            compactor.unmonitor(&namespace)?;
            println!("{namespace} is unmonitored");
        } else if let Some(ref path) = self.from_db {
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
