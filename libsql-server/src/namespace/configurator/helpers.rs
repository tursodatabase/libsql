use std::path::Path;
use std::sync::Weak;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use anyhow::Context as _;
use bottomless::replicator::Options;
use bytes::Bytes;
use enclose::enclose;
use futures::Stream;
use libsql_sys::EncryptionConfig;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use tokio::io::AsyncBufReadExt as _;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;

use crate::connection::config::DatabaseConfig;
use crate::connection::connection_manager::InnerWalManager;
use crate::connection::legacy::MakeLegacyConnection;
use crate::connection::{Connection as _, MakeConnection, MakeThrottledConnection};
use crate::database::{PrimaryConnection, PrimaryConnectionMaker};
use crate::error::LoadDumpError;
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::replication_wal::{make_replication_wal_wrapper, ReplicationWalWrapper};
use crate::namespace::{
    NamespaceBottomlessDbId, NamespaceBottomlessDbIdInit, NamespaceName, ResolveNamespacePathFn,
    RestoreOption,
};
use crate::replication::{FrameNo, ReplicationLogger};
use crate::stats::Stats;
use crate::{SqldStorage, StatsSender, BLOCKING_RT, DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT};

use super::{BaseNamespaceConfig, PrimaryConfig};

const WASM_TABLE_CREATE: &str =
    "CREATE TABLE libsql_wasm_func_table (name text PRIMARY KEY, body text) WITHOUT ROWID;";

#[tracing::instrument(skip_all)]
pub(super) async fn make_primary_connection_maker(
    primary_config: &PrimaryConfig,
    base_config: &BaseNamespaceConfig,
    meta_store_handle: &MetaStoreHandle,
    db_path: &Path,
    name: &NamespaceName,
    restore_option: RestoreOption,
    block_writes: Arc<AtomicBool>,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    resolve_attach_path: ResolveNamespacePathFn,
    broadcaster: BroadcasterHandle,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    encryption_config: Option<EncryptionConfig>,
) -> crate::Result<(
    Arc<PrimaryConnectionMaker>,
    ReplicationWalWrapper,
    Arc<Stats>,
)> {
    let db_config = meta_store_handle.get();
    let bottomless_db_id = NamespaceBottomlessDbId::from_config(&db_config);
    // FIXME: figure how to to it per-db
    let mut is_dirty = {
        let sentinel_path = db_path.join(".sentinel");
        if sentinel_path.try_exists()? {
            if std::env::var("LIBSQL_IGNORE_DIRTY_LOG").is_ok() {
                tracing::warn!("ignoring dirty log");
                false
            } else {
                true
            }
        } else {
            tokio::fs::File::create(&sentinel_path).await?;
            false
        }
    };

    // FIXME: due to a bug in logger::checkpoint_db we call regular checkpointing code
    // instead of our virtual WAL one. It's a bit tangled to fix right now, because
    // we need WAL context for checkpointing, and WAL context needs the ReplicationLogger...
    // So instead we checkpoint early, *before* bottomless gets initialized. That way
    // we're sure bottomless won't try to back up any existing WAL frames and will instead
    // treat the existing db file as the source of truth.

    let bottomless_replicator = match primary_config.bottomless_replication {
        Some(ref options) => {
            tracing::debug!("Checkpointing before initializing bottomless");
            crate::replication::primary::logger::checkpoint_db(&db_path.join("data"))?;
            tracing::debug!("Checkpointed before initializing bottomless");
            let options = make_bottomless_options(options, bottomless_db_id, name.clone());
            let (replicator, did_recover) =
                init_bottomless_replicator(db_path.join("data"), options, &restore_option).await?;
            tracing::debug!("Completed init of bottomless replicator");
            is_dirty |= did_recover;
            Some(replicator)
        }
        None => None,
    };

    tracing::debug!("Checking fresh db");
    let is_fresh_db = check_fresh_db(&db_path)?;
    // switch frame-count checkpoint to time-based one
    let auto_checkpoint = if primary_config.checkpoint_interval.is_some() {
        0
    } else {
        DEFAULT_AUTO_CHECKPOINT
    };

    let logger = Arc::new(ReplicationLogger::open(
        &db_path,
        primary_config.max_log_size,
        primary_config.max_log_duration,
        is_dirty,
        auto_checkpoint,
        primary_config.scripted_backup.clone(),
        name.clone(),
        encryption_config.clone(),
    )?);

    tracing::debug!("sending stats");

    let stats = make_stats(
        &db_path,
        join_set,
        meta_store_handle.clone(),
        base_config.stats_sender.clone(),
        name.clone(),
        logger.new_frame_notifier.subscribe(),
    )
    .await?;

    tracing::debug!("Making replication wal wrapper");
    let wal_wrapper = make_replication_wal_wrapper(bottomless_replicator, logger.clone());

    tracing::debug!("Opening libsql connection");

    let connection_maker = Arc::new(
        MakeLegacyConnection::new(
            db_path.to_path_buf(),
            wal_wrapper.clone(),
            stats.clone(),
            broadcaster,
            meta_store_handle.clone(),
            base_config.extensions.clone(),
            base_config.max_response_size,
            base_config.max_total_response_size,
            auto_checkpoint,
            logger.new_frame_notifier.subscribe(),
            encryption_config,
            block_writes,
            resolve_attach_path,
            make_wal_manager.clone(),
        )
        .await?
        .throttled(
            base_config.max_concurrent_connections.clone(),
            Some(DB_CREATE_TIMEOUT),
            base_config.max_total_response_size,
            base_config.max_concurrent_requests,
        ),
    );

    tracing::debug!("Completed opening libsql connection");

    join_set.spawn(run_storage_monitor(
        Arc::downgrade(&stats),
        connection_maker.clone(),
    ));

    // this must happen after we create the connection maker. The connection maker old on a
    // connection to ensure that no other connection is closing while we try to open the dump.
    // that would cause a SQLITE_LOCKED error.
    match restore_option {
        RestoreOption::Dump(_) if !is_fresh_db => {
            Err(LoadDumpError::LoadDumpExistingDb)?;
        }
        RestoreOption::Dump(dump) => {
            let conn = connection_maker.create().await?;
            tracing::debug!("Loading dump");
            load_dump(dump, conn).await?;
            tracing::debug!("Done loading dump");
        }
        _ => { /* other cases were already handled when creating bottomless */ }
    }

    join_set.spawn(run_periodic_compactions(logger.clone()));

    tracing::debug!("Done making primary connection");

    Ok((connection_maker, wal_wrapper, stats))
}

pub(super) fn make_bottomless_options(
    options: &Options,
    namespace_db_id: NamespaceBottomlessDbId,
    name: NamespaceName,
) -> Options {
    let mut options = options.clone();
    let mut db_id = match namespace_db_id {
        NamespaceBottomlessDbId::Namespace(id) => id,
        // FIXME(marin): I don't like that, if bottomless is enabled, proper config must be passed.
        NamespaceBottomlessDbId::NotProvided => options.db_id.unwrap_or_default(),
    };

    db_id = format!("ns-{db_id}:{name}");
    options.db_id = Some(db_id);
    options
}

async fn init_bottomless_replicator(
    path: impl AsRef<std::path::Path>,
    options: bottomless::replicator::Options,
    restore_option: &RestoreOption,
) -> anyhow::Result<(bottomless::replicator::Replicator, bool)> {
    tracing::debug!("Initializing bottomless replication");
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid db path"))?
        .to_owned();
    let mut replicator = bottomless::replicator::Replicator::with_options(path, options).await?;

    let (generation, timestamp) = match restore_option {
        RestoreOption::Latest | RestoreOption::Dump(_) => (None, None),
        RestoreOption::Generation(generation) => (Some(*generation), None),
        RestoreOption::PointInTime(timestamp) => (None, Some(*timestamp)),
    };

    let (action, did_recover) = replicator.restore(generation, timestamp).await?;
    match action {
        bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
            replicator.new_generation().await;
            if let Some(_handle) = replicator.snapshot_main_db_file(true).await? {
                tracing::trace!("got snapshot handle after restore with generation upgrade");
            }
            // Restoration process only leaves the local WAL file if it was
            // detected to be newer than its remote counterpart.
            replicator.maybe_replicate_wal().await?
        }
        bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
            replicator.set_generation(gen);
        }
    }

    Ok((replicator, did_recover))
}

async fn run_periodic_compactions(logger: Arc<ReplicationLogger>) -> anyhow::Result<()> {
    // calling `ReplicationLogger::maybe_compact()` is cheap if the compaction does not actually
    // take place, so we can afford to poll it very often for simplicity
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let handle = BLOCKING_RT.spawn_blocking(enclose! {(logger) move || {
            logger.maybe_compact()
        }});
        handle
            .await
            .expect("Compaction task crashed")
            .context("Compaction failed")?;
    }
}

async fn load_dump<S>(dump: S, conn: PrimaryConnection) -> crate::Result<(), LoadDumpError>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    let mut reader = tokio::io::BufReader::new(StreamReader::new(dump));
    let mut curr = String::new();
    let mut line = String::new();
    let mut skipped_wasm_table = false;
    let mut n_stmt = 0;
    let mut line_id = 0;

    while let Ok(n) = reader.read_line(&mut curr).await {
        line_id += 1;
        if n == 0 {
            break;
        }
        let trimmed = curr.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            curr.clear();
            continue;
        }
        // FIXME: it's well known bug that comment ending with semicolon will be handled incorrectly by currend dump processing code
        let statement_end = trimmed.ends_with(';');

        // we want to concat original(non-trimmed) lines as trimming will join all them in one
        // single-line statement which is incorrect if comments in the end are present
        line.push_str(&curr);
        curr.clear();

        // This is a hack to ignore the libsql_wasm_func_table table because it is already created
        // by the system.
        if !skipped_wasm_table && line.trim() == WASM_TABLE_CREATE {
            skipped_wasm_table = true;
            line.clear();
            continue;
        }

        if statement_end {
            n_stmt += 1;
            // dump must be performd within a txn
            if n_stmt > 2 && conn.is_autocommit().await.unwrap() {
                return Err(LoadDumpError::NoTxn);
            }

            line = tokio::task::spawn_blocking({
                let conn = conn.clone();
                move || -> crate::Result<String, LoadDumpError> {
                    conn.with_raw(|conn| conn.execute(&line, ())).map_err(|e| {
                        LoadDumpError::Internal(format!("line: {}, error: {}", line_id, e))
                    })?;
                    Ok(line)
                }
            })
            .await??;
            line.clear();
        } else {
            line.push(' ');
        }
    }
    tracing::debug!("loaded {} lines from dump", line_id);

    if !conn.is_autocommit().await.unwrap() {
        tokio::task::spawn_blocking({
            let conn = conn.clone();
            move || -> crate::Result<(), LoadDumpError> {
                conn.with_raw(|conn| conn.execute("rollback", ()))?;
                Ok(())
            }
        })
        .await??;
        return Err(LoadDumpError::NoCommit);
    }

    Ok(())
}

fn check_fresh_db(path: &Path) -> crate::Result<bool> {
    let is_fresh = !path.join("wallog").try_exists()?;
    Ok(is_fresh)
}

pub(super) async fn make_stats(
    db_path: &Path,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    meta_store_handle: MetaStoreHandle,
    stats_sender: StatsSender,
    name: NamespaceName,
    mut current_frame_no: watch::Receiver<Option<FrameNo>>,
) -> anyhow::Result<Arc<Stats>> {
    tracing::debug!("creating stats type");
    let stats = Stats::new(name.clone(), db_path, join_set).await?;

    // the storage monitor is optional, so we ignore the error here.
    tracing::debug!("stats created, sending stats");
    let _ = stats_sender
        .send((name.clone(), meta_store_handle, Arc::downgrade(&stats)))
        .await;

    join_set.spawn({
        let stats = stats.clone();
        // initialize the current_frame_no value
        current_frame_no
            .borrow_and_update()
            .map(|fno| stats.set_current_frame_no(fno));
        async move {
            while current_frame_no.changed().await.is_ok() {
                current_frame_no
                    .borrow_and_update()
                    .map(|fno| stats.set_current_frame_no(fno));
            }
            Ok(())
        }
    });

    tracing::debug!("done sending stats, and creating bg tasks");

    Ok(stats)
}

// Periodically check the storage used by the database and save it in the Stats structure.
// TODO: Once we have a separate fiber that does WAL checkpoints, running this routine
// right after checkpointing is exactly where it should be done.
pub(crate) async fn run_storage_monitor<M: MakeConnection>(
    stats: Weak<Stats>,
    connection_maker: Arc<MakeThrottledConnection<M>>,
) -> anyhow::Result<()> {
    // on initialization, the database file doesn't exist yet, so we wait a bit for it to be
    // created
    tokio::time::sleep(Duration::from_secs(1)).await;

    let duration = tokio::time::Duration::from_secs(60);
    loop {
        let Some(stats) = stats.upgrade() else {
            return Ok(());
        };

        match connection_maker.untracked().await {
            Ok(conn) => {
                let _ = BLOCKING_RT
                    .spawn_blocking(move || {
                        conn.with_raw(|conn| {
                            if let Ok(tx) = conn.transaction() {
                                let page_count = tx.query_row("pragma page_count;", [], |row| {
                                    row.get::<usize, u64>(0)
                                });
                                let freelist_count =
                                    tx.query_row("pragma freelist_count;", [], |row| {
                                        row.get::<usize, u64>(0)
                                    });
                                if let (Ok(page_count), Ok(freelist_count)) =
                                    (page_count, freelist_count)
                                {
                                    let storage_bytes_used = (page_count - freelist_count) * 4096;
                                    stats.set_storage_bytes_used(storage_bytes_used);
                                }
                            }
                        })
                    })
                    .await;
            }
            Err(e) => {
                tracing::warn!("failed to open connection for storage monitor: {e}, trying again in {duration:?}");
            }
        }

        tokio::time::sleep(duration).await;
    }
}

pub(super) async fn cleanup_primary(
    base: &BaseNamespaceConfig,
    primary_config: &PrimaryConfig,
    namespace: &NamespaceName,
    db_config: &DatabaseConfig,
    prune_all: bool,
    bottomless_db_id_init: NamespaceBottomlessDbIdInit,
) -> crate::Result<()> {
    let ns_path = base.base_path.join("dbs").join(namespace.as_str());
    if let Some(ref options) = primary_config.bottomless_replication {
        let bottomless_db_id = match bottomless_db_id_init {
            NamespaceBottomlessDbIdInit::Provided(db_id) => db_id,
            NamespaceBottomlessDbIdInit::FetchFromConfig => {
                NamespaceBottomlessDbId::from_config(db_config)
            }
        };
        let options = make_bottomless_options(options, bottomless_db_id, namespace.clone());
        let replicator = bottomless::replicator::Replicator::with_options(
            ns_path.join("data").to_str().unwrap(),
            options,
        )
        .await?;
        if prune_all {
            let delete_all = replicator.delete_all(None).await?;
            // perform hard deletion in the background
            tokio::spawn(delete_all.commit());
        } else {
            // for soft delete make sure that local db is fully backed up
            replicator.savepoint().confirmed().await?;
        }
    }

    if ns_path.try_exists()? {
        tracing::debug!("removing database directory: {}", ns_path.display());
        tokio::fs::remove_dir_all(ns_path).await?;
    }

    Ok(())
}

pub async fn cleanup_libsql(
    namespace: &NamespaceName,
    registry: &WalRegistry<StdIO, SqldStorage>,
    base_path: &Path,
) -> crate::Result<()> {
    let namespace = namespace.clone().into();
    if let Some(shared) = registry.tombstone(&namespace).await {
        // shutdown the registry, don't seal the current segment so that it's not
        tokio::task::spawn_blocking({
            let shared = shared.clone();
            move || shared.shutdown()
        })
        .await
        .unwrap()?;
    }

    let ns_db_path = base_path.join("dbs").join(namespace.as_str());
    if ns_db_path.try_exists()? {
        tracing::debug!("removing database directory: {}", ns_db_path.display());
        let _ = tokio::fs::remove_dir_all(ns_db_path).await;
    }

    let ns_wals_path = base_path.join("wals").join(namespace.as_str());
    if ns_wals_path.try_exists()? {
        tracing::debug!("removing database directory: {}", ns_wals_path.display());
        if let Err(e) = tokio::fs::remove_dir_all(ns_wals_path).await {
            // what can go wrong?:
            match e.kind() {
                // alright, there's nothing to delete anyway
                std::io::ErrorKind::NotFound => (),
                _ => {
                    // something unexpected happened, this namespaces is in a bad state.
                    // The entry will not be removed from the registry to prevent another
                    // namespace with the same name to be reuse the same wal files. a
                    // manual intervention is necessary
                    // FIXME: on namespace creation, we could ensure that this directory is
                    // clean.
                    tracing::error!("error deleting `{namespace}` wal directory, manual intervention may be necessary: {e}");
                    return Err(e.into());
                }
            }
        }
    }

    // when all is cleaned, leave place for next one
    registry.remove(&namespace).await;

    Ok(())
}
