use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql_replication::LIBSQL_PAGE_SIZE;
use libsql_sys::wal::Sqlite3WalManager;
use libsql_wal::io::StdIO;
use libsql_wal::replication::injector::Injector;
use libsql_wal::segment::{Frame, FrameHeader};
use libsql_wal::storage::Storage;
use libsql_wal::{registry::WalRegistry, segment::sealed::SealedSegment};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use zerocopy::FromZeroes;

#[cfg(not(feature = "durable-wal"))]
use libsql_sys::wal::either::Either as EitherWAL;
#[cfg(feature = "durable-wal")]
use libsql_sys::wal::either::Either3 as EitherWAL;

use crate::namespace::broadcasters::BroadcasterRegistry;
use crate::namespace::configurator::{
    BaseNamespaceConfig, NamespaceConfigurators, PrimaryConfig, PrimaryConfigurator,
};
use crate::namespace::meta_store::{MetaStore, MetaStoreHandle};
use crate::namespace::NamespaceStore;

/// The process for migrating from bottomless to libsql wal is simple:
/// 1) iteratate over all namespaces, and make sure that they   
pub async fn bottomless_migrate<S>(
    meta_store: MetaStore,
    storage: Arc<S>,
    base_config: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
) -> anyhow::Result<()>
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
{
    tracing::info!("attempting bottomless migration to libsql-wal");

    let tmp = TempDir::new()?;

    tokio::fs::create_dir_all(tmp.path().join("dbs")).await?;
    tokio::fs::create_dir_all(tmp.path().join("wals")).await?;

    let configs_stream = meta_store.namespaces();
    tokio::pin!(configs_stream);

    let (sender, _) = tokio::sync::mpsc::channel(1);
    let tmp_registry = Arc::new(WalRegistry::new(tmp.path().join("wals"), storage, sender)?);

    let mut configurators = NamespaceConfigurators::default();

    let make_wal_manager = Arc::new(|| EitherWAL::A(Sqlite3WalManager::default()));
    let primary_configurator =
        PrimaryConfigurator::new(base_config.clone(), primary_config, make_wal_manager);
    configurators.with_primary(primary_configurator);

    let dummy_store = NamespaceStore::new(
        false,
        false,
        1000,
        meta_store.clone(),
        NamespaceConfigurators::default(),
        crate::database::DatabaseKind::Primary,
    )
    .await?;

    while let Some(config) = configs_stream.next().await {
        migrate_one(
            &configurators,
            config,
            dummy_store.clone(),
            tmp.path(),
            tmp_registry.clone(),
            &base_config.base_path,
        )
        .await?;
    }

    Ok(())
}

/// this may not be the most efficient method to perform a migration, but it has the advantage of
/// being atomic. when all namespaces are migrated, be rename the dbs and wals folders from the tmp
/// directory, in that order. If we don't find a wals folder in the db directory, we'll just
/// atttempt migrating again, because:
/// - either the migration didn't happen
/// - a crash happened before we could swap the directories
#[tracing::instrument(skip_all, fields(namespace = config.namespace().as_str()))]
async fn migrate_one<S>(
    configurators: &NamespaceConfigurators,
    config: MetaStoreHandle,
    dummy_store: NamespaceStore,
    tmp: &Path,
    tmp_registry: Arc<WalRegistry<StdIO, S>>,
    base_path: &Path,
) -> anyhow::Result<()>
where
    S: Storage<Segment = SealedSegment<std::fs::File>>,
{
    let broadcasters = BroadcasterRegistry::default();
    // TODO: check if we already have a backup for this db from storage
    tracing::info!("started db migrating");
    // we load the namespace ensuring it's restored to the latest version
    configurators
        .configure_primary()?
        .setup(
            config.clone(),
            crate::namespace::RestoreOption::Latest,
            config.namespace(),
            // don't care about reset
            Box::new(|_| ()),
            // don't care about attach
            Arc::new(|_| Ok(PathBuf::new().into())),
            dummy_store.clone(),
            broadcasters.handle(config.namespace().clone()),
        )
        .await?;

    let db_path = tmp
        .join("dbs")
        .join(config.namespace().as_str())
        .join("data");
    let registry = tmp_registry.clone();
    let namespace = config.namespace().clone();
    let shared = tokio::task::spawn_blocking(move || registry.open(&db_path, &namespace.into()))
        .await
        .unwrap()
        .unwrap();

    let mut tx = shared.begin_read(0).into();
    shared.upgrade(&mut tx).unwrap();
    let guard = tx
        .into_write()
        .unwrap_or_else(|_| panic!("should be a write txn"))
        .into_lock_owned();
    let mut injector = Injector::new(shared.clone(), guard, 10)?;
    let orig_db_path = base_path
        .join("dbs")
        .join(config.namespace().as_str())
        .join("data");
    let mut orig_db_file = tokio::fs::File::open(orig_db_path).await?;
    let orig_db_file_len = orig_db_file.metadata().await?.len();
    for i in 0..(orig_db_file_len / LIBSQL_PAGE_SIZE as u64) {
        let mut frame: Box<Frame> = Frame::new_box_zeroed();
        *frame.header_mut() = FrameHeader {
            page_no: (i as u32 + 1).into(),
            size_after: 0.into(),
            frame_no: (i + 1).into(),
        };
        orig_db_file.read_exact(frame.data_mut()).await?;
        injector.insert_frame(frame).await?;
    }

    tracing::info!("sucessfull migration");

    Ok(())
}
