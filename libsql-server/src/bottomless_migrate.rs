use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::Sqlite3WalManager;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::replication::injector::Injector;
use libsql_wal::segment::{Frame, FrameHeader};
use libsql_wal::storage::NoStorage;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use zerocopy::{FromBytes, FromZeroes};

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
/// 1) iteratate over all namespaces, and make sure that they are up to date with bottomless by
///    loading them
/// 2) with a dummy registry, in a temp directory, with no storage, and no checkpointer, inject all the pages from the
///    original db into a new temp db
/// 3) when all namespace have been successfully migrated, make the dbs and wals folders permanent
pub async fn bottomless_migrate(
    meta_store: MetaStore,
    base_config: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
) -> anyhow::Result<()> {
    let base_dbs_dir = base_config.base_path.join("dbs");
    let base_dbs_dir_tmp = base_config.base_path.join("_dbs");
    // the previous migration failed. The _dbs is still present, but the wals is not. In this case
    // we delete the current dbs if it exists and replace it with _dbs, and attempt migration again
    if base_dbs_dir_tmp.try_exists()? {
        tokio::fs::remove_dir_all(&base_dbs_dir).await?;
        tokio::fs::rename(&base_dbs_dir_tmp, &base_dbs_dir).await?;
    }

    tracing::info!("attempting bottomless migration to libsql-wal");

    let tmp = TempDir::new()?;

    tokio::fs::create_dir_all(tmp.path().join("dbs")).await?;
    tokio::fs::create_dir_all(tmp.path().join("wals")).await?;

    let configs_stream = meta_store.namespaces();
    tokio::pin!(configs_stream);

    let (sender, mut rcv) = tokio::sync::mpsc::channel(1);

    // we are not checkpointing anything, be we want to drain the receiver
    tokio::spawn(async move {
        loop {
            match rcv.recv().await {
                Some(libsql_wal::checkpointer::CheckpointMessage::Shutdown) | None => break,
                Some(_) => (),
            }
        }
    });

    let tmp_registry = Arc::new(WalRegistry::new(
        tmp.path().join("wals"),
        NoStorage.into(),
        sender,
    )?);

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

    tmp_registry.shutdown().await?;

    // unix prevents atomically renaming directories with mv, so we first rename dbs to _dbs, then
    // move the new dbs and wals, then remove old dbs.
    // when we perform a check form migration, whe verify if _dbs exists. If it exists, and wals
    // doesn't exist, then we restore it, otherwise, we delete it.
    tokio::fs::rename(&base_dbs_dir, &base_dbs_dir_tmp).await?;
    tokio::fs::rename(tmp.path().join("dbs"), base_dbs_dir).await?;
    tokio::fs::rename(tmp.path().join("wals"), base_config.base_path.join("wals")).await?;
    tokio::fs::remove_dir_all(base_config.base_path.join("_dbs")).await?;

    Ok(())
}

/// this may not be the most efficient method to perform a migration, but it has the advantage of
/// being atomic. when all namespaces are migrated, be rename the dbs and wals folders from the tmp
/// directory, in that order. If we don't find a wals folder in the db directory, we'll just
/// atttempt migrating again, because:
/// - either the migration didn't happen
/// - a crash happened before we could swap the directories
#[tracing::instrument(skip_all, fields(namespace = config.namespace().as_str()))]
async fn migrate_one(
    configurators: &NamespaceConfigurators,
    config: MetaStoreHandle,
    dummy_store: NamespaceStore,
    tmp: &Path,
    tmp_registry: Arc<WalRegistry<StdIO, NoStorage>>,
    base_path: &Path,
) -> anyhow::Result<()> {
    let broadcasters = BroadcasterRegistry::default();
    // TODO: check if we already have a backup for this db from storage
    tracing::info!("started namespace migration");
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

    let db_dir = tmp.join("dbs").join(config.namespace().as_str());
    tokio::fs::create_dir_all(&db_dir).await?;
    let db_path = db_dir.join("data");
    let registry = tmp_registry.clone();
    let namespace = config.namespace().clone();
    let shared = tokio::task::spawn_blocking({
        let registry = registry.clone();
        move || registry.open(&db_path, &namespace.into())
    })
    .await
    .unwrap()?;

    let mut injector = Injector::new(shared.clone(), 10)?;
    let orig_db_path = base_path
        .join("dbs")
        .join(config.namespace().as_str())
        .join("data");
    let mut orig_db_file = tokio::fs::File::open(orig_db_path).await?;
    let mut db_size = usize::MAX;
    let mut current = 0;
    while current < db_size {
        let mut frame: Box<Frame> = Frame::new_box_zeroed();
        orig_db_file.read_exact(frame.data_mut()).await?;
        if current == 0 {
            let header: Sqlite3DbHeader = Sqlite3DbHeader::read_from_prefix(frame.data()).unwrap();
            db_size = header.db_size.get() as usize;
        }
        let size_after = if current == db_size - 1 {
            db_size as u32
        } else {
            0
        };
        *frame.header_mut() = FrameHeader {
            page_no: (current as u32 + 1).into(),
            size_after: size_after.into(),
            frame_no: (current as u64 + 1).into(),
        };
        injector.insert_frame(frame).await?;
        current += 1;
    }

    drop(injector);

    tokio::task::spawn_blocking(move || shared.seal_current())
        .await
        .unwrap()?;

    tracing::info!("sucessfull migration");

    Ok(())
}
