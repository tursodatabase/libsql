use std::sync::{Arc, Mutex};

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::Replicator;
use libsql_storage::LockManager;
use libsql_sys::wal::wrapper::{WalWrapper, WrappedWal};

use crate::replication::primary::replication_logger_wal::{
    ReplicationLoggerWal, ReplicationLoggerWalManager,
};
use crate::replication::ReplicationLogger;

pub type ReplicationWalManager =
    WalWrapper<Option<BottomlessWalWrapper>, ReplicationLoggerWalManager>;
pub type ReplicationWal = WrappedWal<Option<BottomlessWalWrapper>, ReplicationLoggerWal>;

pub fn make_replication_wal(
    bottomless: Option<Replicator>,
    logger: Arc<ReplicationLogger>,
    lock_manager: Arc<Mutex<LockManager>>,
) -> ReplicationWalManager {
    let wal_manager = libsql_storage::DurableWalManager::new(lock_manager);
    WalWrapper::new(
        bottomless.map(|b| BottomlessWalWrapper::new(Arc::new(std::sync::Mutex::new(Some(b))))),
        ReplicationLoggerWalManager::new(wal_manager, logger),
    )
}
