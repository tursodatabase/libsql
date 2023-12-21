use std::sync::Arc;

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::Replicator;
use libsql_sys::wal::wrapper::{WalWrapper, WrappedWal};

use crate::replication::ReplicationLogger;
use crate::replication::primary::replication_logger_wal::{ReplicationLoggerWalManager, ReplicationLoggerWal};

pub type ReplicationWalManager = WalWrapper<Option<BottomlessWalWrapper>, ReplicationLoggerWalManager>;
pub type ReplicationWal = WrappedWal<Option<BottomlessWalWrapper>, ReplicationLoggerWal>;

pub fn make_replication_wal(bottomless: Option<Replicator>, logger: Arc<ReplicationLogger>) -> ReplicationWalManager {
    WalWrapper::new(bottomless.map(BottomlessWalWrapper::new), ReplicationLoggerWalManager::new(logger))
}
