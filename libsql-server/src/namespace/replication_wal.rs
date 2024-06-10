use std::sync::Arc;

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::Replicator;
use libsql_sys::wal::wrapper::{Then, WrapWal};

use crate::connection::connection_manager::ManagedConnectionWal;
use crate::replication::primary::replication_logger_wal::ReplicationLoggerWalWrapper;
use crate::replication::ReplicationLogger;

pub type ReplicationWalWrapper =
    Then<ReplicationLoggerWalWrapper, Option<BottomlessWalWrapper>, ManagedConnectionWal>;

pub fn make_replication_wal_wrapper(
    bottomless: Option<Replicator>,
    logger: Arc<ReplicationLogger>,
) -> ReplicationWalWrapper {
    ReplicationLoggerWalWrapper::new(logger).then(
        bottomless.map(|b| BottomlessWalWrapper::new(Arc::new(std::sync::Mutex::new(Some(b))))),
    )
}
