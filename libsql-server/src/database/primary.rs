use std::sync::Arc;

use bottomless::bottomless_wal::BottomlessWalWrapper;
use libsql_sys::wal::wrapper::WalWrapper;

use crate::connection::libsql::{LibSqlConnection, MakeLibSqlConn};
use crate::connection::{MakeThrottledConnection, TrackedConnection};
use crate::namespace::replication_wal::{ReplicationWal, ReplicationWalManager};
use crate::replication::primary::replication_logger_wal::ReplicationLoggerWalManager;

use super::Result;

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationWal>>;
pub type PrimaryConnectionMaker = MakeThrottledConnection<
    MakeLibSqlConn<WalWrapper<Option<BottomlessWalWrapper>, ReplicationLoggerWalManager>>,
>;

pub struct PrimaryDatabase {
    pub wal_manager: ReplicationWalManager,
    pub connection_maker: Arc<PrimaryConnectionMaker>,
}

impl PrimaryDatabase {
    pub fn connection_maker(&self) -> Arc<PrimaryConnectionMaker> {
        self.connection_maker.clone()
    }

    pub fn destroy(self) {
        self.wal_manager
            .wrapped()
            .logger()
            .closed_signal
            .send_replace(true);
    }

    pub async fn shutdown(self) -> Result<()> {
        self.wal_manager
            .wrapped()
            .logger()
            .closed_signal
            .send_replace(true);
        let wal_manager = self.wal_manager;
        if let Some(mut replicator) = tokio::task::spawn_blocking(move || {
            wal_manager.wrapper().as_ref().and_then(|r| r.shutdown())
        })
        .await?
        {
            replicator.shutdown_gracefully().await?;
        }

        Ok(())
    }
}
