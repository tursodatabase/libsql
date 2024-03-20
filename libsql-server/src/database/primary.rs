use bottomless::SavepointTracker;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::connection::libsql::{LibSqlConnection, MakeLibSqlConn};
use crate::connection::{MakeThrottledConnection, TrackedConnection};
use crate::namespace::replication_wal::ReplicationWalWrapper;

use super::Result;

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationWalWrapper>>;
pub type PrimaryConnectionMaker = MakeThrottledConnection<MakeLibSqlConn<ReplicationWalWrapper>>;

pub struct PrimaryDatabase {
    pub wal_wrapper: ReplicationWalWrapper,
    pub connection_maker: Arc<PrimaryConnectionMaker>,
    pub block_writes: Arc<AtomicBool>,
}

impl PrimaryDatabase {
    pub fn connection_maker(&self) -> Arc<PrimaryConnectionMaker> {
        self.connection_maker.clone()
    }

    pub fn destroy(self) {
        self.wal_wrapper
            .wrapper()
            .logger()
            .closed_signal
            .send_replace(true);
    }

    pub async fn shutdown(self) -> Result<()> {
        self.wal_wrapper
            .wrapper()
            .logger()
            .closed_signal
            .send_replace(true);
        let wal_wrapper = self.wal_wrapper;
        if let Some(mut replicator) = tokio::task::spawn_blocking(move || {
            wal_wrapper.wrapped().as_ref().and_then(|r| r.shutdown())
        })
        .await?
        {
            replicator.shutdown_gracefully().await?;
        }

        Ok(())
    }

    pub fn backup_savepoint(&self) -> Option<SavepointTracker> {
        if let Some(wal) = self.wal_wrapper.wrapped() {
            if let Some(savepoint) = wal.backup_savepoint() {
                return Some(savepoint);
            }
        }
        None
    }
}
