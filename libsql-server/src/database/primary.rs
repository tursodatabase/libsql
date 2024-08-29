use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::connection::legacy::{LegacyConnection, MakeLegacyConnection};
use crate::connection::{MakeThrottledConnection, TrackedConnection};
use crate::namespace::replication_wal::ReplicationWalWrapper;

use super::Result;

pub type PrimaryConnection = TrackedConnection<LegacyConnection<ReplicationWalWrapper>>;
pub type PrimaryConnectionMaker =
    MakeThrottledConnection<MakeLegacyConnection<ReplicationWalWrapper>>;

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
        if let Some(maybe_replicator) = wal_wrapper.wrapped().as_ref() {
            if let Some(mut replicator) = maybe_replicator.shutdown().await {
                replicator.shutdown_gracefully().await?;
            }
        }

        Ok(())
    }

    pub(crate) fn replicator(
        &self,
    ) -> Option<Arc<tokio::sync::Mutex<Option<bottomless::replicator::Replicator>>>> {
        if let Some(wal) = self.wal_wrapper.wrapped() {
            return Some(wal.replicator());
        }
        None
    }
}
