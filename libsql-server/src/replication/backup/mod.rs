pub mod bottomless;

use futures_core::Stream;
use libsql_replication::frame::FrameMut;
use libsql_replication::snapshot::SnapshotFile;
use uuid::{Timestamp, Uuid};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("remote service failure: {0}")]
    ConnectionFailure(#[from] opendal::Error),
    #[error("failed to restore database from snapshot `{0}`")]
    SnapshotRestoreFailed(String),
    #[error("error when processing snapshot contents: {0}")]
    SnapshotError(#[from] libsql_replication::snapshot::Error),
    #[error("failed to parse generation: {0}")]
    GenerationParseFailed(#[from] uuid::Error),
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("snapshots were missing frames - expected {0} but got {1}")]
    MissingFrames(u64, u64),
    #[error("failed to read change counter for generation `{0}`")]
    ChangeCounterError(Uuid),
}

#[async_trait::async_trait]
pub trait Backup {
    async fn backup(&mut self, change_counter: u64, snapshot: SnapshotFile) -> Result<()>;
}

#[async_trait::async_trait]
pub trait Restore {
    type Stream: Stream<Item = Result<FrameMut>> + Send + Unpin;

    async fn restore(&mut self, options: RestoreOptions) -> Result<Self::Stream>;
}

/// Restoration options passed to [Restore::restore] method in order to precise poin-in-time and
/// branch of database history should be restored. Default: the most recent timestamp.
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// (Optional) generation describing potential branch in backup timeline from which restoration
    /// should happen. This may be useful if we have several different backup branches.
    pub generation: Option<Uuid>,
    /// (Optional) point-in-time (with max. second precision) up-to which a database should be
    /// restored. The precise point in time is a subject of granularity in which snapshots are being
    /// made.
    pub point_in_time: Option<Timestamp>,
    /// Current change counter of the database.
    pub change_counter: u64,
}
