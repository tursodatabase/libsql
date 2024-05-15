use chrono::{DateTime, Utc};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::NamespaceName;
use crate::Result;

mod fs;
mod s3;

pub struct SegmentMeta {
    namespace: NamespaceName,
    start_frame_no: u64,
    end_frame_no: u64,
    create_at: DateTime<Utc>,
}

pub struct RestoreRequest {}

pub struct RestoreOptions {
    /// Namespace to restore
    namespace: NamespaceName,
    /// If provided, will restore up to the most recent segment lesser or equal to `before`
    before: Option<DateTime<Utc>>,
}

pub struct DbMeta {
    max_frame_no: u64,
}

pub trait Storage: Send + Sync {
    /// Config type associated with the Storage
    type Config: Send + Sync;

    /// Store `segment_data` with its associated `meta`
    async fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl AsyncRead,
    ) -> Result<()>;

    /// Fetch a segment for `namespace` containing `frame_no`, and writes it to `dest`.
    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
        _frame_no: u64,
        _dest: impl AsyncWrite,
    ) -> Result<()>;

    /// Fetch meta for `namespace`
    async fn meta(&self, _config: &Self::Config, _namespace: NamespaceName) -> Result<DbMeta>;

    /// Fetch meta batch
    /// implemented in terms of `meta`, can be specialized if implementation is able to query a
    /// batch more efficiently.
    async fn meta_batch(
        &self,
        _config: &Self::Config,
        _namespaces: Vec<NamespaceName>,
    ) -> Result<Vec<DbMeta>> {
        todo!()
    }

    /// Restore namespace, and return the frame index.
    /// The default implementation is implemented in terms of fetch_segment, but it can be
    /// overridden for a more specific implementation if available; for example, a remote storage
    /// server could directly stream the necessary pages, rather than fetching segments until
    /// fully restored.
    fn restore(
        &self,
        _config: &Self::Config,
        _restore_options: RestoreOptions,
        _dest: impl AsyncWrite,
    ) -> Result<u64> {
        todo!("provide default restore implementation")
    }
}
