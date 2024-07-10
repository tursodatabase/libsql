#![allow(dead_code)]
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::io::AsyncWrite;
use uuid::Uuid;

use super::Result;
use crate::io::file::FileExt;
use libsql_sys::name::NamespaceName;

pub mod fs;
pub mod s3;

#[derive(Debug)]
pub struct SegmentMeta {
    pub namespace: NamespaceName,
    pub segment_id: Uuid,
    pub start_frame_no: u64,
    pub end_frame_no: u64,
    pub created_at: DateTime<Utc>,
}

pub struct RestoreRequest {}

pub struct RestoreOptions {
    /// Namespace to restore
    namespace: NamespaceName,
    /// If provided, will restore up to the most recent segment lesser or equal to `before`
    before: Option<DateTime<Utc>>,
}

pub struct DbMeta {
    pub max_frame_no: u64,
}

pub trait Backend: Send + Sync + 'static {
    /// Config type associated with the Storage
    type Config: Send + Sync + 'static;

    /// Store `segment_data` with its associated `meta`
    fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Fetch a segment for `namespace` containing `frame_no`, and writes it to `dest`.
    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
        _frame_no: u64,
        _dest_path: &Path,
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

    /// Returns the default configuration for this storage
    fn default_config(&self) -> Arc<Self::Config>;
}

impl<T: Backend> Backend for Arc<T> {
    type Config = T::Config;

    fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send {
        self.as_ref()
            .store(config, meta, segment_data, segment_index)
    }

    async fn fetch_segment(
        &self,
        config: &Self::Config,
        namespace: NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<()> {
        self.as_ref()
            .fetch_segment(config, namespace, frame_no, dest_path)
            .await
    }

    async fn meta(&self, config: &Self::Config, namespace: NamespaceName) -> Result<DbMeta> {
        self.as_ref().meta(config, namespace).await
    }

    fn default_config(&self) -> Arc<Self::Config> {
        self.as_ref().default_config()
    }
}
