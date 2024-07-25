#![allow(dead_code)]
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fst::Map;
use uuid::Uuid;

use super::{RestoreOptions, Result};
use super::{RestoreOptions, Result, SegmentKey};
use crate::io::file::FileExt;
use libsql_sys::name::NamespaceName;

// pub mod fs;
#[cfg(feature = "s3")]
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
        config: &Self::Config,
        namespace: &NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<Map<Vec<u8>>>;

    /// Fetch meta for `namespace`
    fn meta(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
    ) -> impl Future<Output = Result<DbMeta>> + Send;

    async fn restore(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        restore_options: RestoreOptions,
        dest: impl FileExt,
    ) -> Result<()>;

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
        namespace: &NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<fst::Map<Vec<u8>>> {
        self.as_ref()
            .fetch_segment(config, namespace, frame_no, dest_path)
            .await
    }

    async fn meta(&self, config: &Self::Config, namespace: &NamespaceName) -> Result<DbMeta> {
        self.as_ref().meta(config, namespace).await
    }

    fn default_config(&self) -> Arc<Self::Config> {
        self.as_ref().default_config()
    }

    async fn restore(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        restore_options: RestoreOptions,
        dest: impl FileExt,
    ) -> Result<()> {
        self.as_ref()
            .restore(config, namespace, restore_options, dest)
            .await
    }
}
