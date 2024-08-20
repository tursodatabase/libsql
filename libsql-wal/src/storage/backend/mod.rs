#![allow(dead_code)]
use std::sync::Arc;
use std::{future::Future, path::Path};

use chrono::{DateTime, Utc};
use fst::Map;
use uuid::Uuid;

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
    type Config: Clone + Send + Sync + 'static;

    /// Store `segment_data` with its associated `meta`
    fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send;

    async fn find_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        frame_no: u64,
    ) -> Result<SegmentKey>;

    async fn fetch_segment_index(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
    ) -> Result<Map<Arc<[u8]>>>;

    /// Fetch a segment for `namespace` containing `frame_no`, and writes it to `dest`.
    async fn fetch_segment_data_to_file(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<()>;

    // this method taking self: Arc<Self> is an infortunate consequence of rust type system making
    // impl FileExt variant with all the arguments, with no escape hatch...
    async fn fetch_segment_data(
        self: Arc<Self>,
        config: Self::Config,
        namespace: NamespaceName,
        key: SegmentKey,
    ) -> Result<impl FileExt>;

    // /// Fetch a segment for `namespace` containing `frame_no`, and writes it to `dest`.
    async fn fetch_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<Map<Arc<[u8]>>>;

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
    fn default_config(&self) -> Self::Config;
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
    ) -> Result<fst::Map<Arc<[u8]>>> {
        self.as_ref()
            .fetch_segment(config, namespace, frame_no, dest_path)
            .await
    }

    async fn meta(&self, config: &Self::Config, namespace: &NamespaceName) -> Result<DbMeta> {
        self.as_ref().meta(config, namespace).await
    }

    fn default_config(&self) -> Self::Config {
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

    async fn find_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        frame_no: u64,
    ) -> Result<SegmentKey> {
        self.as_ref()
            .find_segment(config, namespace, frame_no)
            .await
    }

    async fn fetch_segment_index(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
    ) -> Result<Map<Arc<[u8]>>> {
        self.as_ref()
            .fetch_segment_index(config, namespace, key)
            .await
    }

    async fn fetch_segment_data_to_file(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<()> {
        self.as_ref()
            .fetch_segment_data_to_file(config, namespace, key, file)
            .await
    }

    async fn fetch_segment_data(
        self: Arc<Self>,
        config: Self::Config,
        namespace: NamespaceName,
        key: SegmentKey,
    ) -> Result<impl FileExt> {
        // this implementation makes no sense (Arc<Arc<T>>)
        self.as_ref()
            .clone()
            .fetch_segment_data(config, namespace, key)
            .await
    }
}
