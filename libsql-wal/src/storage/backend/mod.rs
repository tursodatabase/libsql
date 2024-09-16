#![allow(dead_code)]
use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fst::Map;
use tokio_stream::Stream;
use uuid::Uuid;

use super::{RestoreOptions, Result, SegmentInfo, SegmentKey};
use crate::io::file::FileExt;
use crate::segment::compacted::CompactedSegmentDataHeader;
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
    pub segment_timestamp: DateTime<Utc>,
}

pub struct RestoreRequest {}

pub struct DbMeta {
    pub max_frame_no: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum FindSegmentReq {
    /// returns a segment containing this frame
    EndFrameNoLessThan(u64),
    /// Returns the segment with closest timestamp less than or equal to the requested timestamp
    Timestamp(DateTime<Utc>),
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

    fn find_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        req: FindSegmentReq,
    ) -> impl Future<Output = Result<SegmentKey>> + Send;

    fn fetch_segment_index(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
    ) -> impl Future<Output = Result<Map<Arc<[u8]>>>> + Send;

    /// Fetch a segment for `namespace` containing `frame_no`, and writes it to `dest`.
    async fn fetch_segment_data_to_file(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<CompactedSegmentDataHeader>;

    // this method taking self: Arc<Self> is an infortunate consequence of rust type system making
    // impl FileExt variant with all the arguments, with no escape hatch...
    fn fetch_segment_data(
        self: Arc<Self>,
        config: Self::Config,
        namespace: NamespaceName,
        key: SegmentKey,
    ) -> impl Future<Output = Result<impl FileExt>> + Send;

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

    fn list_segments<'a>(
        &'a self,
        config: Self::Config,
        namespace: &'a NamespaceName,
        until: u64,
    ) -> impl Stream<Item = Result<SegmentInfo>> + 'a;

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
        req: FindSegmentReq,
    ) -> Result<SegmentKey> {
        self.as_ref().find_segment(config, namespace, req).await
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
    ) -> Result<CompactedSegmentDataHeader> {
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

    fn list_segments<'a>(
        &'a self,
        config: Self::Config,
        namespace: &'a NamespaceName,
        until: u64,
    ) -> impl Stream<Item = Result<SegmentInfo>> + 'a {
        self.as_ref().list_segments(config, namespace, until)
    }
}
