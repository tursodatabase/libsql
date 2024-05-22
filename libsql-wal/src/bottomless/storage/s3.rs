//! S3 implementation of storage

use std::future::Future;

use super::Storage;
use crate::{bottomless::Result, io::file::FileExt, name::NamespaceName};

pub struct S3Storage {}

pub struct S3Config {}

impl Storage for S3Storage {
    type Config = S3Config;

    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
        _frame_no: u64,
        _dest: impl tokio::io::AsyncWrite,
    ) -> Result<()> {
        todo!()
    }

    async fn meta(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
    ) -> Result<super::DbMeta> {
        todo!()
    }

    fn default_config(&self) -> std::sync::Arc<Self::Config> {
        todo!()
    }

    fn store(
        &self,
        config: &Self::Config,
        meta: super::SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send {
        todo!();
        #[allow(unreachable_code)]
        std::future::ready(Ok(()))
    }
}
