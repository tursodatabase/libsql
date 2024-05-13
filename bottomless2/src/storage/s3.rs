//! S3 implementation of storage

use super::Storage;

pub struct S3Storage {}

pub struct S3Config {}

impl Storage for S3Storage {
    type Config = S3Config;

    async fn store(
        &self,
        _config: &Self::Config,
        _meta: super::SegmentMeta,
        _segment_data: impl tokio::io::AsyncRead,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: crate::NamespaceName,
        _frame_no: u64,
        _dest: impl tokio::io::AsyncWrite,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn meta(
        &self,
        _config: &Self::Config,
        _namespace: crate::NamespaceName,
    ) -> crate::Result<super::DbMeta> {
        todo!()
    }
}
