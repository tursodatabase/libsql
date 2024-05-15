use tokio::io::{AsyncRead, AsyncWrite};

use crate::NamespaceName;
use crate::Result;

use super::Storage;

pub struct FsStorage {}

impl Storage for FsStorage {
    type Config = ();

    async fn store(
        &self,
        _config: &Self::Config,
        _meta: super::SegmentMeta,
        _segment_data: impl AsyncRead,
    ) -> Result<()> {
        todo!()
    }

    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
        _frame_no: u64,
        _dest: impl AsyncWrite,
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
}
