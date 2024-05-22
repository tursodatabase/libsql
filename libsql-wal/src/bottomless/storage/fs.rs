use std::future::Future;

use tokio::io::AsyncWrite;

use crate::bottomless::Result;
use crate::name::NamespaceName;

use super::Storage;

pub struct FsStorage {}

impl Storage for FsStorage {
    type Config = ();

    fn store(
        &self,
        config: &Self::Config,
        meta: super::SegmentMeta,
        segment_data: impl crate::io::file::FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send {
        todo!();
        #[allow(unreachable_code)]
        std::future::ready(Ok(()))
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

    fn default_config(&self) -> std::sync::Arc<Self::Config> {
        todo!()
    }
}
