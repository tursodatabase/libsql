use async_trait::async_trait;
use bytes::Bytes;
use libsql_storage::rpc::Frame;

#[async_trait]
pub trait FrameStore: Send + Sync {
    async fn insert_frame(&self, namespace: &str, page_no: u32, frame: bytes::Bytes) -> u64;
    #[allow(dead_code)]
    async fn insert_frames(&self, namespace: &str, max_frame_no: u64, frames: Vec<Frame>) -> u64;
    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes>;
    async fn find_frame(&self, namespace: &str, page_no: u32) -> Option<u64>;
    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u32>;
    async fn frames_in_wal(&self, namespace: &str) -> u64;
    async fn destroy(&self, namespace: &str);
}

#[derive(Default)]
pub struct FrameData {
    pub(crate) page_no: u32,
    pub(crate) data: Bytes,
}
