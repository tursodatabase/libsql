use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait FrameStore: Send + Sync {
    async fn insert_frame(&self, namespace: &str, page_no: u64, frame: bytes::Bytes) -> u64;
    #[allow(dead_code)]
    async fn insert_frames(&self, namespace: &str, frames: Vec<FrameData>) -> u64;
    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes>;
    async fn find_frame(&self, namespace: &str, page_no: u64) -> Option<u64>;
    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u64>;
    async fn frames_in_wal(&self, namespace: &str) -> u64;
    async fn destroy(&self, namespace: &str);
}

#[derive(Default)]
pub struct FrameData {
    pub(crate) page_no: u64,
    pub(crate) data: Bytes,
}
