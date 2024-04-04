pub trait FrameStore {
    fn insert_frame(&mut self, page_no: u64, frame: bytes::Bytes) -> u64;
    fn read_frame(&self, frame_no: u64) -> Option<&bytes::Bytes>;
    fn find_frame(&self, page_no: u64) -> Option<u64>;
    fn frame_page_no(&self, frame_no: u64) -> Option<u64>;
    fn frames_in_wal(&self) -> u64;
}
