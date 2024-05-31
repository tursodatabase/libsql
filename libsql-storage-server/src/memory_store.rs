use std::collections::BTreeMap;

use crate::store::FrameData;
use crate::store::FrameStore;
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Default)]
pub(crate) struct InMemFrameStore {
    // contains a frame data, key is the frame number
    frames: BTreeMap<u64, FrameData>,
    // pages map contains the page number as a key and the list of frames for the page as a value
    pages: BTreeMap<u64, Vec<u64>>,
    max_frame_no: u64,
}

impl InMemFrameStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FrameStore for InMemFrameStore {
    // inserts a new frame for the page number and returns the new frame value
    async fn insert_frame(&mut self, _namespace: &str, page_no: u64, frame: Bytes) -> u64 {
        let frame_no = self.max_frame_no + 1;
        self.max_frame_no = frame_no;
        self.frames.insert(
            frame_no,
            FrameData {
                page_no,
                data: frame,
            },
        );
        self.pages
            .entry(page_no)
            .or_insert_with(Vec::new)
            .push(frame_no);
        frame_no
    }

    async fn insert_frames(&mut self, _namespace: &str, _frames: Vec<FrameData>) -> u64 {
        todo!()
    }

    async fn read_frame(&self, _namespace: &str, frame_no: u64) -> Option<bytes::Bytes> {
        self.frames.get(&frame_no).map(|frame| frame.data.clone())
    }

    // given a page number, return the maximum frame for the page
    async fn find_frame(&self, _namespace: &str, page_no: u64) -> Option<u64> {
        self.pages
            .get(&page_no)
            .map(|frames| *frames.last().unwrap())
    }

    // given a frame num, return the page number
    async fn frame_page_no(&self, _namespace: &str, frame_no: u64) -> Option<u64> {
        self.frames.get(&frame_no).map(|frame| frame.page_no)
    }

    async fn frames_in_wal(&self, _namespace: &str) -> u64 {
        self.max_frame_no
    }

    async fn destroy(&mut self, _namespace: &str) {
        self.frames.clear();
        self.pages.clear();
        self.max_frame_no = 0;
    }
}
