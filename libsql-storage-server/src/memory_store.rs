use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::store::FrameData;
use crate::store::FrameStore;
use async_trait::async_trait;
use libsql_storage::rpc::Frame;

#[derive(Default)]
pub(crate) struct InMemFrameStore {
    inner: Mutex<InMemInternal>,
}

#[derive(Default)]
struct InMemInternal {
    // contains a frame data, key is the frame number
    frames: BTreeMap<u64, FrameData>,
    // pages map contains the page number as a key and the list of frames for the page as a value
    pages: BTreeMap<u32, Vec<u64>>,
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
    async fn insert_frame(&self, _namespace: &str, page_no: u32, frame: bytes::Bytes) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let frame_no = inner.max_frame_no + 1;
        inner.max_frame_no = frame_no;
        inner.frames.insert(
            frame_no,
            FrameData {
                page_no,
                data: frame,
            },
        );
        inner
            .pages
            .entry(page_no)
            .or_insert_with(Vec::new)
            .push(frame_no);
        frame_no
    }

    async fn insert_frames(&self, _namespace: &str, _max_frame_no: u64, frames: Vec<Frame>) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        for frame in frames {
            let frame_no = inner.max_frame_no + 1;
            inner.max_frame_no = frame_no;
            let page_no = frame.page_no;
            inner.frames.insert(
                frame_no,
                FrameData {
                    page_no,
                    data: frame.data.into(),
                },
            );
            inner
                .pages
                .entry(page_no)
                .or_insert_with(Vec::new)
                .push(frame_no);
            tracing::trace!("inserted for page {} frame {}", page_no, frame_no)
        }
        let count = inner.max_frame_no;
        count
    }

    async fn read_frame(&self, _namespace: &str, frame_no: u64) -> Option<bytes::Bytes> {
        self.inner
            .lock()
            .unwrap()
            .frames
            .get(&frame_no)
            .map(|frame| frame.data.clone())
    }

    // given a page number, return the maximum frame for the page
    async fn find_frame(&self, _namespace: &str, page_no: u32) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .pages
            .get(&page_no)
            .map(|frames| *frames.last().unwrap())
    }

    // given a frame num, return the page number
    async fn frame_page_no(&self, _namespace: &str, frame_no: u64) -> Option<u32> {
        self.inner
            .lock()
            .unwrap()
            .frames
            .get(&frame_no)
            .map(|frame| frame.page_no)
    }

    async fn frames_in_wal(&self, _namespace: &str) -> u64 {
        self.inner.lock().unwrap().max_frame_no
    }

    async fn destroy(&self, _namespace: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.frames.clear();
        inner.pages.clear();
        inner.max_frame_no = 0;
    }
}
