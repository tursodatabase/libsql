use std::pin::Pin;
use std::sync::Arc;

use fst::{IntoStreamer, Streamer};
use libsql_sys::name::NamespaceName;
use roaring::RoaringBitmap;
use tokio_stream::Stream;
use zerocopy::FromZeroes;

use crate::io::buf::ZeroCopyBoxIoBuf;
use crate::segment::Frame;
use crate::storage::backend::FindSegmentReq;
use crate::storage::Storage;

use super::Result;

pub trait ReplicateFromStorage: Sync + Send + 'static {
    fn stream<'a>(
        &'a self,
        seen: &'a mut RoaringBitmap,
        current: u64,
        until: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<Box<Frame>>> + 'a + Send>>;
}

pub struct StorageReplicator<S> {
    storage: Arc<S>,
    namespace: NamespaceName,
}

impl<S> StorageReplicator<S> {
    pub fn new(storage: Arc<S>, namespace: NamespaceName) -> Self {
        Self { storage, namespace }
    }
}

impl<S> ReplicateFromStorage for StorageReplicator<S>
where
    S: Storage,
{
    fn stream<'a>(
        &'a self,
        seen: &'a mut roaring::RoaringBitmap,
        mut current: u64,
        until: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<Box<Frame>>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            loop {
                let key = self.storage.find_segment(&self.namespace, FindSegmentReq::EndFrameNoLessThan(current), None).await?;
                let index = self.storage.fetch_segment_index(&self.namespace, &key, None).await?;
                let mut pages = index.into_stream();
                let mut maybe_seg = None;
                while let Some((page, offset)) = pages.next() {
                    let page = u32::from_be_bytes(page.try_into().unwrap());
                    // this segment contains data we are interested in, lazy dowload the segment
                    if !seen.contains(page) {
                        seen.insert(page);
                        let segment = match maybe_seg {
                            Some(ref seg) => seg,
                            None => {
                                maybe_seg = Some(self.storage.fetch_segment_data(&self.namespace, &key, None).await?);
                                maybe_seg.as_ref().unwrap()
                            },
                        };

                        let (frame, ret) = segment.read_frame(ZeroCopyBoxIoBuf::new_uninit(Frame::new_box_zeroed()), offset as u32).await;
                        ret?;
                        let frame = frame.into_inner();
                        debug_assert_eq!(frame.header().size_after(), 0, "all frames in a compacted segment should have size_after set to 0");
                        if frame.header().frame_no() >= until {
                            yield frame;
                        }
                    };
                }

                if key.start_frame_no <= until {
                    break
                }
                current = key.start_frame_no - 1;
            }
        })
    }
}
