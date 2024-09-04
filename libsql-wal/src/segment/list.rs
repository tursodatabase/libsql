use core::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use fst::raw::IndexedValue;
use fst::Streamer;
use roaring::RoaringBitmap;
use tokio_stream::Stream;
use uuid::Uuid;
use zerocopy::FromZeroes;

use crate::error::Result;
use crate::io::buf::{ZeroCopyBoxIoBuf, ZeroCopyBuf};
use crate::io::{FileExt, Io};
use crate::segment::Frame;
use crate::{LibsqlFooter, LIBSQL_MAGIC, LIBSQL_PAGE_SIZE, LIBSQL_WAL_VERSION};

use super::Segment;

#[derive(Debug)]
pub struct SegmentList<Seg> {
    list: List<Seg>,
    checkpointing: AtomicBool,
}

impl<Seg> Default for SegmentList<Seg> {
    fn default() -> Self {
        Self {
            list: Default::default(),
            checkpointing: Default::default(),
        }
    }
}

impl<Seg> Deref for SegmentList<Seg> {
    type Target = List<Seg>;

    fn deref(&self) -> &Self::Target {
        &self.list
    }
}

impl<Seg> SegmentList<Seg>
where
    Seg: Segment,
{
    pub(crate) fn push(&self, segment: Seg) {
        self.list.prepend(segment);
    }
    /// attempt to read page_no with frame_no less than max_frame_no. Returns whether such a page
    /// was found
    pub(crate) fn read_page(
        &self,
        page_no: u32,
        max_frame_no: u64,
        buf: &mut [u8],
    ) -> Result<bool> {
        let mut prev_seg = u64::MAX;
        let mut current = self.list.head.load();
        let mut i = 0;
        while let Some(link) = &*current {
            let last = link.item.last_committed();
            assert!(prev_seg > last);
            prev_seg = last;
            if link.item.read_page(page_no, max_frame_no, buf)? {
                tracing::trace!("found {page_no} in segment {i}");
                return Ok(true);
            }

            i += 1;
            current = link.next.load();
        }

        Ok(false)
    }

    /// Checkpoints as many segments as possible to the main db file, and return the checkpointed
    /// frame_no, if anything was checkpointed
    #[tracing::instrument(skip_all)]
    pub async fn checkpoint<IO: Io>(
        &self,
        db_file: &IO::File,
        until_frame_no: u64,
        log_id: Uuid,
        io: &IO,
    ) -> Result<Option<u64>> {
        struct Guard<'a>(&'a AtomicBool);
        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }

        if self
            .checkpointing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(None);
        }

        let _g = Guard(&self.checkpointing);

        let mut segs = Vec::new();
        let mut current = self.head.load();
        // find the longest chain of segments that can be checkpointed, iow, segments that do not have
        // readers pointing to them
        while let Some(segment) = &*current {
            // skip any segment more recent than until_frame_no
            tracing::debug!(
                last_committed = segment.last_committed(),
                until = until_frame_no
            );
            if segment.last_committed() <= until_frame_no {
                if !segment.is_checkpointable() {
                    segs.clear();
                } else {
                    segs.push(segment.clone());
                }
            }
            current = segment.next.load();
        }

        // nothing to checkpoint rn
        if segs.is_empty() {
            tracing::debug!("nothing to checkpoint");
            return Ok(None);
        }

        let size_after = segs.first().unwrap().size_after();

        let index_iter = segs.iter().map(|s| s.index());

        let mut union = send_fst_ops::SendUnion::from_index_iter(index_iter);

        let mut buf = ZeroCopyBuf::<Frame>::new_uninit();
        let mut last_replication_index = 0;
        while let Some((k, v)) = union.next() {
            let page_no = u32::from_be_bytes(k.try_into().unwrap());
            tracing::trace!(page_no);
            let v = v.iter().min_by_key(|i| i.index).unwrap();
            let offset = v.value as u32;

            let seg = &segs[v.index];
            let (frame, ret) = seg.item.read_frame_offset_async(offset, buf).await;
            ret?;
            assert_eq!(frame.get_ref().header().page_no(), page_no);
            last_replication_index =
                last_replication_index.max(frame.get_ref().header().frame_no());
            let read_buf = frame.map_slice(|f| f.get_ref().data());
            let (read_buf, ret) = db_file
                .write_all_at_async(read_buf, (page_no as u64 - 1) * 4096)
                .await;
            ret?;
            buf = read_buf.into_inner();
        }

        // update the footer at the end of the db file.
        let footer = LibsqlFooter {
            magic: LIBSQL_MAGIC.into(),
            version: LIBSQL_WAL_VERSION.into(),
            replication_index: last_replication_index.into(),
            log_id: log_id.as_u128().into(),
        };

        db_file.set_len(size_after as u64 * LIBSQL_PAGE_SIZE as u64)?;

        let footer_offset = size_after as usize * LIBSQL_PAGE_SIZE as usize;
        let (_, ret) = db_file
            .write_all_at_async(ZeroCopyBuf::new_init(footer), footer_offset as u64)
            .await;
        ret?;

        // todo: truncate if necessary
        //// TODO: make async
        db_file.sync_all()?;

        for seg in segs.iter() {
            seg.destroy(io).await;
        }

        let mut current = self.head.compare_and_swap(&segs[0], None);
        if Arc::ptr_eq(&segs[0], current.as_ref().unwrap()) {
            // nothing to do
        } else {
            loop {
                let next = current
                    .as_ref()
                    .unwrap()
                    .next
                    .compare_and_swap(&segs[0], None);
                if Arc::ptr_eq(&segs[0], next.as_ref().unwrap()) {
                    break;
                } else {
                    current = next;
                }
            }
        }

        self.len.fetch_sub(segs.len(), Ordering::Relaxed);

        tracing::debug!(until = last_replication_index, "checkpointed");

        Ok(Some(last_replication_index))
    }

    /// returns a stream of pages from the sealed segment list, and what's the lowest replication index
    /// that was covered. If the returned index is less than start frame_no, the missing frames
    /// must be read somewhere else.
    pub async fn stream_pages_from<'a>(
        &self,
        current_fno: u64,
        until_fno: u64,
        seen: &'a mut RoaringBitmap,
    ) -> (
        impl Stream<Item = crate::error::Result<Box<Frame>>> + 'a,
        u64,
    ) {
        // collect all the segments we need to read from to be up to date.
        // We keep a reference to them so that they are not discarded while we read them.
        let mut segments = Vec::new();
        let mut current = self.list.head.load();
        while current.is_some() {
            let current_ref = current.as_ref().unwrap();
            if current_ref.item.last_committed() >= until_fno {
                segments.push(current_ref.clone());
                current = current_ref.next.load();
            } else {
                break;
            }
        }

        if segments.is_empty() {
            return (
                tokio_util::either::Either::Left(tokio_stream::empty()),
                current_fno,
            );
        }

        let new_current = segments
            .last()
            .map(|s| s.start_frame_no())
            .unwrap()
            .max(until_fno);

        let stream = async_stream::try_stream! {
            let index_iter = segments.iter().map(|s| s.index());
            let mut union = send_fst_ops::SendUnion::from_index_iter(index_iter);
            while let Some((key_bytes, indexes)) = union.next() {
                let page_no = u32::from_be_bytes(key_bytes.try_into().unwrap());
                // we already have a more recent version of this page.
                if seen.contains(page_no) {
                    continue;
                }
                let IndexedValue { index: segment_offset, value: frame_offset } = indexes.iter().min_by_key(|i| i.index).unwrap();
                let segment = &segments[*segment_offset];

                // we can ignore any frame with a replication index less than start_frame_no
                if segment.start_frame_no() + frame_offset < until_fno {
                    continue
                }

                let buf = ZeroCopyBoxIoBuf::new(Frame::new_box_zeroed());
                let (buf, ret) = segment.read_frame_offset_async(*frame_offset as u32, buf).await;
                ret?;
                let mut frame = buf.into_inner();
                frame.header_mut().size_after = 0.into();
                seen.insert(page_no);
                yield frame;
            }
        };

        (tokio_util::either::Either::Right(stream), new_current)
    }

    pub(crate) fn last(&self) -> Option<Seg>
    where
        Seg: Clone,
    {
        let mut current = self.list.head.load().clone();
        loop {
            match current.as_ref() {
                Some(c) => {
                    if c.next.load().is_none() {
                        return Some(c.item.clone());
                    }
                    current = c.next.load().clone();
                }
                None => return None,
            }
        }
    }
}

struct Node<T> {
    item: T,
    next: ArcSwapOption<Node<T>>,
}

impl<T> Deref for Node<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

pub struct List<T> {
    head: ArcSwapOption<Node<T>>,
    len: AtomicUsize,
}

impl<T: fmt::Debug> fmt::Debug for List<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        let mut current = self.head.load();
        while current.is_some() {
            list.entry(&current.as_ref().unwrap().item);
            current = current.as_ref().unwrap().next.load();
        }
        list.finish()
    }
}

impl<F> Default for List<F> {
    fn default() -> Self {
        Self {
            head: Default::default(),
            len: Default::default(),
        }
    }
}

impl<T> List<T> {
    /// Prepend the list with the passed sealed segment
    pub fn prepend(&self, item: T) {
        let node = Arc::new(Node {
            item,
            next: self.head.load().clone().into(),
        });

        self.head.swap(Some(node));
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Call f on the head of the segments list, if it exists. The head of the list is the most
    /// recent segment.
    pub fn with_head<R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let head = self.head.load();
        head.as_ref().map(|link| f(&link.item))
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

mod send_fst_ops {
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;

    use fst::map::{OpBuilder, Union};

    /// Safety: Union contains a Box<dyn trait> that doesn't require Send, to it's not send.
    /// That's an issue for us, but all the indexes we have are safe to send, so we're good.
    /// FIXME: we could implement union ourselves.
    unsafe impl Send for SendUnion<'_> {}
    unsafe impl Sync for SendUnion<'_> {}

    #[repr(transparent)]
    pub(super) struct SendUnion<'a>(Union<'a>);

    impl<'a> SendUnion<'a> {
        pub fn from_index_iter<I>(iter: I) -> Self
        where
            I: Iterator<Item = &'a fst::map::Map<Arc<[u8]>>>,
        {
            let op = iter.collect::<OpBuilder>().union();
            Self(op)
        }
    }

    impl<'a> Deref for SendUnion<'a> {
        type Target = Union<'a>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<'a> DerefMut for SendUnion<'a> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Seek, Write};
    use tempfile::{tempfile, NamedTempFile};
    use tokio_stream::StreamExt as _;

    use crate::test::{seal_current_segment, TestEnv};

    use super::*;

    #[tokio::test]
    async fn stream_pages() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB(16));", ())
            .unwrap();
        conn.execute("CREATE INDEX i1 ON t1(b);", ()).unwrap();

        for _ in 0..100 {
            for _ in 0..10 {
                conn.execute(
                    "REPLACE INTO t1 VALUES(abs(random() % 500), randomblob(16));",
                    (),
                )
                .unwrap();
            }
            seal_current_segment(&shared);
        }

        seal_current_segment(&shared);

        let current = shared.current.load();
        let segment_list = current.tail();
        let mut seen = RoaringBitmap::new();
        let (stream, _) = segment_list.stream_pages_from(0, 0, &mut seen).await;
        tokio::pin!(stream);

        let mut file = NamedTempFile::new().unwrap();
        let mut tx = shared.begin_read(999999).into();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            let mut buffer = [0; 4096];
            shared
                .read_page(&mut tx, frame.header.page_no(), &mut buffer)
                .unwrap();
            assert_eq!(buffer, frame.data());
            file.write_all(frame.data()).unwrap();
        }

        drop(tx);

        *shared.durable_frame_no.lock() = 999999;
        shared.checkpoint().await.unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut copy_bytes = Vec::new();
        file.read_to_end(&mut copy_bytes).unwrap();

        let mut orig_bytes = Vec::new();
        shared
            .db_file
            .try_clone()
            .unwrap()
            .read_to_end(&mut orig_bytes)
            .unwrap();

        assert_eq!(db_payload(&orig_bytes), db_payload(&copy_bytes));
    }

    #[tokio::test]
    async fn stream_pages_skip_before_start_fno() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("CREATE TABLE test(x);", ()).unwrap();

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }

        seal_current_segment(&shared);

        let current = shared.current.load();
        let segment_list = current.tail();
        let mut seen = RoaringBitmap::new();
        let (stream, replicated_until) = segment_list.stream_pages_from(0, 10, &mut seen).await;
        tokio::pin!(stream);

        assert_eq!(replicated_until, 10);

        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            assert!(frame.header().frame_no() >= 10);
        }
    }

    #[tokio::test]
    async fn stream_pages_ignore_already_seen_pages() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("CREATE TABLE test(x);", ()).unwrap();

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }

        seal_current_segment(&shared);

        let current = shared.current.load();
        let segment_list = current.tail();
        let mut seen = RoaringBitmap::from_sorted_iter([1]).unwrap();
        let (stream, replicated_until) = segment_list.stream_pages_from(0, 1, &mut seen).await;
        tokio::pin!(stream);

        assert_eq!(replicated_until, 1);

        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            assert_ne!(!frame.header().page_no(), 1);
        }
    }

    #[tokio::test]
    async fn stream_pages_resume_replication() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("CREATE TABLE test(x);", ()).unwrap();

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }

        seal_current_segment(&shared);

        let current = shared.current.load();
        let segment_list = current.tail();
        let mut seen = RoaringBitmap::new();
        let (stream, replicated_until) = segment_list.stream_pages_from(0, 1, &mut seen).await;
        tokio::pin!(stream);

        assert_eq!(replicated_until, 1);

        let mut tmp = tempfile().unwrap();

        let mut last_offset = 0;
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            let offset = (frame.header().page_no() - 1) * 4096;
            tmp.write_all_at(frame.data(), offset as u64).unwrap();
            last_offset = last_offset.max(frame.header().frame_no());
        }

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }

        seal_current_segment(&shared);

        let mut seen = RoaringBitmap::new();
        let (stream, replicated_until) = segment_list
            .stream_pages_from(0, last_offset, &mut seen)
            .await;
        tokio::pin!(stream);

        assert_eq!(replicated_until, last_offset);

        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            let offset = (frame.header().page_no() - 1) * 4096;
            tmp.write_all_at(frame.data(), offset as u64).unwrap();
        }

        *shared.durable_frame_no.lock() = 999999;

        shared.checkpoint().await.unwrap();
        tmp.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut copy_bytes = Vec::new();
        tmp.read_to_end(&mut copy_bytes).unwrap();

        let mut orig_bytes = Vec::new();
        shared
            .db_file
            .try_clone()
            .unwrap()
            .read_to_end(&mut orig_bytes)
            .unwrap();

        assert_eq!(db_payload(&copy_bytes), db_payload(&orig_bytes));
    }

    #[tokio::test]
    async fn stream_start_frame_no_before_sealed_segments() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("CREATE TABLE test(x);", ()).unwrap();

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }

        seal_current_segment(&shared);
        *shared.durable_frame_no.lock() = 999999;
        shared.checkpoint().await.unwrap();

        for _ in 0..10 {
            conn.execute("INSERT INTO test VALUES(42)", ()).unwrap();
        }
        seal_current_segment(&shared);

        let current = shared.current.load();
        let segment_list = current.tail();
        let mut seen = RoaringBitmap::new();
        let (stream, replicated_from) = segment_list.stream_pages_from(0, 0, &mut seen).await;
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(_) = stream.next().await {
            count += 1;
        }

        assert_eq!(count, 1);
        assert_eq!(replicated_from, 13);
    }

    fn db_payload(db: &[u8]) -> &[u8] {
        let size = (db.len() / 4096) * 4096;
        &db[..size]
    }
}
