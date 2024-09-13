use std::hash::Hasher;
use std::io::{BufWriter, IoSlice, Write};
use std::num::NonZeroU64;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use fst::MapBuilder;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use tokio_stream::Stream;
use uuid::Uuid;
use zerocopy::little_endian::U32;
use zerocopy::{AsBytes, FromZeroes};

use crate::io::buf::{IoBufMut, ZeroCopyBoxIoBuf, ZeroCopyBuf};
use crate::io::file::FileExt;
use crate::io::Inspect;
use crate::segment::{checked_frame_offset, SegmentFlags};
use crate::segment::{frame_offset, page_offset, sealed::SealedSegment};
use crate::transaction::{Transaction, TxGuardOwned, TxGuardShared};
use crate::{LIBSQL_MAGIC, LIBSQL_PAGE_SIZE, LIBSQL_WAL_VERSION};

use super::list::SegmentList;
use super::{CheckedFrame, Frame, FrameHeader, SegmentHeader};

use crate::error::Result;

pub struct CurrentSegment<F> {
    path: PathBuf,
    index: SegmentIndex,
    header: Mutex<SegmentHeader>,
    file: Arc<F>,
    /// Read lock count on this segment. Each begin_read increments the count of readers on the current
    /// lock
    read_locks: Arc<AtomicU64>,
    sealed: AtomicBool,
    /// current running checksum
    current_checksum: AtomicU32,
    tail: Arc<SegmentList<SealedSegment<F>>>,
}

impl<F> CurrentSegment<F> {
    /// Create a new segment from the given path and metadata. The file pointed to by path must not
    /// exist.
    pub fn create(
        segment_file: F,
        path: PathBuf,
        start_frame_no: NonZeroU64,
        db_size: u32,
        tail: Arc<SegmentList<SealedSegment<F>>>,
        salt: u32,
        log_id: Uuid,
    ) -> Result<Self>
    where
        F: FileExt,
    {
        let mut header = SegmentHeader {
            start_frame_no: start_frame_no.get().into(),
            last_commited_frame_no: 0.into(),
            size_after: db_size.into(),
            index_offset: 0.into(),
            index_size: 0.into(),
            header_cheksum: 0.into(),
            flags: 0.into(),
            magic: LIBSQL_MAGIC.into(),
            version: LIBSQL_WAL_VERSION.into(),
            salt: salt.into(),
            page_size: LIBSQL_PAGE_SIZE.into(),
            log_id: log_id.as_u128().into(),
            frame_count: 0.into(),
            sealed_at_timestamp: 0.into(),
        };

        header.recompute_checksum();

        segment_file.write_all_at(header.as_bytes(), 0)?;

        Ok(Self {
            path: path.to_path_buf(),
            index: SegmentIndex::new(start_frame_no.get()),
            header: Mutex::new(header),
            file: segment_file.into(),
            read_locks: Arc::new(AtomicU64::new(0)),
            sealed: AtomicBool::default(),
            tail,
            current_checksum: salt.into(),
        })
    }

    pub fn log_id(&self) -> Uuid {
        Uuid::from_u128(self.header.lock().log_id.get())
    }

    pub fn is_empty(&self) -> bool {
        self.header.lock().is_empty()
    }

    pub fn with_header<R>(&self, f: impl FnOnce(&SegmentHeader) -> R) -> R {
        let header = self.header.lock();
        f(&header)
    }

    pub fn last_committed(&self) -> u64 {
        self.header.lock().last_committed()
    }

    pub fn next_frame_no(&self) -> NonZeroU64 {
        self.header.lock().next_frame_no()
    }

    pub fn count_committed(&self) -> usize {
        self.header.lock().frame_count()
    }

    pub fn db_size(&self) -> u32 {
        self.header.lock().size_after.get()
    }

    pub fn current_checksum(&self) -> u32 {
        self.current_checksum.load(Ordering::Relaxed)
    }

    /// insert a bunch of frames in the Wal. The frames needn't be ordered, therefore, on commit
    /// the last frame no needs to be passed alongside the new size_after.
    #[tracing::instrument(skip_all)]
    pub async fn inject_frames(
        &self,
        frames: Vec<Box<Frame>>,
        // (size_after, last_frame_no)
        commit_data: Option<(u32, u64)>,
        tx: &mut TxGuardOwned<F>,
    ) -> Result<Vec<Box<Frame>>>
    where
        F: FileExt,
    {
        assert!(!self.sealed.load(Ordering::SeqCst));
        assert_eq!(
            tx.savepoints.len(),
            1,
            "injecting wal should not use savepoints"
        );
        {
            let tx = tx.deref_mut();
            // let mut commit_frame_written = false;
            let current_savepoint = tx.savepoints.last_mut().expect("no savepoints initialized");
            let mut frames = frame_list_to_option(frames);
            // For each frame, we compute and write the frame checksum, followed by the frame
            // itself as an array of CheckedFrame
            for i in 0..frames.len() {
                let offset = tx.next_offset;
                let current_checksum = current_savepoint.current_checksum;
                let mut digest = crc32fast::Hasher::new_with_initial(current_checksum);
                digest.write(frames[i].as_ref().unwrap().as_bytes());
                let new_checksum = digest.finalize();
                let (_buf, ret) = self
                    .file
                    .write_all_at_async(
                        ZeroCopyBuf::new_init(zerocopy::byteorder::little_endian::U32::new(
                            new_checksum,
                        )),
                        checked_frame_offset(offset),
                    )
                    .await;
                ret?;

                let buf = ZeroCopyBoxIoBuf::new(frames[i].take().unwrap());
                let (buf, ret) = self
                    .file
                    .write_all_at_async(buf, frame_offset(offset))
                    .await;
                ret?;

                let frame = buf.into_inner();

                current_savepoint
                    .index
                    .insert(frame.header().page_no(), offset);
                current_savepoint.current_checksum = new_checksum;
                tx.next_offset += 1;
                frames[i] = Some(frame);
            }

            if let Some((size_after, last_frame_no)) = commit_data {
                if tx.not_empty() {
                    let mut header = { *self.header.lock() };
                    header.last_commited_frame_no = last_frame_no.into();
                    header.size_after = size_after.into();
                    // set frames unordered because there are no guarantees that we received frames
                    // in order.
                    header.set_flags(header.flags().union(SegmentFlags::FRAME_UNORDERED));
                    {
                        let savepoint = tx.savepoints.first().unwrap();
                        header.frame_count = (header.frame_count.get()
                            + (tx.next_offset - savepoint.next_offset) as u64)
                            .into();
                    }
                    header.recompute_checksum();

                    let (header, ret) = self
                        .file
                        .write_all_at_async(ZeroCopyBuf::new_init(header), 0)
                        .await;

                    ret?;

                    // self.file.sync_data().unwrap();
                    tx.merge_savepoints(&self.index);
                    // set the header last, so that a transaction does not witness a write before
                    // it's actually committed.
                    self.current_checksum
                        .store(tx.current_checksum(), Ordering::Relaxed);
                    *self.header.lock() = header.into_inner();

                    tx.is_commited = true;
                }
            }

            let frames = options_to_frame_list(frames);

            Ok(frames)
        }
    }

    #[tracing::instrument(skip(self, pages, tx))]
    pub fn insert_pages<'a>(
        &self,
        pages: impl Iterator<Item = (u32, &'a [u8])>,
        size_after: Option<u32>,
        tx: &mut TxGuardShared<F>,
    ) -> Result<Option<u64>>
    where
        F: FileExt,
    {
        assert!(!self.sealed.load(Ordering::SeqCst));
        {
            let tx = tx.deref_mut();
            let mut pages = pages.peekable();
            // let mut commit_frame_written = false;
            let current_savepoint = tx.savepoints.last_mut().expect("no savepoints initialized");
            while let Some((page_no, page)) = pages.next() {
                // optim: if the page is already present, overwrite its content
                if let Some(offset) = current_savepoint.index.get(&page_no) {
                    tracing::trace!(page_no, "recycling frame");
                    self.file.write_all_at(page, page_offset(*offset))?;
                    // we overwrote a frame, record that for later rewrite
                    tx.recompute_checksum = Some(
                        tx.recompute_checksum
                            .map(|old| old.min(*offset))
                            .unwrap_or(*offset),
                    );
                    continue;
                }

                tracing::trace!(page_no, "inserting new frame");
                let size_after = if let Some(size) = size_after {
                    pages.peek().is_none().then_some(size).unwrap_or(0)
                } else {
                    0
                };

                let frame_no = tx.next_frame_no;
                let header = FrameHeader {
                    page_no: page_no.into(),
                    size_after: size_after.into(),
                    frame_no: frame_no.into(),
                };

                // only compute checksum if we don't need to recompute it later
                let checksum = if tx.recompute_checksum.is_none() {
                    let mut digest =
                        crc32fast::Hasher::new_with_initial(current_savepoint.current_checksum);
                    digest.write(header.as_bytes());
                    digest.write(page);
                    digest.finalize()
                } else {
                    0
                };

                let checksum_bytes = checksum.to_le_bytes();
                // We write a instance of a ChecksummedFrame
                let slices = &[
                    IoSlice::new(&checksum_bytes),
                    IoSlice::new(header.as_bytes()),
                    IoSlice::new(&page),
                ];
                let offset = tx.next_offset;
                debug_assert_eq!(
                    self.header.lock().start_frame_no.get() + offset as u64,
                    frame_no
                );
                self.file
                    .write_at_vectored(slices, checked_frame_offset(offset))?;
                assert!(
                    current_savepoint.index.insert(page_no, offset).is_none(),
                    "existing frames should be recycled"
                );
                current_savepoint.current_checksum = checksum;
                tx.next_frame_no += 1;
                tx.next_offset += 1;
            }
        }

        // commit
        if let Some(size_after) = size_after {
            if tx.not_empty() {
                let new_checksum = if let Some(offset) = tx.recompute_checksum {
                    self.recompute_checksum(offset, tx.next_offset - 1)?
                } else {
                    tx.current_checksum()
                };

                #[cfg(debug_assertions)]
                {
                    // ensure that file checksum for that transaction is valid
                    let from = {
                        let header = self.header.lock();
                        if header.last_commited_frame_no() == 0 {
                            0
                        } else {
                            (header.last_commited_frame_no() - header.start_frame_no.get()) as u32
                        }
                    };

                    self.assert_valid_checksum(from, tx.next_offset - 1)?;
                }

                let last_frame_no = tx.next_frame_no - 1;
                let mut header = { *self.header.lock() };
                header.last_commited_frame_no = last_frame_no.into();
                header.size_after = size_after.into();
                // count how many frames were appeneded: basically last appeneded offset - initial
                // offset
                let tx = tx.deref_mut();
                let savepoint = tx.savepoints.first().unwrap();
                header.frame_count = (header.frame_count.get()
                    + (tx.next_offset - savepoint.next_offset) as u64)
                    .into();
                header.recompute_checksum();

                self.file.write_all_at(header.as_bytes(), 0)?;
                // todo: sync if sync mode is EXTRA
                // self.file.sync_data().unwrap();
                tx.merge_savepoints(&self.index);
                // set the header last, so that a transaction does not witness a write before
                // it's actually committed.
                *self.header.lock() = header;
                self.current_checksum.store(new_checksum, Ordering::Relaxed);

                tx.is_commited = true;

                return Ok(Some(last_frame_no));
            }
        }
        Ok(None)
    }

    /// return the offset of the frame for page_no, with frame_no no larger that max_frame_no, if
    /// it exists
    pub fn find_frame(&self, page_no: u32, tx: &Transaction<F>) -> Option<u32> {
        // if it's a write transaction, check its transient index first
        if let Transaction::Write(ref tx) = tx {
            if let Some(offset) = tx.find_frame_offset(page_no) {
                return Some(offset);
            }
        }

        // not a write tx, or page is not in write tx, look into the segment
        self.index.locate(page_no, tx.max_offset)
    }

    /// reads the page conainted in frame at offset into buf
    #[tracing::instrument(skip(self, buf))]
    pub fn read_page_offset(&self, offset: u32, buf: &mut [u8]) -> Result<()>
    where
        F: FileExt,
    {
        tracing::trace!("read page");
        debug_assert_eq!(buf.len(), 4096);
        self.file.read_exact_at(buf, page_offset(offset))?;

        Ok(())
    }

    async fn read_frame_offset_async<B>(&self, offset: u32, buf: B) -> (B, std::io::Result<()>)
    where
        F: FileExt,
        B: IoBufMut + Send + 'static,
    {
        let byte_offset = frame_offset(offset);
        self.file.read_exact_at_async(buf, byte_offset).await
    }

    #[allow(dead_code)]
    pub fn frame_header_at(&self, offset: u32) -> Result<FrameHeader>
    where
        F: FileExt,
    {
        let mut header = FrameHeader::new_zeroed();
        self.file
            .read_exact_at(header.as_bytes_mut(), frame_offset(offset))?;
        Ok(header)
    }

    /// It is expected that sealing is performed under a write lock
    #[tracing::instrument(skip_all)]
    pub fn seal(&self, now: DateTime<Utc>) -> Result<Option<SealedSegment<F>>>
    where
        F: FileExt,
    {
        let mut header = self.header.lock();
        let index_offset = header.frame_count() as u32;
        let index_byte_offset = checked_frame_offset(index_offset);
        let mut cursor = self.file.cursor(index_byte_offset);
        let writer = BufWriter::new(&mut cursor);

        let current = self.current_checksum();
        let mut digest = crc32fast::Hasher::new_with_initial(current);
        let mut writer = Inspect::new(writer, |data: &[u8]| {
            digest.write(data);
        });
        self.index.merge_all(&mut writer)?;
        let mut writer = writer.into_inner();
        let index_checksum = digest.finalize();
        let index_size = writer.get_ref().count();
        writer.write_all(&index_checksum.to_le_bytes())?;

        writer.into_inner().map_err(|e| e.into_parts().0)?;
        // we perform a first sync to ensure that all the segment has been flushed to disk. We then
        // write the header and flush again. We want to guarantee that if we find a segement marked
        // as "SEALED", then there was no partial flush.
        //
        // If a segment is found that doesn't have the SEALED flag, then we enter crash recovery,
        // and we need to check the segment.
        self.file.sync_all()?;

        header.index_offset = index_byte_offset.into();
        header.index_size = index_size.into();
        let flags = header.flags();
        header.set_flags(flags | SegmentFlags::SEALED);
        header.sealed_at_timestamp = (now.timestamp_millis() as u64).into();
        header.recompute_checksum();
        self.file.write_all_at(header.as_bytes(), 0)?;

        // flush the header.
        self.file.sync_all()?;

        let sealed = SealedSegment::open(
            self.file.clone(),
            self.path.clone(),
            self.read_locks.clone(),
        )?;

        // we only flip the sealed mark when no more error can occur, or we risk to deadlock a read
        // transaction waiting for a more recent version of the segment that is never going to arrive
        assert!(
            self.sealed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok(),
            "attempt to seal an already sealed segment"
        );

        tracing::debug!("segment sealed");

        Ok(sealed)
    }

    pub fn last_committed_frame_no(&self) -> u64 {
        let header = self.header.lock();
        if header.last_commited_frame_no.get() == 0 {
            header.start_frame_no.get()
        } else {
            header.last_commited_frame_no.get()
        }
    }

    pub fn inc_reader_count(&self) {
        self.read_locks().fetch_add(1, Ordering::SeqCst);
    }

    /// return true if the reader count is 0
    pub fn dec_reader_count(&self) -> bool {
        self.read_locks().fetch_sub(1, Ordering::SeqCst) - 1 == 0
    }

    pub fn read_locks(&self) -> &AtomicU64 {
        self.read_locks.as_ref()
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    pub fn tail(&self) -> &Arc<SegmentList<SealedSegment<F>>> {
        &self.tail
    }

    /// returns all the frames that changed between start_frame_no and the current commit index
    pub(crate) fn frame_stream_from<'a>(
        &'a self,
        start_frame_no: u64,
        seen: &'a mut RoaringBitmap,
    ) -> (impl Stream<Item = Result<Box<Frame>>> + 'a, u64, u32)
    where
        F: FileExt,
    {
        let (seg_start_frame_no, last_committed, db_size) =
            self.with_header(|h| (h.start_frame_no.get(), h.last_committed(), h.size_after()));
        let replicated_until = seg_start_frame_no
            // if current is empty, start_frame_no doesn't exist
            .min(last_committed)
            .max(start_frame_no);

        // TODO: optim, we could read less frames if we had a mapping from frame_no to page_no in
        // the index
        let stream = async_stream::try_stream! {
            if !self.is_empty() {
                let mut frame_offset = (last_committed - seg_start_frame_no) as u32;
                loop {
                    let buf = ZeroCopyBoxIoBuf::new(Frame::new_box_zeroed());
                    let (buf, res) = self.read_frame_offset_async(frame_offset, buf).await;
                    res?;

                    let mut frame = buf.into_inner();
                    frame.header_mut().size_after = 0.into();
                    let page_no = frame.header().page_no();

                    let frame_no = frame.header().frame_no();
                    if frame_no < start_frame_no {
                        break
                    }

                    if !seen.contains(page_no) {
                        seen.insert(page_no);
                        yield frame;
                    }

                    if frame_offset == 0 {
                        break
                    }

                    frame_offset -= 1;
                }
            }
        };

        (stream, replicated_until, db_size)
    }

    fn recompute_checksum(&self, start_offset: u32, until_offset: u32) -> Result<u32>
    where
        F: FileExt,
    {
        let mut current_checksum = if start_offset == 0 {
            self.header.lock().salt.get()
        } else {
            // we get the checksum from the frame just before the the start offset
            let frame_offset = checked_frame_offset(start_offset - 1);
            let mut out = U32::new(0);
            self.file.read_exact_at(out.as_bytes_mut(), frame_offset)?;
            out.get()
        };

        let mut checked_frame: Box<CheckedFrame> = CheckedFrame::new_box_zeroed();
        for offset in start_offset..=until_offset {
            let frame_offset = checked_frame_offset(offset);
            self.file
                .read_exact_at(checked_frame.as_bytes_mut(), frame_offset)?;
            current_checksum = checked_frame.frame.checksum(current_checksum);
            self.file
                .write_all_at(&current_checksum.to_le_bytes(), frame_offset)?;
        }

        Ok(current_checksum)
    }

    /// test fuction to ensure checksum integrity
    #[cfg(debug_assertions)]
    #[track_caller]
    fn assert_valid_checksum(&self, from: u32, until: u32) -> Result<()>
    where
        F: FileExt,
    {
        let mut frame: Box<CheckedFrame> = CheckedFrame::new_box_zeroed();
        let mut current_checksum = if from != 0 {
            let offset = checked_frame_offset(from - 1);
            self.file.read_exact_at(frame.as_bytes_mut(), offset)?;
            frame.checksum.get()
        } else {
            self.header.lock().salt.get()
        };

        for i in from..=until {
            let offset = checked_frame_offset(i);
            self.file.read_exact_at(frame.as_bytes_mut(), offset)?;
            current_checksum = frame.frame.checksum(current_checksum);
            assert_eq!(
                current_checksum,
                frame.checksum.get(),
                "invalid checksum at offset {i}"
            );
        }

        Ok(())
    }
}

fn frame_list_to_option(frames: Vec<Box<Frame>>) -> Vec<Option<Box<Frame>>> {
    // this is safe because Option<Box<T>> and Box<T> are the same size and Frame is sized:
    // https://doc.rust-lang.org/std/option/index.html#representation
    unsafe { std::mem::transmute(frames) }
}

fn options_to_frame_list(frames: Vec<Option<Box<Frame>>>) -> Vec<Box<Frame>> {
    debug_assert!(frames.iter().all(|f| f.is_some()));
    // this is safe because Option<Box<T>> and Box<T> are the same size and Frame is sized:
    // https://doc.rust-lang.org/std/option/index.html#representation
    unsafe { std::mem::transmute(frames) }
}

impl<F> Drop for CurrentSegment<F> {
    fn drop(&mut self) {
        // todo: if reader is 0 and segment is sealed, register for compaction.
    }
}

/// TODO: implement spill-to-disk when txn is too large
/// TODO: optimize that data structure with something more custom. I can't find a wholy satisfying
/// structure in the wild.
pub(crate) struct SegmentIndex {
    start_frame_no: u64,
    // TODO: measure perf, and consider using https://docs.rs/bplustree/latest/bplustree/
    index: SkipMap<u32, RwLock<Vec<u32>>>,
}

impl SegmentIndex {
    pub fn new(start_frame_no: u64) -> Self {
        Self {
            start_frame_no,
            index: Default::default(),
        }
    }

    fn locate(&self, page_no: u32, max_offset: u64) -> Option<u32> {
        let offsets = self.index.get(&page_no)?;
        let offsets = offsets.value().read();
        offsets
            .iter()
            .rev()
            .find(|fno| **fno as u64 <= max_offset)
            .copied()
    }

    #[tracing::instrument(skip_all)]
    fn merge_all<W: Write>(&self, writer: W) -> Result<()> {
        let mut builder = MapBuilder::new(writer)?;
        let Some(mut entry) = self.index.front() else {
            return Ok(());
        };
        loop {
            let offset = *entry.value().read().last().unwrap();
            builder.insert(entry.key().to_be_bytes(), offset as u64)?;
            if !entry.move_next() {
                break;
            }
        }

        builder.finish()?;
        Ok(())
    }

    pub(crate) fn insert(&self, page_no: u32, offset: u32) {
        let entry = self.index.get_or_insert(page_no, Default::default());
        let mut offsets = entry.value().write();
        if offsets.is_empty() || *offsets.last().unwrap() < offset {
            offsets.push(offset);
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, Read};

    use chrono::{DateTime, Utc};
    use hashbrown::HashMap;
    use insta::assert_debug_snapshot;
    use rand::rngs::ThreadRng;
    use tempfile::{tempdir, tempfile};
    use tokio_stream::StreamExt;
    use uuid::Uuid;

    use crate::io::{FileExt, Io};
    use crate::test::{seal_current_segment, TestEnv};

    use super::*;

    #[tokio::test]
    async fn current_stream_frames() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();
        for _ in 0..50 {
            conn.execute("insert into test values (randomblob(256))", ())
                .unwrap();
        }

        let mut seen = RoaringBitmap::new();
        let current = shared.current.load();
        let (stream, replicated_until, size_after) = current.frame_stream_from(1, &mut seen);
        tokio::pin!(stream);
        assert_eq!(replicated_until, 1);
        assert_eq!(size_after, 6);

        let mut tmp = tempfile().unwrap();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            let offset = (frame.header().page_no() - 1) * 4096;
            tmp.write_all_at(frame.data(), offset as _).unwrap();
        }

        seal_current_segment(&shared);
        *shared.durable_frame_no.lock() = 999999;
        shared.checkpoint().await.unwrap();

        let mut orig = Vec::new();
        shared
            .db_file
            .try_clone()
            .unwrap()
            .read_to_end(&mut orig)
            .unwrap();

        let mut copy = Vec::new();
        tmp.read_to_end(&mut copy).unwrap();

        assert_eq!(db_payload(&copy), db_payload(&orig));
    }

    #[tokio::test]
    async fn current_stream_frames_incomplete() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();

        for _ in 0..50 {
            conn.execute("insert into test values (randomblob(256))", ())
                .unwrap();
        }

        seal_current_segment(&shared);

        for _ in 0..50 {
            conn.execute("insert into test values (randomblob(256))", ())
                .unwrap();
        }

        let mut seen = RoaringBitmap::new();
        {
            let current = shared.current.load();
            let (stream, replicated_until, size_after) = current.frame_stream_from(1, &mut seen);
            tokio::pin!(stream);
            assert_eq!(replicated_until, 60);
            assert_eq!(size_after, 9);
            assert_eq!(stream.fold(0, |count, _| count + 1).await, 6);
        }
        assert_debug_snapshot!(seen);
    }

    #[tokio::test]
    async fn current_stream_too_recent_frame_no() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();

        let mut seen = RoaringBitmap::new();
        let current = shared.current.load();
        let (stream, replicated_until, size_after) = current.frame_stream_from(100, &mut seen);
        tokio::pin!(stream);
        assert_eq!(replicated_until, 100);
        assert_eq!(stream.fold(0, |count, _| count + 1).await, 0);
        assert_eq!(size_after, 2);
    }

    #[tokio::test]
    async fn current_stream_empty_segment() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();
        seal_current_segment(&shared);

        let mut seen = RoaringBitmap::new();
        let current = shared.current.load();
        let (stream, replicated_until, size_after) = current.frame_stream_from(1, &mut seen);
        tokio::pin!(stream);
        assert_eq!(replicated_until, 2);
        assert_eq!(size_after, 2);
        assert_eq!(stream.fold(0, |count, _| count + 1).await, 0);
    }

    #[tokio::test]
    async fn crash_on_flush() {
        #[derive(Clone, Default)]
        struct SyncFailBufferIo {
            inner: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>>>,
        }

        struct File {
            path: PathBuf,
            io: SyncFailBufferIo,
        }

        impl File {
            fn inner(&self) -> Arc<Mutex<Vec<u8>>> {
                self.io.inner.lock().get(&self.path).cloned().unwrap()
            }
        }

        impl FileExt for File {
            fn len(&self) -> std::io::Result<u64> {
                Ok(self.inner().lock().len() as u64)
            }

            fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> std::io::Result<usize> {
                let mut written = 0;
                for buf in bufs {
                    self.write_at(buf.as_bytes(), written + offset)?;
                    written += buf.len() as u64;
                }
                Ok(written as _)
            }

            fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
                let data = self.inner();
                let mut data = data.lock();
                let new_len = offset as usize + buf.len();
                let old_len = data.len();
                if old_len < new_len {
                    data.extend(std::iter::repeat(0).take(new_len - old_len));
                }
                data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
                Ok(buf.len())
            }

            fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
                let inner = self.inner();
                let inner = inner.lock();
                if offset >= inner.len() as u64 {
                    return Ok(0);
                }

                let read_len = buf.len().min(inner.len() - offset as usize);
                buf[..read_len]
                    .copy_from_slice(&inner[offset as usize..offset as usize + read_len]);
                Ok(read_len)
            }

            fn sync_all(&self) -> std::io::Result<()> {
                // simulate a flush that only flushes half the pages and then fail
                let inner = self.inner();
                let inner = inner.lock();
                // just keep 5 pages from the log. The log will be incomplete and frames will be
                // broken.
                std::fs::write(&self.path, &inner[..4096 * 5])?;
                Err(io::Error::new(io::ErrorKind::BrokenPipe, ""))
            }

            fn set_len(&self, _len: u64) -> std::io::Result<()> {
                todo!()
            }

            async fn read_exact_at_async<B: IoBufMut + Send + 'static>(
                &self,
                mut buf: B,
                offset: u64,
            ) -> (B, std::io::Result<()>) {
                let slice = unsafe {
                    std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_total())
                };
                let ret = self.read_at(slice, offset);
                (buf, ret.map(|_| ()))
            }

            async fn read_at_async<B: IoBufMut + Send + 'static>(
                &self,
                _buf: B,
                _offset: u64,
            ) -> (B, std::io::Result<usize>) {
                todo!()
            }

            async fn write_all_at_async<B: crate::io::buf::IoBuf + Send + 'static>(
                &self,
                _buf: B,
                _offset: u64,
            ) -> (B, std::io::Result<()>) {
                todo!()
            }
        }

        impl Io for SyncFailBufferIo {
            type File = File;
            type Rng = ThreadRng;
            type TempFile = File;

            fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
                std::fs::create_dir_all(path)
            }

            fn open(
                &self,
                _create_new: bool,
                _read: bool,
                _write: bool,
                path: &std::path::Path,
            ) -> std::io::Result<Self::File> {
                let mut inner = self.inner.lock();
                if !inner.contains_key(path) {
                    let data = if path.exists() {
                        std::fs::read(path)?
                    } else {
                        vec![]
                    };
                    inner.insert(path.to_owned(), Arc::new(Mutex::new(data)));
                }

                Ok(File {
                    path: path.into(),
                    io: self.clone(),
                })
            }

            fn tempfile(&self) -> std::io::Result<Self::TempFile> {
                todo!()
            }

            fn now(&self) -> DateTime<Utc> {
                Utc::now()
            }

            fn uuid(&self) -> uuid::Uuid {
                Uuid::new_v4()
            }

            fn hard_link(
                &self,
                _src: &std::path::Path,
                _dst: &std::path::Path,
            ) -> std::io::Result<()> {
                todo!()
            }

            fn with_rng<F, R>(&self, f: F) -> R
            where
                F: FnOnce(&mut Self::Rng) -> R,
            {
                f(&mut rand::thread_rng())
            }

            fn remove_file_async(
                &self,
                path: &std::path::Path,
            ) -> impl std::future::Future<Output = io::Result<()>> + Send {
                async move { std::fs::remove_file(path) }
            }
        }

        let tmp = Arc::new(tempdir().unwrap());
        {
            let env = TestEnv::new_io_and_tmp(SyncFailBufferIo::default(), tmp.clone(), false);
            let conn = env.open_conn("test");
            let shared = env.shared("test");

            conn.execute("create table test (x)", ()).unwrap();
            for _ in 0..6 {
                conn.execute("insert into test values (1234)", ()).unwrap();
            }

            // trigger a flush, that will fail. When we reopen the db, the log should need recovery
            // this simulates a crash before flush
            {
                let mut tx = shared.begin_read(99999).into();
                shared.upgrade(&mut tx).unwrap();
                let mut guard = tx.as_write_mut().unwrap().lock();
                guard.commit();
                let _ = shared.swap_current(&mut guard);
            }
        }

        {
            let env = TestEnv::new_io_and_tmp(SyncFailBufferIo::default(), tmp.clone(), false);
            let conn = env.open_conn("test");
            // the db was recovered: we lost some rows, but it still works
            conn.query_row("select count(*) from test", (), |row| {
                assert_eq!(row.get::<_, u32>(0).unwrap(), 2);
                Ok(())
            })
            .unwrap();
        }
    }

    fn db_payload(db: &[u8]) -> &[u8] {
        let size = (db.len() / 4096) * 4096;
        &db[..size]
    }
}
