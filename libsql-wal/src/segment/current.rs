use std::collections::BTreeMap;
use std::io::{BufWriter, IoSlice, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use fst::MapBuilder;
use parking_lot::{Mutex, RwLock};
use zerocopy::{AsBytes, FromZeroes};

use crate::fs::file::FileExt;
use crate::segment::{frame_offset, page_offset, sealed::SealedSegment};
use crate::transaction::{Transaction, WriteTransaction};

use super::list::SegmentList;
use super::{FrameHeader, SegmentHeader};

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
    tail: Arc<SegmentList<F>>,
}

impl<F> CurrentSegment<F> {
    /// Create a new segment from the given path and metadata. The file pointed to by path must not
    /// exist.
    pub fn create(
        segment_file: F,
        path: PathBuf,
        start_frame_no: NonZeroU64,
        db_size: u32,
        tail: Arc<SegmentList<F>>,
    ) -> Result<Self>
    where
        F: FileExt,
    {
        let mut header = SegmentHeader {
            start_frame_no: start_frame_no.get().into(),
            last_commited_frame_no: 0.into(),
            db_size: db_size.into(),
            index_offset: 0.into(),
            index_size: 0.into(),
            header_cheksum: 0.into(),
        };

        header.recompute_checksum();

        segment_file.write_all_at(header.as_bytes(), 0)?;

        Ok(Self {
            path: path.to_path_buf(),
            index: SegmentIndex::default(),
            header: Mutex::new(header),
            file: segment_file.into(),
            read_locks: Arc::new(AtomicU64::new(0)),
            sealed: AtomicBool::default(),
            tail,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.count_committed() == 0
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
        self.header.lock().count_committed()
    }

    pub fn db_size(&self) -> u32 {
        self.header.lock().db_size.get()
    }

    #[tracing::instrument(skip(self, pages, tx))]
    pub fn insert_pages<'a>(
        &self,
        pages: impl Iterator<Item = (u32, &'a [u8])>,
        size_after: Option<u32>,
        tx: &mut WriteTransaction<F>,
    ) -> Result<()>
    where
        F: FileExt,
    {
        assert!(!self.sealed.load(Ordering::SeqCst));
        tx.enter(move |tx| {
            let mut pages = pages.peekable();
            // let mut commit_frame_written = false;
            let current_savepoint = tx.savepoints.last_mut().expect("no savepoints initialized");
            while let Some((page_no, page)) = pages.next() {
                tracing::trace!(page_no, "inserting page");
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
                let slices = &[IoSlice::new(header.as_bytes()), IoSlice::new(&page)];
                tx.next_frame_no += 1;
                let offset = tx.next_offset;
                tx.next_offset += 1;
                self.file.write_at_vectored(slices, frame_offset(offset))?;
                current_savepoint.index.insert(page_no, offset);
            }

            if let Some(size_after) = size_after {
                if tx.not_empty() {
                    let last_frame_no = tx.next_frame_no - 1;
                    let mut header = { *self.header.lock() };
                    header.last_commited_frame_no = last_frame_no.into();
                    header.db_size = size_after.into();
                    header.recompute_checksum();

                    self.file.write_all_at(header.as_bytes(), 0)?;
                    // self.file.sync_data().unwrap();
                    tx.merge_savepoints(&mut self.index.index.write());
                    // set the header last, so that a transaction does not witness a write before
                    // it's actually committed.
                    *self.header.lock() = header;

                    tx.is_commited = true;
                }
            }

            Ok(())
        })
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
        self.index.locate(page_no, tx.max_frame_no)
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

    #[allow(dead_code)]
    fn frame_header_at(&self, offset: u32) -> Result<FrameHeader>
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
    pub fn seal(&self) -> Result<Option<SealedSegment<F>>>
    where
        F: FileExt,
    {
        let mut header = self.header.lock();
        let index_offset = header.count_committed() as u32;
        let index_byte_offset = frame_offset(index_offset);
        let mut cursor = self.file.cursor(index_byte_offset);
        let mut writer = BufWriter::new(&mut cursor);
        self.index.merge_all(&mut writer)?;
        writer.into_inner().map_err(|e| e.into_parts().0)?;
        header.index_offset = index_byte_offset.into();
        header.index_size = cursor.count().into();
        header.recompute_checksum();
        self.file.write_all_at(header.as_bytes(), 0)?;
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

    pub fn dec_reader_count(&self) {
        self.read_locks().fetch_sub(1, Ordering::SeqCst);
    }

    pub fn read_locks(&self) -> &AtomicU64 {
        self.read_locks.as_ref()
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    pub fn tail(&self) -> &Arc<SegmentList<F>> {
        &self.tail
    }
}
impl<F> Drop for CurrentSegment<F> {
    fn drop(&mut self) {
        // todo: if reader is 0 and segment is sealed, register for compaction.
    }
}

/// TODO: implement spill-to-disk when txn is too large
#[derive(Default)]
struct SegmentIndex {
    start_frame_no: u64,
    index: RwLock<BTreeMap<u32, Vec<u32>>>,
}

impl SegmentIndex {
    fn locate(&self, page_no: u32, max_frame_no: u64) -> Option<u32> {
        let index = self.index.read();
        let offsets = index.get(&page_no)?;
        offsets
            .iter()
            .rev()
            .find(|fno| self.start_frame_no + **fno as u64 <= max_frame_no)
            .copied()
    }

    #[tracing::instrument(skip_all)]
    fn merge_all<W: Write>(&self, writer: W) -> Result<()> {
        let index = self.index.read();
        let mut builder = MapBuilder::new(writer)?;
        for (key, entries) in index.iter() {
            let offset = *entries.last().unwrap();
            builder.insert(key.to_be_bytes(), offset as u64)?;
        }

        builder.finish()?;
        Ok(())
    }
}
