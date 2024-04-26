use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use fst::{map::OpBuilder, Streamer};
use libsql_sys::ffi::Sqlite3DbHeader;
use memoffset::offset_of;
use zerocopy::{AsBytes, FromZeroes};

use crate::error::Result;
use crate::fs::file::FileExt;
use crate::segment::{sealed::SealedSegment, Frame};

struct SegmentNode<F> {
    segment: SealedSegment<F>,
    next: ArcSwapOption<SegmentNode<F>>,
}

pub struct SegmentList<F> {
    head: ArcSwapOption<SegmentNode<F>>,
    len: AtomicUsize,
    /// Whether the segment list is already being checkpointed
    checkpointing: AtomicBool,
}

impl<F> Default for SegmentList<F> {
    fn default() -> Self {
        Self {
            head: Default::default(),
            len: Default::default(),
            checkpointing: Default::default(),
        }
    }
}

impl<F> SegmentList<F> {
    /// Prepend the list with the passed sealed segment
    pub fn push_log(&self, segment: SealedSegment<F>) {
        let segment = Arc::new(SegmentNode {
            segment,
            next: self.head.load().clone().into(),
        });

        self.head.swap(Some(segment));
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Call f on the head of the segments list, if it exists. The head of the list is the most
    /// recent segment.
    pub fn with_head<R>(&self, f: impl FnOnce(&SealedSegment<F>) -> R) -> Option<R> {
        let head = self.head.load();
        head.as_ref().map(|link| f(&link.segment))
    }

    /// attempt to read page_no with frame_no less than max_frame_no. Returns whether such a page
    /// was found
    pub fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> Result<bool>
    where
        F: FileExt,
    {
        let mut prev_seg = u64::MAX;
        let mut current = self.head.load();
        let mut i = 0;
        while let Some(link) = &*current {
            let last = link.segment.header().last_commited_frame_no();
            assert!(prev_seg > last);
            prev_seg = last;
            if link.segment.read_page(page_no, max_frame_no, buf)? {
                tracing::trace!("found {page_no} in segment {i}");
                return Ok(true);
            }

            i += 1;
            current = link.next.load();
        }

        Ok(false)
    }

    pub fn checkpoint(&self, db_file: &F) -> Result<()>
    where
        F: FileExt,
    {
        if self
            .checkpointing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let mut segs = Vec::new();
        let mut current = self.head.load();
        // This is the last element in the list that is not part of the segments to be
        // checkpointed. All the folowing segments will be checkpointed. After checkpoint, we set
        // this link's next to None.
        let mut last_untaken = None;
        // find the longest chain of segments that can be checkpointed, iow, segments that do not have
        // readers pointing to them
        while let Some(link) = &*current {
            if link.segment.read_locks.load(Ordering::SeqCst) != 0 {
                segs.clear();
                last_untaken = current.clone();
            } else {
                segs.push(link.clone());
            }
            current = link.next.load();
        }

        // nothing to checkpoint rn
        if segs.is_empty() {
            return Ok(());
        }

        let size_after = segs.first().unwrap().segment.header().db_size();

        let mut union = segs
            .iter()
            .map(|s| s.segment.index())
            .collect::<OpBuilder>()
            .union();
        let mut buf = Frame::new_box_zeroed();
        let mut last_replication_index = 0;
        while let Some((k, v)) = union.next() {
            let page_no = u32::from_be_bytes(k.try_into().unwrap());
            let v = v.iter().min_by_key(|i| i.index).unwrap();
            let seg = &segs[v.index];
            let offset = v.value as u32;
            seg.segment.read_frame_offset(offset, &mut buf)?;
            assert_eq!(buf.header().page_no(), page_no);
            last_replication_index = last_replication_index.max(buf.header().frame_no());
            db_file.write_all_at(&buf.data(), (page_no as u64 - 1) * 4096)?;
        }

        let last_replication_index =
            zerocopy::byteorder::big_endian::U64::new(last_replication_index);
        db_file.write_all_at(
            last_replication_index.as_bytes(),
            offset_of!(Sqlite3DbHeader, replication_index) as _,
        )?;

        db_file.sync_all()?;

        match last_untaken {
            Some(link) => {
                assert!(Arc::ptr_eq(&link.next.load().as_ref().unwrap(), &segs[0]));
                link.next.swap(None);
            }
            // everything up to head was checkpointed
            None => {
                assert!(Arc::ptr_eq(&*self.head.load().as_ref().unwrap(), &segs[0]));
                self.head.swap(None);
            }
        }

        drop(union);

        self.len.fetch_sub(segs.len(), Ordering::Relaxed);

        for seg in segs {
            seg.segment.checkpointed();
        }

        db_file.set_len(size_after as u64 * 4096)?;

        self.checkpointing.store(false, Ordering::SeqCst);

        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }
}
