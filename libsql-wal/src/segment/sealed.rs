use std::collections::BTreeMap;
use std::hash::Hasher;
use std::io::{BufWriter, ErrorKind, Write};
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use chrono::prelude::{DateTime, Utc};
use fst::{Map, MapBuilder, Streamer};
use zerocopy::{AsBytes, FromZeroes};

use crate::error::Result;
use crate::io::buf::{IoBufMut, ZeroCopyBuf};
use crate::io::file::{BufCopy, FileExt};
use crate::io::Inspect;
use crate::segment::{checked_frame_offset, CheckedFrame};
use crate::{LIBSQL_MAGIC, LIBSQL_WAL_VERSION};

use super::compacted::{CompactedSegmentDataFooter, CompactedSegmentDataHeader};
use super::{frame_offset, page_offset, Frame, Segment, SegmentFlags, SegmentHeader};

/// an immutable, wal segment
#[derive(Debug)]
pub struct SealedSegment<F> {
    inner: Arc<SealedSegmentInner<F>>,
}

impl<F> Clone for SealedSegment<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct SealedSegmentInner<F> {
    pub read_locks: Arc<AtomicU64>,
    header: SegmentHeader,
    file: Arc<F>,
    index: Map<Arc<[u8]>>,
    path: PathBuf,
}

impl<F> std::fmt::Debug for SealedSegmentInner<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedSegment")
            .field("read_locks", &self.read_locks)
            .field("header", &self.header)
            .field("index", &self.index)
            .field("path", &self.path)
            .finish()
    }
}

impl<F> SealedSegment<F> {
    pub fn empty(f: F) -> Self {
        Self {
            inner: SealedSegmentInner {
                read_locks: Default::default(),
                header: SegmentHeader::new_zeroed(),
                file: Arc::new(f),
                index: Map::default().map_data(Into::into).unwrap(),
                path: PathBuf::new(),
            }
            .into(),
        }
    }
}

impl<F> Deref for SealedSegment<F> {
    type Target = SealedSegmentInner<F>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<F> Segment for SealedSegment<F>
where
    F: FileExt,
{
    async fn compact(&self, out_file: &impl FileExt, id: uuid::Uuid) -> Result<Vec<u8>> {
        let mut hasher = crc32fast::Hasher::new();

        let header = CompactedSegmentDataHeader {
            frame_count: (self.index().len() as u32).into(),
            segment_id: id.as_u128().into(),
            start_frame_no: self.header().start_frame_no,
            end_frame_no: self.header().last_commited_frame_no,
            size_after: self.header.size_after,
            version: LIBSQL_WAL_VERSION.into(),
            magic: LIBSQL_MAGIC.into(),
            page_size: self.header().page_size,
            timestamp: self.header.sealed_at_timestamp,
        };

        hasher.update(header.as_bytes());
        let (_, ret) = out_file
            .write_all_at_async(ZeroCopyBuf::new_init(header), 0)
            .await;
        ret?;

        let mut pages = self.index().stream();
        let mut buffer = Box::new(ZeroCopyBuf::<Frame>::new_uninit());
        let mut out_index = fst::MapBuilder::memory();
        let mut current_offset = 0;

        while let Some((page_no_bytes, offset)) = pages.next() {
            let (mut b, ret) = self.read_frame_offset_async(offset as _, buffer).await;
            ret?;
            // transaction boundaries in a segment are completely erased. The responsibility is on
            // the consumer of the segment to place the transaction boundary such that all frames from
            // the segment are applied within the same transaction.
            b.get_mut().header_mut().set_size_after(0);
            hasher.update(&b.get_ref().as_bytes());
            let dest_offset =
                size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
            let (mut b, ret) = out_file.write_all_at_async(b, dest_offset as u64).await;
            ret?;
            out_index
                .insert(page_no_bytes, current_offset as _)
                .unwrap();
            current_offset += 1;
            b.deinit();
            buffer = b;
        }

        let footer = CompactedSegmentDataFooter {
            checksum: hasher.finalize().into(),
        };

        let footer_offset =
            size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
        let (_, ret) = out_file
            .write_all_at_async(ZeroCopyBuf::new_init(footer), footer_offset as _)
            .await;
        ret?;

        Ok(out_index.into_inner().unwrap())
    }

    #[inline]
    fn start_frame_no(&self) -> u64 {
        self.header.start_frame_no.get()
    }

    #[inline]
    fn last_committed(&self) -> u64 {
        self.header.last_committed()
    }

    fn index(&self) -> &fst::Map<Arc<[u8]>> {
        &self.index
    }

    fn is_storable(&self) -> bool {
        // we don't store unordered segments, since they only happen in two cases:
        // - in a replica: no need for storage
        // - in a primary, on recovery from storage: we don't want to override remote
        // segment.
        !self
            .header()
            .flags()
            .contains(SegmentFlags::FRAME_UNORDERED)
    }

    fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> std::io::Result<bool> {
        if self.header().start_frame_no.get() > max_frame_no {
            return Ok(false);
        }

        let index = self.index();
        if let Some(offset) = index.get(page_no.to_be_bytes()) {
            self.read_page_offset(offset as u32, buf)?;

            return Ok(true);
        }

        Ok(false)
    }

    fn is_checkpointable(&self) -> bool {
        let read_locks = self.read_locks.load(Ordering::Relaxed);
        tracing::debug!(read_locks);
        read_locks == 0
    }

    fn size_after(&self) -> u32 {
        self.header().size_after()
    }

    async fn read_frame_offset_async<B>(&self, offset: u32, buf: B) -> (B, Result<()>)
    where
        B: IoBufMut + Send + 'static,
    {
        assert_eq!(buf.bytes_total(), size_of::<Frame>());
        let frame_offset = frame_offset(offset);
        let (buf, ret) = self.file.read_exact_at_async(buf, frame_offset as _).await;
        (buf, ret.map_err(Into::into))
    }

    fn destroy<IO: crate::io::Io>(&self, io: &IO) -> impl std::future::Future<Output = ()> {
        async move {
            if let Err(e) = io.remove_file_async(&self.path).await {
                tracing::error!("failed to remove segment file {:?}: {e}", self.path);
            }
        }
    }

    fn timestamp(&self) -> DateTime<Utc> {
        assert_ne!(
            self.header().sealed_at_timestamp.get(),
            0,
            "segment was not sealed properly"
        );
        DateTime::from_timestamp_millis(self.header().sealed_at_timestamp.get() as _)
            .expect("this should be a guaranteed roundtrip with DateTime::timestamp_millis")
    }
}

impl<F: FileExt> SealedSegment<F> {
    pub fn open(file: Arc<F>, path: PathBuf, read_locks: Arc<AtomicU64>) -> Result<Option<Self>> {
        let mut header: SegmentHeader = SegmentHeader::new_zeroed();
        file.read_exact_at(header.as_bytes_mut(), 0)?;

        header.check()?;

        let index_offset = header.index_offset.get();
        let index_len = header.index_size.get();

        if header.is_empty() {
            std::fs::remove_file(path)?;
            return Ok(None);
        }

        // This happens in case of crash: the segment is not empty, but it wasn't sealed. We need to
        // recover the index, and seal the segment.
        if !header.flags().contains(SegmentFlags::SEALED) {
            assert_eq!(header.index_offset.get(), 0, "{header:?}");
            return Self::recover(file, path, header).map(Some);
        }

        let mut slice = vec![0; index_len as usize];
        file.read_exact_at(&mut slice, index_offset)?;
        let index = Map::new(slice.into())?;
        Ok(Some(Self {
            inner: SealedSegmentInner {
                file,
                path,
                read_locks,
                index,
                header,
            }
            .into(),
        }))
    }

    fn recover(file: Arc<F>, path: PathBuf, mut header: SegmentHeader) -> Result<Self> {
        assert!(!header.is_empty());
        assert_eq!(header.index_size.get(), 0);
        assert_eq!(header.index_offset.get(), 0);
        assert!(!header.flags().contains(SegmentFlags::SEALED));

        let mut current_checksum = header.salt.get();
        tracing::trace!("recovering unsealed segment at {path:?}");
        let mut index = BTreeMap::new();
        let mut frame: Box<CheckedFrame> = CheckedFrame::new_box_zeroed();
        let mut current_tx = Vec::new();
        let mut last_committed = 0;
        let mut size_after = 0;
        let mut frame_count = 0;
        // When the segment is ordered, then the biggest frame_no is the last commited
        // frame. This is not the case for an unordered segment (in case of recovery or
        // a replica), so we track the biggest frame_no and set last_commited to that
        // value on a commit frame
        let mut max_seen_frame_no = 0;
        for i in 0.. {
            let offset = checked_frame_offset(i as u32);
            match file.read_exact_at(frame.as_bytes_mut(), offset) {
                Ok(_) => {
                    let new_checksum = frame.frame.checksum(current_checksum);
                    // this is the first checksum that doesn't match the checksum chain, drop the
                    // transaction and any frame after that.
                    if new_checksum != frame.checksum.get() {
                        tracing::warn!(
                            "found invalid checksum in segment, dropping {} frames",
                            header.last_committed() - last_committed
                        );
                        break;
                    }
                    current_checksum = new_checksum;
                    frame_count += 1;

                    // this must always hold for a ordered segment.
                    #[cfg(debug_assertions)]
                    {
                        if !header.flags().contains(SegmentFlags::FRAME_UNORDERED) {
                            assert!(frame.frame.header().frame_no() > max_seen_frame_no);
                        }
                    }

                    max_seen_frame_no = max_seen_frame_no.max(frame.frame.header.frame_no());

                    current_tx.push(frame.frame.header().page_no());
                    if frame.frame.header.is_commit() {
                        last_committed = max_seen_frame_no;
                        size_after = frame.frame.header().size_after();
                        let base_offset = (i + 1) - current_tx.len();
                        for (frame_offset, page_no) in current_tx.drain(..).enumerate() {
                            index.insert(page_no, (base_offset + frame_offset) as u32);
                        }
                    }
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        let index_offset = frame_count as u32;
        let index_byte_offset = checked_frame_offset(index_offset);
        let cursor = file.cursor(index_byte_offset);
        let writer = BufCopy::new(cursor);
        let writer = BufWriter::new(writer);
        let mut digest = crc32fast::Hasher::new_with_initial(current_checksum);
        let mut writer = Inspect::new(writer, |data: &[u8]| {
            digest.write(data);
        });
        let mut builder = MapBuilder::new(&mut writer)?;
        for (k, v) in index.into_iter() {
            builder.insert(k.to_be_bytes(), v as u64).unwrap();
        }
        builder.finish().unwrap();
        let writer = writer.into_inner();
        let index_size = writer.get_ref().get_ref().count();
        let index_checksum = digest.finalize();
        let (mut cursor, index_bytes) = writer
            .into_inner()
            .map_err(|e| e.into_parts().0)?
            .into_parts();
        cursor.write_all(&index_checksum.to_le_bytes())?;
        header.index_offset = index_byte_offset.into();
        header.index_size = index_size.into();
        header.last_commited_frame_no = last_committed.into();
        header.size_after = size_after.into();
        let flags = header.flags();
        header.set_flags(flags | SegmentFlags::SEALED);
        header.recompute_checksum();
        file.write_all_at(header.as_bytes(), 0)?;
        let index = Map::new(index_bytes.into()).unwrap();

        Ok(SealedSegment {
            inner: SealedSegmentInner {
                read_locks: Default::default(),
                header,
                file,
                index,
                path,
            }
            .into(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_page_offset(&self, offset: u32, buf: &mut [u8]) -> std::io::Result<()> {
        let page_offset = page_offset(offset) as usize;
        self.file.read_exact_at(buf, page_offset as _)?;

        Ok(())
    }

    pub fn read_frame_offset(&self, offset: u32, frame: &mut Frame) -> Result<()> {
        let offset = frame_offset(offset);
        self.file.read_exact_at(frame.as_bytes_mut(), offset as _)?;
        Ok(())
    }
}

impl<F> SealedSegment<F> {
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub async fn read_page_offset_async<B>(&self, offset: u32, buf: B) -> (B, Result<()>)
    where
        B: IoBufMut + Send + 'static,
        F: FileExt,
    {
        assert_eq!(buf.bytes_total(), 4096);
        let page_offset = page_offset(offset) as usize;
        let (buf, ret) = self.file.read_exact_at_async(buf, page_offset as _).await;
        (buf, ret.map_err(Into::into))
    }
}
