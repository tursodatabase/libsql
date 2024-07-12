use std::collections::BTreeMap;
use std::io::BufWriter;
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use fst::{Map, MapBuilder, Streamer};
use zerocopy::{AsBytes, FromZeroes};

use crate::error::Result;
use crate::io::buf::{IoBufMut, ZeroCopyBuf};
use crate::io::file::{BufCopy, FileExt};

use super::compacted::{CompactedSegmentDataFooter, CompactedSegmentDataHeader};
use super::{frame_offset, page_offset, Frame, FrameHeader, Segment, SegmentHeader};

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
            frame_count: (self.index().len() as u64).into(),
            segment_id: id.as_u128().into(),
            start_frame_no: self.header().start_frame_no,
            end_frame_no: self.header().last_commited_frame_no,
        };

        hasher.update(header.as_bytes());
        let (_, ret) = out_file
            .write_all_at_async(ZeroCopyBuf::new_init(header), 0)
            .await;
        ret?;

        let mut pages = self.index().stream();
        // todo: use Frame::Zeroed somehow, so that header is aligned?
        let mut buffer = Box::new(ZeroCopyBuf::<Frame>::new_uninit());
        let mut out_index = fst::MapBuilder::memory();
        let mut current_offset = 0;

        while let Some((page_no_bytes, offset)) = pages.next() {
            let (b, ret) = self.read_frame_offset_async(offset as _, buffer).await;
            ret.unwrap();
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

    async fn read_frame_offset_async<B>(&self, offset: u32, buf: B) -> (B, Result<()>)
    where
        B: IoBufMut + Send + 'static,
    {
        assert_eq!(buf.bytes_total(), size_of::<Frame>());
        let frame_offset = frame_offset(offset);
        let (buf, ret) = self.file.read_exact_at_async(buf, frame_offset as _).await;
        (buf, ret.map_err(Into::into))
    }

    fn is_checkpointable(&self) -> bool {
        self.read_locks.load(Ordering::Relaxed) == 0
    }

    fn size_after(&self) -> u32 {
        self.header().size_after()
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
        if index_offset == 0 {
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
        tracing::trace!("recovering unsealed segment at {path:?}");
        let mut index = BTreeMap::new();
        assert!(!header.is_empty());
        let mut frame_header = FrameHeader::new_zeroed();
        for i in 0..header.count_committed() {
            let offset = frame_offset(i as u32);
            file.read_exact_at(frame_header.as_bytes_mut(), offset)?;
            index.insert(frame_header.page_no.get(), i as u32);
        }

        let index_offset = header.count_committed() as u32;
        let index_byte_offset = frame_offset(index_offset);
        let cursor = file.cursor(index_byte_offset);
        let writer = BufCopy::new(cursor);
        let mut writer = BufWriter::new(writer);
        let mut builder = MapBuilder::new(&mut writer)?;
        for (k, v) in index.into_iter() {
            builder.insert(k.to_be_bytes(), v as u64).unwrap();
        }
        builder.finish().unwrap();
        let (cursor, index_bytes) = writer
            .into_inner()
            .map_err(|e| e.into_parts().0)?
            .into_parts();
        header.index_offset = index_byte_offset.into();
        header.index_size = cursor.count().into();
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
