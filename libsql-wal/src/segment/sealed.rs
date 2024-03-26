use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use fst::{Map, MapBuilder};
use zerocopy::{AsBytes, FromZeroes};

use crate::error::Result;
use crate::fs::file::{BufCopy, FileExt};

use super::{frame_offset, page_offset, Frame, FrameHeader, SegmentHeader};

/// an immutable, wal segment
pub struct SealedSegment<F> {
    pub read_locks: Arc<AtomicU64>,
    header: SegmentHeader,
    file: Arc<F>,
    index: Map<Vec<u8>>,
    path: PathBuf,
    checkpointed: AtomicBool,
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
        let index = Map::new(slice)?;
        Ok(Some(Self {
            file,
            path,
            read_locks,
            checkpointed: false.into(),
            index,
            header,
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
        let index = Map::new(index_bytes).unwrap();

        Ok(SealedSegment {
            read_locks: Default::default(),
            header,
            file,
            index,
            path,
            checkpointed: false.into(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn index(&self) -> &Map<Vec<u8>> {
        &self.index
    }

    pub fn read_page_offset(&self, offset: u32, buf: &mut [u8]) -> Result<()> {
        let page_offset = page_offset(offset) as usize;
        self.file.read_exact_at(buf, page_offset as _)?;

        Ok(())
    }

    pub fn read_frame_offset(&self, offset: u32, frame: &mut Frame) -> Result<()> {
        let offset = frame_offset(offset);
        self.file.read_exact_at(frame.as_bytes_mut(), offset as _)?;
        Ok(())
    }

    pub fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> Result<bool> {
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

    pub(crate) fn checkpointed(&self) {
        self.checkpointed.store(true, Ordering::SeqCst);
    }
}

impl<F> Drop for SealedSegment<F> {
    fn drop(&mut self) {
        if self.checkpointed.load(Ordering::SeqCst) {
            // todo: recycle?;
            if let Err(e) = std::fs::remove_file(&self.path) {
                tracing::error!("failed to remove segment file: {e}");
            }
        }
    }
}
