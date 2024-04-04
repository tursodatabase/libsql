use std::io::{IoSlice, Write};
use std::mem::size_of;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

use fst::map::OpBuilder;
use parking_lot::{RwLock, Mutex};
use fst::{map::Map, Streamer, MapBuilder};
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{AsBytes, FromZeroes, FromBytes};

use crate::file::FileExt;
use crate::transaction::{WriteTransaction, ReadTransaction};

pub struct Log {
    index: LogIndex,
    header: Mutex<LogHeader>,
    file: File,
    /// Read lock count on this Log. Each begin_read increments the count of readers on the current
    /// lock
    pub read_locks: AtomicU64,
    pub sealed: AtomicBool,
}

impl Drop for Log {
    fn drop(&mut self) {
        // todo: if reader is 0 and log is sealed, register for compaction.
    }
}

#[derive(Default)]
struct LogIndex {
    segments: RwLock<Vec<(u64, Map<Vec<u8>>)>>,
}

impl LogIndex {
    fn locate(&self, page_no: u32, max_frame_no: u64) -> Option<(u32, u32)> {
        let segs = self.segments.read();
        let key = page_no.to_be_bytes();
        for (frame_no, index) in segs.iter().rev() {
            if *frame_no > max_frame_no {
                continue
            }

            if let Some(value) = index.get(key) {
                return Some(index_entry_split(value))
            }
        }

        None
    }

    #[tracing::instrument(skip_all)]
    fn merge_all<W: Write>(&self, writer: W) {
        let segs = self.segments.read();
        let mut union = segs.iter().map(|(_, m)| m).collect::<OpBuilder>().union();
        let mut builder = MapBuilder::new(writer).unwrap();
        while let Some((key, entries)) = union.next() {
            let value = entries.iter().max_by_key(|e| e.index).unwrap().value;
            builder.insert(key, value).unwrap();
        }

        builder.finish().unwrap();
    }
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, Clone, Copy)]
pub struct LogHeader {
    start_frame_no: U64,
    pub last_commited_frame_no: U64,
    pub db_size: U32,
    /// byte offset of the index. If 0, then the index wasn't written, and must be recovered.
    /// If non-0, the log is sealed, and must not be written to anymore
    index_offset: U64,
    index_size: U64,
}

/// split the index entry value into it's components: (frame_no, offset)
pub fn index_entry_split(k: u64) -> (u32, u32) {
    let offset = (k & u32::MAX as u64) as u32;
    let frame_no = (k >> 32) as u32;
    (frame_no, offset)
}

/// split the index entry value into it's components: (frame_no, offset)
fn index_entry_merge(offset: u32, frame_no_offset: u32) -> u64 {
    (frame_no_offset as u64) << 32 | offset as u64
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
struct FrameHeader {
    page_no: U32,
    size_after: U32,
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
struct Frame {
    header: FrameHeader,
    data: [u8; 4096]
}

fn byte_offset(offset: u32) -> u64 {
    (size_of::<LogHeader>() + (offset as usize) * size_of::<Frame>()) as u64
}

impl Log {
    /// Create a new log from the given path and metadata. The file pointed to by path must not
    /// exist.
    pub fn create(path: &Path, start_frame_no: u64, db_size: u32) -> Self {
        let log_file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .unwrap();

        let header = LogHeader {
            start_frame_no: start_frame_no.into(),
            last_commited_frame_no: start_frame_no.into(),
            db_size: db_size.into(),
            index_offset: 0.into(),
            index_size: 0.into(),
        };

        log_file.write_all_at(header.as_bytes(), 0).unwrap();

        Self {
            index: LogIndex::default(),
            header: Mutex::new(header),
            file: log_file,
            read_locks: AtomicU64::new(0),
            sealed: AtomicBool::default(),
        }
    }

    pub fn len(&self) -> usize {
        let header = self.header.lock();
        (header.last_commited_frame_no.get() - header.start_frame_no.get()) as usize
    }

    /// Returns the db size and the last commited frame_no
    pub fn begin_read_infos(&self) -> (u64, u32){
        let header = self.header.lock();
        (header.last_commited_frame_no.get(), header.db_size.get())
    }

    pub fn last_commited(&self) -> u64 {
        self.header.lock().last_commited_frame_no.get()
    }

    pub fn frames_in_log(&self) -> u64 {
        let header = self.header.lock();
        header.last_commited_frame_no.get() - header.start_frame_no.get()
    }

    pub fn db_size(&self) -> u32 {
        self.header.lock().db_size.get()
    }

    #[tracing::instrument(skip(self, pages, txn))]
    pub fn insert_pages<'a>(
        &self,
        pages: impl Iterator<Item = (u32, &'a [u8])>,
        size_after: Option<u32>,
        txn: &mut WriteTransaction,
        ) {
        assert!(!self.sealed.load(Ordering::SeqCst));
        txn.enter(move |txn| {
            let mut new_index = fst::map::MapBuilder::memory();
            let mut pages = pages.peekable();
            let mut commit_frame_written = false;
            while let Some((page_no, page)) = pages.next() {
                tracing::trace!(page_no, "inserting page");
                match txn.index.as_ref().and_then(|i| i.get(&page_no.to_be_bytes())) {
                    Some(x) => {
                        let header = FrameHeader {
                            page_no: page_no.into(),
                            // set the size_after if it's the last frame in a commit batch
                            size_after: 0.into(),
                        };
                        // there is already an occurence of this page in the current transaction, replace it
                        let (_, offset) = index_entry_split(x);
                        let slices = &[
                            IoSlice::new(header.as_bytes()),
                            IoSlice::new(page),
                        ];

                        self.file.write_at_vectored(slices, byte_offset(offset) as u64).unwrap();
                    }
                    None => {
                        let size_after = if let Some(size) = size_after {
                            pages.peek().is_none().then_some(size).unwrap_or(0)
                        } else {
                            0
                        };

                        commit_frame_written = size_after != 0;

                        let header = FrameHeader {
                            page_no: page_no.into(),
                            size_after: size_after.into(),
                        };
                        let frame_no = txn.next_frame_no;
                        let frame_no_bytes = frame_no.to_be_bytes();
                        let slices = &[
                            IoSlice::new(header.as_bytes()),
                            IoSlice::new(&page[..4096 - 8]),
                            // store the replication index in big endian as per SQLite convention,
                            // at the end of the page
                            IoSlice::new(&frame_no_bytes),
                        ];
                        txn.next_frame_no += 1;
                        let offset = txn.next_offset;
                        txn.next_offset += 1;
                        self.file.write_at_vectored(slices, byte_offset(offset)).unwrap();
                        new_index.insert(page_no.to_be_bytes(), index_entry_merge(offset, (frame_no - self.header.lock().start_frame_no.get()) as u32)).unwrap();
                    }
                }

            }

            if let Some(ref old_index) = txn.index {
                txn.index = Some(merge_indexes(old_index, &new_index.into_map()));
            } else {
                txn.index = Some(new_index.into_map());
            }

            if let Some(size_after) = size_after {
                if let Some(index) = txn.index.take() {
                    let last_frame_no = txn.next_frame_no - 1;
                    let header = {
                        let mut lock = self.header.lock();
                        lock.last_commited_frame_no = last_frame_no.into();
                        lock.db_size = size_after.into();
                        *lock
                    };

                    if !commit_frame_written {
                        // need to patch the last frame header
                        self.patch_frame_size_after(txn.next_offset - 1, size_after);
                    }

                    self.file.write_all_at(header.as_bytes(), 0).unwrap();
                    // self.file.sync_data().unwrap();
                    self.index.segments.write().push((last_frame_no, index));
                }

                txn.is_commited = true;
            }

        })
    }

    fn patch_frame_size_after(&self, offset: u32, size_after: u32) {
        let offset = byte_offset(offset) + memoffset::offset_of!(FrameHeader, size_after) as u64;
        self.file.write_all_at(&size_after.to_le_bytes(), offset).unwrap()
    }

    /// return the offset of the frame for page_no, with frame_no no larger that max_frame_no, if
    /// it exists
    pub fn find_frame(&self, page_no: u32, tx: &ReadTransaction) -> Option<(u64, u32)> {
        self.index.locate(page_no, tx.max_frame_no).map(|(frame_no_offset, offset)| (self.header.lock().start_frame_no.get() + frame_no_offset as u64, offset))
    }

    /// reads the page conainted in frame at offset into buf
    #[tracing::instrument(skip(self, buf))]
    pub fn read_page_offset(&self, offset: u32, buf: &mut [u8]) {
        tracing::trace!("read page");
        debug_assert_eq!(buf.len(), 4096);
        self.file.read_exact_at(buf, page_offset(offset)).unwrap()
    }

    #[allow(dead_code)]
    fn frame_header_at(&self, offset: u32) -> FrameHeader {
        let mut header = FrameHeader::new_zeroed();
        self.file.read_exact_at(header.as_bytes_mut(), byte_offset(offset)).unwrap();
        header
    }

    #[tracing::instrument(skip_all)]
    pub fn seal(&self) -> SealedLog {
        assert!(self.sealed.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok(), "attempt to seal an already sealed log");
        let mut header = self.header.lock();
        let index_offset = header.last_commited_frame_no.get() - header.start_frame_no.get();
        let index_byte_offset = byte_offset(index_offset as u32);
        let mut cursor = self.file.cursor(index_byte_offset);
        self.index.merge_all(&mut cursor);
        header.index_offset = index_byte_offset.into();
        header.index_size = cursor.count().into();
        self.file.write_all_at(header.as_bytes(), 0).unwrap();
        self.file.sync_data().unwrap();

        tracing::debug!("log sealed");

        SealedLog::open(&self.file)
    }
}

fn page_offset(offset: u32) -> u64 {
    byte_offset(offset) + size_of::<FrameHeader>() as u64
}

/// an immutable, sealed, memory mapped log file.
pub struct SealedLog {
    map: memmap::Mmap,
}

impl SealedLog {
    pub fn open(file: &File) -> Self {
        let map = unsafe { memmap::Mmap::map(file).unwrap() };
        Self { map }
    }

    pub fn header(&self) -> LogHeader {
        LogHeader::read_from_prefix(&self.map[..]).unwrap()
    }

    pub fn index(&self) -> Map<&[u8]> {
        let header = self.header();
        let index_offset = header.index_offset.get() as usize;
        let index_size = header.index_size.get() as usize;
        if index_offset == 0 {
            panic!("unsealed log");
        }
        Map::new(&self.map[index_offset..index_offset + index_size]).unwrap()
    }

    pub fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> bool {
        if self.header().last_commited_frame_no.get() > max_frame_no {
            return false
        }

        let index = self.index();
        if let Some(value) = index.get(page_no.to_be_bytes()) {
            let (_, offset) = index_entry_split(value);
            let page_offset = page_offset(offset) as usize;
            buf.copy_from_slice(&self.map[page_offset..page_offset + 4096]);
            return true;
        }

        false
    }
}

fn merge_indexes(old: &Map<Vec<u8>>, new: &Map<Vec<u8>>) -> Map<Vec<u8>> {
    let mut union = fst::map::OpBuilder::new()
        .add(old)
        .add(new) .union();

    let mut builder = MapBuilder::memory();

    while let Some((key, vals)) = union.next() {
        assert_eq!(vals.len(), 1);
        builder.insert(key, vals[0].value).unwrap();
    }

    builder.into_map()
}
