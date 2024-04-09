use std::fs::File;
use std::io::{IoSlice, Write, BufWriter};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use fst::map::OpBuilder;
use fst::{map::Map, MapBuilder, Streamer};
use parking_lot::{Mutex, RwLock};
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::file::FileExt;
use crate::transaction::{Transaction, WriteTransaction};

pub struct Log {
    path: PathBuf,
    index: LogIndex,
    header: Mutex<LogHeader>,
    file: File,
    /// Read lock count on this Log. Each begin_read increments the count of readers on the current
    /// lock
    pub read_locks: Arc<AtomicU64>,
    pub sealed: AtomicBool,
}

impl Drop for Log {
    fn drop(&mut self) {
        // todo: if reader is 0 and log is sealed, register for compaction.
    }
}

#[derive(Default)]
struct LogIndex {
    savepoints: RwLock<Vec<(u64, Map<Vec<u8>>)>>,
}

impl LogIndex {
    fn locate(&self, page_no: u32, max_frame_no: u64) -> Option<(u32, u32)> {
        let segs = self.savepoints.read();
        let key = page_no.to_be_bytes();
        for (frame_no, index) in segs.iter().rev() {
            if *frame_no > max_frame_no {
                continue;
            }

            if let Some(value) = index.get(key) {
                return Some(index_entry_split(value));
            }
        }

        None
    }

    #[tracing::instrument(skip_all)]
    fn merge_all<W: Write>(&self, writer: W) {
        let segs = self.savepoints.read();
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
    data: [u8; 4096],
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
            path: path.to_path_buf(),
            index: LogIndex::default(),
            header: Mutex::new(header),
            file: log_file,
            read_locks: Arc::new(AtomicU64::new(0)),
            sealed: AtomicBool::default(),
        }
    }

    pub fn len(&self) -> usize {
        let header = self.header.lock();
        (header.last_commited_frame_no.get() - header.start_frame_no.get()) as usize
    }

    /// Returns the db size and the last commited frame_no
    #[tracing::instrument(skip_all)]
    pub fn begin_read_infos(&self) -> (u64, u32) {
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

    #[tracing::instrument(skip(self, pages, tx))]
    pub fn insert_pages<'a>(
        &self,
        pages: impl Iterator<Item = (u32, &'a [u8])>,
        size_after: Option<u32>,
        tx: &mut WriteTransaction,
    ) {
        let before = Instant::now();
        assert!(!self.sealed.load(Ordering::SeqCst));
        tx.enter(move |tx| {
//            println!("enter_txn_ctx: {}", before.elapsed().as_micros());1
            let mut new_index = fst::map::MapBuilder::memory();
            let mut pages = pages.peekable();
            let mut commit_frame_written = false;
            let current_savepoint = tx.savepoints.last_mut().unwrap();
            while let Some((page_no, page)) = pages.next() {
                let in_loop = Instant::now();
                tracing::trace!(page_no, "inserting page");
                match current_savepoint
                    .index
                    .as_ref()
                    .and_then(|i| i.get(&page_no.to_be_bytes()))
                {
                    Some(x) => {
                        let header = FrameHeader {
                            page_no: page_no.into(),
                            // set the size_after if it's the last frame in a commit batch
                            size_after: 0.into(),
                        };
                        // there is already an occurence of this page in the current transaction, replace it
                        let (_, offset) = index_entry_split(x);
                        let slices = &[IoSlice::new(header.as_bytes()), IoSlice::new(page)];

//                        println!("replace_before: {}", in_loop.elapsed().as_micros());1

                        self.file
                            .write_at_vectored(slices, byte_offset(offset) as u64)
                            .unwrap();
//                        println!("replace_after: {}", in_loop.elapsed().as_micros());1
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
                        let frame_no = tx.next_frame_no;
                        let frame_no_bytes = frame_no.to_be_bytes();
                        let slices = &[
                            IoSlice::new(header.as_bytes()),
                            IoSlice::new(&page[..4096 - 8]),
                            // store the replication index in big endian as per SQLite convention,
                            // at the end of the page
                            IoSlice::new(&frame_no_bytes),
                        ];
                        tx.next_frame_no += 1;
                        let offset = tx.next_offset;
                        tx.next_offset += 1;
//                        println!("insert_before: {}", in_loop.elapsed().as_micros());1
                        self.file
                            .write_at_vectored(slices, byte_offset(offset))
                            .unwrap();
//                        println!("insert_after: {}", in_loop.elapsed().as_micros());1
                        new_index
                            .insert(
                                page_no.to_be_bytes(),
                                index_entry_merge(
                                    offset,
                                    (frame_no - self.header.lock().start_frame_no.get()) as u32,
                                ),
                            )
                            .unwrap();
                    }
                }
            }

//            println!("write_pages: {}", before.elapsed().as_micros());1

            if let Some(ref old_index) = current_savepoint.index {
                let indexes = &[old_index, &new_index.into_map()];
                current_savepoint.index = Some(merge_indexes(indexes.iter().map(|x| *x)));
            } else {
                current_savepoint.index = Some(new_index.into_map());
            }

            if let Some(size_after) = size_after {
                if tx.savepoints.len() == 1 && tx.savepoints.last().unwrap().index.is_none() {
                    // nothing to do
                } else {
                    let indexes = tx.savepoints.iter().filter_map(|i| i.index.as_ref());
                    let merged = merge_indexes(indexes);
                    if !merged.is_empty() {
                        let last_frame_no = tx.next_frame_no - 1;
                        let mut header = { *self.header.lock() };
                        header.last_commited_frame_no = last_frame_no.into();
                        header.db_size = size_after.into();

                        if !commit_frame_written {
                            // need to patch the last frame header
                            self.patch_frame_size_after(tx.next_offset - 1, size_after);
                        }

                        self.file.write_all_at(header.as_bytes(), 0).unwrap();
                        // self.file.sync_data().unwrap();
                        self.index.savepoints.write().push((last_frame_no, merged));
                        // set the header last, so that a transaction does not witness a write before
                        // it's actually committed.
                        *self.header.lock() = header;
                    }
                }

                tx.is_commited = true;
            }
        });

//        println!("full_insert: {}", before.elapsed().as_micros());1

    }

    fn patch_frame_size_after(&self, offset: u32, size_after: u32) {
        let offset = byte_offset(offset) + memoffset::offset_of!(FrameHeader, size_after) as u64;
        self.file
            .write_all_at(&size_after.to_le_bytes(), offset)
            .unwrap()
    }

    /// return the offset of the frame for page_no, with frame_no no larger that max_frame_no, if
    /// it exists
    pub fn find_frame(&self, page_no: u32, tx: &Transaction) -> Option<(u64, u32)> {
        // TODO: ensure that we are looking in the same log as the passed transaction
        // this is a write transaction, check the transient index for request page
        if let Transaction::Write(ref tx) = tx {
            if let Some((frame_no_offset, offset)) = tx.find_frame(page_no) {
                return Some((
                    self.header.lock().start_frame_no.get() + frame_no_offset as u64,
                    offset,
                ));
            }
        }

        // not a write tx, or page is not in write tx, look into the log
        self.index
            .locate(page_no, tx.max_frame_no)
            .map(|(frame_no_offset, offset)| {
                (
                    self.header.lock().start_frame_no.get() + frame_no_offset as u64,
                    offset,
                )
            })
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
        self.file
            .read_exact_at(header.as_bytes_mut(), byte_offset(offset))
            .unwrap();
        header
    }

    #[tracing::instrument(skip_all)]
    pub fn seal(&self) -> SealedLog {
        let before = Instant::now();
        assert!(
            self.sealed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok(),
            "attempt to seal an already sealed log"
        );
        let mut header = self.header.lock();
//        println!("header_locked: {}", before.elapsed().as_micros());1
        let index_offset = header.last_commited_frame_no.get() - header.start_frame_no.get();
        let index_byte_offset = byte_offset(index_offset as u32);
        let mut cursor = self.file.cursor(index_byte_offset);
        let mut writer = BufWriter::new(&mut cursor);
        self.index.merge_all(&mut writer);
        writer.into_inner().unwrap();
//        println!("index_merged: {}", before.elapsed().as_micros());1
        header.index_offset = index_byte_offset.into();
        header.index_size = cursor.count().into();
        self.file.write_all_at(header.as_bytes(), 0).unwrap();

        tracing::debug!("log sealed");

        SealedLog::open(&self.file, self.path.clone(), self.read_locks.clone())
    }
}

fn page_offset(offset: u32) -> u64 {
    byte_offset(offset) + size_of::<FrameHeader>() as u64
}

/// an immutable, sealed, memory mapped log file.
pub struct SealedLog {
    pub read_locks: Arc<AtomicU64>,
    path: PathBuf,
    map: memmap::Mmap,
    checkpointed: AtomicBool,
}

impl SealedLog {
    pub fn open(file: &File, path: PathBuf, read_locks: Arc<AtomicU64>) -> Self {
        let map = unsafe { memmap::Mmap::map(file).unwrap() };
        Self { map, path, read_locks, checkpointed: false.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> LogHeader {
        LogHeader::read_from_prefix(&self.map[..]).unwrap()
    }

    // TODO: building the Map on each call could be costly (maybe?), find a way to cache it and
    // return a ref. It will likely require some unsafe, since the index references the mmaped log,
    // owned by self.
    pub fn index(&self) -> Map<&[u8]> {
        let header = self.header();
        let index_offset = header.index_offset.get() as usize;
        let index_size = header.index_size.get() as usize;
        if index_offset == 0 {
            panic!("unsealed log");
        }
        Map::new(&self.map[index_offset..index_offset + index_size]).unwrap()
    }

    pub fn read_offset(&self, offset: u32) -> &[u8] {
        let page_offset = page_offset(offset) as usize;
        &self.map[page_offset..page_offset + 4096]
    }

    pub fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> bool {
        if self.header().last_commited_frame_no.get() > max_frame_no {
            return false;
        }

        let index = self.index();
        if let Some(value) = index.get(page_no.to_be_bytes()) {
            let (_, offset) = index_entry_split(value);
            buf.copy_from_slice(self.read_offset(offset));
            return true;
        }

        false
    }

    pub(crate) fn checkpointed(&self) {
        self.checkpointed.store(true, Ordering::SeqCst);
    }
}

impl Drop for SealedLog {
    fn drop(&mut self) {
        if self.checkpointed.load(Ordering::SeqCst) {
            if let Err(e) = std::fs::remove_file(self.path()) {
                tracing::error!("failed to remove log file: {e}");
            }
        }
    }
}

fn merge_indexes<'a>(indexes: impl Iterator<Item = &'a Map<Vec<u8>>>) -> Map<Vec<u8>> {
    let mut union = indexes.collect::<OpBuilder>().union();
    let mut builder = MapBuilder::memory();

    while let Some((key, vals)) = union.next() {
        let max = vals.iter().max_by_key(|x| x.index).unwrap();
        builder.insert(key, max.value).unwrap();
    }

    builder.into_map()
}
