use std::ffi::{c_int, c_void};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::ops::DerefMut;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::libsql::ffi::{
    types::{XWalFrameFn, XWalUndoFn},
    PgHdr, Wal,
};
use crate::libsql::{ffi::PageHdrIter, wal_hook::WalHook};

// Clone is necessary only because opening a database may fail, and we need to clone the empty
// struct.
#[derive(Clone)]
pub struct WalLoggerHook {
    /// Current frame index, updated on each commit.
    buffer: Vec<WalLogEntry>,
    logger: Arc<WalLogger>,
}

unsafe impl WalHook for WalLoggerHook {
    fn on_frames(
        &mut self,
        wal: *mut Wal,
        page_size: c_int,
        page_headers: *mut PgHdr,
        ntruncate: u32,
        is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        assert_eq!(page_size, 4096);

        let rc = unsafe {
            orig(
                wal,
                page_size,
                page_headers,
                ntruncate,
                is_commit,
                sync_flags,
            )
        };
        if rc != crate::libsql::ffi::SQLITE_OK {
            return rc;
        }

        for (page_no, data) in PageHdrIter::new(page_headers, page_size as _) {
            self.write_frame(page_no, data)
        }

        if is_commit != 0 {
            self.commit(page_size, ntruncate, is_commit != 0, sync_flags);
        }

        rc
    }

    fn on_undo(
        &mut self,
        wal: *mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        self.rollback();
        unsafe { orig(wal, func, ctx) }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum WalLogEntry {
    Frame {
        page_no: u32,
        data: Bytes,
    },
    Commit {
        page_size: i32,
        size_after: u32,
        is_commit: bool,
        sync_flags: i32,
    },
}

impl WalLoggerHook {
    pub fn new(logger: Arc<WalLogger>) -> Self {
        Self {
            buffer: Default::default(),
            logger,
        }
    }

    fn write_frame(&mut self, page_no: u32, data: &[u8]) {
        let entry = WalLogEntry::Frame {
            page_no,
            data: Bytes::copy_from_slice(data),
        };
        self.buffer.push(entry);
    }

    fn commit(&mut self, page_size: i32, size_after: u32, is_commit: bool, sync_flags: i32) {
        let entry = WalLogEntry::Commit {
            page_size,
            size_after,
            is_commit,
            sync_flags,
        };
        self.buffer.push(entry);
        self.logger.append(&self.buffer);
        self.buffer.clear();
    }

    fn rollback(&mut self) {
        self.buffer.clear();
    }
}

pub struct WalLogger {
    current_offset: Mutex<usize>,
    /// first index present in the file
    start_offset: usize,
    log_file: File,
}

#[derive(Serialize, Deserialize)]
struct WalLoggerFileHeader {
    version: u8,
    start_index: u64,
}

impl WalLogger {
    /// size of a single frame
    pub const FRAME_SIZE: usize = 4112;
    /// Size of the file header
    pub const HEADER_SIZE: usize = 4096;

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().join("wallog");
        let mut log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let mut file_end = log_file.metadata()?.len();

        let mut header_buf = [0; 4096];
        let header = if file_end == 0 {
            let header = WalLoggerFileHeader {
                version: 1,
                start_index: 0,
            };
            bincode::serialize_into(Cursor::new(&mut header_buf[..]), &header)?;
            log_file.write_all(&header_buf)?;
            file_end = 4096;
            header
        } else {
            log_file.read_exact(&mut header_buf)?;
            let header: WalLoggerFileHeader = bincode::deserialize(&header_buf)?;
            header
        };

        Ok(Self {
            current_offset: Mutex::new(file_end as usize),
            start_offset: header.start_index as _,
            log_file,
        })
    }

    fn append(&self, frames: &[WalLogEntry]) {
        let mut lock = self.current_offset.lock();
        let mut current_offset = *lock;
        for frame in frames.iter() {
            #[cfg(any(debug_assertions, test))]
            if let WalLogEntry::Frame { ref data, .. } = frame {
                assert_eq!(data.len(), 4096);
            }

            let mut buffer = BytesMut::zeroed(Self::FRAME_SIZE);
            bincode::serialize_into(Cursor::new(buffer.deref_mut()), frame).unwrap();
            self.log_file
                .write_all_at(&buffer, current_offset as _)
                // TODO: Handle write error
                .unwrap();
            current_offset += Self::FRAME_SIZE;
        }

        *lock = current_offset;
        tracing::debug!("new WAL offset: {current_offset}");
    }

    /// Returns frame at `index`.
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    // TODO: implement log compaction
    // TODO: implement page cache
    pub fn get_entry(&self, offset: usize) -> anyhow::Result<Option<WalLogEntry>> {
        if offset < self.start_offset {
            return Ok(None);
        }
        let read_offset = Self::HEADER_SIZE + (offset - self.start_offset) * Self::FRAME_SIZE;

        if read_offset >= *self.current_offset.lock() {
            return Ok(None);
        }

        let mut buffer = BytesMut::zeroed(Self::FRAME_SIZE);
        self.log_file.read_exact_at(&mut buffer, read_offset as _)?;
        let entry: WalLogEntry = bincode::deserialize(&buffer)?;

        Ok(Some(entry))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();

        assert_eq!(*logger.current_offset.lock(), WalLogger::HEADER_SIZE);

        let frames = (0..10)
            .map(|i| WalLogEntry::Frame {
                data: Bytes::from(vec![i; 4096]),
                page_no: i as _,
            })
            .collect::<Vec<_>>();
        logger.append(&frames);

        for i in 0..10 {
            let frame = logger.get_entry(i).unwrap().unwrap();
            let WalLogEntry::Frame{ page_no, data } =  frame else {panic!()};
            assert_eq!(page_no, i as u32);
            assert!(data.iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            *logger.current_offset.lock(),
            WalLogger::HEADER_SIZE + 10 * WalLogger::FRAME_SIZE
        );
    }

    #[test]
    fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();
        assert!(logger.get_entry(1).unwrap().is_none());
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();
        let entry = WalLogEntry::Frame {
            page_no: 0,
            data: vec![0; 3].into(),
        };
        logger.append(&[entry]);
    }
}
