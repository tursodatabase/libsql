use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use libsql_sys::wal::PageHeaders;
use parking_lot::{RwLock, Mutex};

use crate::log::SealedLog;
use crate::transaction::{ReadTransaction, WriteTransaction};
use crate::{log::Log, transaction::Transaction};

pub struct SharedWal {
    pub current: Arc<Log>,
    pub segments: RwLock<VecDeque<SealedLog>>,
    /// Current transaction id
    pub tx_id: Arc<Mutex<Option<u64>>>,
    pub next_tx_id: AtomicU64,
}

impl SharedWal {
    pub fn new(path: &Path) -> Self {
        let current = Arc::new(Log::create(path, 0, 0));
        Self {
            current,
            tx_id: Default::default(),
            next_tx_id: Default::default(),
            segments: Default::default(),
        }
    }

    pub fn db_size(&self) -> u32 {
        self.current.db_size()
    }

    pub fn begin_read(&self) -> ReadTransaction {
        // FIXME: this is not enough to just increment the counter, we must make sure that the log
        // is not sealed. If the log is sealed, retry with the current log
        self.current.read_locks.fetch_add(1, Ordering::SeqCst);
        ReadTransaction {
            max_frame_no: self.current.last_commited(),
            log: self.current.clone(),
        }
    }

    pub fn reset_tx(&self, tx: &mut WriteTransaction) {
        tx.index = None;
        tx.next_frame_no = self.current.last_commited() + 1;
        tx.next_offset = self.current.frames_in_log() as u32;
    }

    pub fn upgrade(&self, read_txn: ReadTransaction) -> WriteTransaction {
        dbg!();
        let mut lock = self.tx_id.lock();

        match *lock {
            Some(_id) => todo!("there's already a txn"),
            None => {
                let id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
                // we read two fields in the header. There is no risk that a transaction commit in
                // between the two reads because this would require that:
                // 1) there would be a running txn
                // 2) that transaction held the lock to tx_id (be in a transaction critical section)
                let last_commited = self.current.last_commited();
                if read_txn.max_frame_no != last_commited {
                    todo!("busy snapshot")
                }
                let next_offset = self.current.frames_in_log() as u32;
                *lock = Some(id);
                WriteTransaction {
                    id,
                    lock: self.tx_id.clone(),
                    index: None,
                    next_frame_no: last_commited + 1,
                    next_offset,
                    is_commited: false,
                    read_tx: read_txn,
                }
            }
        }
    }

    pub fn read_frame(&self, tx: &Transaction, page_no: u32, buffer: &mut [u8]) {
        match self.current.find_frame(page_no, tx) {
            Some((_, offset)) => return self.current.read_page_offset(offset, buffer),
            None => {
                // locate in segments
                self.read_from_segments(page_no, buffer)
                // TODO: lookup db file if no match
            },
        }
    }

    fn read_from_segments(&self, page_no: u32, buf: &mut [u8]) {
        let segs = self.segments.read();
        for seg in segs.iter().rev() {
            if seg.read_page(page_no, buf) {
                return
            }
        }
    }

    pub fn insert_frames(
        &self,
        txn: &mut WriteTransaction,
        pages: &mut PageHeaders,
        size_after: u32,
    ) {
        self.current
            .insert_pages(pages.iter(), (size_after != 0).then_some(size_after), txn);
    }
}
