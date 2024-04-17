use std::collections::BTreeMap;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use crossbeam::deque::Injector;
use crossbeam::sync::Unparker;
use libsql_sys::wal::PageHeaders;
use parking_lot::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::file::FileExt;
use crate::log::Log;
use crate::name::NamespaceName;
use crate::registry::WalRegistry;
use crate::segment_list::SegmentList;
use crate::transaction::Transaction;
use crate::transaction::{ReadTransaction, Savepoint, WriteTransaction};

#[derive(Default)]
pub struct WalLock {
    pub tx_id: Mutex<Option<u64>>,
    pub reserved: Mutex<Option<u64>>,
    pub next_tx_id: AtomicU64,
    pub waiters: Injector<(Unparker, u64)>,
}

pub struct SharedWal {
    pub current: ArcSwap<Log>,
    pub segments: SegmentList,
    pub wal_lock: Arc<WalLock>,
    /// Current transaction id
    pub db_file: File,
    pub namespace: NamespaceName,
    pub registry: Arc<WalRegistry>,
}

impl SharedWal {
    pub fn db_size(&self) -> u32 {
        self.current.load().db_size()
    }

    #[tracing::instrument(skip_all)]
    pub fn begin_read(&self, conn_id: u64) -> ReadTransaction {
        // FIXME: this is not enough to just increment the counter, we must make sure that the log
        // is not sealed. If the log is sealed, retry with the current log
        loop {
            let current = self.current.load();
            // FIXME: This function comes up a lot more than in should in profiling. I suspect that
            // this is caused by those expensive loads here
            current.read_locks.fetch_add(1, Ordering::SeqCst);
            if current.sealed.load(Ordering::SeqCst) {
                continue;
            }
            let (max_frame_no, db_size) = current.begin_read_infos();
            return ReadTransaction {
                max_frame_no,
                log: current.clone(),
                db_size,
                created_at: Instant::now(),
                conn_id,
                pages_read: 0,
            };
        }
    }

    /// Upgrade a read transaction to a write transaction
    pub fn upgrade(&self, tx: &mut Transaction) -> Result<()> {
        loop {
            match tx {
                Transaction::Write(_) => unreachable!("already in a write transaction"),
                Transaction::Read(read_tx) => {
                    {
                        let mut reserved = self.wal_lock.reserved.lock();
                        match *reserved {
                            // we have already reserved the slot, go ahead and try to acquire
                            Some(id) if id == read_tx.conn_id => {
                                tracing::trace!("taking reserved slot");
                                reserved.take();
                                let lock = self.wal_lock.tx_id.lock();
                                let write_tx = self.acquire_write(read_tx, lock, reserved)?;
                                *tx = Transaction::Write(write_tx);
                                //                                println!("upgraded: {}", before.elapsed().as_micros());1
                                return Ok(());
                            }
                            _ => (),
                        }
                    }

                    let lock = self.wal_lock.tx_id.lock();
                    match *lock {
                        None if self.wal_lock.waiters.is_empty() => {
                            let write_tx =
                                self.acquire_write(read_tx, lock, self.wal_lock.reserved.lock())?;
                            *tx = Transaction::Write(write_tx);
                            //                            println!("upgraded: {}", before.elapsed().as_micros());1
                            return Ok(());
                        }
                        Some(_) | None => {
                            tracing::trace!(
                                "txn currently held by another connection, registering to wait queue"
                            );
                            let parker = crossbeam::sync::Parker::new();
                            let unpaker = parker.unparker().clone();
                            self.wal_lock.waiters.push((unpaker, read_tx.conn_id));
                            drop(lock);
                            parker.park();
                        }
                    }
                }
            }
        }
    }

    fn acquire_write(
        &self,
        read_tx: &ReadTransaction,
        mut tx_id_lock: MutexGuard<Option<u64>>,
        mut reserved: MutexGuard<Option<u64>>,
    ) -> Result<WriteTransaction> {
        let id = self.wal_lock.next_tx_id.fetch_add(1, Ordering::Relaxed);
        // we read two fields in the header. There is no risk that a transaction commit in
        // between the two reads because this would require that:
        // 1) there would be a running txn
        // 2) that transaction held the lock to tx_id (be in a transaction critical section)
        let current = self.current.load();
        let last_commited = current.last_committed();
        if read_tx.max_frame_no != last_commited {
            if read_tx.pages_read <= 1 {
                // this transaction hasn't read anything yet, it will retry to
                // acquire the lock, reserved the slot so that it can make
                // progress quickly
                tracing::debug!("reserving tx slot");
                reserved.replace(read_tx.conn_id);
            }
            return Err(Error::BusySnapshot);
        }
        let next_offset = current.count_committed() as u32;
        let next_frame_no =current.next_frame_no().get();
        *tx_id_lock = Some(id);

        Ok(WriteTransaction {
            id,
            wal_lock: self.wal_lock.clone(),
            savepoints: vec![Savepoint {
                next_offset,
                next_frame_no,
                index: BTreeMap::new(),
            }],
            next_frame_no,
            next_offset,
            is_commited: false,
            read_tx: read_tx.clone(),
        })
    }

    #[tracing::instrument(skip(self, tx, buffer))]
    pub fn read_frame(&self, tx: &mut Transaction, page_no: u32, buffer: &mut [u8]) -> Result<()> {
        match tx.log.find_frame(page_no, tx) {
            Some(offset) => {
                tx.log.read_page_offset(offset, buffer)? },
            None => {
                // locate in segments
                if !self.segments.read_page(page_no, tx.max_frame_no, buffer)? {
                    // read from db_file
                    self.db_file
                        .read_exact_at(buffer, (page_no as u64 - 1) * 4096)?;
                }
            }
        }

        tx.pages_read += 1;

        // let frame_no = u64::from_be_bytes(buffer[4096 - 8..].try_into().unwrap());
        // tracing::trace!(frame_no, tx = tx.max_frame_no, "read page");
        // assert!(
        //     dbg!(frame_no) <= dbg!(tx.max_frame_no()),
        //     "read frame out of transaction boundaries"
        // );
        //
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(tx_id = tx.id))]
    pub fn insert_frames(
        &self,
        tx: &mut WriteTransaction,
        pages: &mut PageHeaders,
        size_after: u32,
    ) -> Result<()> {
        let current = self.current.load();
        current.insert_pages(pages.iter(), (size_after != 0).then_some(size_after), tx)?;

        // TODO: use config for max log size
        if tx.is_commited() && current.count_committed() > 1000 {
            self.registry.swap_current(self, tx)?;
        }

        // TODO: remove, stupid strategy for tests
        // ok, we still hold a write txn
        if self.segments.len() > 10 {
            self.segments.checkpoint(&self.db_file)?;
        }

        Ok(())
    }
}
