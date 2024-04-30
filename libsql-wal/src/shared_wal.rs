use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use crossbeam::deque::Injector;
use crossbeam::sync::Unparker;
use libsql_sys::wal::PageHeaders;
use parking_lot::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::fs::file::FileExt;
use crate::fs::FileSystem;
use crate::name::NamespaceName;
use crate::registry::WalRegistry;
use crate::segment::current::CurrentSegment;
use crate::transaction::{ReadTransaction, Savepoint, Transaction, WriteTransaction};

#[derive(Default)]
pub struct WalLock {
    pub(crate) tx_id: Mutex<Option<u64>>,
    /// When a writer is popped from the write queue, its write transaction may not be reading from the most recent
    /// snapshot. In this case, we return `SQLITE_BUSY_SNAPHSOT` to the caller. If no reads were performed
    /// with that transaction before upgrading, then the caller will call us back immediately after re-acquiring
    /// a read mark.
    /// Without the reserved slot, the writer would be re-enqueued, a writer before it would be inserted,
    /// and we'd find ourselves in the initial situation. Instead, we use the reserved slot to bypass the queue when the
    /// writer tried to re-acquire the write lock.
    pub(crate) reserved: Mutex<Option<u64>>,
    next_tx_id: AtomicU64,
    pub(crate) waiters: Injector<(Unparker, u64)>,
}

pub struct SharedWal<FS: FileSystem> {
    pub(crate) current: ArcSwap<CurrentSegment<FS::File>>,
    pub(crate) wal_lock: Arc<WalLock>,
    pub(crate) db_file: FS::File,
    pub(crate) namespace: NamespaceName,
    pub(crate) registry: Arc<WalRegistry<FS>>,
}

impl<FS: FileSystem> SharedWal<FS> {
    pub fn db_size(&self) -> u32 {
        self.current.load().db_size()
    }

    #[tracing::instrument(skip_all)]
    pub fn begin_read(&self, conn_id: u64) -> ReadTransaction<FS::File> {
        // FIXME: this is not enough to just increment the counter, we must make sure that the segment
        // is not sealed. If the segment is sealed, retry with the current segment
        let current = self.current.load();
        current.inc_reader_count();
        let (max_frame_no, db_size) =
            current.with_header(|header| (header.last_committed(), header.db_size()));
        let id = self.wal_lock.next_tx_id.fetch_add(1, Ordering::Relaxed);
        ReadTransaction {
            id,
            max_frame_no,
            current: current.clone(),
            db_size,
            created_at: Instant::now(),
            conn_id,
            pages_read: 0,
        }
    }

    /// Upgrade a read transaction to a write transaction
    pub fn upgrade(&self, tx: &mut Transaction<FS::File>) -> Result<()> {
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
                            return Ok(());
                        }
                        Some(_) | None => {
                            tracing::trace!(
                                "txn currently held by another connection, registering to wait queue"
                            );
                            let parker = crossbeam::sync::Parker::new();
                            let unparker = parker.unparker().clone();
                            self.wal_lock.waiters.push((unparker, read_tx.conn_id));
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
        read_tx: &ReadTransaction<FS::File>,
        mut tx_id_lock: MutexGuard<Option<u64>>,
        mut reserved: MutexGuard<Option<u64>>,
    ) -> Result<WriteTransaction<FS::File>> {
        // we read two fields in the header. There is no risk that a transaction commit in
        // between the two reads because this would require that:
        // 1) there would be a running txn
        // 2) that transaction held the lock to tx_id (be in a transaction critical section)
        let current = self.current.load();
        let last_commited = current.last_committed();
        if read_tx.max_frame_no != last_commited || current.is_sealed() {
            if read_tx.pages_read <= 1 {
                // this transaction hasn't read anything yet, it will retry to
                // acquire the lock, reserved the slot so that it can make
                // progress quickly
                // TODO: is it possible that we upgrade the read lock ourselves, so we don't need
                // that reserved stuff anymore? If nothing was read, just upgrade the read,
                // otherwise return snapshot busy and let the connection do the cleanup.
                tracing::debug!("reserving tx slot");
                reserved.replace(read_tx.conn_id);
            }
            return Err(Error::BusySnapshot);
        }
        let next_offset = current.count_committed() as u32;
        let next_frame_no = current.next_frame_no().get();
        *tx_id_lock = Some(read_tx.id);

        Ok(WriteTransaction {
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
    pub fn read_frame(
        &self,
        tx: &mut Transaction<FS::File>,
        page_no: u32,
        buffer: &mut [u8],
    ) -> Result<()> {
        match tx.current.find_frame(page_no, tx) {
            Some(offset) => tx.current.read_page_offset(offset, buffer)?,
            None => {
                // locate in segments
                if !tx
                    .current
                    .tail()
                    .read_page(page_no, tx.max_frame_no, buffer)?
                {
                    // read from db_file
                    tracing::trace!(page_no, "reading from main file");
                    self.db_file
                        .read_exact_at(buffer, (page_no as u64 - 1) * 4096)?;
                }
            }
        }

        tx.pages_read += 1;

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(tx_id = tx.id))]
    pub fn insert_frames(
        &self,
        tx: &mut WriteTransaction<FS::File>,
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
        if current.tail().len() > 10 {
            current.tail().checkpoint(&self.db_file)?;
        }

        Ok(())
    }

    pub fn last_committed_frame_no(&self) -> u64 {
        let current = self.current.load();
        current.last_committed_frame_no()
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
}
