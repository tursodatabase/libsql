use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use crossbeam::deque::Injector;
use crossbeam::sync::Unparker;
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::checkpointer::CheckpointMessage;
use crate::error::{Error, Result};
use crate::io::file::FileExt;
use crate::io::Io;
use crate::replication::storage::ReplicateFromStorage;
use crate::segment::current::CurrentSegment;
use crate::segment_swap_strategy::SegmentSwapStrategy;
use crate::transaction::{ReadTransaction, Savepoint, Transaction, TxGuard, WriteTransaction};
use libsql_sys::name::NamespaceName;

#[derive(Default)]
pub struct WalLock {
    pub(crate) tx_id: Arc<async_lock::Mutex<Option<u64>>>,
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

pub(crate) trait SwapLog<IO: Io>: Sync + Send + 'static {
    fn swap_current(&self, shared: &SharedWal<IO>, tx: &dyn TxGuard<IO::File>) -> Result<()>;
}

pub struct SharedWal<IO: Io> {
    pub(crate) current: ArcSwap<CurrentSegment<IO::File>>,
    pub(crate) wal_lock: Arc<WalLock>,
    pub(crate) db_file: IO::File,
    pub(crate) namespace: NamespaceName,
    pub(crate) registry: Arc<dyn SwapLog<IO>>,
    #[allow(dead_code)] // used by replication
    pub(crate) checkpointed_frame_no: AtomicU64,
    /// max frame_no acknowledged by the durable storage
    pub(crate) durable_frame_no: Arc<Mutex<u64>>,
    pub(crate) new_frame_notifier: tokio::sync::watch::Sender<u64>,
    pub(crate) stored_segments: Box<dyn ReplicateFromStorage>,
    pub(crate) shutdown: AtomicBool,
    pub(crate) checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
    pub(crate) io: Arc<IO>,
    pub(crate) swap_strategy: Box<dyn SegmentSwapStrategy>,
}

impl<IO: Io> SharedWal<IO> {
    #[tracing::instrument(skip(self), fields(namespace = self.namespace.as_str()))]
    pub fn shutdown(&self) -> Result<()> {
        tracing::info!("started namespace shutdown");
        self.shutdown.store(true, Ordering::SeqCst);
        // fixme: for infinite loop
        let mut tx = loop {
            let mut tx = Transaction::Read(self.begin_read(u64::MAX));
            match self.upgrade(&mut tx) {
                Ok(_) => break tx,
                Err(Error::BusySnapshot) => continue,
                Err(e) => return Err(e),
            }
        };

        {
            let mut tx = tx.as_write_mut().unwrap().lock();
            tx.commit();
            self.registry.swap_current(self, &tx)?;
        }
        // The current segment will not be used anymore. It's empty, but we still seal it so that
        // the next startup doesn't find an unsealed segment.
        self.current.load().seal(self.io.now())?;
        tracing::info!("namespace shutdown");
        Ok(())
    }

    pub fn new_frame_notifier(&self) -> watch::Receiver<u64> {
        self.new_frame_notifier.subscribe()
    }

    pub fn db_size(&self) -> u32 {
        self.current.load().db_size()
    }

    pub fn log_id(&self) -> Uuid {
        self.current.load().log_id()
    }

    pub fn durable_frame_no(&self) -> u64 {
        *self.durable_frame_no.lock()
    }

    #[tracing::instrument(skip_all)]
    pub fn begin_read(&self, conn_id: u64) -> ReadTransaction<IO::File> {
        // FIXME: this is not enough to just increment the counter, we must make sure that the segment
        // is not sealed. If the segment is sealed, retry with the current segment
        let current = self.current.load();
        current.inc_reader_count();
        let (max_frame_no, db_size, max_offset) = current.with_header(|header| {
            (
                header.last_committed(),
                header.size_after(),
                header.frame_count() as u64,
            )
        });
        let id = self.wal_lock.next_tx_id.fetch_add(1, Ordering::Relaxed);
        ReadTransaction {
            id,
            max_frame_no,
            current: current.clone(),
            db_size,
            created_at: Instant::now(),
            conn_id,
            pages_read: 0,
            namespace: self.namespace.clone(),
            checkpoint_notifier: self.checkpoint_notifier.clone(),
            max_offset,
        }
    }

    /// Upgrade a read transaction to a write transaction
    pub fn upgrade(&self, tx: &mut Transaction<IO::File>) -> Result<()> {
        loop {
            match tx {
                Transaction::Write(_) => unreachable!("already in a write transaction"),
                Transaction::Read(read_tx) => {
                    let mut reserved = self.wal_lock.reserved.lock();
                    match *reserved {
                        // we have already reserved the slot, go ahead and try to acquire
                        Some(id) if id == read_tx.conn_id => {
                            tracing::trace!("taking reserved slot");
                            reserved.take();
                            let lock = self.wal_lock.tx_id.lock_blocking();
                            assert!(lock.is_none());
                            let write_tx = self.acquire_write(read_tx, lock, reserved)?;
                            *tx = Transaction::Write(write_tx);
                            return Ok(());
                        }
                        None => {
                            let lock = self.wal_lock.tx_id.lock_blocking();
                            if lock.is_none() && self.wal_lock.waiters.is_empty() {
                                let write_tx = self.acquire_write(read_tx, lock, reserved)?;
                                *tx = Transaction::Write(write_tx);
                                return Ok(());
                            }
                        }
                        _ => (),
                    }

                    tracing::trace!(
                        "txn currently held by another connection, registering to wait queue"
                    );

                    let parker = crossbeam::sync::Parker::new();
                    let unparker = parker.unparker().clone();
                    self.wal_lock.waiters.push((unparker, read_tx.conn_id));
                    drop(reserved);
                    parker.park();
                }
            }
        }
    }

    fn acquire_write(
        &self,
        read_tx: &ReadTransaction<IO::File>,
        mut tx_id_lock: async_lock::MutexGuard<Option<u64>>,
        mut reserved: MutexGuard<Option<u64>>,
    ) -> Result<WriteTransaction<IO::File>> {
        assert!(reserved.is_none() || *reserved == Some(read_tx.conn_id));
        assert!(tx_id_lock.is_none());
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
        let current_checksum = current.current_checksum();

        Ok(WriteTransaction {
            wal_lock: self.wal_lock.clone(),
            savepoints: vec![Savepoint {
                current_checksum,
                next_offset,
                next_frame_no,
                index: BTreeMap::new(),
            }],
            next_frame_no,
            next_offset,
            current_checksum,
            is_commited: false,
            read_tx: read_tx.clone(),
            recompute_checksum: None,
        })
    }

    #[tracing::instrument(skip(self, tx, buffer))]
    pub fn read_page(
        &self,
        tx: &mut Transaction<IO::File>,
        page_no: u32,
        buffer: &mut [u8],
    ) -> Result<()> {
        match tx.current.find_frame(page_no, tx) {
            Some(offset) => {
                // some debug assertions to make sure invariants hold
                #[cfg(debug_assertions)]
                {
                    if let Ok(header) = tx.current.frame_header_at(offset) {
                        // the frame we got is not more recent than max frame_no
                        assert!(
                            header.frame_no() <= tx.max_frame_no(),
                            "read frame is greater than max frame, {}, {}",
                            header.frame_no(),
                            tx.max_frame_no()
                        );
                        // the page we got is the page we asked for
                        assert_eq!(header.page_no(), page_no);
                    }
                }

                tx.current.read_page_offset(offset, buffer)?;
            }
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
    pub fn insert_frames<'a>(
        &self,
        tx: &mut WriteTransaction<IO::File>,
        pages: impl Iterator<Item = (u32, &'a [u8])>,
        size_after: Option<u32>,
    ) -> Result<()> {
        let current = self.current.load();
        let mut tx = tx.lock();
        if let Some(last_committed) = current.insert_pages(pages, size_after, &mut tx)? {
            self.new_frame_notifier.send_replace(last_committed);
        }

        if tx.is_commited() && self.swap_strategy.should_swap(current.count_committed()) {
            self.swap_current(&tx)?;
            self.swap_strategy.swapped();
        }

        Ok(())
    }

    /// Cut the current log, and register it for storage
    pub fn seal_current(&self) -> Result<()> {
        let mut tx = self.begin_read(u64::MAX).into();
        self.upgrade(&mut tx)?;

        let ret = {
            let mut guard = tx.as_write_mut().unwrap().lock();
            guard.commit();
            self.swap_current(&mut guard)
        };
        // make sure the tx is always ended before it's dropped!
        // FIXME: this is an issue with this design, since downgrade consume self, we can't have a
        // drop implementation. The should probably have a Option<WriteTxnInner>, to that we can
        // take &mut Self instead.
        tx.end();

        ret
    }

    /// Swap the current log. A write lock must be held, but the transaction must be must be committed already.
    pub(crate) fn swap_current(&self, tx: &impl TxGuard<IO::File>) -> Result<()> {
        self.registry.swap_current(self, tx)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn checkpoint(&self) -> Result<Option<u64>> {
        let durable_frame_no = *self.durable_frame_no.lock();
        let checkpointed_frame_no = self
            .current
            .load()
            .tail()
            .checkpoint(&self.db_file, durable_frame_no, self.log_id(), &self.io)
            .await?;
        if let Some(checkpointed_frame_no) = checkpointed_frame_no {
            self.checkpointed_frame_no
                .store(checkpointed_frame_no, Ordering::SeqCst);
        }

        Ok(checkpointed_frame_no)
    }

    pub fn last_committed_frame_no(&self) -> u64 {
        let current = self.current.load();
        current.last_committed_frame_no()
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }
}

#[cfg(test)]
mod test {
    use crate::test::{seal_current_segment, TestEnv};

    use super::*;

    #[tokio::test]
    async fn checkpoint() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        assert_eq!(shared.checkpointed_frame_no.load(Ordering::Relaxed), 0);

        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (12)", ()).unwrap();
        conn.execute("insert into test values (12)", ()).unwrap();

        assert_eq!(shared.checkpointed_frame_no.load(Ordering::Relaxed), 0);

        seal_current_segment(&shared);

        *shared.durable_frame_no.lock() = 999999;

        let frame_no = shared.checkpoint().await.unwrap().unwrap();
        assert_eq!(frame_no, 4);
        assert_eq!(shared.checkpointed_frame_no.load(Ordering::Relaxed), 4);

        assert!(shared.checkpoint().await.unwrap().is_none());
    }
}
