use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use crossbeam::deque::Injector;
use crossbeam::sync::Unparker;
use futures::Stream;
use libsql_sys::ffi::Sqlite3DbHeader;
use parking_lot::{Mutex, MutexGuard};
use rand::Rng as _;
use roaring::RoaringBitmap;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;
use zerocopy::{AsBytes as _, FromZeroes as _};

use crate::checkpointer::CheckpointMessage;
use crate::error::{Error, Result};
use crate::io::buf::ZeroCopyBoxIoBuf;
use crate::io::file::FileExt;
use crate::io::Io;
use crate::replication::storage::{ReplicateFromStorage, StorageReplicator};
use crate::segment::current::CurrentSegment;
use crate::segment::list::SegmentList;
use crate::segment::sealed::SealedSegment;
use crate::segment::Segment as _;
use crate::segment::{Frame, FrameHeader};
use crate::segment_swap_strategy::duration::DurationSwapStrategy;
use crate::segment_swap_strategy::frame_count::FrameCountSwapStrategy;
use crate::segment_swap_strategy::SegmentSwapStrategy;
use crate::storage::{OnStoreCallback, Storage};
use crate::transaction::{ReadTransaction, Savepoint, Transaction, TxGuard, WriteTransaction};
use crate::{LibsqlFooter, LIBSQL_PAGE_SIZE};
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

pub struct SharedWal<IO: Io, S> {
    pub(crate) current: ArcSwap<CurrentSegment<IO::File>>,
    pub(crate) wal_lock: Arc<WalLock>,
    pub(crate) db_file: IO::File,
    pub(crate) namespace: NamespaceName,
    pub(crate) checkpointed_frame_no: AtomicU64,
    /// max frame_no acknowledged by the durable storage
    pub(crate) durable_frame_no: Arc<Mutex<u64>>,
    pub(crate) new_frame_notifier: tokio::sync::watch::Sender<u64>,
    pub(crate) stored_segments: Box<dyn ReplicateFromStorage>,
    pub(crate) shutdown: AtomicBool,
    pub(crate) checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
    pub(crate) io: Arc<IO>,
    pub(crate) swap_strategy: Box<dyn SegmentSwapStrategy>,
    pub(crate) wals_path: PathBuf,
    pub(crate) storage: Arc<S>,
}

impl<IO, S> SharedWal<IO, S>
where
    IO: Io,
{
    #[tracing::instrument(skip(self), fields(namespace = self.namespace.as_str()))]
    pub fn shutdown(&self) -> Result<()>
    where
        S: Storage<Segment = SealedSegment<IO::File>>,
    {
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
            self.swap_current(&tx)?;
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
            savepoints: vec![Savepoint::new(next_offset, next_frame_no, current_checksum)],
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
    ) -> Result<()>
    where
        S: Storage<Segment = SealedSegment<IO::File>>,
    {
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
    pub fn seal_current(&self) -> Result<()>
    where
        S: Storage<Segment = SealedSegment<IO::File>>,
    {
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
    pub(crate) fn swap_current(&self, tx: &impl TxGuard<IO::File>) -> Result<()>
    where
        S: Storage<Segment = SealedSegment<IO::File>>,
    {
        assert!(tx.is_commited());
        let current = self.current.load();
        if current.is_empty() {
            return Ok(());
        }
        let start_frame_no = current.next_frame_no();
        let path = self.wals_path.join(format!("{start_frame_no:020}.seg"));

        let segment_file = self.io.open(true, true, true, &path)?;
        let salt = self.io.with_rng(|rng| rng.gen());
        let new = CurrentSegment::create(
            segment_file,
            path,
            start_frame_no,
            current.db_size(),
            current.tail().clone(),
            salt,
            current.log_id(),
        )?;
        // sealing must the last fallible operation, because we don't want to end up in a situation
        // where the current log is sealed and it wasn't swapped.
        if let Some(sealed) = current.seal(self.io.now())? {
            new.tail().push(sealed.clone());
            maybe_store_segment(
                self.storage.as_ref(),
                &self.checkpoint_notifier,
                &self.namespace,
                &self.durable_frame_no,
                sealed,
            );
        }

        self.current.swap(Arc::new(new));
        tracing::debug!("current segment swapped");

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

    /// read frames from the main db file.
    pub(crate) fn replicate_from_db_file<'a>(
        &'a self,
        seen: &'a RoaringBitmap,
        tx: &'a ReadTransaction<IO::File>,
        until: u64,
    ) -> impl Stream<Item = crate::replication::Result<Box<Frame>>> + Send + 'a
    where
        S: Send + Sync,
    {
        async_stream::try_stream! {
            let mut all = RoaringBitmap::new();
            all.insert_range(1..=tx.db_size);
            let to_take = all - seen;
            for page_no in to_take {
                let mut frame = Frame::new_box_zeroed();
                *frame.header_mut() = FrameHeader {
                    page_no: page_no.into(),
                    size_after: 0.into(),
                    // we don't really know what the frame_no is, so we set it to a number less that any other frame_no
                    frame_no: until.into(),
                };
                let buf = unsafe { ZeroCopyBoxIoBuf::new_uninit_partial(frame, size_of::<FrameHeader>()) };
                let (buf, ret) = self.db_file.read_exact_at_async(buf, (page_no as u64 - 1) * LIBSQL_PAGE_SIZE as u64).await;
                ret?;
                let frame = buf.into_inner();
                yield frame;
            }
        }
    }

    /// Open the shared wal at path. The caller must ensure that no other process is calling this
    /// conccurently.
    pub(crate) fn try_open(
        io: Arc<IO>,
        storage: Arc<S>,
        checkpoint_notifier: &tokio::sync::mpsc::Sender<CheckpointMessage>,
        namespace: &NamespaceName,
        db_path: &Path,
    ) -> Result<Self>
    where
        S: Storage<Segment = SealedSegment<IO::File>>,
    {
        let db_file = io.open(false, true, true, db_path)?;
        let db_file_len = db_file.len()?;
        let header = if db_file_len > 0 {
            let mut header: Sqlite3DbHeader = Sqlite3DbHeader::new_zeroed();
            db_file.read_exact_at(header.as_bytes_mut(), 0)?;
            Some(header)
        } else {
            None
        };

        let footer = try_read_footer(&db_file)?;

        let mut checkpointed_frame_no = footer.map(|f| f.replication_index.get()).unwrap_or(0);

        // the trick here to prevent sqlite to open our db is to create a dir <db-name>-wal. Sqlite
        // will think that this is a wal file, but it's in fact a directory and it will not like
        // it.
        let mut wals_path = db_path.to_owned();
        wals_path.set_file_name(format!(
            "{}-wal",
            db_path.file_name().unwrap().to_str().unwrap()
        ));
        io.create_dir_all(&wals_path)?;
        // TODO: handle that with abstract io
        let dir = walkdir::WalkDir::new(&wals_path)
            .sort_by_file_name()
            .into_iter();

        // we only checkpoint durable frame_no so this is a good first estimate without an actual
        // network call.
        let durable_frame_no = Arc::new(Mutex::new(checkpointed_frame_no));

        let list = SegmentList::default();
        for entry in dir {
            let entry = entry.map_err(|e| e.into_io_error().unwrap())?;
            if entry
                .path()
                .extension()
                .map(|e| e.to_str().unwrap() != "seg")
                .unwrap_or(true)
            {
                continue;
            }

            let file = io.open(false, true, true, entry.path())?;

            if let Some(sealed) = SealedSegment::open(
                file.into(),
                entry.path().to_path_buf(),
                Default::default(),
                io.now(),
            )? {
                list.push(sealed.clone());
                maybe_store_segment(
                    storage.as_ref(),
                    &checkpoint_notifier,
                    &namespace,
                    &durable_frame_no,
                    sealed,
                );
            }
        }

        let log_id = match footer {
            Some(footer) if list.is_empty() => footer.log_id(),
            None if list.is_empty() => io.uuid(),
            Some(footer) => {
                let log_id = list
                    .with_head(|h| h.header().log_id.get())
                    .expect("non-empty list should have a head");
                let log_id = Uuid::from_u128(log_id);
                assert_eq!(log_id, footer.log_id());
                log_id
            }
            None => {
                let log_id = list
                    .with_head(|h| h.header().log_id.get())
                    .expect("non-empty list should have a head");
                Uuid::from_u128(log_id)
            }
        };

        // if there is a tail, then the latest checkpointed frame_no is one before the the
        // start frame_no of the tail. We must read it from the tail, because a partial
        // checkpoint may have occured before a crash.
        if let Some(last) = list.last() {
            checkpointed_frame_no = (last.start_frame_no() - 1).max(1)
        }

        let (db_size, next_frame_no) = list
            .with_head(|segment| {
                let header = segment.header();
                (header.size_after(), header.next_frame_no())
            })
            .unwrap_or_else(|| match header {
                Some(header) => (
                    header.db_size.get(),
                    NonZeroU64::new(checkpointed_frame_no + 1)
                        .unwrap_or(NonZeroU64::new(1).unwrap()),
                ),
                None => (0, NonZeroU64::new(1).unwrap()),
            });

        let current_segment_path = wals_path.join(format!("{next_frame_no:020}.seg"));

        let segment_file = io.open(true, true, true, &current_segment_path)?;
        let salt = io.with_rng(|rng| rng.gen());

        let current = arc_swap::ArcSwap::new(Arc::new(CurrentSegment::create(
            segment_file,
            current_segment_path,
            next_frame_no,
            db_size,
            list.into(),
            salt,
            log_id,
        )?));

        let (new_frame_notifier, _) = tokio::sync::watch::channel(next_frame_no.get() - 1);

        // FIXME: make swap strategy configurable
        // This strategy will perform a swap if either the wal is bigger than 20k frames, or older
        // than 10 minutes, or if the frame count is greater than a 1000 and the wal was last
        // swapped more than 30 secs ago
        let swap_strategy = Box::new(
            DurationSwapStrategy::new(Duration::from_secs(5 * 60))
                .or(FrameCountSwapStrategy::new(20_000))
                .or(FrameCountSwapStrategy::new(1000)
                    .and(DurationSwapStrategy::new(Duration::from_secs(30)))),
        );

        Ok(Self {
            current,
            wal_lock: Default::default(),
            db_file,
            namespace: namespace.clone(),
            checkpointed_frame_no: checkpointed_frame_no.into(),
            new_frame_notifier,
            durable_frame_no,
            stored_segments: Box::new(StorageReplicator::new(storage.clone(), namespace.clone())),
            shutdown: false.into(),
            checkpoint_notifier: checkpoint_notifier.clone(),
            io,
            storage,
            swap_strategy,
            wals_path: wals_path.to_owned(),
        })
    }
}

fn try_read_footer(db_file: &impl FileExt) -> Result<Option<LibsqlFooter>> {
    let len = db_file.len()?;
    if len as usize % LIBSQL_PAGE_SIZE as usize == size_of::<LibsqlFooter>() {
        let mut footer: LibsqlFooter = LibsqlFooter::new_zeroed();
        let footer_offset = (len / LIBSQL_PAGE_SIZE as u64) * LIBSQL_PAGE_SIZE as u64;
        db_file.read_exact_at(footer.as_bytes_mut(), footer_offset)?;
        footer.validate()?;
        Ok(Some(footer))
    } else {
        Ok(None)
    }
}

#[tracing::instrument(skip_all, fields(namespace = namespace.as_str(), start_frame_no = seg.start_frame_no()))]
fn maybe_store_segment<S: Storage>(
    storage: &S,
    notifier: &tokio::sync::mpsc::Sender<CheckpointMessage>,
    namespace: &NamespaceName,
    durable_frame_no: &Arc<Mutex<u64>>,
    seg: S::Segment,
) {
    if seg.last_committed() > *durable_frame_no.lock() {
        let cb: OnStoreCallback = Box::new({
            let notifier = notifier.clone();
            let durable_frame_no = durable_frame_no.clone();
            let namespace = namespace.clone();
            move |fno| {
                Box::pin(async move {
                    update_durable(fno, notifier, durable_frame_no, namespace).await;
                })
            }
        });
        storage.store(namespace, seg, None, cb);
    } else {
        // segment can be checkpointed right away.
        // FIXME: this is only necessary because some tests call this method in an async context.
        #[cfg(debug_assertions)]
        {
            let namespace = namespace.clone();
            let notifier = notifier.clone();
            tokio::spawn(async move {
                let _ = notifier.send(CheckpointMessage::Namespace(namespace)).await;
            });
        }

        #[cfg(not(debug_assertions))]
        {
            let _ = notifier.blocking_send(CheckpointMessage::Namespace(namespace.clone()));
        }

        tracing::debug!(
            segment_end = seg.last_committed(),
            durable_frame_no = *durable_frame_no.lock(),
            "segment doesn't contain any new data"
        );
    }
}

async fn update_durable(
    new_durable: u64,
    notifier: mpsc::Sender<CheckpointMessage>,
    durable_frame_no_slot: Arc<Mutex<u64>>,
    namespace: NamespaceName,
) {
    {
        let mut g = durable_frame_no_slot.lock();
        if *g < new_durable {
            *g = new_durable;
        }
    }
    let _ = notifier.send(CheckpointMessage::Namespace(namespace)).await;
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
