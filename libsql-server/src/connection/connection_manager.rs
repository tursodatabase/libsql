use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::deque::Steal;
use crossbeam::sync::{Parker, Unparker};
use hashbrown::HashMap;
use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use libsql_sys::wal::{CheckpointMode, Sqlite3Wal, Sqlite3WalManager, Wal};
use metrics::atomics::AtomicU64;
use parking_lot::{Mutex, MutexGuard};
use rusqlite::ErrorCode;

use super::connection_core::CoreConnection;
use super::TXN_TIMEOUT;

pub type ConnId = u64;
pub type InnerWalManager = Sqlite3WalManager;

pub type InnerWal = Sqlite3Wal;
pub type ManagedConnectionWal = WrappedWal<ManagedConnectionWalWrapper, InnerWal>;

#[derive(Copy, Clone, Debug)]
struct Slot {
    id: ConnId,
    started_at: Instant,
    state: SlotState,
}

#[derive(Clone)]
struct Abort(Arc<dyn Fn() + Send + Sync + 'static>);

impl Abort {
    fn from_conn<T: Wal + Send + 'static>(conn: &Arc<Mutex<CoreConnection<T>>>) -> Self {
        let conn = Arc::downgrade(conn);
        Self(Arc::new(move || {
            conn.upgrade()
                .expect("connection still owns the slot, so it must exist")
                .lock()
                .force_rollback();
        }))
    }

    fn abort(&self) {
        (self.0)()
    }
}

#[derive(Clone)]
pub struct ConnectionManager {
    inner: Arc<ConnectionManagerInner>,
}

impl ConnectionManager {
    pub(super) fn register_connection<T: Wal + Send + Send + 'static>(
        &self,
        conn: &Arc<Mutex<CoreConnection<T>>>,
        id: ConnId,
    ) {
        let abort = Abort::from_conn(conn);
        self.inner.abort_handle.lock().insert(id, abort);
    }
}

impl Deref for ConnectionManager {
    type Target = ConnectionManagerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ConnectionManager {
    pub fn new(txn_timeout_duration: Duration) -> ConnectionManager {
        Self {
            inner: Arc::new(ConnectionManagerInner {
                txn_timeout_duration,
                ..Default::default()
            }),
        }
    }
}

pub struct ConnectionManagerInner {
    /// When a slot becomes available, the connection allowed to make progress is put here
    /// the connection currently holding the lock
    /// bool: acquired
    current: Mutex<Option<Slot>>,
    /// map of registered connections
    abort_handle: Mutex<HashMap<ConnId, Abort>>,
    /// threads waiting to acquire the lock
    /// todo: limit how many can be push
    write_queue: crossbeam::deque::Injector<(ConnId, Unparker)>,
    txn_timeout_duration: Duration,
    /// the time we are given to acquire a transaction after we were given a slot
    acquire_timeout_duration: Duration,
    next_conn_id: AtomicU64,
    sync_token: AtomicU64,
}

impl Default for ConnectionManagerInner {
    fn default() -> Self {
        Self {
            current: Default::default(),
            abort_handle: Default::default(),
            write_queue: Default::default(),
            txn_timeout_duration: TXN_TIMEOUT,
            acquire_timeout_duration: Duration::from_millis(15),
            next_conn_id: Default::default(),
            sync_token: AtomicU64::new(0),
        }
    }
}

#[derive(Clone)]
pub struct ManagedConnectionWalWrapper {
    id: ConnId,
    manager: ConnectionManager,
}

impl ManagedConnectionWalWrapper {
    pub(crate) fn new(manager: ConnectionManager) -> Self {
        let id = manager.inner.next_conn_id.fetch_add(1, Ordering::SeqCst);
        Self { id, manager }
    }

    pub fn id(&self) -> ConnId {
        self.id
    }

    fn acquire(&self) -> libsql_sys::wal::Result<()> {
        let parker = Parker::new();
        let mut enqueued = false;
        let enqueued_at = Instant::now();
        let sync_token = self.manager.sync_token.load(Ordering::SeqCst);
        loop {
            let mut current = self.manager.current.lock();
            // if current is not currently us, and we havent enqueued yet, then enqueue
            // current can be us in two cases:
            // - in previous iteration, the queue was empty, and we popped ourselves
            // - we tried to acquire the lock during the previous iteration, but the underlying
            // method returned an error and we had to retry immediately, by re-entering this
            // function.
            if self.manager.sync_token.load(Ordering::SeqCst) != sync_token {
                return Err(rusqlite::ffi::Error {
                    code: ErrorCode::DatabaseBusy,
                    extended_code: 517, // stale read
                });
            }
            // If other connection is about to checkpoint - we better to immediately return.
            //
            // The reason is that write transaction are upgraded from read transactions in SQLite.
            // Due to this, every write transaction need to hold SHARED-WAL lock and if we will
            // block write transaction here - we will prevent checkpoint process from restarting the WAL
            // (because it needs to acquire EXCLUSIVE-WAL lock)
            //
            // So, the scenario is following:
            // T0: we have a bunch of SELECT queries which will execute till time T2
            // T1: CHECKPOINT process is starting: it holds CKPT and WRITE lock and attempt to acquire
            //     EXCLUSIVE-WAL locks one by one in order to check the position of readers. CHECKPOINT will
            //     use busy handler and can potentially acquire lock not from the first attempt.
            // T2: CHECKPOINT process were able to check all WAL reader positions (by acquiring lock or atomically check reader position)
            //     and started to transfer WAL to the DB file
            // T3: INSERT query starts executing: it started as a read transaction and holded SHARED-WAL lock but then it needs to
            //     upgrade to write transaction through begin_write_txn call
            // T4: CHECKPOINT transferred all pages from WAL to DB file and need to check if it can restart the WAL. In order to
            //     do that it needs to hold all EXCLUSIVE-WAL locks to make sure that all readers use only DB file
            //
            // In the scenario above, if we will park INSERT at the time T3 - CHECKPOINT will be unable to hold EXCLUSIVE-WAL
            // locks and so WAL will not be truncated.
            // In case when DB has continious load with overlapping reads and writes - this problem became very noticeable
            // as it can defer WAL truncation a lot.
            //
            // Also, such implementation is more aligned with LibSQL/SQLite behaviour where sqlite3WalBeginWriteTransaction
            // immediately abort with SQLITE_BUSY error if it can't acquire WRITE lock (which CHECKPOINT also take before start of the work)
            // and busy handler (e.g. retries) for writes are invoked by SQLite at upper layer of request processing.
            match *current {
                Some(Slot {
                    id,
                    state: SlotState::Acquired(SlotType::Checkpoint),
                    ..
                }) if id != self.id => {
                    return Err(rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY));
                }
                _ => {}
            }
            // note, that it's important that we return SQLITE_BUSY error for CHECKPOINT starvation problem before that condition
            // because after we will add something to the write_queue - we can't easily abort execution of acquire() method
            if current.as_mut().map_or(true, |slot| slot.id != self.id) && !enqueued {
                self.manager
                    .write_queue
                    .push((self.id, parker.unparker().clone()));
                enqueued = true;
                tracing::debug!("enqueued");
            }
            match *current {
                Some(ref mut slot) => {
                    tracing::debug!("current slot: {slot:?}");
                    // this is us, the previous connection put us here when it closed the
                    // transaction
                    if slot.id == self.id {
                        assert!(
                            slot.state.is_notified() || slot.state.is_failure(),
                            "{slot:?}"
                        );
                        slot.state = SlotState::Acquiring;
                        tracing::debug!(
                            line = line!(),
                            "got lock after: {:?}",
                            enqueued_at.elapsed()
                        );
                        break;
                    } else {
                        // not us, maybe we need to steal the lock?
                        let since_started = slot.started_at.elapsed();
                        let deadline = slot.started_at + self.manager.txn_timeout_duration;
                        match slot.state {
                            SlotState::Acquired(..) => {
                                if since_started >= self.manager.txn_timeout_duration {
                                    let id = slot.id;
                                    drop(current);
                                    let handle = {
                                        self.manager
                                            .inner
                                            .abort_handle
                                            .lock()
                                            .get(&id)
                                            .unwrap()
                                            .clone()
                                    };
                                    // the guard must be dropped before rolling back, or end write txn will
                                    // deadlock
                                    tracing::debug!("forcing rollback of {id}");
                                    handle.abort();
                                    tracing::debug!(line = line!(), "parking");
                                    parker.park();
                                    tracing::debug!(line = line!(), "unparked");
                                } else {
                                    // otherwise we wait for the txn to timeout, or to be unparked by it
                                    let deadline =
                                        slot.started_at + self.manager.inner.txn_timeout_duration;
                                    drop(current);
                                    tracing::debug!(line = line!(), "parking");
                                    parker.park_deadline(deadline);
                                    tracing::debug!(
                                        line = line!(),
                                        "before_deadline?: {:?}",
                                        Instant::now() < deadline
                                    );
                                }
                            }
                            // we may want to limit how long a lock takes to go from notified
                            // to acquiring
                            SlotState::Acquiring | SlotState::Notified => {
                                drop(current);
                                tracing::debug!(line = line!(), "parking");
                                parker.park_deadline(deadline);
                                tracing::debug!(
                                    line = line!(),
                                    "unparked after before_deadline?: {:?}",
                                    Instant::now() < deadline
                                );
                            }
                            SlotState::Failure => {
                                if since_started >= self.manager.inner.acquire_timeout_duration {
                                    // the connection failed to acquire a transaction during the grace
                                    // period. schedule the next transaction
                                    match self.schedule_next(&mut current) {
                                        Some(id) if id == self.id => {
                                            current.as_mut().unwrap().state = SlotState::Acquiring;
                                            break;
                                        }
                                        Some(_) => {
                                            drop(current);
                                            tracing::debug!(line = line!(), "parking");
                                            parker.park();
                                            tracing::debug!(line = line!(), "unparked");
                                        }
                                        None => {
                                            *current = Some(Slot {
                                                id: self.id,
                                                started_at: Instant::now(),
                                                state: SlotState::Acquiring,
                                            });
                                            break;
                                        }
                                    }
                                } else {
                                    tracing::trace!("noticed failure from id={}, parking until end of grace period", slot.id);
                                    let deadline = slot.started_at
                                        + self.manager.inner.acquire_timeout_duration;
                                    drop(current);
                                    tracing::debug!(line = line!(), "parking");
                                    parker.park_deadline(deadline);
                                    tracing::debug!(
                                        line = line!(),
                                        "unparked after before_deadline?: {:?}",
                                        Instant::now() < deadline
                                    );
                                }
                            }
                        }
                    }
                }
                None => match self.schedule_next(&mut current) {
                    Some(id) if id == self.id => {
                        current.as_mut().unwrap().state = SlotState::Acquiring;
                        break;
                    }
                    Some(_) => {
                        drop(current);
                        tracing::debug!(line = line!(), "parking");
                        parker.park();
                        tracing::debug!(line = line!(), "unparked");
                    }
                    None => {
                        *current = Some(Slot {
                            id: self.id,
                            started_at: Instant::now(),
                            state: SlotState::Acquiring,
                        })
                    }
                },
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, current))]
    #[track_caller]
    fn schedule_next(&self, current: &mut MutexGuard<Option<Slot>>) -> Option<ConnId> {
        let next = loop {
            match self.manager.write_queue.steal() {
                Steal::Empty => break None,
                Steal::Success(item) => break Some(item),
                Steal::Retry => (),
            }
        };

        match next {
            Some((id, unpaker)) => {
                tracing::debug!(line = line!(), "unparking id={id}");
                **current = Some(Slot {
                    id,
                    started_at: Instant::now(),
                    state: SlotState::Notified,
                });
                unpaker.unpark();
                Some(id)
            }
            None => None,
        }
    }

    #[tracing::instrument(skip(self))]
    #[track_caller]
    fn release(&self) {
        let mut current = self.manager.current.lock();
        let Some(slot) = current.take() else {
            unreachable!("no lock to release")
        };

        assert_eq!(slot.id, self.id);

        tracing::debug!("transaction finished after {:?}", slot.started_at.elapsed());
        match self.schedule_next(&mut current) {
            Some(_) => (),
            None => {
                *current = None;
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum SlotType {
    WriteTxn,
    Checkpoint,
}

#[derive(Copy, Clone, Debug)]
enum SlotState {
    Notified,
    Acquiring,
    Acquired(SlotType),
    Failure,
}

impl SlotState {
    /// Returns `true` if the slot state is [`Notified`].
    ///
    /// [`Notified`]: SlotState::Notified
    #[must_use]
    fn is_notified(&self) -> bool {
        matches!(self, Self::Notified)
    }

    /// Returns `true` if the slot state is [`Failure`].
    ///
    /// [`Failure`]: SlotState::Failure
    #[must_use]
    fn is_failure(&self) -> bool {
        matches!(self, Self::Failure)
    }
}

impl WrapWal<InnerWal> for ManagedConnectionWalWrapper {
    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn begin_write_txn(&mut self, wrapped: &mut InnerWal) -> libsql_sys::wal::Result<()> {
        tracing::debug!("begin write");
        self.acquire()?;
        match wrapped.begin_write_txn() {
            Ok(_) => {
                tracing::debug!("transaction acquired");
                let mut lock = self.manager.current.lock();
                lock.as_mut().unwrap().state = SlotState::Acquired(SlotType::WriteTxn);

                Ok(())
            }
            Err(e) => {
                if !matches!(e.code, ErrorCode::DatabaseBusy) {
                    // this is not a retriable error
                    tracing::debug!("error acquiring lock, releasing: {e}");
                    self.release();
                } else {
                    let mut lock = self.manager.current.lock();
                    lock.as_mut().unwrap().state = SlotState::Failure;
                    tracing::debug!("error acquiring lock: {e}");
                }
                Err(e)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn checkpoint(
        &mut self,
        wrapped: &mut InnerWal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn libsql_sys::wal::BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn libsql_sys::wal::CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> libsql_sys::wal::Result<()> {
        let before = Instant::now();
        self.acquire()?;
        self.manager.current.lock().as_mut().unwrap().state =
            SlotState::Acquired(SlotType::Checkpoint);

        let mode = if rand::random::<f32>() < 0.1 {
            CheckpointMode::Truncate
        } else {
            mode
        };

        if mode as i32 >= CheckpointMode::Restart as i32 {
            tracing::debug!("forcing queue sync");
            self.manager.sync_token.fetch_add(1, Ordering::SeqCst);
            let queue_len = self.manager.write_queue.len();
            for _ in 0..queue_len {
                let (id, unparker) = self.manager.write_queue.steal().success().unwrap();
                tracing::debug!("forcing queue sync for id={id}");
                unparker.unpark();
            }
        }

        tracing::debug!("attempted checkpoint mode: {mode:?}");
        let ret = wrapped.checkpoint(
            db,
            mode,
            busy_handler,
            sync_flags,
            buf,
            checkpoint_cb,
            in_wal,
            backfilled,
        );

        self.release();

        tracing::debug!("checkpoint called: {:?}", before.elapsed());
        ret
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn begin_read_txn(&mut self, wrapped: &mut InnerWal) -> libsql_sys::wal::Result<bool> {
        tracing::debug!("begin read txn");
        wrapped.begin_read_txn()
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn end_read_txn(&mut self, wrapped: &mut InnerWal) {
        wrapped.end_read_txn();
        {
            let current = self.manager.current.lock();
            // end read will only close the write txn if we actually acquired one, so only release
            // if the slot acquire the transaction lock
            if let Some(Slot {
                id,
                state: SlotState::Acquired(..),
                ..
            }) = *current
            {
                // releasing read transaction releases the write lock (see wal.c)
                if id == self.id {
                    drop(current);
                    self.release();
                }
            }
        }
        tracing::debug!("end read txn");
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn end_write_txn(&mut self, wrapped: &mut InnerWal) -> libsql_sys::wal::Result<()> {
        wrapped.end_write_txn()?;
        tracing::debug!("end write txn");
        self.release();

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn close<M: libsql_sys::wal::WalManager<Wal = InnerWal>>(
        &mut self,
        manager: &M,
        wrapped: &mut InnerWal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        let before = Instant::now();
        let ret = manager.close(wrapped, db, sync_flags, None);
        {
            let current = self.manager.current.lock();
            if let Some(slot @ Slot { id, .. }) = *current {
                if id == self.id {
                    tracing::debug!(
                        id = self.id,
                        "connection closed without releasing lock: {slot:?}"
                    );
                    drop(current);
                    self.release()
                }
            }
        }

        self.manager.inner.abort_handle.lock().remove(&self.id);
        tracing::debug!(id = self.id, "closed in {:?}", before.elapsed());
        ret
    }
}
