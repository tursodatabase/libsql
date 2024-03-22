use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::deque::Steal;
use crossbeam::sync::{Parker, Unparker};
use hashbrown::HashMap;
use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use libsql_sys::wal::{CheckpointMode, Sqlite3Wal, Wal};
use metrics::atomics::AtomicU64;
use parking_lot::Mutex;
use rusqlite::ErrorCode;

use super::libsql::Connection;
use super::TXN_TIMEOUT;

pub type ConnId = u64;

pub type ManagedConnectionWal = WrappedWal<ManagedConnectionWalWrapper, Sqlite3Wal>;

#[derive(Clone)]
struct Abort(Arc<dyn Fn() + Send + Sync + 'static>);

impl Abort {
    fn from_conn<T: Wal + Send + 'static>(conn: &Arc<Mutex<Connection<T>>>) -> Self {
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
        conn: &Arc<Mutex<Connection<T>>>,
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

impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

pub struct ConnectionManagerInner {
    /// When a slot becomes available, the connection allowed to make progress is put here
    /// the connection currently holding the lock
    /// bool: acquired
    current: Mutex<Option<(ConnId, Instant, bool)>>,
    /// map of registered connections
    abort_handle: Mutex<HashMap<ConnId, Abort>>,
    /// threads waiting to acquire the lock
    /// todo: limit how many can be push
    write_queue: crossbeam::deque::Injector<(ConnId, Unparker)>,
    txn_timeout_duration: Duration,
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
            if current.map_or(true, |(id, _, _)| id != self.id) && !enqueued {
                self.manager
                    .write_queue
                    .push((self.id, parker.unparker().clone()));
                enqueued = true;
                tracing::debug!("enqueued");
            }
            match *current {
                Some((id, started_at, acquired)) => {
                    // this is us, the previous connection put us here when it closed the
                    // transaction
                    if id == self.id {
                        assert!(!acquired);
                        tracing::debug!(
                            line = line!(),
                            "got lock after: {:?}",
                            enqueued_at.elapsed()
                        );
                        break;
                    } else {
                        // not us, maybe we need to steal the lock?
                        drop(current);
                        if started_at.elapsed() >= self.manager.inner.txn_timeout_duration {
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
                            parker.park();
                            tracing::debug!(line = line!(), "unparked");
                        } else {
                            // otherwise we wait for the txn to timeout, or to be unparked by it
                            let before = Instant::now();
                            let deadline = started_at + self.manager.inner.txn_timeout_duration;
                            parker.park_deadline(
                                started_at + self.manager.inner.txn_timeout_duration,
                            );
                            tracing::debug!(
                                line = line!(),
                                "unparked after: {:?}, before_deadline: {:?}",
                                before.elapsed(),
                                Instant::now() < deadline
                            );
                        }
                    }
                }
                None => {
                    let next = loop {
                        match self.manager.write_queue.steal() {
                            Steal::Empty => break None,
                            Steal::Success(item) => break Some(item),
                            Steal::Retry => (),
                        }
                    };

                    match next {
                        Some((id, _)) if id == self.id => {
                            // this is us!
                            *current = Some((self.id, Instant::now(), false));
                            tracing::debug!("got lock after: {:?}", enqueued_at.elapsed());
                            break;
                        }
                        Some((id, unpaker)) => {
                            tracing::debug!(line = line!(), "unparking id={id}");
                            *current = Some((id, Instant::now(), false));
                            drop(current);
                            unpaker.unpark();
                            parker.park();
                        }
                        None => unreachable!(),
                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn release(&self) {
        let mut current = self.manager.current.lock();
        let Some((id, started_at, _)) = current.take() else {
            unreachable!("no lock to release")
        };

        assert_eq!(id, self.id);

        tracing::debug!("transaction finished after {:?}", started_at.elapsed());
        let next = loop {
            match self.manager.write_queue.steal() {
                Steal::Empty => break None,
                Steal::Success(item) => break Some(item),
                Steal::Retry => (),
            }
        };

        if let Some((id, unparker)) = next {
            tracing::debug!(line = line!(), "unparking id={id}");
            *current = Some((id, Instant::now(), false));
            unparker.unpark()
        } else {
            *current = None;
        }
    }
}

impl WrapWal<Sqlite3Wal> for ManagedConnectionWalWrapper {
    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn begin_write_txn(&mut self, wrapped: &mut Sqlite3Wal) -> libsql_sys::wal::Result<()> {
        tracing::debug!("begin write");
        self.acquire()?;
        match wrapped.begin_write_txn() {
            Ok(_) => {
                tracing::debug!("transaction acquired");
                let mut lock = self.manager.current.lock();
                lock.as_mut().unwrap().2 = true;

                Ok(())
            }
            Err(e) => {
                if !matches!(e.code, ErrorCode::DatabaseBusy) {
                    // this is not a retriable error
                    tracing::debug!("error acquiring lock, releasing: {e}");
                    self.release();
                } else {
                    tracing::debug!("error acquiring lock: {e}");
                }
                Err(e)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn checkpoint(
        &mut self,
        wrapped: &mut Sqlite3Wal,
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
    fn begin_read_txn(&mut self, wrapped: &mut Sqlite3Wal) -> libsql_sys::wal::Result<bool> {
        tracing::debug!("begin read txn");
        wrapped.begin_read_txn()
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn end_read_txn(&mut self, wrapped: &mut Sqlite3Wal) {
        wrapped.end_read_txn();
        {
            let current = self.manager.current.lock();
            if let Some((id, _, true)) = *current {
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
    fn end_write_txn(&mut self, wrapped: &mut Sqlite3Wal) -> libsql_sys::wal::Result<()> {
        wrapped.end_write_txn()?;
        tracing::debug!("end write txn");
        self.release();

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn close<M: libsql_sys::wal::WalManager<Wal = Sqlite3Wal>>(
        &mut self,
        manager: &M,
        wrapped: &mut Sqlite3Wal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        let before = Instant::now();
        let ret = manager.close(wrapped, db, sync_flags, None);
        {
            tracing::debug!(line = line!(), "unparked");
            let current = self.manager.current.lock();
            if let Some((id, _, _)) = *current {
                if id == self.id {
                    tracing::debug!("connection closed without releasing lock");
                    drop(current);
                    self.release()
                }
            }
        }

        self.manager.inner.abort_handle.lock().remove(&self.id);
        tracing::debug!("closed in {:?}", before.elapsed());
        ret
    }
}
