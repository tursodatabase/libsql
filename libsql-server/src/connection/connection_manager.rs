use std::ops::Deref;
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
}

impl Default for ConnectionManagerInner {
    fn default() -> Self {
        Self {
            current: Default::default(),
            abort_handle: Default::default(),
            write_queue: Default::default(),
            txn_timeout_duration: TXN_TIMEOUT,
            next_conn_id: Default::default(),
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
        let id = manager
            .inner
            .next_conn_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self { id, manager }
    }

    pub fn id(&self) -> ConnId {
        self.id
    }

    fn acquire(&self) {
        let parker = Parker::new();
        let mut enqueued = false;
        let enqueued_at = Instant::now();
        loop {
            let mut current = self.manager.current.lock();
            // if current is not currently us, and we havent enqueued yet, then enqueue
            // current can be us in two cases:
            // - in previous iteration, the queue was empty, and we popped ourselves
            // - we tried to acquire the lock during the previous iteration, but the underlying
            // method returned an error and we had to retry immediately, by re-entering this
            // function.
            if current.map_or(true, |(id, _, _)| id != self.id) && !enqueued {
                self.manager
                    .write_queue
                    .push((self.id, parker.unparker().clone()));
                tracing::debug!("enqueued");
                enqueued = true;
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
    }
}

impl WrapWal<Sqlite3Wal> for ManagedConnectionWalWrapper {
    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn begin_write_txn(&mut self, wrapped: &mut Sqlite3Wal) -> libsql_sys::wal::Result<()> {
        tracing::debug!("begin write");
        self.acquire();
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
                    tracing::debug!("error acquiring lock, dropping slot: {e}");
                    self.manager.current.lock().take();
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
        self.acquire();
        let mode = if rand::random::<f32>() < 0.1 {
            CheckpointMode::Truncate
        } else {
            mode
        };
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

        let mut current = self.manager.current.lock();
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

        tracing::debug!("checkpoint called: {:?}", before.elapsed());
        ret
        // Ok(())
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
            let mut current = self.manager.current.lock();
            if let Some((id, _, true)) = *current {
                if id == self.id {
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
        }
        tracing::debug!("end read txn");
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn end_write_txn(&mut self, wrapped: &mut Sqlite3Wal) -> libsql_sys::wal::Result<()> {
        wrapped.end_write_txn()?;
        tracing::debug!("end write txn");
        let mut current = self.manager.current.lock();
        let before = Instant::now();
        let Some((id, started_at, true)) = current.take() else {
            unreachable!()
        };
        tracing::debug!("lock acquired in {:?}", before.elapsed());
        tracing::debug!("transaction finished after {:?}", started_at.elapsed());
        assert_eq!(id, self.id, "multiple write transaction at the same time??");
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
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = self.id))]
    fn close<M: libsql_sys::wal::WalManager<Wal = Sqlite3Wal>>(
        &mut self,
        manager: &M,
        wrapped: &mut Sqlite3Wal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        let before = Instant::now();
        let ret = manager.close(wrapped, db, sync_flags, scratch);
        {
            tracing::debug!(line = line!(), "unparked");
            let mut current = self.manager.current.lock();
            if let Some((id, _, _)) = *current {
                if id == self.id {
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
        }

        self.manager.inner.abort_handle.lock().remove(&self.id);
        tracing::debug!("closed in {:?}", before.elapsed());
        ret
    }
}
