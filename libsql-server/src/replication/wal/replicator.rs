#![allow(dead_code)]

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use libsql_replication::frame::{Frame, FrameBorrowed, FrameMut};
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal, WrappedWal};
use libsql_sys::wal::{
    BusyHandler, CheckpointCallback, CheckpointMode, Sqlite3Db, Sqlite3Wal, Sqlite3WalManager, Wal,
};
use metrics::atomics::AtomicU64;
use parking_lot::{Mutex, RwLock};
use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL};
use tokio_util::sync::ReusableBoxFuture;
use zerocopy::FromZeroes;

use crate::connection::libsql::open_conn_enable_checkpoint;
use crate::namespace::NamespaceName;
use crate::replication::snapshot_store::SnapshotStore;
use crate::replication::FrameNo;
use crate::BLOCKING_RT;

use super::frame_notifier::FrameNotifier;
use super::get_base_frame_no;

type Result<T, E = Error> = std::result::Result<T, E>;

const SQLD_LOG_OK: i32 = 200;
const SQLD_NEED_SNAPSHOT: i32 = 201;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("need snapshot")]
    NeedSnaphot,
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("snapshot store error: {0}")]
    SnapshotStore(#[from] super::super::snapshot_store::Error),
    #[error("snapshot error: {0}")]
    Snapshot(#[from] libsql_replication::snapshot::Error),
    #[error("snapshot not found for frame {0}")]
    SnapshotNotFound(FrameNo),
}

type ReplicatorWal = WrappedWal<ReplicatorWrapper, Sqlite3Wal>;

#[derive(Clone)]
struct ReplicatorWrapper {
    out_sink: tokio::sync::mpsc::Sender<Frame>,
    next_frame_no: Arc<AtomicU64>,
    commit_indexes: Arc<RwLock<HashMap<u32, u32>>>,
}

impl ReplicatorWrapper {
    fn try_replicate<W: Wal>(&mut self, wal: &mut W) -> libsql_sys::wal::Result<()> {
        let base_replication_index = get_base_frame_no(wal)?;
        if self.next_frame_no.load(Ordering::Relaxed) <= base_replication_index {
            return Err(rusqlite::ffi::Error::new(SQLD_NEED_SNAPSHOT));
        } else {
            let last_frame_in_wal = wal.frames_in_wal();
            let start_frame_no =
                (self.next_frame_no.load(Ordering::Relaxed) - base_replication_index as u64) as u32;
            for i in start_frame_no..=last_frame_in_wal {
                let mut frame = FrameBorrowed::new_box_zeroed();
                match wal.read_frame(NonZeroU32::new(i).unwrap(), frame.page_mut()) {
                    Ok(()) => {
                        let frame_no = base_replication_index + i as u64;
                        frame.header_mut().frame_no = frame_no.into();
                        frame.header_mut().page_no = wal
                            .frame_page_no(NonZeroU32::new(i).unwrap())
                            .unwrap()
                            .get()
                            .into();
                        self.next_frame_no.store(frame_no + 1, Ordering::Relaxed);
                        if let Some(&size_after) = self.commit_indexes.read().get(&i) {
                            frame.header_mut().size_after = size_after.into();
                        }
                        let frame = FrameMut::from(frame);
                        self.out_sink.blocking_send(frame.into()).unwrap();
                    }
                    Err(_) => todo!(),
                }
            }
        }

        Err(rusqlite::ffi::Error::new(SQLD_LOG_OK))
    }
}

/// What should the replicator do when it reaches the end of the log
#[derive(Clone)]
pub enum ReplicationBehavior {
    WaitForFrame { notifier: FrameNotifier },
    Exit,
}

impl<T: Wal> WrapWal<T> for ReplicatorWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        _db: &mut Sqlite3Db,
        _mode: CheckpointMode,
        _busy_handler: Option<&mut dyn BusyHandler>,
        _sync_flags: u32,
        _buf: &mut [u8],
        _checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        _in_wal: Option<&mut i32>,
        _backfilled: Option<&mut i32>,
    ) -> libsql_sys::wal::Result<()> {
        wrapped.begin_read_txn()?;
        let ret = self.try_replicate(wrapped);
        wrapped.end_read_txn();
        ret
    }

    fn close<M: libsql_sys::wal::WalManager<Wal = T>>(
        &mut self,
        manager: &M,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        // prevent checkpoint on close
        manager.close(wrapped, db, sync_flags, None)
    }
}

pub struct Replicator {
    conn: Arc<Mutex<libsql_sys::Connection<ReplicatorWal>>>,
    receiver: tokio::sync::mpsc::Receiver<Frame>,
    store: SnapshotStore,
    namespace: NamespaceName,
    next_frame_no: Arc<AtomicU64>,
    replication_behavior: ReplicationBehavior,
}

impl Replicator {
    pub fn new(
        db_path: &Path,
        next_frame_no: FrameNo,
        namespace: NamespaceName,
        store: SnapshotStore,
        replication_behavior: ReplicationBehavior,
        commit_indexes: Arc<RwLock<HashMap<u32, u32>>>,
        encryption_key: Option<Bytes>,
    ) -> crate::Result<Self> {
        // Replication starts at 1, but legacy may ask for 0, so we interpret index 0 as 1.
        let next_frame_no = next_frame_no.max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let next_frame_no = Arc::new(AtomicU64::new(next_frame_no));
        let wal_manager = WalWrapper::new(
            ReplicatorWrapper {
                next_frame_no: next_frame_no.clone(),
                out_sink: sender,
                commit_indexes,
            },
            Sqlite3WalManager::default(),
        );

        let conn = open_conn_enable_checkpoint(db_path, wal_manager, None, u32::MAX, encryption_key)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            receiver,
            store,
            namespace,
            next_frame_no,
            replication_behavior,
        })
    }

    fn next_frame_no(&self) -> FrameNo {
        self.next_frame_no
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn stream_snapshot(&mut self) -> Result<impl Stream<Item = Result<Frame>>> {
        let Some(file) = self
            .store
            .find_file(&self.namespace, self.next_frame_no())
            .await?
        else {
            return Err(Error::SnapshotNotFound(self.next_frame_no()));
        };
        let size_after = file.header().size_after;
        let next_frame_no = self.next_frame_no();
        let next_frame_no_ref = self.next_frame_no.clone();
        Ok(async_stream::try_stream! {
            let mut attempted_frame_no = None;
            let stream = file.into_stream_mut().peekable();
            tokio::pin!(stream);
            while let Some(frame) = stream.as_mut().next().await {
                let mut frame = frame?;
                if attempted_frame_no.is_none() {
                    attempted_frame_no = Some(frame.header().frame_no.get());
                }
                if frame.header().frame_no.get() <= next_frame_no || stream.as_mut().peek().await.is_none() {
                    frame.header_mut().size_after = size_after;
                }
                yield frame.into()
            }

            if let Some(frame_no) = attempted_frame_no {
                next_frame_no_ref.store(frame_no + 1, Ordering::SeqCst);
            }
        })
    }

    pub fn stream_frames(&mut self) -> impl Stream<Item = Result<Frame>> + '_ {
        let mut fut = ReusableBoxFuture::new(std::future::poll_fn(|_| Poll::Pending));
        async_stream::stream! {
            loop {
                let conn = self.conn.clone();
                fut.set(async move {
                    BLOCKING_RT.spawn_blocking(move || {
                        let conn = conn.lock();
                        // force wal openning
                        unsafe {
                            let rc = sqlite3_wal_checkpoint_v2(
                                conn.handle(),
                                std::ptr::null_mut(),
                                SQLITE_CHECKPOINT_FULL,
                                std::ptr::null_mut(),
                                std::ptr::null_mut(),
                            );
                            match rc {
                                SQLD_NEED_SNAPSHOT => Err(Error::NeedSnaphot),
                                SQLD_LOG_OK => Ok(()),
                                _ => todo!(),
                            }
                        }
                    }).await.unwrap()
                });

                loop {
                    tokio::select! {
                        ret = &mut fut => {
                            // drain remaining frames
                            while let Ok(frame) = self.receiver.try_recv() {
                                yield Ok(frame);
                            }

                            match ret.map(|_| &self.replication_behavior) {
                                Ok(ReplicationBehavior::Exit)  => return (), // no more frames
                                Ok(ReplicationBehavior::WaitForFrame { notifier }) => {
                                    notifier.wait_for(self.next_frame_no()).await;
                                    break
                                },
                                Err(e) => {
                                    match e {
                                        Error::NeedSnaphot => {
                                            let stream = match self.stream_snapshot().await {
                                                Ok(stream) => stream,
                                                Err(e) => {
                                                    yield Err(e);
                                                    return
                                                },
                                            };
                                            tokio::pin!(stream);
                                            while let Some(frame) = stream.next().await {
                                                yield frame;
                                            }
                                        },
                                        e => yield Err(e),
                                    }
                                }
                            }

                            // caught up with snapshot, try to replicate from wal again.
                            break
                        }
                        Some(frame) = self.receiver.recv() => {
                            yield Ok(frame)
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use libsql_sys::wal::WalManager;
    use rusqlite::ffi::SQLITE_CHECKPOINT_TRUNCATE;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    use crate::connection::libsql::open_conn;
    use crate::replication::wal::frame_notifier::FrameNotifier;
    use crate::replication::wal::replication_index_injector::ReplicationIndexInjectorWrapper;

    use super::*;

    #[tokio::test]
    async fn basic_stream_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_manager = Sqlite3WalManager::default().wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn(tmp.path(), wal_manager, None, None).unwrap();

        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();

        let mut replicator = Replicator::new(
            tmp.path(),
            1,
            name,
            store,
            ReplicationBehavior::Exit,
            Default::default(),
            None,
        )
        .unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            1
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            2
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            3
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn stream_frame_after_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_manager = Sqlite3WalManager::default().wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, u32::MAX, None).unwrap();
        conn.busy_timeout(std::time::Duration::from_millis(100))
            .unwrap();

        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        let name = NamespaceName::from_string("test".into()).unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let mut replicator = Replicator::new(
            tmp.path(),
            1,
            name,
            store,
            ReplicationBehavior::Exit,
            Default::default(),
            None,
        )
        .unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            1
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            2
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            3
        );
        assert!(stream.next().await.is_none());

        unsafe {
            let mut in_log = 0;
            let mut backfilled = 0;
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_TRUNCATE,
                &mut in_log,
                &mut backfilled,
            );
            assert_eq!(rc, 0);
            // all frames were backfilled
            assert_eq!(in_log, 0);
            assert_eq!(backfilled, 0);
        }

        conn.execute("insert into test values (123)", ()).unwrap();

        let name = NamespaceName::from_string("test".into()).unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let mut replicator = Replicator::new(
            tmp.path(),
            0,
            name,
            store,
            ReplicationBehavior::Exit,
            Default::default(),
            None,
        )
        .unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(Error::SnapshotNotFound(_))
        ));

        // frame 4 is the injected frame_no frame, it's part of the snapshot
        let name = NamespaceName::from_string("test".into()).unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let mut replicator = Replicator::new(
            tmp.path(),
            5,
            name,
            store,
            ReplicationBehavior::Exit,
            Default::default(),
            None,
        )
        .unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            5
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn new_frame_notified() {
        let tmp = tempfile::tempdir().unwrap();
        let notifier = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(notifier.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, u32::MAX, None).unwrap();

        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let mut replicator = Replicator::new(
            tmp.path(),
            1,
            name,
            store,
            ReplicationBehavior::WaitForFrame { notifier },
            Default::default(),
            None,
        )
        .unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        let ret = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(ret.is_err());

        conn.execute("create table test (x)", ()).unwrap();

        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            1
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            2
        );
        let ret = timeout(Duration::from_millis(100), stream.next()).await;
        assert!(ret.is_err());
    }
}
