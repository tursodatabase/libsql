#![allow(dead_code)]
use std::sync::Arc;

use libsql_sys::wal::{wrapper::WrapWal, Wal};
use tokio::sync::watch;

use crate::replication::FrameNo;

use super::get_base_frame_no;

#[derive(Clone)]
pub struct FrameNotifier {
    notifier: Arc<watch::Sender<FrameNo>>,
}

impl FrameNotifier {
    pub async fn wait_for(&self, frame: FrameNo) {
        self.notifier
            .subscribe()
            .wait_for(|&current| current >= frame)
            .await
            .unwrap();
    }

    pub fn current(&self) -> FrameNo {
        *self.notifier.borrow()
    }

    pub fn new() -> Self {
        let (sender, _) = watch::channel(0);
        let notifier = Arc::new(sender);
        Self { notifier }
    }

    pub fn watcher(&self) -> watch::Receiver<FrameNo> {
        self.notifier.subscribe()
    }
}

impl<W: Wal> WrapWal<W> for FrameNotifier {
    fn open<M: libsql_sys::wal::WalManager<Wal = W>>(
        &self,
        manager: &M,
        vfs: &mut libsql_sys::wal::Vfs,
        file: &mut libsql_sys::wal::Sqlite3File,
        no_shm_mode: std::ffi::c_int,
        max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> libsql_sys::wal::Result<W> {
        let mut wal = manager.open(vfs, file, no_shm_mode, max_log_size, db_path)?;
        wal.begin_read_txn()?;
        self.notifier.send_if_modified(|current| {
            if *current == 0 {
                let in_wal = wal.frames_in_wal() as FrameNo;
                let base = get_base_frame_no(&mut wal).unwrap();
                *current = base + in_wal;
                true
            } else {
                false
            }
        });

        wal.end_read_txn();

        Ok(wal)
    }

    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<usize> {
        let num_frames =
            wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;
        self.notifier.send_modify(|current| {
            *current += num_frames as FrameNo;
        });

        Ok(num_frames)
    }
}

#[cfg(test)]
mod test {
    use std::os::unix::prelude::FileExt;
    use std::path::PathBuf;

    use libsql_sys::ffi::Sqlite3DbHeader;
    use libsql_sys::wal::{BusyHandler, CheckpointCallback, Sqlite3WalManager, WalManager};
    use rusqlite::ffi::{
        sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL, SQLITE_CHECKPOINT_PASSIVE,
    };
    use tempfile::tempdir;
    use zerocopy::{AsBytes, FromZeroes};

    use crate::connection::libsql::{open_conn, open_conn_enable_checkpoint};
    use crate::replication::wal::replication_index_injector::ReplicationIndexInjectorWrapper;

    use super::*;

    #[tokio::test]
    async fn notified() {
        let tmp = tempdir().unwrap();
        let notifier = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(notifier.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000, None).unwrap();
        assert_eq!(notifier.current(), 0);
        // force wal initialization
        let _ = conn.execute("select * from test", ());
        assert_eq!(notifier.current(), 0);
        conn.execute("create table test (x)", ()).unwrap();
        assert_eq!(notifier.current(), 2);
        conn.execute("insert into test values (123)", ()).unwrap();
        assert_eq!(notifier.current(), 3);

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_PASSIVE,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0)
        }

        assert_eq!(notifier.current(), 4);
        conn.execute("insert into test values (123)", ()).unwrap();
        assert_eq!(notifier.current(), 5);
    }

    #[tokio::test]
    async fn initialize_correctly() {
        let tmp = tempdir().unwrap();
        let notifier = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(notifier.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000, None).unwrap();
        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        assert_eq!(notifier.current(), 4);

        let notifier2 = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(notifier2.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let conn2 = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000, None).unwrap();
        conn2
            .query_row("select count(*) from test", (), |_| Ok(()))
            .unwrap();
        assert_eq!(notifier2.current(), 4);
    }

    #[tokio::test]
    async fn multiple_connections_writing() {
        const COUNT_CONN: usize = 128;
        let tmp = tempdir().unwrap();
        let notifier = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(notifier.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let mut dbs = Vec::new();
        for _ in 0..COUNT_CONN {
            let conn =
                open_conn_enable_checkpoint(tmp.path(), wal_manager.clone(), None, 1000, None).unwrap();
            dbs.push(conn)
        }

        dbs.first()
            .unwrap()
            .execute("create table test (x)", ())
            .unwrap();

        let mut join_set = tokio::task::JoinSet::new();
        for db in dbs {
            join_set.spawn_blocking(move || {
                db.execute("insert into test values (42)", ()).unwrap();
            });
        }

        while join_set.join_next().await.transpose().unwrap().is_some() {}

        let conn =
            open_conn_enable_checkpoint(tmp.path(), wal_manager.clone(), None, 1000, None).unwrap();

        unsafe {
            let mut in_wal = 0;
            let mut backfilled = 0;
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                &mut in_wal,
                &mut backfilled,
            );
            assert_eq!(rc, 0);
            assert_eq!(in_wal, backfilled);
        }

        let file = std::fs::File::open(tmp.path().join("data")).unwrap();
        let mut header = Sqlite3DbHeader::new_zeroed();
        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.replication_index.get(), notifier.current());
    }

    #[tokio::test]
    async fn partial_checkpoint() {
        #[derive(Clone)]
        struct TakeReaderWrapper(PathBuf, FrameNotifier);

        impl<W: Wal> WrapWal<W> for TakeReaderWrapper {
            fn checkpoint(
                &mut self,
                wrapped: &mut W,
                db: &mut libsql_sys::wal::Sqlite3Db,
                mode: libsql_sys::wal::CheckpointMode,
                busy_handler: Option<&mut dyn BusyHandler>,
                sync_flags: u32,
                // temporary scratch buffer
                buf: &mut [u8],
                checkpoint_cb: Option<&mut dyn CheckpointCallback>,
                in_wal: Option<&mut i32>,
                backfilled: Option<&mut i32>,
            ) -> libsql_sys::wal::Result<()> {
                let mut conn = open_conn(&self.0, Sqlite3WalManager::default(), None, None).unwrap();
                // take a read lock
                let txn = conn.transaction().unwrap();
                txn.query_row("select count(*) from test", (), |_| Ok(()))
                    .unwrap();

                let conn = open_conn(
                    &self.0,
                    Sqlite3WalManager::default()
                        .wrap(self.1.clone())
                        .wrap(ReplicationIndexInjectorWrapper),
                    None,
                    None,
                )
                .unwrap();
                // insert stuff into the wal
                conn.execute("insert into test values (12)", ()).unwrap();
                wrapped.checkpoint(
                    db,
                    mode,
                    busy_handler,
                    sync_flags,
                    buf,
                    checkpoint_cb,
                    in_wal,
                    backfilled,
                )
            }
        }
        let tmp = tempdir().unwrap();
        let notifier = FrameNotifier::new();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(TakeReaderWrapper(
                tmp.path().to_path_buf(),
                notifier.clone(),
            ))
            .wrap(notifier.clone())
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000, None).unwrap();

        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        assert_eq!(notifier.current(), 3);

        unsafe {
            let mut in_wal = 0;
            let mut backfilled = 0;
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_PASSIVE,
                &mut in_wal,
                &mut backfilled,
            );
            assert_eq!(rc, 0);
            assert_eq!(in_wal, 5);
            assert_eq!(backfilled, 4);
        }

        assert_eq!(notifier.current(), 5);
        conn.execute("insert into test values (123)", ()).unwrap();
        assert_eq!(notifier.current(), 6);
    }
}
