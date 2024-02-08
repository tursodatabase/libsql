#![allow(dead_code)]

use std::num::NonZeroU32;

use libsql_replication::frame::{FrameBorrowed, FrameHeader};
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal};
use libsql_sys::wal::{BusyHandler, CheckpointCallback, Sqlite3WalManager, Wal};
use zerocopy::FromBytes;

use crate::namespace::NamespaceName;
use crate::replication::snapshot_store::{SnapshotBuilder, SnapshotStore};
use crate::replication::FrameNo;

type CompactorWal = WalWrapper<CompactorWrapper, Sqlite3WalManager>;

#[derive(Clone)]
pub struct CompactorWrapper {
    store: SnapshotStore,
    name: NamespaceName,
}

impl CompactorWrapper {
    pub fn new(store: SnapshotStore, name: NamespaceName) -> Self {
        Self { store, name }
    }
}

impl<T: Wal> WrapWal<T> for CompactorWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> libsql_sys::wal::Result<()> {
        struct CompactorCallback<'a> {
            inner: Option<&'a mut dyn CheckpointCallback>,
            builder: Option<SnapshotBuilder>,
            base_frame_no: Option<FrameNo>,
            last_seen: u32,
        }

        impl<'a> CheckpointCallback for CompactorCallback<'a> {
            fn frame(
                &mut self,
                max_safe_frame_no: u32,
                page: &[u8],
                page_no: NonZeroU32,
                frame_no: NonZeroU32,
            ) -> libsql_sys::wal::Result<()> {
                assert!(self.last_seen > frame_no.get());
                self.last_seen = frame_no.get();
                // We retreive the base_replication_index. The first time this method is being
                // called, it must be with page 1, patched with the current replication index,
                // because we just injected it.
                let base_frame_no = match self.base_frame_no {
                    None => {
                        assert_eq!(page_no.get(), 1);
                        // first frame must be newly injected frame , with the final frame_index
                        let header = Sqlite3DbHeader::read_from_prefix(page).unwrap();
                        let base_frame_no = header.replication_index.get() - frame_no.get() as u64;
                        self.base_frame_no = Some(base_frame_no);
                        base_frame_no
                    }
                    Some(frame_no) => frame_no,
                };
                let absolute_frame_no = base_frame_no + frame_no.get() as u64;
                let frame = FrameBorrowed::from_parts(
                    &FrameHeader {
                        checksum: 0.into(), // TODO!: handle checksum
                        frame_no: absolute_frame_no.into(),
                        page_no: page_no.get().into(),
                        size_after: 0.into(),
                    },
                    page,
                );

                self.builder.as_mut().unwrap().add_frame(&frame).unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.frame(max_safe_frame_no, page, page_no, frame_no);
                }

                Ok(())
            }

            fn finish(&mut self) -> libsql_sys::wal::Result<()> {
                self.builder
                    .take()
                    .unwrap()
                    .finish(self.base_frame_no.unwrap() + 1)
                    .unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.finish();
                }

                Ok(())
            }
        }

        wrapped.begin_read_txn()?;
        let db_size = wrapped.db_size();
        wrapped.end_read_txn();

        let builder = self.store.builder(self.name.clone(), db_size).unwrap();
        let mut cb = CompactorCallback {
            inner: checkpoint_cb,
            builder: Some(builder),
            base_frame_no: None,
            last_seen: u32::MAX,
        };

        wrapped.checkpoint(
            db,
            mode,
            busy_handler,
            sync_flags,
            buf,
            Some(&mut cb),
            in_wal,
            backfilled,
        )
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use libsql_sys::wal::Sqlite3WalManager;
    use libsql_sys::wal::WalManager;
    use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_TRUNCATE};
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    use crate::connection::libsql::{open_conn, open_conn_enable_checkpoint};
    use crate::replication::wal::replication_index_injector::ReplicationIndexInjectorWrapper;

    use super::*;

    #[tokio::test]
    async fn compact_wal_simple() {
        let tmp = tempdir().unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(CompactorWrapper::new(store.clone(), name.clone()))
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000, None).unwrap();
        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_TRUNCATE,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }

        assert!(store.find(&name, 1).unwrap().is_some());
        assert!(store.find(&name, 0).unwrap().is_none());
        assert!(store.find(&name, 10).unwrap().is_none());

        let snapshot = store.find_file(&name, 1).await.unwrap().unwrap();
        assert_eq!(snapshot.header().size_after.get(), 2);
        let stream = snapshot.into_stream_mut();

        tokio::pin!(stream);

        let next = stream.next().await.unwrap().unwrap();
        assert_eq!(next.header().frame_no.get(), 6);
        assert_eq!(next.header().page_no.get(), 1);
        let next = stream.next().await.unwrap().unwrap();
        assert_eq!(next.header().frame_no.get(), 5);
        assert_eq!(next.header().page_no.get(), 2);
        assert!(stream.next().await.is_none());

        let tmp2 = tempdir().unwrap();
        let mut db_file = std::fs::File::create(tmp2.path().join("data")).unwrap();
        let snapshot = store.find_file(&name, 1).await.unwrap().unwrap();
        let stream = snapshot.into_stream_mut();

        tokio::pin!(stream);

        db_file
            .write_all(stream.next().await.unwrap().unwrap().page())
            .unwrap();
        db_file
            .write_all(stream.next().await.unwrap().unwrap().page())
            .unwrap();
        db_file.flush().unwrap();

        let conn = open_conn(tmp2.path(), Sqlite3WalManager::default(), None, None).unwrap();

        conn.query_row("select count(*) from test", (), |r| {
            assert_eq!(r.get::<_, usize>(0).unwrap(), 3);
            Ok(())
        })
        .unwrap();
    }
}
