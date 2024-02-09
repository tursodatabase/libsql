use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::{
    BusyHandler, CheckpointCallback, Result, Sqlite3Wal, Sqlite3WalManager, WalManager,
};
use libsql_sys::wal::{PageHeaders, Sqlite3Db, Sqlite3File, UndoHandler};
use libsql_sys::wal::{Vfs, Wal};
use rusqlite::ffi::{libsql_pghdr, SQLITE_IOERR, SQLITE_SYNC_NORMAL};
use zerocopy::FromBytes;

use crate::replication::ReplicationLogger;
use crate::LIBSQL_PAGE_SIZE;

use super::logger::WalPage;

#[derive(Clone)]
pub struct ReplicationLoggerWalManager {
    sqlite_wal_manager: Sqlite3WalManager,
    logger: Arc<ReplicationLogger>,
}

impl ReplicationLoggerWalManager {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
        Self {
            sqlite_wal_manager: Sqlite3WalManager::new(),
            logger,
        }
    }

    pub fn logger(&self) -> Arc<ReplicationLogger> {
        self.logger.clone()
    }
}

impl WalManager for ReplicationLoggerWalManager {
    type Wal = ReplicationLoggerWal;

    fn use_shared_memory(&self) -> bool {
        self.sqlite_wal_manager.use_shared_memory()
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> Result<Self::Wal> {
        let inner = self
            .sqlite_wal_manager
            .open(vfs, file, no_shm_mode, max_log_size, db_path)?;
        Ok(Self::Wal {
            inner,
            buffer: Default::default(),
            logger: self.logger.clone(),
        })
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        self.sqlite_wal_manager
            .close(&mut wal.inner, db, sync_flags, scratch)
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        self.sqlite_wal_manager.destroy_log(vfs, db_path)
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        self.sqlite_wal_manager.log_exists(vfs, db_path)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        self.sqlite_wal_manager.destroy()
    }
}

pub struct ReplicationLoggerWal {
    inner: Sqlite3Wal,
    buffer: Vec<WalPage>,
    logger: Arc<ReplicationLogger>,
}

impl ReplicationLoggerWal {
    fn write_frame(&mut self, page_no: u32, data: &[u8]) {
        let entry = WalPage {
            page_no,
            size_after: 0,
            data: Bytes::copy_from_slice(data),
        };
        self.buffer.push(entry);
    }

    /// write buffered pages to the logger, without committing.
    fn flush(&mut self, size_after: u32) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.buffer.last_mut().unwrap().size_after = size_after;
            self.logger.write_pages(&self.buffer)?;
            self.buffer.clear();
        }

        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        let new_frame_no = self.logger.commit()?;
        tracing::trace!("new frame committed {new_frame_no:?}");
        self.logger.new_frame_notifier.send_replace(new_frame_no);
        Ok(())
    }

    fn rollback(&mut self) {
        self.logger.log_file.write().rollback();
        self.buffer.clear();
    }

    pub fn logger(&self) -> &ReplicationLogger {
        self.logger.as_ref()
    }
}

impl Wal for ReplicationLoggerWal {
    fn limit(&mut self, size: i64) {
        self.inner.limit(size)
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        self.inner.begin_read_txn()
    }

    fn end_read_txn(&mut self) {
        self.inner.end_read_txn()
    }

    fn find_frame(&mut self, page_no: NonZeroU32) -> Result<Option<NonZeroU32>> {
        self.inner.find_frame(page_no)
    }

    fn read_frame(&mut self, frame_no: NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        self.inner.read_frame(frame_no, buffer)
    }

    fn db_size(&self) -> u32 {
        self.inner.db_size()
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        self.inner.begin_write_txn()
    }

    fn end_write_txn(&mut self) -> Result<()> {
        self.inner.end_write_txn()
    }

    fn undo<U: UndoHandler>(&mut self, undo_handler: Option<&mut U>) -> Result<()> {
        self.rollback();
        self.inner.undo(undo_handler)
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        self.inner.savepoint(rollback_data)
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        self.inner.savepoint_undo(rollback_data)
    }

    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<usize> {
        assert_eq!(page_size, 4096);
        let iter = page_headers.iter();
        for (page_no, data) in iter {
            self.write_frame(page_no, data);
        }
        if let Err(e) = self.flush(size_after) {
            tracing::error!("error writing to replication log: {e}");
            // returning IO_ERR ensure that xUndo will be called by sqlite.
            return Err(rusqlite::ffi::Error::new(SQLITE_IOERR));
        }

        let num_frames =
            self.inner
                .insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        if is_commit {
            if let Err(e) = self.commit() {
                // If we reach this point, it means that we have committed a transaction to sqlite wal,
                // but failed to commit it to the shadow WAL, which leaves us in an inconsistent state.
                tracing::error!(
                    "fatal error: log failed to commit: inconsistent replication log: {e}"
                );
                std::process::abort();
            }

            if let Err(e) = self
                .logger
                .log_file
                .write()
                .maybe_compact(self.logger.compactor().clone(), self.logger.db_path())
            {
                tracing::error!("fatal error: {e}, exiting");
                std::process::abort()
            }
        }

        Ok(num_frames)
    }

    fn checkpoint(
        &mut self,
        db: &mut Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> Result<()> {
        self.inject_replication_index(db)?;
        self.inner.checkpoint(
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

    fn exclusive_mode(&mut self, op: c_int) -> Result<()> {
        self.inner.exclusive_mode(op)
    }

    fn uses_heap_memory(&self) -> bool {
        self.inner.uses_heap_memory()
    }

    fn set_db(&mut self, db: &mut Sqlite3Db) {
        self.inner.set_db(db)
    }

    fn callback(&self) -> i32 {
        self.inner.callback()
    }

    fn frames_in_wal(&self) -> u32 {
        self.inner.frames_in_wal()
    }

    fn db_file(&self) -> &Sqlite3File {
        self.inner.db_file()
    }

    fn backfilled(&self) -> u32 {
        self.inner.backfilled()
    }

    fn frame_page_no(&self, frame_no: NonZeroU32) -> Option<NonZeroU32> {
        self.inner.frame_page_no(frame_no)
    }
}

impl ReplicationLoggerWal {
    fn inject_replication_index(&mut self, _db: &mut Sqlite3Db) -> Result<()> {
        let data = &mut [0; LIBSQL_PAGE_SIZE as _];
        // We retreive the freshest version of page 1. Either most recent page 1 is in the WAL, or
        // it is in the main db file
        match self.find_frame(NonZeroU32::new(1).unwrap())? {
            Some(fno) => {
                self.read_frame(fno, data)?;
            }
            None => {
                self.inner.db_file().read_at(data, 0)?;
            }
        }

        let header = Sqlite3DbHeader::mut_from_prefix(data).expect("invalid database header");
        header.replication_index =
            (self.logger().new_frame_notifier.borrow().unwrap_or(0) + 1).into();
        #[cfg(feature = "encryption")]
        let pager = libsql_sys::connection::leak_pager(_db.as_ptr());
        #[cfg(not(feature = "encryption"))]
        let pager = std::ptr::null_mut();
        let mut header = libsql_pghdr {
            pPage: std::ptr::null_mut(),
            pData: data.as_mut_ptr() as _,
            pExtra: std::ptr::null_mut(),
            pCache: std::ptr::null_mut(),
            pDirty: std::ptr::null_mut(),
            pPager: pager,
            pgno: 1,
            pageHash: 0x02, // DIRTY
            flags: 0,
            nRef: 0,
            pDirtyNext: std::ptr::null_mut(),
            pDirtyPrev: std::ptr::null_mut(),
        };

        let mut headers = unsafe { PageHeaders::from_raw(&mut header) };

        // to retrieve the database size, you must be within a read transaction
        self.begin_read_txn()?;
        let db_size = self.db_size();
        self.end_read_txn();

        self.begin_write_txn()?;
        self.insert_frames(
            LIBSQL_PAGE_SIZE as _,
            &mut headers,
            db_size, // the database doesn't change; there's always a page 1.
            true,
            SQLITE_SYNC_NORMAL, // we'll checkpoint right after, no need for full sync
        )?;
        self.end_write_txn()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use libsql_sys::wal::{
        wrapper::{WalWrapper, WrapWal},
        CheckpointMode,
    };
    use metrics::atomics::AtomicU64;
    use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL};
    use tempfile::tempdir;

    use super::*;

    /// In this test, we will perform a bunch of additions, and then checkpoint. We then check that
    /// the replication index has been flushed to the main db file.
    #[tokio::test]
    async fn check_replication_index() {
        // a wrap wal implementation that catches call to checkpoint, and store the value of the
        // replication index found on page 1 of the main database file.
        #[derive(Clone, Default)]
        struct VerifyReplicationIndex(Arc<AtomicU64>);

        impl WrapWal<ReplicationLoggerWal> for VerifyReplicationIndex {
            fn checkpoint(
                &mut self,
                wrapped: &mut ReplicationLoggerWal,
                db: &mut super::Sqlite3Db,
                mode: CheckpointMode,
                busy_handler: Option<&mut dyn BusyHandler>,
                sync_flags: u32,
                // temporary scratch buffer
                buf: &mut [u8],
                checkpoint_cb: Option<&mut dyn CheckpointCallback>,
                in_wal: Option<&mut i32>,
                backfilled: Option<&mut i32>,
            ) -> libsql_sys::wal::Result<()> {
                wrapped.checkpoint(
                    db,
                    mode,
                    busy_handler,
                    sync_flags,
                    buf,
                    checkpoint_cb,
                    in_wal,
                    backfilled,
                )?;
                let buf = &mut [0; LIBSQL_PAGE_SIZE as _];
                wrapped.inner.db_file().read_at(buf, 0).unwrap();
                let header = Sqlite3DbHeader::mut_from_prefix(buf).unwrap();
                self.0.store(
                    header.replication_index.into(),
                    std::sync::atomic::Ordering::Relaxed,
                );

                Ok(())
            }
        }

        let tmp = tempdir().unwrap();
        let verify_replication_index = VerifyReplicationIndex::default();
        let logger = Arc::new(
            ReplicationLogger::open(
                tmp.path(),
                10000000,
                None,
                false,
                100000,
                None,
                "test".into(),
                None,
            )
            .unwrap(),
        );
        let wal_manager = WalWrapper::new(
            verify_replication_index.clone(),
            ReplicationLoggerWalManager::new(logger.clone()),
        );
        let db = crate::connection::libsql::open_conn_active_checkpoint(
            tmp.path(),
            wal_manager,
            None,
            u32::MAX,
            None,
        )
        .unwrap();

        db.execute("create table test (x)", ()).unwrap();
        for _ in 0..100 {
            db.execute("insert into test values (42)", ()).unwrap();
        }

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                db.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        };

        assert_eq!(
            verify_replication_index
                .0
                .load(std::sync::atomic::Ordering::Relaxed),
            logger.new_frame_notifier.borrow().unwrap()
        );
    }

    #[tokio::test]
    async fn checkpoint_empty_database() {
        let tmp = tempdir().unwrap();
        let logger = Arc::new(
            ReplicationLogger::open(
                tmp.path(),
                10000000,
                None,
                false,
                100000,
                None,
                "test".into(),
                None,
            )
            .unwrap(),
        );

        let wal_manager = ReplicationLoggerWalManager::new(logger.clone());
        let db = crate::connection::libsql::open_conn_active_checkpoint(
            tmp.path(),
            wal_manager,
            None,
            u32::MAX,
            None,
        )
        .unwrap();

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                db.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        };
    }
}
