use std::ffi::{c_int, CStr};
use std::sync::Arc;

use bytes::Bytes;
use libsql_sys::wal::Vfs;
use libsql_sys::wal::{BusyHandler, CreateSqlite3Wal, CreateWal, Result, Sqlite3Wal};
use libsql_sys::wal::{PageHeaders, Sqlite3Db, Sqlite3File, UndoHandler};
use rusqlite::ffi::SQLITE_IOERR;

use crate::replication::ReplicationLogger;

use super::logger::WalPage;

#[derive(Clone)]
pub struct CreateReplicationLoggerWal {
    sqlite_create_wal: CreateSqlite3Wal,
    logger: Arc<ReplicationLogger>,
}

impl CreateReplicationLoggerWal {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
        Self {
            sqlite_create_wal: CreateSqlite3Wal::new(),
            logger,
        }
    }
}

impl CreateWal for CreateReplicationLoggerWal {
    type Wal = ReplicationLoggerWal;

    fn use_shared_memory(&self) -> bool {
        self.sqlite_create_wal.use_shared_memory()
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
            .sqlite_create_wal
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
        scratch: &mut [u8],
    ) -> Result<()> {
        self.sqlite_create_wal
            .close(&mut wal.inner, db, sync_flags, scratch)
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        self.sqlite_create_wal.destroy_log(vfs, db_path)
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        self.sqlite_create_wal.log_exists(vfs, db_path)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        self.sqlite_create_wal.destroy()
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

impl libsql_sys::wal::Wal for ReplicationLoggerWal {
    fn limit(&mut self, size: i64) {
        self.inner.limit(size)
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        self.inner.begin_read_txn()
    }

    fn end_read_txn(&mut self) {
        self.inner.end_read_txn()
    }

    fn find_frame(&mut self, page_no: u32) -> Result<u32> {
        self.inner.find_frame(page_no)
    }

    fn read_frame(&mut self, frame_no: u32, buffer: &mut [u8]) -> Result<()> {
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
    ) -> Result<()> {
        assert_eq!(page_size, 4096);
        let iter = unsafe { page_headers.iter() };
        for (page_no, data) in iter {
            self.write_frame(page_no, data);
        }
        if let Err(e) = self.flush(size_after) {
            tracing::error!("error writing to replication log: {e}");
            // returning IO_ERR ensure that xUndo will be called by sqlite.
            return Err(rusqlite::ffi::Error::new(SQLITE_IOERR));
        }

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

            if let Err(e) = self.logger.log_file.write().maybe_compact(
                self.logger.compactor().clone(),
                size_after,
                self.logger.db_path(),
            ) {
                tracing::error!("fatal error: {e}, exiting");
                std::process::abort()
            }
        }

        Ok(())
    }

    fn checkpoint<B: BusyHandler>(
        &mut self,
        db: &mut Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> Result<(u32, u32)> {
        self.inner
            .checkpoint(db, mode, busy_handler, sync_flags, buf)
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

    fn last_fame_index(&self) -> u32 {
        self.inner.last_fame_index()
    }
}
