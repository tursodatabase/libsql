use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use libsql_sys::ffi::{SQLITE_BUSY, SQLITE_IOERR_WRITE};
use libsql_sys::wal::{
    CheckpointMode, Error, PageHeaders, Result, Sqlite3Db, Sqlite3File, UndoHandler, Vfs, Wal,
    WalManager,
};

use crate::replicator::Replicator;

#[derive(Clone)]
pub struct CreateBottomlessWal<T> {
    inner: T,
    replicator: Arc<Mutex<Option<Replicator>>>,
}

impl<T> CreateBottomlessWal<T> {
    pub fn new(inner: T, replicator: Replicator) -> Self {
        Self {
            inner,
            replicator: Arc::new(Mutex::new(Some(replicator))),
        }
    }

    pub fn shutdown(&self) -> Option<Replicator> {
        self.replicator.lock().unwrap().take()
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: WalManager> WalManager for CreateBottomlessWal<T> {
    type Wal = BottomlessWal<T::Wal>;

    fn use_shared_memory(&self) -> bool {
        self.inner.use_shared_memory()
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
            .inner
            .open(vfs, file, no_shm_mode, max_log_size, db_path)?;
        Ok(Self::Wal {
            inner,
            replicator: self.replicator.clone(),
        })
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        self.inner.close(&mut wal.inner, db, sync_flags, scratch)
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        self.inner.destroy_log(vfs, db_path)
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        self.inner.log_exists(vfs, db_path)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        self.inner.destroy()
    }
}

pub struct BottomlessWal<T> {
    inner: T,
    replicator: Arc<Mutex<Option<Replicator>>>,
}

impl<T> BottomlessWal<T> {
    fn try_with_replicator<Ret>(&self, f: impl FnOnce(&mut Replicator) -> Ret) -> Result<Ret> {
        let mut lock = self.replicator.lock().unwrap();
        match &mut *lock {
            Some(replicator) => Ok(f(replicator)),
            None => Err(Error::new(SQLITE_IOERR_WRITE)),
        }
    }
}

impl<T: Wal> Wal for BottomlessWal<T> {
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
        self.inner.undo(undo_handler)
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        self.inner.savepoint(rollback_data)
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        self.inner.savepoint_undo(rollback_data)?;

        {
            let last_valid_frame = rollback_data[0];
            self.try_with_replicator(|replicator| {
                let prev_valid_frame = replicator.peek_last_valid_frame();
                tracing::trace!(
                    "Savepoint: rolling back from frame {prev_valid_frame} to {last_valid_frame}",
                );
            })?;
        }

        Ok(())
    }

    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<()> {
        let last_valid_frame = self.inner.last_fame_index();

        self.inner
            .insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        self.try_with_replicator(|replicator| {
            if let Err(e) = replicator.set_page_size(page_size as usize) {
                tracing::error!("fatal error during backup: {e}, exiting");
                std::process::abort()
            }
            replicator.register_last_valid_frame(last_valid_frame);
            let new_valid_valid_frame_index = self.inner.last_fame_index();
            replicator.submit_frames(new_valid_valid_frame_index - last_valid_frame);
        })?;

        Ok(())
    }

    fn checkpoint<B: libsql_sys::wal::BusyHandler>(
        &mut self,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> Result<(u32, u32)> {
        {
            tracing::trace!("bottomless checkpoint");

            /* In order to avoid partial checkpoints, passive checkpoint
             ** mode is not allowed. Only TRUNCATE checkpoints are accepted,
             ** because these are guaranteed to block writes, copy all WAL pages
             ** back into the main database file and reset the frame number.
             ** In order to avoid autocheckpoint on close (that's too often),
             ** checkpoint attempts weaker than TRUNCATE are ignored.
             */
            if mode < CheckpointMode::Truncate {
                tracing::trace!("Ignoring a checkpoint request weaker than TRUNCATE: {mode:?}");
                // Return an error to signal to sqlite that the WAL was not checkpointed, and it is
                // therefore not safe to delete it.
                return Err(Error::new(SQLITE_BUSY));
            }
        }

        #[allow(clippy::await_holding_lock)]
        // uncontended -> only gets called under a libSQL write lock
        {
            let runtime = tokio::runtime::Handle::current();
            self.try_with_replicator(|replicator| {
                let last_known_frame = replicator.last_known_frame();
                replicator.request_flush();
                if last_known_frame == 0 {
                    tracing::debug!("No committed changes in this generation, not snapshotting");
                    replicator.skip_snapshot_for_current_generation();
                    return Err(Error::new(SQLITE_BUSY));
                }
                if let Err(e) = runtime.block_on(replicator.wait_until_committed(last_known_frame))
                {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm {} frames backup: {}",
                        last_known_frame,
                        e
                    );
                    return Err(Error::new(SQLITE_IOERR_WRITE));
                }
                if let Err(e) = runtime.block_on(replicator.wait_until_snapshotted()) {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm database snapshot backup: {}",
                        e
                    );
                    return Err(Error::new(SQLITE_IOERR_WRITE));
                }

                Ok(())
            })??;
        }

        let ret = self
            .inner
            .checkpoint(db, mode, busy_handler, sync_flags, buf)?;

        #[allow(clippy::await_holding_lock)]
        // uncontended -> only gets called under a libSQL write lock
        {
            let runtime = tokio::runtime::Handle::current();
            self.try_with_replicator(|replicator| {
                let _prev = replicator.new_generation();
                if let Err(e) =
                    runtime.block_on(async move { replicator.snapshot_main_db_file().await })
                {
                    tracing::error!("Failed to snapshot the main db file during checkpoint: {e}");
                    return Err(Error::new(SQLITE_IOERR_WRITE));
                }
                Ok(())
            })??;
        }

        Ok(ret)
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
