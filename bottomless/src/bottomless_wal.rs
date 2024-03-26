use std::ffi::c_int;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::completion_progress::SavepointTracker;
use libsql_sys::ffi::{SQLITE_BUSY, SQLITE_IOERR_WRITE};
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal};
use libsql_sys::wal::{
    BusyHandler, CheckpointCallback, CheckpointMode, Error, Result, Sqlite3Db, Wal,
};

use crate::replicator::Replicator;

pub type BottomlessWal<T> = WalWrapper<BottomlessWalWrapper, T>;

#[derive(Clone)]
pub struct BottomlessWalWrapper {
    replicator: Arc<Mutex<Option<Replicator>>>,
}

impl BottomlessWalWrapper {
    pub fn new(replicator: Arc<Mutex<Option<Replicator>>>) -> Self {
        Self { replicator }
    }

    fn try_with_replicator<Ret>(&self, f: impl FnOnce(&mut Replicator) -> Ret) -> Result<Ret> {
        let mut lock = self.replicator.lock().unwrap();
        match &mut *lock {
            Some(replicator) => Ok(f(replicator)),
            None => Err(Error::new(SQLITE_IOERR_WRITE)),
        }
    }

    pub fn backup_savepoint(&self) -> Option<SavepointTracker> {
        let lock = self.replicator.lock().unwrap();
        match &*lock {
            None => None,
            Some(replicator) => Some(replicator.savepoint()),
        }
    }

    pub fn shutdown(&self) -> Option<Replicator> {
        self.replicator.lock().unwrap().take()
    }
}

impl<T: Wal> WrapWal<T> for BottomlessWalWrapper {
    fn savepoint_undo(
        &mut self,
        wrapped: &mut T,
        rollback_data: &mut [u32],
    ) -> libsql_sys::wal::Result<()> {
        wrapped.savepoint_undo(rollback_data)?;

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
        wrapped: &mut T,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<usize> {
        let last_valid_frame = wrapped.frames_in_wal();

        let num_frames =
            wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        self.try_with_replicator(|replicator| {
            if let Err(e) = replicator.set_page_size(page_size as usize) {
                tracing::error!("fatal error during backup: {e}, exiting");
                std::process::abort()
            }
            replicator.register_last_valid_frame(last_valid_frame);
            let new_valid_valid_frame_index = wrapped.frames_in_wal();
            replicator.submit_frames(new_valid_valid_frame_index - last_valid_frame);
        })?;

        Ok(num_frames)
    }

    #[tracing::instrument(skip_all, fields(in_wal = in_wal, backfilled = backfilled))]
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> Result<()> {
        let before = Instant::now();
        {
            tracing::trace!("bottomless checkpoint: {mode:?}");

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
                tracing::debug!("commited after {:?}", before.elapsed());
                if let Err(e) = runtime.block_on(replicator.wait_until_snapshotted()) {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm database snapshot backup: {}",
                        e
                    );
                    return Err(Error::new(SQLITE_IOERR_WRITE));
                }
                tracing::debug!("snapshotted after {:?}", before.elapsed());

                Ok(())
            })??;
        }

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

        tracing::debug!("underlying checkpoint call after {:?}", before.elapsed());

        #[allow(clippy::await_holding_lock)]
        // uncontended -> only gets called under a libSQL write lock
        {
            let runtime = tokio::runtime::Handle::current();
            self.try_with_replicator(|replicator| {
                if let Err(e) = runtime.block_on(async move {
                    replicator.new_generation().await;
                    replicator.snapshot_main_db_file(false).await
                }) {
                    tracing::error!("Failed to snapshot the main db file during checkpoint: {e}");
                    return Err(Error::new(SQLITE_IOERR_WRITE));
                }
                Ok(())
            })??;
        }

        tracing::debug!("checkpoint finnished after {:?}", before.elapsed());

        Ok(())
    }

    fn close<M: libsql_sys::wal::WalManager<Wal = T>>(
        &mut self,
        manager: &M,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        // prevent unmonitored checkpoints
        manager.close(wrapped, db, sync_flags, None)
    }
}
