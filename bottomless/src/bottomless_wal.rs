use std::sync::{Arc, Mutex};

use libsql_sys::ffi::{SQLITE_BUSY, SQLITE_IOERR_WRITE};
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal};
use libsql_sys::wal::{CheckpointMode, Error, Result, Wal};

use crate::replicator::Replicator;

pub type BottomlessWal<T> = WalWrapper<BottomlessWalWrapper, T>;

#[derive(Clone)]
pub struct BottomlessWalWrapper {
    replicator: Arc<Mutex<Option<Replicator>>>,
}

impl BottomlessWalWrapper {
    pub fn new(replicator: Replicator) -> Self {
        Self {
            replicator: Arc::new(Mutex::new(Some(replicator))),
        }
    }

    fn try_with_replicator<Ret>(&self, f: impl FnOnce(&mut Replicator) -> Ret) -> Result<Ret> {
        let mut lock = self.replicator.lock().unwrap();
        match &mut *lock {
            Some(replicator) => Ok(f(replicator)),
            None => Err(Error::new(SQLITE_IOERR_WRITE)),
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
        sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<()> {
        let last_valid_frame = wrapped.last_fame_index();

        wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        self.try_with_replicator(|replicator| {
            if let Err(e) = replicator.set_page_size(page_size as usize) {
                tracing::error!("fatal error during backup: {e}, exiting");
                std::process::abort()
            }
            replicator.register_last_valid_frame(last_valid_frame);
            let new_valid_valid_frame_index = wrapped.last_fame_index();
            replicator.submit_frames(new_valid_valid_frame_index - last_valid_frame);
        })?;

        Ok(())
    }

    fn checkpoint<B: libsql_sys::wal::BusyHandler>(
        &mut self,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> libsql_sys::wal::Result<(u32, u32)> {
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

        let ret = wrapped.checkpoint(db, mode, busy_handler, sync_flags, buf)?;

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
}
