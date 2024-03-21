use std::sync::Arc;

use bytes::Bytes;
use libsql_sys::wal::wrapper::WrapWal;
use libsql_sys::wal::Wal;
use rusqlite::ffi::SQLITE_IOERR;

use crate::replication::ReplicationLogger;

use super::logger::WalPage;

pub struct ReplicationLoggerWalWrapper {
    logger: Arc<ReplicationLogger>,
    buffer: Vec<WalPage>,
}

impl Clone for ReplicationLoggerWalWrapper {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            buffer: Vec::new(),
        }
    }
}

impl<W: Wal> WrapWal<W> for ReplicationLoggerWalWrapper {
    fn undo<U: libsql_sys::wal::UndoHandler>(
        &mut self,
        wrapped: &mut W,
        handler: Option<&mut U>,
    ) -> libsql_sys::wal::Result<()> {
        self.rollback();
        wrapped.undo(handler)
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
            wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

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
}

impl ReplicationLoggerWalWrapper {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
        Self {
            logger,
            buffer: Vec::new(),
        }
    }

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

    pub(crate) fn logger(&self) -> Arc<ReplicationLogger> {
        self.logger.clone()
    }
}

#[cfg(test)]
mod test {
    use libsql_sys::wal::{Sqlite3WalManager, WalManager};
    use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL};
    use tempfile::tempdir;

    use super::*;

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

        let wal_manager = ReplicationLoggerWalWrapper::new(logger.clone());
        let db = crate::connection::libsql::open_conn_active_checkpoint(
            tmp.path(),
            Sqlite3WalManager::default().wrap(wal_manager),
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
