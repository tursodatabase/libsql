use std::sync::Arc;

use fst::Streamer;
use libsql_sys::wal::{Wal, WalManager};

use crate::name::NamespaceName;
use crate::registry::WalRegistry;
use crate::shared_wal::SharedWal;
use crate::transaction::Transaction;

pub struct LibsqlWalManager {
    pub registry: Arc<WalRegistry>,
    pub name: NamespaceName,
}

pub struct LibsqlWal {
    last_read_frame_no: Option<u64>,
    tx: Option<Transaction>,
    shared: Arc<SharedWal>,
}

impl WalManager for LibsqlWalManager {
    type Wal = LibsqlWal;

    fn use_shared_memory(&self) -> bool {
        false
    }

    fn open(
        &self,
        _vfs: &mut libsql_sys::wal::Vfs,
        file: &mut libsql_sys::wal::Sqlite3File,
        _no_shm_mode: std::ffi::c_int,
        _max_log_size: i64,
        _db_path: &std::ffi::CStr,
    ) -> libsql_sys::wal::Result<Self::Wal> {
        let shared = self.registry.open(self.name.clone(), file);
        Ok(LibsqlWal {
            last_read_frame_no: None,
            tx: None,
            shared,
        })
    }

    fn close(
        &self,
        _wal: &mut Self::Wal,
        _db: &mut libsql_sys::wal::Sqlite3Db,
        _sync_flags: std::ffi::c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        Ok(())
    }

    fn destroy_log(
        &self,
        _vfs: &mut libsql_sys::wal::Vfs,
        _db_path: &std::ffi::CStr,
    ) -> libsql_sys::wal::Result<()> {
        Ok(())
    }

    fn log_exists(
        &self,
        _vfs: &mut libsql_sys::wal::Vfs,
        _db_path: &std::ffi::CStr,
    ) -> libsql_sys::wal::Result<bool> {
        Ok(true)
    }

    fn destroy(self)
    where
        Self: Sized,
    { }
}

impl Wal for LibsqlWal {
    #[tracing::instrument(skip_all)]
    fn limit(&mut self, _size: i64) {}

    #[tracing::instrument(skip_all)]
    fn begin_read_txn(&mut self) -> libsql_sys::wal::Result<bool> {
        tracing::trace!("begin read");
        let tx = self.shared.begin_read();
        let invalidate_cache = self
            .last_read_frame_no
            .map(|idx| tx.max_frame_no != idx)
            .unwrap_or(false);
        self.tx = Some(Transaction::Read(tx));

        tracing::debug!(invalidate_cache, "read started");
        Ok(invalidate_cache)
    }

    #[tracing::instrument(skip_all)]
    fn end_read_txn(&mut self) {
        let tx = match self.tx.take() {
            Some(Transaction::Read(tx)) => tx,
            Some(Transaction::Write(tx)) => tx.downgrade(),
            None => return,
        };

        tracing::trace!("end read tx");

        self.last_read_frame_no = Some(tx.max_frame_no);
    }

    #[tracing::instrument(skip_all)]
    fn find_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> libsql_sys::wal::Result<Option<std::num::NonZeroU32>> {
        tracing::trace!(page_no, "find frame");
        // this is a trick: we defer the frame read to the `read_frame` method. The read_frame
        // method will read from the journal if the page exist, or from the db_file if it doesn't
        Ok(Some(page_no))
    }

    #[tracing::instrument(skip_all)]
    fn read_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
        buffer: &mut [u8],
    ) -> libsql_sys::wal::Result<()> {
        tracing::trace!(page_no, "reading frame");
        let tx = self.tx.as_ref().unwrap();
        self.shared.read_frame(tx, page_no.get(), buffer);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn db_size(&self) -> u32 {
        let db_size = self.shared.db_size();
        tracing::trace!(db_size, "db_size");
        db_size
    }

    #[tracing::instrument(skip_all)]
    fn begin_write_txn(&mut self) -> libsql_sys::wal::Result<()> {
        tracing::trace!("begin write");
        match self.tx.take() {
            Some(Transaction::Read(tx)) => {
                let tx = self.shared.upgrade(tx);
                tracing::debug!("write lock acquired");
                self.tx = Some(Transaction::Write(tx));
            }
            other => {
                self.tx = other;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn end_write_txn(&mut self) -> libsql_sys::wal::Result<()> {
        tracing::trace!("end write");
        match self.tx.take() {
            Some(Transaction::Write(tx)) => {
                self.tx = Some(Transaction::Read(tx.downgrade()));
            },
            other => {
                self.tx = other;
            },
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn undo<U: libsql_sys::wal::UndoHandler>(
        &mut self,
        handler: Option<&mut U>,
    ) -> libsql_sys::wal::Result<()> {
        match self.tx {
            Some(Transaction::Write(ref mut tx)) => {
                assert!(!tx.is_commited());
                if let Some((handler, index)) = handler.zip(tx.index.as_ref()) {
                    let mut keys = index.keys();
                    while let Some(key) = keys.next() {
                        let page_no = u32::from_be_bytes(key.try_into().unwrap());
                        if let Err(e) = handler.handle_undo(page_no) {
                            tracing::debug!("undo handler error: {e}");
                            break
                        }
                    }
                }
                
                self.shared.reset_tx(tx);

                tracing::debug!("rolled back tx");

                Ok(())
            },
            _ => Ok(())
        }
    }

    #[tracing::instrument(skip_all)]
    fn savepoint(&mut self, _rollback_data: &mut [u32]) {
        todo!()
    }

    #[tracing::instrument(skip_all)]
    fn savepoint_undo(&mut self, _rollback_data: &mut [u32]) -> libsql_sys::wal::Result<()> {
        todo!()
    }

    #[tracing::instrument(skip_all)]
    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        _is_commit: bool,
        _sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<usize> {
        assert_eq!(page_size, 4096);
        match self.tx.as_mut() {
            Some(Transaction::Write(ref mut tx)) => {
                self.shared.insert_frames(tx, page_headers, size_after);
            }
            _ => todo!("no write transaction"),
        }
        Ok(0)
    }

    #[tracing::instrument(skip_all)]
    fn checkpoint(
        &mut self,
        _db: &mut libsql_sys::wal::Sqlite3Db,
        _mode: libsql_sys::wal::CheckpointMode,
        _busy_handler: Option<&mut dyn libsql_sys::wal::BusyHandler>,
        _sync_flags: u32,
        _buf: &mut [u8],
        _checkpoint_cb: Option<&mut dyn libsql_sys::wal::CheckpointCallback>,
        _in_wal: Option<&mut i32>,
        _backfilled: Option<&mut i32>,
    ) -> libsql_sys::wal::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn exclusive_mode(&mut self, op: std::ffi::c_int) -> libsql_sys::wal::Result<()> {
        tracing::trace!(op, "trying to acquire exclusive mode");
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn uses_heap_memory(&self) -> bool {
        true
    }

    #[tracing::instrument(skip_all)]
    fn set_db(&mut self, _db: &mut libsql_sys::wal::Sqlite3Db) { }

    #[tracing::instrument(skip_all)]
    fn callback(&self) -> i32 {
        0
    }

    #[tracing::instrument(skip_all)]
    fn frames_in_wal(&self) -> u32 {
        0
    }
}
