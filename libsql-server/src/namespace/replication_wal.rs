use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;
use std::sync::Arc;

use bottomless::{
    bottomless_wal::{BottomlessWal, CreateBottomlessWal},
    replicator::Replicator,
};
use libsql_sys::wal::{
    BusyHandler, CheckpointMode, PageHeaders, Result, Sqlite3Db, Sqlite3File, UndoHandler, Vfs,
    Wal, WalManager,
};

use crate::replication::{
    primary::replication_logger_wal::{ReplicationLoggerWal, ReplicationLoggerWalManager},
    ReplicationLogger,
};

/// Depending on the configuration, we use different backends for replication. This WalManager
/// implementation allows runtime selection of the backend.
#[derive(Clone)]
pub enum ReplicationWalManager {
    Bottomless(CreateBottomlessWal<ReplicationLoggerWalManager>),
    Logger(ReplicationLoggerWalManager),
}

impl ReplicationWalManager {
    pub fn shutdown(&self) -> Option<Replicator> {
        match self {
            ReplicationWalManager::Bottomless(bottomless) => bottomless.shutdown(),
            ReplicationWalManager::Logger(_) => None,
        }
    }

    pub fn logger(&self) -> Arc<ReplicationLogger> {
        match self {
            ReplicationWalManager::Bottomless(bottomless) => bottomless.inner().logger(),
            ReplicationWalManager::Logger(wal) => wal.logger(),
        }
    }
}

impl WalManager for ReplicationWalManager {
    type Wal = ReplicationWal;

    fn use_shared_memory(&self) -> bool {
        match self {
            ReplicationWalManager::Bottomless(inner) => inner.use_shared_memory(),
            ReplicationWalManager::Logger(inner) => inner.use_shared_memory(),
        }
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> Result<Self::Wal> {
        match self {
            ReplicationWalManager::Bottomless(inner) => inner
                .open(vfs, file, no_shm_mode, max_log_size, db_path)
                .map(ReplicationWal::Bottomless),
            ReplicationWalManager::Logger(inner) => inner
                .open(vfs, file, no_shm_mode, max_log_size, db_path)
                .map(ReplicationWal::Logger),
        }
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        match (self, wal) {
            (ReplicationWalManager::Bottomless(inner), ReplicationWal::Bottomless(wal)) => {
                inner.close(wal, db, sync_flags, scratch)
            }
            (ReplicationWalManager::Logger(inner), ReplicationWal::Logger(wal)) => {
                inner.close(wal, db, sync_flags, scratch)
            }
            _ => unreachable!(),
        }
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        match self {
            ReplicationWalManager::Bottomless(inner) => inner.destroy_log(vfs, db_path),
            ReplicationWalManager::Logger(inner) => inner.destroy_log(vfs, db_path),
        }
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        match self {
            ReplicationWalManager::Bottomless(inner) => inner.log_exists(vfs, db_path),
            ReplicationWalManager::Logger(inner) => inner.log_exists(vfs, db_path),
        }
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        match self {
            ReplicationWalManager::Bottomless(inner) => inner.destroy(),
            ReplicationWalManager::Logger(inner) => inner.destroy(),
        }
    }
}

pub enum ReplicationWal {
    Bottomless(BottomlessWal<ReplicationLoggerWal>),
    Logger(ReplicationLoggerWal),
}

impl Wal for ReplicationWal {
    fn limit(&mut self, size: i64) {
        match self {
            ReplicationWal::Bottomless(inner) => inner.limit(size),
            ReplicationWal::Logger(inner) => inner.limit(size),
        }
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.begin_read_txn(),
            ReplicationWal::Logger(inner) => inner.begin_read_txn(),
        }
    }

    fn end_read_txn(&mut self) {
        match self {
            ReplicationWal::Bottomless(inner) => inner.end_read_txn(),
            ReplicationWal::Logger(inner) => inner.end_read_txn(),
        }
    }

    fn find_frame(&mut self, page_no: NonZeroU32) -> Result<Option<NonZeroU32>> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.find_frame(page_no),
            ReplicationWal::Logger(inner) => inner.find_frame(page_no),
        }
    }

    fn read_frame(&mut self, frame_no: NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.read_frame(frame_no, buffer),
            ReplicationWal::Logger(inner) => inner.read_frame(frame_no, buffer),
        }
    }

    fn db_size(&self) -> u32 {
        match self {
            ReplicationWal::Bottomless(inner) => inner.db_size(),
            ReplicationWal::Logger(inner) => inner.db_size(),
        }
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.begin_write_txn(),
            ReplicationWal::Logger(inner) => inner.begin_write_txn(),
        }
    }

    fn end_write_txn(&mut self) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.end_write_txn(),
            ReplicationWal::Logger(inner) => inner.end_write_txn(),
        }
    }

    fn undo<U: UndoHandler>(&mut self, undo_handler: Option<&mut U>) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.undo(undo_handler),
            ReplicationWal::Logger(inner) => inner.undo(undo_handler),
        }
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        match self {
            ReplicationWal::Bottomless(inner) => inner.savepoint(rollback_data),
            ReplicationWal::Logger(inner) => inner.savepoint(rollback_data),
        }
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.savepoint_undo(rollback_data),
            ReplicationWal::Logger(inner) => inner.savepoint_undo(rollback_data),
        }
    }

    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => {
                inner.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
            }
            ReplicationWal::Logger(inner) => {
                inner.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
            }
        }
    }

    fn checkpoint<B: BusyHandler>(
        &mut self,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        buf: &mut [u8],
    ) -> Result<(u32, u32)> {
        match self {
            ReplicationWal::Bottomless(inner) => {
                inner.checkpoint(db, mode, busy_handler, sync_flags, buf)
            }
            ReplicationWal::Logger(inner) => {
                inner.checkpoint(db, mode, busy_handler, sync_flags, buf)
            }
        }
    }

    fn exclusive_mode(&mut self, op: c_int) -> Result<()> {
        match self {
            ReplicationWal::Bottomless(inner) => inner.exclusive_mode(op),
            ReplicationWal::Logger(inner) => inner.exclusive_mode(op),
        }
    }

    fn uses_heap_memory(&self) -> bool {
        match self {
            ReplicationWal::Bottomless(inner) => inner.uses_heap_memory(),
            ReplicationWal::Logger(inner) => inner.uses_heap_memory(),
        }
    }

    fn set_db(&mut self, db: &mut Sqlite3Db) {
        match self {
            ReplicationWal::Bottomless(inner) => inner.set_db(db),
            ReplicationWal::Logger(inner) => inner.set_db(db),
        }
    }

    fn callback(&self) -> i32 {
        match self {
            ReplicationWal::Bottomless(inner) => inner.callback(),
            ReplicationWal::Logger(inner) => inner.callback(),
        }
    }

    fn last_fame_index(&self) -> u32 {
        match self {
            ReplicationWal::Bottomless(inner) => inner.last_fame_index(),
            ReplicationWal::Logger(inner) => inner.last_fame_index(),
        }
    }
}
