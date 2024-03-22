use super::{Wal, WalManager};
#[derive(Debug, Clone)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Wal for Either<L, R>
where
    L: Wal,
    R: Wal,
{
    fn limit(&mut self, size: i64) {
        match self {
            Either::Left(l) => l.limit(size),
            Either::Right(r) => r.limit(size),
        }
    }

    fn begin_read_txn(&mut self) -> super::Result<bool> {
        match self {
            Either::Left(l) => l.begin_read_txn(),
            Either::Right(r) => r.begin_read_txn(),
        }
    }

    fn end_read_txn(&mut self) {
        match self {
            Either::Left(l) => l.end_read_txn(),
            Either::Right(r) => r.end_read_txn(),
        }
    }

    fn find_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> super::Result<Option<std::num::NonZeroU32>> {
        match self {
            Either::Left(l) => l.find_frame(page_no),
            Either::Right(r) => r.find_frame(page_no),
        }
    }

    fn read_frame(
        &mut self,
        frame_no: std::num::NonZeroU32,
        buffer: &mut [u8],
    ) -> super::Result<()> {
        match self {
            Either::Left(l) => l.read_frame(frame_no, buffer),
            Either::Right(r) => r.read_frame(frame_no, buffer),
        }
    }

    fn db_size(&self) -> u32 {
        match self {
            Either::Left(l) => l.db_size(),
            Either::Right(r) => r.db_size(),
        }
    }

    fn begin_write_txn(&mut self) -> super::Result<()> {
        match self {
            Either::Left(l) => l.begin_write_txn(),
            Either::Right(r) => r.begin_write_txn(),
        }
    }

    fn end_write_txn(&mut self) -> super::Result<()> {
        match self {
            Either::Left(l) => l.end_write_txn(),
            Either::Right(r) => r.end_write_txn(),
        }
    }

    fn undo<U: super::UndoHandler>(&mut self, handler: Option<&mut U>) -> super::Result<()> {
        match self {
            Either::Left(l) => l.undo(handler),
            Either::Right(r) => r.undo(handler),
        }
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        match self {
            Either::Left(l) => l.savepoint(rollback_data),
            Either::Right(r) => r.savepoint(rollback_data),
        }
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> super::Result<()> {
        match self {
            Either::Left(l) => l.savepoint_undo(rollback_data),
            Either::Right(r) => r.savepoint_undo(rollback_data),
        }
    }

    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut super::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> super::Result<usize> {
        match self {
            Either::Left(l) => {
                l.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
            }
            Either::Right(r) => {
                r.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
            }
        }
    }

    fn checkpoint(
        &mut self,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut dyn super::BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn super::CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> super::Result<()> {
        match self {
            Either::Left(l) => l.checkpoint(
                db,
                mode,
                busy_handler,
                sync_flags,
                buf,
                checkpoint_cb,
                in_wal,
                backfilled,
            ),
            Either::Right(r) => r.checkpoint(
                db,
                mode,
                busy_handler,
                sync_flags,
                buf,
                checkpoint_cb,
                in_wal,
                backfilled,
            ),
        }
    }

    fn exclusive_mode(&mut self, op: std::ffi::c_int) -> super::Result<()> {
        match self {
            Either::Left(l) => l.exclusive_mode(op),
            Either::Right(r) => r.exclusive_mode(op),
        }
    }

    fn uses_heap_memory(&self) -> bool {
        match self {
            Either::Left(l) => l.uses_heap_memory(),
            Either::Right(r) => r.uses_heap_memory(),
        }
    }

    fn set_db(&mut self, db: &mut super::Sqlite3Db) {
        match self {
            Either::Left(l) => l.set_db(db),
            Either::Right(r) => r.set_db(db),
        }
    }

    fn callback(&self) -> i32 {
        match self {
            Either::Left(l) => l.callback(),
            Either::Right(r) => r.callback(),
        }
    }

    fn frames_in_wal(&self) -> u32 {
        match self {
            Either::Left(l) => l.frames_in_wal(),
            Either::Right(r) => r.frames_in_wal(),
        }
    }
}

impl<L, R> WalManager for Either<L, R>
where
    L: WalManager,
    R: WalManager,
{
    type Wal = Either<L::Wal, R::Wal>;

    fn use_shared_memory(&self) -> bool {
        match self {
            Either::Left(l) => l.use_shared_memory(),
            Either::Right(r) => r.use_shared_memory(),
        }
    }

    fn open(
        &self,
        vfs: &mut super::Vfs,
        file: &mut super::Sqlite3File,
        no_shm_mode: std::ffi::c_int,
        max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> super::Result<Self::Wal> {
        match self {
            Either::Left(l) => l
                .open(vfs, file, no_shm_mode, max_log_size, db_path)
                .map(Either::Left),
            Either::Right(r) => r
                .open(vfs, file, no_shm_mode, max_log_size, db_path)
                .map(Either::Right),
        }
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut super::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> super::Result<()> {
        match (self, wal) {
            (Either::Left(l), Either::Left(wal)) => l.close(wal, db, sync_flags, scratch),
            (Either::Right(r), Either::Right(wal)) => r.close(wal, db, sync_flags, scratch),
            _ => unreachable!(),
        }
    }

    fn destroy_log(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<()> {
        match self {
            Either::Left(l) => l.destroy_log(vfs, db_path),
            Either::Right(r) => r.destroy_log(vfs, db_path),
        }
    }

    fn log_exists(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<bool> {
        match self {
            Either::Left(l) => l.log_exists(vfs, db_path),
            Either::Right(r) => r.log_exists(vfs, db_path),
        }
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        match self {
            Either::Left(l) => l.destroy(),
            Either::Right(r) => r.destroy(),
        }
    }
}
