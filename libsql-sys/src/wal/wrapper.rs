use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;

use super::{BusyHandler, CheckpointCallback, Sqlite3File, Vfs, Wal, WalManager};

/// A convenient wrapper struct that implement WAL with a `wrapper` where the wrapper needs to
/// implement `WrapWal` instead of `Wal`, where all methods delegate to wrapped by default.
#[derive(Clone)]
pub struct WalWrapper<T, W> {
    wrapper: T,
    wrapped: W,
}

impl<T, W> WalWrapper<T, W>
where
    T: WrapWal<W::Wal> + Clone,
    W: WalManager,
{
    pub fn new(wrapper: T, wrapped: W) -> Self {
        Self { wrapper, wrapped }
    }

    pub fn wrapped(&self) -> &W {
        &self.wrapped
    }

    pub fn wrapper(&self) -> &T {
        &self.wrapper
    }
}

pub struct WrappedWal<T, W> {
    wrapper: T,
    wrapped: W,
}

impl<T, U> WalManager for WalWrapper<T, U>
where
    T: WrapWal<U::Wal> + Clone,
    U: WalManager,
{
    type Wal = WrappedWal<T, U::Wal>;

    fn use_shared_memory(&self) -> bool {
        self.wrapped.use_shared_memory()
    }

    fn open(
        &self,
        vfs: &mut super::Vfs,
        file: &mut super::Sqlite3File,
        no_shm_mode: std::ffi::c_int,
        max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> super::Result<Self::Wal> {
        let wrapped =
            self.wrapper
                .open(&self.wrapped, vfs, file, no_shm_mode, max_log_size, db_path)?;
        Ok(Self::Wal {
            wrapper: self.wrapper.clone(),
            wrapped,
        })
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut super::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> super::Result<()> {
        self.wrapper
            .clone()
            .close(&self.wrapped, &mut wal.wrapped, db, sync_flags, scratch)
    }

    fn destroy_log(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<()> {
        self.wrapped.destroy_log(vfs, db_path)
    }

    fn log_exists(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<bool> {
        self.wrapped.log_exists(vfs, db_path)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        self.wrapped.destroy()
    }
}

impl<T, W> Wal for WrappedWal<T, W>
where
    T: WrapWal<W>,
    W: Wal,
{
    fn limit(&mut self, size: i64) {
        self.wrapper.limit(&mut self.wrapped, size)
    }

    fn begin_read_txn(&mut self) -> super::Result<bool> {
        self.wrapper.begin_read_txn(&mut self.wrapped)
    }

    fn end_read_txn(&mut self) {
        self.wrapper.end_read_txn(&mut self.wrapped)
    }

    fn find_frame(&mut self, page_no: NonZeroU32) -> super::Result<Option<NonZeroU32>> {
        self.wrapper.find_frame(&mut self.wrapped, page_no)
    }

    fn read_frame(&mut self, frame_no: NonZeroU32, buffer: &mut [u8]) -> super::Result<()> {
        self.wrapper.read_frame(&mut self.wrapped, frame_no, buffer)
    }

    fn db_size(&self) -> u32 {
        self.wrapper.db_size(&self.wrapped)
    }

    fn begin_write_txn(&mut self) -> super::Result<()> {
        self.wrapper.begin_write_txn(&mut self.wrapped)
    }

    fn end_write_txn(&mut self) -> super::Result<()> {
        self.wrapper.end_write_txn(&mut self.wrapped)
    }

    fn undo<U: super::UndoHandler>(&mut self, handler: Option<&mut U>) -> super::Result<()> {
        self.wrapper.undo(&mut self.wrapped, handler)
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        self.wrapper.savepoint(&mut self.wrapped, rollback_data)
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> super::Result<()> {
        self.wrapper
            .savepoint_undo(&mut self.wrapped, rollback_data)
    }

    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut super::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> super::Result<usize> {
        self.wrapper.insert_frames(
            &mut self.wrapped,
            page_size,
            page_headers,
            size_after,
            is_commit,
            sync_flags,
        )
    }

    fn checkpoint(
        &mut self,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> super::Result<()> {
        self.wrapper.checkpoint(
            &mut self.wrapped,
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

    fn exclusive_mode(&mut self, op: std::ffi::c_int) -> super::Result<()> {
        self.wrapper.exclusive_mode(&mut self.wrapped, op)
    }

    fn uses_heap_memory(&self) -> bool {
        self.wrapper.uses_heap_memory(&self.wrapped)
    }

    fn set_db(&mut self, db: &mut super::Sqlite3Db) {
        self.wrapper.set_db(&mut self.wrapped, db)
    }

    fn callback(&self) -> i32 {
        self.wrapper.callback(&self.wrapped)
    }

    fn frames_in_wal(&self) -> u32 {
        self.wrapper.frames_in_wal(&self.wrapped)
    }

    fn db_file(&self) -> &super::Sqlite3File {
        self.wrapped.db_file()
    }

    fn backfilled(&self) -> u32 {
        self.wrapped.backfilled()
    }

    fn frame_page_no(&self, frame_no: NonZeroU32) -> Option<NonZeroU32> {
        self.wrapper.frame_page_no(&self.wrapped, frame_no)
    }
}

/// Trait implemented by implementor that only need to wrap around another Wal implementation.
/// By default, all methods delegate to the wrapped wal.
pub trait WrapWal<W: Wal> {
    fn limit(&mut self, wrapped: &mut W, size: i64) {
        wrapped.limit(size)
    }

    fn begin_read_txn(&mut self, wrapped: &mut W) -> super::Result<bool> {
        wrapped.begin_read_txn()
    }

    fn end_read_txn(&mut self, wrapped: &mut W) {
        wrapped.end_read_txn()
    }

    fn find_frame(
        &mut self,
        wrapped: &mut W,
        page_no: NonZeroU32,
    ) -> super::Result<Option<NonZeroU32>> {
        wrapped.find_frame(page_no)
    }

    fn read_frame(
        &mut self,
        wrapped: &mut W,
        frame_no: NonZeroU32,
        buffer: &mut [u8],
    ) -> super::Result<()> {
        wrapped.read_frame(frame_no, buffer)
    }

    fn db_size(&self, wrapped: &W) -> u32 {
        wrapped.db_size()
    }

    fn begin_write_txn(&mut self, wrapped: &mut W) -> super::Result<()> {
        wrapped.begin_write_txn()
    }

    fn end_write_txn(&mut self, wrapped: &mut W) -> super::Result<()> {
        wrapped.end_write_txn()
    }

    fn undo<U: super::UndoHandler>(
        &mut self,
        wrapped: &mut W,
        handler: Option<&mut U>,
    ) -> super::Result<()> {
        wrapped.undo(handler)
    }

    fn savepoint(&mut self, wrapped: &mut W, rollback_data: &mut [u32]) {
        wrapped.savepoint(rollback_data)
    }

    fn savepoint_undo(&mut self, wrapped: &mut W, rollback_data: &mut [u32]) -> super::Result<()> {
        wrapped.savepoint_undo(rollback_data)
    }

    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut super::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> super::Result<usize> {
        wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
    }

    fn checkpoint(
        &mut self,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> super::Result<()> {
        wrapped.checkpoint(
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

    fn exclusive_mode(&mut self, wrapped: &mut W, op: std::ffi::c_int) -> super::Result<()> {
        wrapped.exclusive_mode(op)
    }

    fn uses_heap_memory(&self, wrapped: &W) -> bool {
        wrapped.uses_heap_memory()
    }

    fn set_db(&mut self, wrapped: &mut W, db: &mut super::Sqlite3Db) {
        wrapped.set_db(db)
    }

    fn callback(&self, wrapped: &W) -> i32 {
        wrapped.callback()
    }

    fn frames_in_wal(&self, wrapped: &W) -> u32 {
        wrapped.frames_in_wal()
    }

    fn open<M: WalManager<Wal = W>>(
        &self,
        manager: &M,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> super::Result<W> {
        manager.open(vfs, file, no_shm_mode, max_log_size, db_path)
    }

    fn close<M: WalManager<Wal = W>>(
        &mut self,
        manager: &M,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> super::Result<()> {
        manager.close(wrapped, db, sync_flags, scratch)
    }

    fn frame_page_no(&self, wrapped: &W, frame_no: NonZeroU32) -> Option<NonZeroU32> {
        wrapped.frame_page_no(frame_no)
    }
}

impl<T: WrapWal<W>, W: Wal> WrapWal<W> for Option<T> {
    fn limit(&mut self, wrapped: &mut W, size: i64) {
        match self {
            Some(t) => t.limit(wrapped, size),
            None => wrapped.limit(size),
        }
    }

    fn begin_read_txn(&mut self, wrapped: &mut W) -> super::Result<bool> {
        match self {
            Some(t) => t.begin_read_txn(wrapped),
            None => wrapped.begin_read_txn(),
        }
    }

    fn end_read_txn(&mut self, wrapped: &mut W) {
        match self {
            Some(t) => t.end_read_txn(wrapped),
            None => wrapped.end_read_txn(),
        }
    }

    fn find_frame(
        &mut self,
        wrapped: &mut W,
        page_no: NonZeroU32,
    ) -> super::Result<Option<NonZeroU32>> {
        match self {
            Some(t) => t.find_frame(wrapped, page_no),
            None => wrapped.find_frame(page_no),
        }
    }

    fn read_frame(
        &mut self,
        wrapped: &mut W,
        frame_no: NonZeroU32,
        buffer: &mut [u8],
    ) -> super::Result<()> {
        match self {
            Some(t) => t.read_frame(wrapped, frame_no, buffer),
            None => wrapped.read_frame(frame_no, buffer),
        }
    }

    fn db_size(&self, wrapped: &W) -> u32 {
        match self {
            Some(t) => t.db_size(wrapped),
            None => wrapped.db_size(),
        }
    }

    fn begin_write_txn(&mut self, wrapped: &mut W) -> super::Result<()> {
        match self {
            Some(t) => t.begin_write_txn(wrapped),
            None => wrapped.begin_write_txn(),
        }
    }

    fn end_write_txn(&mut self, wrapped: &mut W) -> super::Result<()> {
        match self {
            Some(t) => t.end_write_txn(wrapped),
            None => wrapped.end_write_txn(),
        }
    }

    fn undo<U: super::UndoHandler>(
        &mut self,
        wrapped: &mut W,
        handler: Option<&mut U>,
    ) -> super::Result<()> {
        match self {
            Some(t) => t.undo(wrapped, handler),
            None => wrapped.undo(handler),
        }
    }

    fn savepoint(&mut self, wrapped: &mut W, rollback_data: &mut [u32]) {
        match self {
            Some(t) => t.savepoint(wrapped, rollback_data),
            None => wrapped.savepoint(rollback_data),
        }
    }

    fn savepoint_undo(&mut self, wrapped: &mut W, rollback_data: &mut [u32]) -> super::Result<()> {
        match self {
            Some(t) => t.savepoint_undo(wrapped, rollback_data),
            None => wrapped.savepoint_undo(rollback_data),
        }
    }

    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut super::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> super::Result<usize> {
        match self {
            Some(t) => t.insert_frames(
                wrapped,
                page_size,
                page_headers,
                size_after,
                is_commit,
                sync_flags,
            ),
            None => {
                wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
            }
        }
    }

    fn checkpoint(
        &mut self,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> super::Result<()> {
        match self {
            Some(t) => t.checkpoint(
                wrapped,
                db,
                mode,
                busy_handler,
                sync_flags,
                buf,
                checkpoint_cb,
                in_wal,
                backfilled,
            ),
            None => wrapped.checkpoint(
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

    fn exclusive_mode(&mut self, wrapped: &mut W, op: std::ffi::c_int) -> super::Result<()> {
        match self {
            Some(t) => t.exclusive_mode(wrapped, op),
            None => wrapped.exclusive_mode(op),
        }
    }

    fn uses_heap_memory(&self, wrapped: &W) -> bool {
        match self {
            Some(t) => t.uses_heap_memory(wrapped),
            None => wrapped.uses_heap_memory(),
        }
    }

    fn set_db(&mut self, wrapped: &mut W, db: &mut super::Sqlite3Db) {
        match self {
            Some(t) => t.set_db(wrapped, db),
            None => wrapped.set_db(db),
        }
    }

    fn callback(&self, wrapped: &W) -> i32 {
        match self {
            Some(t) => t.callback(wrapped),
            None => wrapped.callback(),
        }
    }

    fn frames_in_wal(&self, wrapped: &W) -> u32 {
        match self {
            Some(t) => t.frames_in_wal(wrapped),
            None => wrapped.frames_in_wal(),
        }
    }

    fn close<M: WalManager<Wal = W>>(
        &mut self,
        manager: &M,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> super::Result<()> {
        match self {
            Some(t) => t.close(manager, wrapped, db, sync_flags, scratch),
            None => manager.close(wrapped, db, sync_flags, scratch),
        }
    }
}
