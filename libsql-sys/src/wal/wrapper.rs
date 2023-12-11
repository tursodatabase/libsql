use std::{ffi::c_int, num::NonZeroU32};

use super::{Wal, WalManager};

/// A convenient wrapper struct that implement WAL with a `wrapper` where the wrapper needs to
/// implement `WrapWal` instead of `Wal`, where all methods delegate to wrapped by default.
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
        let wrapped = self
            .wrapped
            .open(vfs, file, no_shm_mode, max_log_size, db_path)?;
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
    ) -> super::Result<()> {
        self.wrapper.insert_frames(
            &mut self.wrapped,
            page_size,
            page_headers,
            size_after,
            is_commit,
            sync_flags,
        )
    }

    fn checkpoint<B: super::BusyHandler>(
        &mut self,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> super::Result<(u32, u32)> {
        self.wrapper
            .checkpoint(&mut self.wrapped, db, mode, busy_handler, sync_flags, buf)
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

    fn last_fame_index(&self) -> u32 {
        self.wrapper.last_fame_index(&self.wrapped)
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
    ) -> super::Result<()> {
        wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
    }

    fn checkpoint<B: super::BusyHandler>(
        &mut self,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        mode: super::CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> super::Result<(u32, u32)> {
        wrapped.checkpoint(db, mode, busy_handler, sync_flags, buf)
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

    fn last_fame_index(&self, wrapped: &W) -> u32 {
        wrapped.last_fame_index()
    }

    fn close<M: WalManager<Wal = W>>(
        &self,
        manager: &M,
        wrapped: &mut W,
        db: &mut super::Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> super::Result<()> {
        manager.close(wrapped, db, sync_flags, scratch)
    }
}
