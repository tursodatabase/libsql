use libsql_sys::wal::{Result, Vfs, Wal, WalManager};
use sieve_cache::SieveCache;
use tonic::transport::Channel;
use tracing::trace;

pub mod rpc {
    #![allow(clippy::all)]
    include!("generated/storage.rs");
}

use rpc::storage_client::StorageClient;

#[derive(Clone)]
pub struct DurableWalManager {}

impl DurableWalManager {
    pub fn new() -> Self {
        Self {}
    }
}

impl WalManager for DurableWalManager {
    type Wal = DurableWal;

    fn use_shared_memory(&self) -> bool {
        trace!("DurableWalManager::use_shared_memory()");
        false
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut libsql_sys::wal::Sqlite3File,
        no_shm_mode: std::ffi::c_int,
        max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> Result<Self::Wal> {
        let db_path = db_path.to_str().unwrap();
        trace!("DurableWalManager::open(db_path: {})", db_path);
        Ok(DurableWal::new())
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        trace!("DurableWalManager::close()");
        Ok(())
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &std::ffi::CStr) -> Result<()> {
        trace!("DurableWalManager::destroy_log()");
        // TODO: implement
        Ok(())
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &std::ffi::CStr) -> Result<bool> {
        trace!("DurableWalManager::log_exists()");
        // TODO: implement
        Ok(false)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        trace!("DurableWalManager::destroy()");
    }
}

pub struct DurableWal {
    client: StorageClient<Channel>,
    page_frames: SieveCache<std::num::NonZeroU32, std::num::NonZeroU32>,
    db_size: u32,
}

impl DurableWal {
    fn new() -> Self {
        let rt = tokio::runtime::Handle::current();
        let client = StorageClient::connect("http://127.0.0.1:5002");
        let mut client = tokio::task::block_in_place(|| rt.block_on(client)).unwrap();

        let req = rpc::DbSizeReq {};
        let resp = client.db_size(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let db_size = resp.into_inner().size as u32;

        let page_frames = SieveCache::new(1000).unwrap();

        Self {
            client,
            page_frames,
            db_size,
        }
    }
}

impl Wal for DurableWal {
    fn limit(&mut self, size: i64) {
        // no op, we go bottomless baby!
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        trace!("DurableWal::begin_read_txn()");
        Ok(true)
    }

    fn end_read_txn(&mut self) {
        trace!("DurableWal::end_read_txn()");
    }

    fn find_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> Result<Option<std::num::NonZeroU32>> {
        trace!("DurableWal::find_frame(page_no: {:?})", page_no);
        if let Some(frame_no) = self.page_frames.get(&page_no) {
            return Ok(Some(*frame_no));
        }
        let rt = tokio::runtime::Handle::current();
        let req = rpc::FindFrameReq {
            page_no: page_no.get() as u64,
        };
        let resp = self.client.find_frame(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let frame_no = resp
            .into_inner()
            .frame_no
            .map(|page_no| std::num::NonZeroU32::new(page_no as u32))
            .flatten();
        if let Some(frame_no) = frame_no {
            self.page_frames.insert(page_no, frame_no);
        }
        Ok(frame_no)
    }

    fn read_frame(&mut self, frame_no: std::num::NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        trace!("DurableWal::read_frame(frame_no: {:?})", frame_no);
        let rt = tokio::runtime::Handle::current();
        let frame_no = frame_no.get() as u64;
        let req = rpc::ReadFrameReq { frame_no };
        let resp = self.client.read_frame(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let frame = resp.into_inner().frame.unwrap();
        buffer.copy_from_slice(&frame);
        Ok(())
    }

    fn frame_page_no(&self, frame_no: std::num::NonZeroU32) -> Option<std::num::NonZeroU32> {
        trace!("DurableWal::frame_page_no(frame_no: {:?})", frame_no);
        let rt = tokio::runtime::Handle::current();
        let frame_no = frame_no.get() as u64;
        let req = rpc::FramePageNumReq { frame_no };
        let resp = self.client.frame_page_num(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let page_no = resp.into_inner().page_no;
        std::num::NonZeroU32::new(page_no as u32)
    }

    fn db_size(&self) -> u32 {
        trace!("DurableWal::db_size() => {}", self.db_size);
        self.db_size
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        trace!("DurableWal::begin_write_txn()");
        Ok(())
    }

    fn end_write_txn(&mut self) -> Result<()> {
        trace!("DurableWal::end_write_txn()");
        Ok(())
    }

    fn undo<U: libsql_sys::wal::UndoHandler>(&mut self, handler: Option<&mut U>) -> Result<()> {
        todo!()
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        todo!()
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        todo!()
    }

    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> Result<usize> {
        trace!("DurableWal::insert_frames(page_size: {}, size_after: {}, is_commit: {}, sync_flags: {})", page_size, size_after, is_commit, sync_flags);
        let rt = tokio::runtime::Handle::current();
        let frames = page_headers
            .iter()
            .map(|header| {
                let (page_no, frame) = header;
                rpc::Frame {
                    page_no: page_no as u64,
                    data: frame.to_vec(),
                }
            })
            .collect();
        let req = rpc::InsertFramesReq { frames };
        let resp = self.client.insert_frames(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        self.db_size = size_after;
        Ok(resp.into_inner().num_frames as usize)
    }

    fn checkpoint(
        &mut self,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn libsql_sys::wal::BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn libsql_sys::wal::CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> Result<()> {
        // checkpoint is a no op
        Ok(())
    }

    fn exclusive_mode(&mut self, op: std::ffi::c_int) -> Result<()> {
        trace!("DurableWal::exclusive_mode(op: {})", op);
        Ok(())
    }

    fn uses_heap_memory(&self) -> bool {
        trace!("DurableWal::uses_heap_memory()");
        false
    }

    fn set_db(&mut self, db: &mut libsql_sys::wal::Sqlite3Db) {
        todo!()
    }

    fn callback(&self) -> i32 {
        trace!("DurableWal::callback()");
        0
    }

    fn frames_in_wal(&mut self) -> u32 {
        trace!("DurableWal::frames_in_wal()");
        let rt = tokio::runtime::Handle::current();
        let req = rpc::FramesInWalReq {};
        let resp = self.client.frames_in_wal(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        resp.into_inner().count
    }

    fn backfilled(&self) -> u32 {
        todo!()
    }

    fn db_file(&self) -> &libsql_sys::wal::Sqlite3File {
        todo!()
    }
}
