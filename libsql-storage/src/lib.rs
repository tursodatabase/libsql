use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use libsql_sys::ffi::{SQLITE_ABORT, SQLITE_BUSY};
use libsql_sys::rusqlite;
use libsql_sys::wal::{Result, Vfs, Wal, WalManager};
use rpc::storage_client::StorageClient;
use sieve_cache::SieveCache;
use tonic::transport::Channel;
use tracing::{error, trace};

pub mod rpc {
    #![allow(clippy::all)]
    include!("generated/storage.rs");
}

// What does (not) work:
// - there are no read txn locks nor upgrades
// - no lock stealing
// - write set is kept in mem
// - txns don't use max_frame_no yet
// - no savepoints, yet
// - no multi tenancy, uses `default` namespace
// - txn can read new frames after it started (since there are no read locks)
// - requires huge memory as it assumes all the txn data fits in the memory

#[derive(Clone, Default)]
pub struct DurableWalConfig {
    storage_server_address: String,
}

#[derive(Clone)]
pub struct DurableWalManager {
    lock_manager: Arc<Mutex<LockManager>>,
    config: DurableWalConfig,
}

impl DurableWalManager {
    pub fn new(lock_manager: Arc<Mutex<LockManager>>, storage_server_address: String) -> Self {
        Self {
            lock_manager,
            config: DurableWalConfig {
                storage_server_address,
            },
        }
    }
}

impl WalManager for DurableWalManager {
    type Wal = DurableWal;

    fn use_shared_memory(&self) -> bool {
        trace!("DurableWalManager::use_shared_memory()");
        false
    }

    #[tracing::instrument(skip_all, fields(_db_path))]
    fn open(
        &self,
        _vfs: &mut Vfs,
        _file: &mut libsql_sys::wal::Sqlite3File,
        _no_shm_mode: std::ffi::c_int,
        _max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> Result<Self::Wal> {
        let _db_path = db_path.to_str().unwrap();
        trace!("DurableWalManager::open()");
        // TODO: use the actual namespace uuid from the connection
        let namespace = "default".to_string();
        let rt = tokio::runtime::Handle::current();
        let resp = DurableWal::new(namespace, self.config.clone(), self.lock_manager.clone());
        let resp = tokio::task::block_in_place(|| rt.block_on(resp));
        Ok(resp)
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        _db: &mut libsql_sys::wal::Sqlite3Db,
        _sync_flags: std::ffi::c_int,
        _scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        trace!("DurableWalManager::close()");
        wal.end_read_txn();
        Ok(())
    }

    fn destroy_log(&self, _vfs: &mut Vfs, _db_path: &std::ffi::CStr) -> Result<()> {
        trace!("DurableWalManager::destroy_log()");
        Ok(())
    }

    fn log_exists(&self, _vfs: &mut Vfs, _db_path: &std::ffi::CStr) -> Result<bool> {
        trace!("DurableWalManager::log_exists()");
        Ok(true)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        trace!("DurableWalManager::destroy()");
    }
}

pub struct DurableWal {
    namespace: String,
    conn_id: String,
    client: StorageClient<Channel>,
    frames_cache: SieveCache<std::num::NonZeroU64, Vec<u8>>,
    write_cache: BTreeMap<u32, rpc::Frame>,
    lock_manager: Arc<Mutex<LockManager>>,
}

impl DurableWal {
    async fn new(
        namespace: String,
        config: DurableWalConfig,
        lock_manager: Arc<Mutex<LockManager>>,
    ) -> Self {
        let client = StorageClient::connect(config.storage_server_address)
            .await
            .unwrap();
        let page_frames = SieveCache::new(1000).unwrap();
        Self {
            namespace,
            conn_id: uuid::Uuid::new_v4().to_string(),
            client,
            frames_cache: page_frames,
            write_cache: BTreeMap::new(),
            lock_manager,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn find_frame_by_page_no(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> Result<Option<std::num::NonZeroU64>> {
        trace!("DurableWal::find_frame_by_page_no()");
        let req = rpc::FindFrameRequest {
            namespace: self.namespace.clone(),
            page_no: page_no.get(),
            max_frame_no: 0,
        };
        let mut binding = self.client.clone();
        let resp = binding.find_frame(req).await.unwrap();
        let frame_no = resp
            .into_inner()
            .frame_no
            .map(|no| std::num::NonZeroU64::new(no))
            .flatten();
        Ok(frame_no)
    }

    async fn frames_count(&self) -> u64 {
        let req = rpc::FramesInWalRequest {
            namespace: self.namespace.clone(),
        };
        let mut binding = self.client.clone();
        let resp = binding.frames_in_wal(req).await.unwrap();
        let count = resp.into_inner().count;
        trace!("DurableWal::frames_in_wal() = {}", count);
        count
    }
}

impl Wal for DurableWal {
    fn limit(&mut self, _size: i64) {}

    fn begin_read_txn(&mut self) -> Result<bool> {
        trace!("DurableWal::begin_read_txn()");
        // TODO:
        // - create a read lock
        // - save the current max_frame_no for this txn
        //
        Ok(true)
    }

    fn end_read_txn(&mut self) {
        trace!("DurableWal::end_read_txn()");
        // TODO: drop both read or write lock
        let mut lock_manager = self.lock_manager.lock().unwrap();
        trace!(
            "DurableWal::end_read_txn() id = {}, unlocked = {}",
            self.conn_id,
            lock_manager.unlock(self.namespace.clone(), self.conn_id.clone())
        );
    }

    // find_frame checks if the given page_no exists in the storage server. If so, it returns the
    // same `page_no` back. The WAL interface expects the value to be u32 but the frames can exceed
    // the limit and is set to u64. So, instead of returning the frame no, it returns the page no
    // back and `read_frame` methods reads the frame by page_no
    #[tracing::instrument(skip(self))]
    fn find_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> Result<Option<std::num::NonZeroU32>> {
        trace!("DurableWal::find_frame()");
        let rt = tokio::runtime::Handle::current();
        // TODO: find_frame should account for `max_frame_no` of this txn
        let frame_no =
            tokio::task::block_in_place(|| rt.block_on(self.find_frame_by_page_no(page_no)))
                .unwrap();
        if frame_no.is_none() {
            return Ok(None);
        }
        return Ok(Some(page_no));
    }

    #[tracing::instrument(skip_all, fields(page_no))]
    fn read_frame(&mut self, page_no: std::num::NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        trace!("DurableWal::read_frame()");
        let rt = tokio::runtime::Handle::current();
        if let Some(frame) = self.write_cache.get(&(u32::from(page_no))) {
            trace!(
                "DurableWal::read_frame(page_no: {:?}) -- write cache hit",
                page_no
            );
            buffer.copy_from_slice(&frame.data);
            return Ok(());
        }
        // TODO: this call is unnecessary since `read_frame` is always called after `find_frame`
        let frame_no =
            tokio::task::block_in_place(|| rt.block_on(self.find_frame_by_page_no(page_no)))
                .unwrap()
                .unwrap();
        // check if the frame exists in the local cache
        if let Some(frame) = self.frames_cache.get(&frame_no) {
            trace!(
                "DurableWal::read_frame(page_no: {:?}) -- read cache hit",
                page_no
            );
            buffer.copy_from_slice(&frame);
            return Ok(());
        }
        let req = rpc::ReadFrameRequest {
            namespace: self.namespace.clone(),
            frame_no: frame_no.get(),
        };
        let mut binding = self.client.clone();
        let resp = binding.read_frame(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let frame = resp.into_inner().frame.unwrap();
        buffer.copy_from_slice(&frame);
        self.frames_cache
            .insert(std::num::NonZeroU64::new(frame_no.get()).unwrap(), frame);
        Ok(())
    }

    fn db_size(&self) -> u32 {
        let rt = tokio::runtime::Handle::current();
        let size = tokio::task::block_in_place(|| rt.block_on(self.frames_count()))
            .try_into()
            .unwrap();
        trace!("DurableWal::db_size() => {}", size);
        size
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        // todo: check if the connection holds a read lock then try to acquire a write lock
        let mut lock_manager = self.lock_manager.lock().unwrap();
        if !lock_manager.lock(self.namespace.clone(), self.conn_id.clone()) {
            trace!(
                "DurableWal::begin_write_txn() lock acquired = false, id = {}",
                self.conn_id
            );
            return Err(rusqlite::ffi::Error::new(SQLITE_BUSY));
        };
        trace!(
            "DurableWal::begin_write_txn() lock acquired = true, id = {}",
            self.conn_id
        );
        Ok(())
    }

    fn end_write_txn(&mut self) -> Result<()> {
        let mut lock_manager = self.lock_manager.lock().unwrap();
        trace!(
            "DurableWal::end_write_txn() id = {}, unlocked = {}",
            self.conn_id,
            lock_manager.unlock(self.namespace.clone(), self.conn_id.clone())
        );
        Ok(())
    }

    fn undo<U: libsql_sys::wal::UndoHandler>(&mut self, _handler: Option<&mut U>) -> Result<()> {
        // TODO: implement undo
        Ok(())
    }

    fn savepoint(&mut self, _rollback_data: &mut [u32]) {
        // TODO: implement savepoint
    }

    fn savepoint_undo(&mut self, _rollback_data: &mut [u32]) -> Result<()> {
        // TODO: implement savepoint_undo
        Ok(())
    }

    #[tracing::instrument(skip(self, page_headers))]
    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> Result<usize> {
        trace!("DurableWal::insert_frames()");
        let rt = tokio::runtime::Handle::current();
        let mut lock_manager = self.lock_manager.lock().unwrap();
        if !lock_manager.is_lock_owner(self.namespace.clone(), self.conn_id.clone()) {
            error!("DurableWal::insert_frames() was called without acquiring lock!",);
            self.write_cache.clear();
            return Err(rusqlite::ffi::Error::new(SQLITE_ABORT));
        };
        // add the updated frames from frame_headers to writeCache
        for (page_no, frame) in page_headers.iter() {
            self.write_cache.insert(
                page_no,
                rpc::Frame {
                    page_no: page_no,
                    data: frame.to_vec(),
                },
            );
            // todo: update size after
        }

        // check if the size_after is > 0, if so then mark txn as committed
        if size_after <= 0 {
            // todo: update new size
            return Ok(0);
        }

        let req = rpc::InsertFramesRequest {
            namespace: self.namespace.clone(),
            frames: self.write_cache.values().cloned().collect(),
        };
        self.write_cache.clear();
        let mut binding = self.client.clone();
        trace!("sending DurableWal::insert_frames() {:?}", req.frames.len());
        let resp = binding.insert_frames(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        Ok(resp.into_inner().num_frames as usize)
    }

    fn checkpoint(
        &mut self,
        _db: &mut libsql_sys::wal::Sqlite3Db,
        _mode: libsql_sys::wal::CheckpointMode,
        _busy_handler: Option<&mut dyn libsql_sys::wal::BusyHandler>,
        _sync_flags: u32,
        // temporary scratch buffer
        _buf: &mut [u8],
        _checkpoint_cb: Option<&mut dyn libsql_sys::wal::CheckpointCallback>,
        _in_wal: Option<&mut i32>,
        _backfilled: Option<&mut i32>,
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
        true
    }

    fn set_db(&mut self, _db: &mut libsql_sys::wal::Sqlite3Db) {}

    fn callback(&self) -> i32 {
        trace!("DurableWal::callback()");
        0
    }

    fn frames_in_wal(&self) -> u32 {
        0
    }
}

pub struct LockManager {
    locks: std::collections::HashMap<String, String>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: std::collections::HashMap::new(),
        }
    }

    pub fn lock(&mut self, namespace: String, conn_id: String) -> bool {
        if let Some(lock) = self.locks.get(&namespace) {
            if lock == &conn_id {
                return true;
            }
            return false;
        }
        self.locks.insert(namespace, conn_id);
        true
    }

    pub fn unlock(&mut self, namespace: String, conn_id: String) -> bool {
        if let Some(lock) = self.locks.get(&namespace) {
            if lock == &conn_id {
                self.locks.remove(&namespace);
                return true;
            }
            return false;
        }
        true
    }

    pub fn is_lock_owner(&mut self, namespace: String, conn_id: String) -> bool {
        if let Some(lock) = self.locks.get(&namespace) {
            if lock == &conn_id {
                return true;
            }
        }
        return false;
    }
}
