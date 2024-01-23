use std::{collections::HashMap, sync::Arc};

use libsql_sys::wal::{wrapper::WrapWal, Wal};
use parking_lot::RwLock;

#[derive(Clone, Debug)]
pub struct RecordCommitWrapper {
    commit_indexes: Arc<RwLock<HashMap<u32, u32>>>,
}

impl RecordCommitWrapper {
    pub fn new(commit_indexes: Arc<RwLock<HashMap<u32, u32>>>) -> Self {
        Self { commit_indexes }
    }
}

impl<W: Wal> WrapWal<W> for RecordCommitWrapper {
    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<usize> {
        let commit_index = wrapped.frames_in_wal();
        if commit_index == 0 {
            self.commit_indexes.write().clear();
        }

        let ret =
            wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)?;

        if is_commit {
            let commit_index = wrapped.frames_in_wal();
            self.commit_indexes.write().insert(commit_index, size_after);
        }

        Ok(ret)
    }
}
