#![allow(dead_code)]

pub mod types;

pub use rusqlite::ffi::{
    libsql_wal_methods, libsql_wal_methods_find, libsql_wal_methods_register,
    libsql_wal_methods_unregister, sqlite3, sqlite3_file, sqlite3_hard_heap_limit64,
    sqlite3_io_methods, sqlite3_soft_heap_limit64, sqlite3_vfs, WalIndexHdr, SQLITE_CANTOPEN,
    SQLITE_CHECKPOINT_FULL, SQLITE_CHECKPOINT_TRUNCATE, SQLITE_IOERR_WRITE, SQLITE_OK,
};

pub use rusqlite::ffi::libsql_pghdr as PgHdr;
pub use rusqlite::ffi::libsql_wal as Wal;
pub use rusqlite::ffi::*;

pub struct PageHdrIter {
    current_ptr: *const PgHdr,
    page_size: usize,
}

impl PageHdrIter {
    pub fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
        }
    }
}

impl std::iter::Iterator for PageHdrIter {
    type Item = (u32, &'static [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.pData as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data));
        self.current_ptr = current_hdr.pDirty;
        item
    }
}
