#![allow(improper_ctypes)]
#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use libsql_sys::rusqlite::{Connection, OpenFlags};
    use libsql_sys::wal::{
        make_wal_manager, BusyHandler, CheckpointCallback, Sqlite3Wal, Sqlite3WalManager, Wal,
        WalManager,
    };

    /// A wal_manager the simple wraps sqlite3 WAL
    struct WrapWalManager {
        inner: Sqlite3WalManager,
    }

    impl WalManager for WrapWalManager {
        type Wal = WrapWal;

        fn use_shared_memory(&self) -> bool {
            self.inner.use_shared_memory()
        }

        fn open(
            &self,
            vfs: &mut libsql_sys::wal::Vfs,
            file: &mut libsql_sys::wal::Sqlite3File,
            no_shm_mode: std::ffi::c_int,
            max_log_size: i64,
            db_path: &std::ffi::CStr,
        ) -> libsql_sys::wal::Result<Self::Wal> {
            self.inner
                .open(vfs, file, no_shm_mode, max_log_size, db_path)
                .map(WrapWal)
        }

        fn close(
            &self,
            wal: &mut Self::Wal,
            db: &mut libsql_sys::wal::Sqlite3Db,
            sync_flags: std::ffi::c_int,
            scratch: Option<&mut [u8]>,
        ) -> libsql_sys::wal::Result<()> {
            self.inner.close(&mut wal.0, db, sync_flags, scratch)
        }

        fn destroy_log(
            &self,
            vfs: &mut libsql_sys::wal::Vfs,
            db_path: &std::ffi::CStr,
        ) -> libsql_sys::wal::Result<()> {
            self.inner.destroy_log(vfs, db_path)
        }

        fn log_exists(
            &self,
            vfs: &mut libsql_sys::wal::Vfs,
            db_path: &std::ffi::CStr,
        ) -> libsql_sys::wal::Result<bool> {
            self.inner.log_exists(vfs, db_path)
        }

        fn destroy(self)
        where
            Self: Sized,
        {
            self.inner.destroy()
        }
    }

    struct WrapWal(Sqlite3Wal);

    impl Wal for WrapWal {
        fn limit(&mut self, size: i64) {
            self.0.limit(size)
        }

        fn begin_read_txn(&mut self) -> libsql_sys::wal::Result<bool> {
            self.0.begin_read_txn()
        }

        fn end_read_txn(&mut self) {
            self.0.end_read_txn()
        }

        fn find_frame(
            &mut self,
            page_no: NonZeroU32,
        ) -> libsql_sys::wal::Result<Option<NonZeroU32>> {
            self.0.find_frame(page_no)
        }

        fn read_frame(
            &mut self,
            frame_no: NonZeroU32,
            buffer: &mut [u8],
        ) -> libsql_sys::wal::Result<()> {
            self.0.read_frame(frame_no, buffer)
        }

        fn db_size(&self) -> u32 {
            self.0.db_size()
        }

        fn begin_write_txn(&mut self) -> libsql_sys::wal::Result<()> {
            self.0.begin_write_txn()
        }

        fn end_write_txn(&mut self) -> libsql_sys::wal::Result<()> {
            self.0.end_write_txn()
        }

        fn undo<U: libsql_sys::wal::UndoHandler>(
            &mut self,
            handler: Option<&mut U>,
        ) -> libsql_sys::wal::Result<()> {
            self.0.undo(handler)
        }

        fn savepoint(&mut self, rollback_data: &mut [u32]) {
            self.0.savepoint(rollback_data)
        }

        fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> libsql_sys::wal::Result<()> {
            self.0.savepoint_undo(rollback_data)
        }

        fn insert_frames(
            &mut self,
            page_size: std::ffi::c_int,
            page_headers: &mut libsql_sys::wal::PageHeaders,
            size_after: u32,
            is_commit: bool,
            sync_flags: std::ffi::c_int,
        ) -> libsql_sys::wal::Result<usize> {
            self.0
                .insert_frames(page_size, page_headers, size_after, is_commit, sync_flags)
        }

        fn checkpoint(
            &mut self,
            db: &mut libsql_sys::wal::Sqlite3Db,
            mode: libsql_sys::wal::CheckpointMode,
            busy_handler: Option<&mut dyn BusyHandler>,
            sync_flags: u32,
            // temporary scratch buffer
            buf: &mut [u8],
            cb: Option<&mut dyn CheckpointCallback>,
            in_wal: Option<&mut i32>,
            backfilled: Option<&mut i32>,
        ) -> libsql_sys::wal::Result<()> {
            self.0.checkpoint(
                db,
                mode,
                busy_handler,
                sync_flags,
                buf,
                cb,
                in_wal,
                backfilled,
            )
        }

        fn exclusive_mode(&mut self, op: std::ffi::c_int) -> libsql_sys::wal::Result<()> {
            self.0.exclusive_mode(op)
        }

        fn uses_heap_memory(&self) -> bool {
            self.0.uses_heap_memory()
        }

        fn set_db(&mut self, db: &mut libsql_sys::wal::Sqlite3Db) {
            self.0.set_db(db)
        }

        fn callback(&self) -> i32 {
            self.0.callback()
        }

        fn frames_in_wal(&self) -> u32 {
            self.0.frames_in_wal()
        }
    }

    #[test]
    fn test_vwal_register() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let wal_manager = make_wal_manager(WrapWalManager {
            inner: Sqlite3WalManager::new(),
        });
        let conn =
            Connection::open_with_flags_and_wal(tmpfile.path(), OpenFlags::default(), wal_manager)
                .unwrap();

        conn.pragma(None, "journal_mode", "wal", |_| Ok(()))
            .unwrap();
        println!("Temporary database created at {:?}", tmpfile.path());
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        println!("Journaling mode: {journal_mode}");
        assert_eq!(journal_mode, "wal".to_string());
        conn.execute("CREATE TABLE t(id)", ()).unwrap();
        conn.execute("INSERT INTO t(id) VALUES (42)", ()).unwrap();
        conn.execute("INSERT INTO t(id) VALUES (zeroblob(8193))", ())
            .unwrap();
        conn.execute("INSERT INTO t(id) VALUES (7.0)", ()).unwrap();

        let seven: f64 = conn
            .query_row("SELECT id FROM t WHERE typeof(id) = 'real'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let blob: Vec<u8> = conn
            .query_row("SELECT id FROM t WHERE typeof(id) = 'blob'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let forty_two: i64 = conn
            .query_row("SELECT id FROM t WHERE typeof(id) = 'integer'", [], |r| {
                r.get(0)
            })
            .unwrap();

        assert_eq!(seven, 7.);
        assert!(blob.iter().all(|v| v == &0_u8));
        assert_eq!(blob.len(), 8193);
        assert_eq!(forty_two, 42);
    }
}
