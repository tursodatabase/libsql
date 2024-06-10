macro_rules! create_either {
    ( $name:ident< $($t:ident),* >) => {
        #[derive(Debug, Clone)]
        pub enum $name< $( $t ),* > {
            $( $t($t) ),*
        }

        impl< $( $t ),* > $crate::wal::Wal for $name< $( $t ),* >
        where
            $( $t: $crate::wal::Wal ),*
        {
            fn limit(&mut self, size: i64) {
                match self {
                    $( $name::$t(inner) => inner.limit(size) ),*
                }
            }

            fn begin_read_txn(&mut self) -> super::Result<bool> {
                match self {
                    $( $name::$t(inner) => inner.begin_read_txn() ),*
                }
            }

            fn end_read_txn(&mut self) {
                match self {
                    $( $name::$t(inner) => inner.end_read_txn() ),*
                }
            }

            fn find_frame(&mut self, page_no: std::num::NonZeroU32) -> super::Result<Option<std::num::NonZeroU32>> {
                match self {
                    $( $name::$t(inner) => inner.find_frame(page_no) ),*
                }
            }

            fn read_frame(&mut self, frame_no: std::num::NonZeroU32, buffer: &mut [u8]) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.read_frame(frame_no, buffer) ),*
                }
            }

            fn db_size(&self) -> u32 {
                match self {
                    $( $name::$t(inner) => inner.db_size() ),*
                }
            }

            fn begin_write_txn(&mut self) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.begin_write_txn() ),*
                }
            }

            fn end_write_txn(&mut self) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.end_write_txn() ),*
                }
            }

            fn undo<U: super::UndoHandler>(&mut self, handler: Option<&mut U>) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.undo(handler) ),*
                }
            }

            fn savepoint(&mut self, rollback_data: &mut [u32]) {
                match self {
                    $( $name::$t(inner) => inner.savepoint(rollback_data) ),*
                }
            }

            fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.savepoint_undo(rollback_data) ),*
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
                    $( $name::$t(inner) => inner.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags) ),*
                }
            }

            fn checkpoint(
                &mut self,
                db: &mut super::Sqlite3Db,
                mode: super::CheckpointMode,
                busy_handler: Option<&mut dyn super::BusyHandler>,
                sync_flags: u32,
                buf: &mut [u8],
                checkpoint_cb: Option<&mut dyn super::CheckpointCallback>,
                in_wal: Option<&mut i32>,
                backfilled: Option<&mut i32>,
            ) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.checkpoint(db, mode, busy_handler, sync_flags, buf, checkpoint_cb, in_wal, backfilled) ),*
                }
            }

            fn exclusive_mode(&mut self, op: std::ffi::c_int) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.exclusive_mode(op) ),*
                }
            }

            fn uses_heap_memory(&self) -> bool {
                match self {
                    $( $name::$t(inner) => inner.uses_heap_memory() ),*
                }
            }

            fn set_db(&mut self, db: &mut super::Sqlite3Db) {
                match self {
                    $( $name::$t(inner) => inner.set_db(db) ),*
                }
            }

            fn callback(&self) -> i32 {
                match self {
                    $( $name::$t(inner) => inner.callback() ),*
                }
            }

            fn frames_in_wal(&self) -> u32 {
                match self {
                    $( $name::$t(inner) => inner.frames_in_wal() ),*
                }
            }
        }

        impl< $( $t ),* > $crate::wal::WalManager for $name< $( $t ),* >
        where
            $( $t: $crate::wal::WalManager ),*
        {
            type Wal = $name< $( $t::Wal ),* >;

            fn use_shared_memory(&self) -> bool {
                match self {
                    $( $name::$t(inner) => inner.use_shared_memory() ),*
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
                    $( $name::$t(inner) => inner.open(vfs, file, no_shm_mode, max_log_size, db_path).map($name::$t) ),*
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
                    $(
                        ($name::$t(inner), $name::$t(wal)) => inner.close(wal, db, sync_flags, scratch),
                    )*
                    _ => unreachable!(),
                }
            }

            fn destroy_log(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<()> {
                match self {
                    $( $name::$t(inner) => inner.destroy_log(vfs, db_path) ),*
                }
            }

            fn log_exists(&self, vfs: &mut super::Vfs, db_path: &std::ffi::CStr) -> super::Result<bool> {
                match self {
                    $( $name::$t(inner) => inner.log_exists(vfs, db_path) ),*
                }
            }

            fn destroy(self)
            where
                Self: Sized,
            {
                match self {
                    $( $name::$t(inner) => inner.destroy() ),*
                }
            }
        }
    };
}

create_either!(Either<A, B>);
create_either!(Either3<A, B, C>);
create_either!(Either4<A, B, C, D>);
