#![allow(improper_ctypes)]
#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use std::ffi::c_void;

    const ERR_MISUSE: i32 = 21;

    #[repr(C)]
    struct Wal {
        vfs: *const c_void,
        db_fd: *const c_void,
        wal_fd: *const c_void,
        callback_value: u32,
        max_wal_size: i64,
        wi_data: i32,
        size_first_block: i32,
        ap_wi_data: *const *mut u32,
        page_size: u32,
        read_lock: i16,
        sync_flags: u8,
        exclusive_mode: u8,
        write_lock: u8,
        checkpoint_lock: u8,
        read_only: u8,
        truncate_on_commit: u8,
        sync_header: u8,
        pad_to_section_boundary: u8,
        b_shm_unreliable: u8,
        hdr: WalIndexHdr,
        min_frame: u32,
        recalculate_checksums: u32,
        wal_name: *const u8,
        n_checkpoints: u32,
        lock_error: u8,
        p_snapshot: *const c_void,
        p_db: *const c_void,
        wal_methods: *mut libsql_wal_methods,
        p_methods_data: *mut c_void,
    }

    #[repr(C)]
    struct WalIndexHdr {
        version: u32,
        unused: u32,
        change: u32,
        is_init: u8,
        big_endian_checksum: u8,
        page_size: u16,
        last_valid_frame: u32,
        n_pages: u32,
        frame_checksum: [u32; 2],
        salt: [u32; 2],
        checksum: [u32; 2],
    }

    #[repr(C)]
    struct libsql_wal_methods {
        iversion: i32,
        open: extern "C" fn(
            vfs: *const c_void,
            file: *const c_void,
            wal_name: *const u8,
            no_shm_mode: i32,
            max_size: i64,
            methods: *mut libsql_wal_methods,
            wal: *mut *const Wal,
        ) -> i32,
        close: extern "C" fn(
            wal: *mut Wal,
            db: *mut c_void,
            sync_flags: i32,
            n_buf: i32,
            z_buf: *mut u8,
        ) -> i32,
        limit: extern "C" fn(wal: *mut Wal, limit: i64),
        begin_read: extern "C" fn(wal: *mut Wal, changed: *mut i32) -> i32,
        end_read: extern "C" fn(wal: *mut Wal) -> i32,
        find_frame: extern "C" fn(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32,
        read_frame: extern "C" fn(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32,
        db_size: extern "C" fn(wal: *mut Wal) -> i32,
        begin_write: extern "C" fn(wal: *mut Wal) -> i32,
        end_write: extern "C" fn(wal: *mut Wal) -> i32,
        undo: extern "C" fn(
            wal: *const extern "C" fn(*mut c_void, i32) -> i32,
            ctx: *mut c_void,
        ) -> i32,
        savepoint: extern "C" fn(wal: *mut Wal, wal_data: *mut u32),
        savepoint_undo: extern "C" fn(wal: *mut Wal, wal_data: *mut u32) -> i32,
        frames: extern "C" fn(
            wal: *mut Wal,
            page_size: u32,
            page_headers: *const PgHdr,
            size_after: i32,
            is_commit: i32,
            sync_flags: i32,
        ) -> i32,
        checkpoint: extern "C" fn(
            wal: *mut Wal,
            db: *mut c_void,
            emode: i32,
            busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
            sync_flags: i32,
            n_buf: i32,
            z_buf: *mut u8,
            frames_in_wal: *mut i32,
            backfilled_frames: *mut i32,
        ) -> i32,
        callback: extern "C" fn(wal: *mut Wal) -> i32,
        exclusive_mode: extern "C" fn(wal: *mut Wal) -> i32,
        heap_memory: extern "C" fn(wal: *mut Wal) -> i32,
        // stubs, only useful with snapshot support compiled-in
        snapshot_get_stub: *const c_void,
        snapshot_open_stub: *const c_void,
        snapshot_recover_stub: *const c_void,
        snapshot_check_stub: *const c_void,
        snapshot_unlock_stub: *const c_void,
        // stub, only useful with zipfs support compiled-in
        framesize_stub: *const c_void,
        file: extern "C" fn(wal: *mut Wal) -> *const c_void,
        // stub, only useful with setlk timeout compiled-in
        write_lock_stub: *const c_void,
        db: extern "C" fn(wal: *mut Wal, db: *const c_void),
        pathname_len: extern "C" fn(orig_len: i32) -> i32,
        get_pathname: extern "C" fn(buf: *mut u8, orig: *const u8, orig_len: i32),
        pre_main_db_open: extern "C" fn(methods: *mut libsql_wal_methods, name: *const i8) -> i32,
        b_uses_shm: i32,
        name: *const u8,
        p_next: *const c_void,

        // User data
        pages: std::collections::HashMap<i32, std::vec::Vec<u8>>,
    }

    #[repr(C)]
    struct PgHdr {
        page: *const c_void,
        data: *const c_void,
        extra: *const c_void,
        pcache: *const c_void,
        dirty: *const PgHdr,
        pager: *const c_void,
        pgno: i32,
        flags: u16,
    }

    extern "C" {
        fn libsql_open(
            filename: *const u8,
            ppdb: *mut *mut rusqlite::ffi::sqlite3,
            flags: i32,
            vfs: *const u8,
            wal: *const u8,
        ) -> i32;
        fn libsql_wal_methods_register(wal_methods: *const libsql_wal_methods) -> i32;
        fn sqlite3_initialize();
    }

    extern "C" fn open(
        vfs: *const c_void,
        _file: *const c_void,
        wal_name: *const u8,
        _no_shm_mode: i32,
        max_size: i64,
        methods: *mut libsql_wal_methods,
        wal: *mut *const Wal,
    ) -> i32 {
        let new_wal = Box::new(Wal {
            vfs,
            db_fd: std::ptr::null(),
            wal_fd: std::ptr::null(),
            callback_value: 0,
            max_wal_size: max_size,
            wi_data: 0,
            size_first_block: 0,
            ap_wi_data: std::ptr::null(),
            page_size: 4096,
            read_lock: 0,
            sync_flags: 0,
            exclusive_mode: 1,
            write_lock: 0,
            checkpoint_lock: 0,
            read_only: 0,
            truncate_on_commit: 0,
            sync_header: 0,
            pad_to_section_boundary: 0,
            b_shm_unreliable: 1,
            hdr: WalIndexHdr {
                version: 1,
                unused: 0,
                change: 0,
                is_init: 0,
                big_endian_checksum: 0,
                page_size: 4096,
                last_valid_frame: 1,
                n_pages: 1,
                frame_checksum: [0, 0],
                salt: [0, 0],
                checksum: [0, 0],
            },
            min_frame: 0,
            recalculate_checksums: 0,
            wal_name,
            n_checkpoints: 0,
            lock_error: 0,
            p_snapshot: std::ptr::null(),
            p_db: std::ptr::null(),
            wal_methods: methods,
            p_methods_data: std::ptr::null_mut(),
        });
        unsafe { *wal = &*new_wal }
        Box::leak(new_wal);
        0
    }
    extern "C" fn close(
        _wal: *mut Wal,
        _db: *mut c_void,
        _sync_flags: i32,
        _n_buf: i32,
        _z_buf: *mut u8,
    ) -> i32 {
        println!("Closing WAL");
        0
    }
    extern "C" fn limit(wal: *mut Wal, limit: i64) {
        println!("Limit: {limit}");
        unsafe { (*wal).max_wal_size = limit }
    }
    extern "C" fn begin_read(_wal: *mut Wal, changed: *mut i32) -> i32 {
        println!("Read started");
        unsafe { *changed = 1 }
        0
    }
    extern "C" fn end_read(_wal: *mut Wal) -> i32 {
        println!("Read ended");
        0
    }
    extern "C" fn find_frame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
        println!("\tLooking for page {pgno}");
        let methods = unsafe { &*(*wal).wal_methods };
        if methods.pages.contains_key(&pgno) {
            println!("\t\tpage found");
            unsafe { *frame = pgno };
        } else {
            println!("\t\tpage not found - serving from the main database file");
        }
        0
    }
    extern "C" fn read_frame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
        println!("\tReading frame {frame}");
        let n_out = n_out as usize;
        let methods = unsafe { &*(*wal).wal_methods };
        let data = methods.pages.get(&(frame as i32)).unwrap();
        if n_out < data.len() {
            return ERR_MISUSE;
        }
        let out_buffer = unsafe { std::slice::from_raw_parts_mut(p_out, n_out) };
        out_buffer.copy_from_slice(data);
        println!("\t\tread {} bytes", data.len());
        0
    }
    extern "C" fn db_size(wal: *mut Wal) -> i32 {
        println!("Db size called");
        let methods = unsafe { &*(*wal).wal_methods };
        methods.pages.len() as i32
    }
    extern "C" fn begin_write(_wal: *mut Wal) -> i32 {
        println!("Write started");
        0
    }
    extern "C" fn end_write(_wal: *mut Wal) -> i32 {
        println!("Write ended");
        0
    }
    extern "C" fn undo(
        _wal: *const extern "C" fn(*mut c_void, i32) -> i32,
        _ctx: *mut c_void,
    ) -> i32 {
        panic!("Not implemented")
    }
    extern "C" fn savepoint(_wal: *mut Wal, _wal_data: *mut u32) {
        panic!("Not implemented")
    }
    extern "C" fn savepoint_undo(_wal: *mut Wal, _wal_data: *mut u32) -> i32 {
        panic!("Not implemented")
    }
    extern "C" fn frames(
        wal: *mut Wal,
        page_size: u32,
        page_headers: *const PgHdr,
        _size_after: i32,
        _is_commit: i32,
        _sync_flags: i32,
    ) -> i32 {
        println!("\tWriting frames...");
        unsafe { (*wal).page_size = page_size };
        let methods = unsafe { &mut *(*wal).wal_methods };
        let mut current_ptr = page_headers;
        loop {
            let current: &PgHdr = unsafe { &*current_ptr };
            println!("\t\tpage {} written", current.pgno);
            let data = unsafe {
                std::slice::from_raw_parts(current.data as *const u8, page_size as usize)
            }
            .to_vec();
            methods.pages.insert(current.pgno, data);
            if current.dirty.is_null() {
                break;
            }
            current_ptr = current.dirty
        }
        0
    }
    extern "C" fn checkpoint(
        _wal: *mut Wal,
        _db: *mut c_void,
        _emode: i32,
        _busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
        _sync_flags: i32,
        _n_buf: i32,
        _z_buf: *mut u8,
        _frames_in_wal: *mut i32,
        _backfilled_frames: *mut i32,
    ) -> i32 {
        println!("Checkpointed");
        0
    }
    extern "C" fn callback(_wal: *mut Wal) -> i32 {
        ERR_MISUSE
    }
    extern "C" fn exclusive_mode(_wal: *mut Wal) -> i32 {
        1
    }
    extern "C" fn heap_memory(wal: *mut Wal) -> i32 {
        unsafe { &*(*wal).wal_methods }.pages.len() as i32 * 64
    }
    extern "C" fn file(_wal: *mut Wal) -> *const c_void {
        panic!("Should never be called")
    }
    extern "C" fn db(_wal: *mut Wal, _db: *const c_void) {}
    extern "C" fn pathname_len(orig_len: i32) -> i32 {
        orig_len + 4
    }
    extern "C" fn get_pathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
        unsafe {
            std::ptr::copy_nonoverlapping(orig, buf, orig_len as usize);
            std::ptr::copy_nonoverlapping(".wal".as_ptr(), buf.offset(orig_len as isize), 4);
        }
    }
    extern "C" fn pre_main_db_open(_methods: *mut libsql_wal_methods, _name: *const i8) -> i32 {
        0
    }

    #[test]
    fn test_vwal_register() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = format!("{}\0", tmpfile.path().to_str().unwrap());
        println!("Temporary database created at {path}");

        let conn = unsafe {
            let mut pdb: *mut rusqlite::ffi::sqlite3 = std::ptr::null_mut();
            let ppdb: *mut *mut rusqlite::ffi::sqlite3 = &mut pdb;
            let mut vwal = Box::new(libsql_wal_methods {
                iversion: 1,
                open,
                close,
                limit,
                begin_read,
                end_read,
                find_frame,
                read_frame,
                db_size,
                begin_write,
                end_write,
                undo,
                savepoint,
                savepoint_undo,
                frames,
                checkpoint,
                callback,
                exclusive_mode,
                heap_memory,
                snapshot_get_stub: std::ptr::null(),
                snapshot_open_stub: std::ptr::null(),
                snapshot_recover_stub: std::ptr::null(),
                snapshot_check_stub: std::ptr::null(),
                snapshot_unlock_stub: std::ptr::null(),
                // stub, only useful with zipfs support compiled-in
                framesize_stub: std::ptr::null(),
                file,
                // stub, only useful with setlk timeout compiled-in
                write_lock_stub: std::ptr::null(),
                db,
                pathname_len,
                get_pathname,
                pre_main_db_open,
                b_uses_shm: 0,
                name: "vwal\0".as_ptr(),
                p_next: std::ptr::null(),
                pages: std::collections::HashMap::new(),
            });

            sqlite3_initialize();
            let register_err = libsql_wal_methods_register(&mut *vwal as *mut libsql_wal_methods);
            assert_eq!(register_err, 0);
            let open_err = libsql_open(path.as_ptr(), ppdb, 6, std::ptr::null(), "vwal\0".as_ptr());
            assert_eq!(open_err, 0);
            Box::leak(vwal);
            Connection::from_handle(pdb).unwrap()
        };
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
