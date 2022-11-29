use log::trace;
use rusqlite::Connection;
use std::ffi::c_void;

#[repr(C)]
pub struct Wal {
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
    // if debug defined: log_error
    // if snapshot defined: p_snapshot
    // if setlk defined: *db
    wal_methods: *mut libsql_wal_methods,
}

// Only here for creating a Wal struct instance, we're not going to use it
#[repr(C)]
pub struct WalIndexHdr {
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
        wal: *mut Wal,
        func: extern "C" fn(*mut c_void, i32) -> i32,
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
    // snapshot: get, open, recover, check, unlock
    // enable_zipvfs: framesize
    file: extern "C" fn(wal: *mut Wal) -> *const c_void,
    db: extern "C" fn(wal: *mut Wal, db: *const c_void),
    pathname_len: extern "C" fn(orig_len: i32) -> i32,
    get_pathname: extern "C" fn(buf: *mut u8, orig: *const u8, orig_len: i32),
    b_uses_shm: i32,
    name: *const u8,
    p_next: *const c_void,

    // User data
    underlying_methods: *const libsql_wal_methods,
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
        flags: std::ffi::c_int,
        vfs: *const u8,
        wal: *const u8,
    ) -> i32;
    fn libsql_wal_methods_register(wal_methods: *const libsql_wal_methods) -> i32;
    fn libsql_wal_methods_find(name: *const u8) -> *const libsql_wal_methods;
}

extern "C" fn open(
    vfs: *const c_void,
    file: *const c_void,
    wal_name: *const u8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32 {
    trace!("Opening {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name as *const i8)
            .to_str()
            .unwrap()
    });
    let orig_methods = unsafe { &*(*methods).underlying_methods };
    (orig_methods.open)(vfs, file, wal_name, no_shm_mode, max_size, methods, wal)
}

fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    unsafe { &*((*(*wal).wal_methods).underlying_methods) }
}

extern "C" fn close(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.close)(wal, db, sync_flags, n_buf, z_buf)
}

extern "C" fn limit(wal: *mut Wal, limit: i64) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.limit)(wal, limit)
}

extern "C" fn begin_read(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.begin_read)(wal, changed)
}

extern "C" fn end_read(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.end_read)(wal)
}

extern "C" fn find_frame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.find_frame)(wal, pgno, frame)
}

extern "C" fn read_frame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.read_frame)(wal, frame, n_out, p_out)
}

extern "C" fn db_size(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.db_size)(wal)
}

extern "C" fn begin_write(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.begin_write)(wal)
}

extern "C" fn end_write(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.end_write)(wal)
}

extern "C" fn undo(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.undo)(wal, func, ctx)
}

extern "C" fn savepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.savepoint)(wal, wal_data)
}

extern "C" fn savepoint_undo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.savepoint_undo)(wal, wal_data)
}

fn print_frames(page_headers: *const PgHdr) {
    let mut current_ptr = page_headers;
    loop {
        let current: &PgHdr = unsafe { &*current_ptr };
        trace!("page {} written to WAL", current.pgno);
        if current.dirty.is_null() {
            break;
        }
        current_ptr = current.dirty
    }
}

extern "C" fn frames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: i32,
    is_commit: i32,
    sync_flags: i32,
) -> i32 {
    print_frames(page_headers);
    let orig_methods = get_orig_methods(wal);
    (orig_methods.frames)(
        wal,
        page_size,
        page_headers,
        size_after,
        is_commit,
        sync_flags,
    )
}

extern "C" fn checkpoint(
    wal: *mut Wal,
    db: *mut c_void,
    emode: i32,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
    frames_in_wal: *mut i32,
    backfilled_frames: *mut i32,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.checkpoint)(
        wal,
        db,
        emode,
        busy_handler,
        sync_flags,
        n_buf,
        z_buf,
        frames_in_wal,
        backfilled_frames,
    )
}

extern "C" fn callback(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.callback)(wal)
}

extern "C" fn exclusive_mode(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.exclusive_mode)(wal)
}

extern "C" fn heap_memory(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.heap_memory)(wal)
}

extern "C" fn file(wal: *mut Wal) -> *const c_void {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.file)(wal)
}

extern "C" fn db(wal: *mut Wal, db: *const c_void) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.db)(wal, db)
}

extern "C" fn pathname_len(orig_len: i32) -> i32 {
    orig_len
}

extern "C" fn get_pathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
}

pub struct WalConnection {
    inner: rusqlite::Connection,
    wal_methods: std::sync::atomic::AtomicPtr<libsql_wal_methods>,
}

impl std::ops::Deref for WalConnection {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::Drop for WalConnection {
    fn drop(&mut self) {
        unsafe {
            rusqlite::ffi::sqlite3_close(self.inner.handle());
        }
        let _ = self.inner;
        let _ = unsafe { Box::from_raw(self.wal_methods.get_mut()) };
    }
}

pub(crate) fn open_with_virtual_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
) -> rusqlite::Result<WalConnection> {
    let vwal_name: *const u8 = "edge_vwal\0".as_ptr();

    let vwal = Box::into_raw(Box::new(libsql_wal_methods {
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
        file,
        db,
        pathname_len,
        get_pathname,
        b_uses_shm: 0,
        name: vwal_name,
        p_next: std::ptr::null(),
        underlying_methods: unsafe { libsql_wal_methods_find(std::ptr::null()) },
    }));

    unsafe {
        let mut pdb: *mut rusqlite::ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut rusqlite::ffi::sqlite3 = &mut pdb;
        let register_err = libsql_wal_methods_register(vwal);
        assert_eq!(register_err, 0);
        let open_err = libsql_open(
            path.as_ref()
                .as_os_str()
                .to_str()
                .ok_or(rusqlite::Error::InvalidQuery)?
                .as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            vwal_name,
        );
        assert_eq!(open_err, 0);
        let conn = Connection::from_handle(pdb)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        trace!(
            "Opening a connection with virtual WAL at {}",
            path.as_ref().display()
        );
        Ok(WalConnection {
            inner: conn,
            wal_methods: std::sync::atomic::AtomicPtr::new(vwal),
        })
    }
}
