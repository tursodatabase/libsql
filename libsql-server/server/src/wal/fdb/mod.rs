use std::ffi::c_void;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Mutex};

use rusqlite::ffi;

pub mod replicator;

const WAL_NORMAL_MODE: u8 = 0;
const WAL_EXCLUSIVE_MODE: u8 = 1;
const WAL_HEAPMEMORY_MODE: u8 = 2;

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
    wal_methods: *mut WalMethods,
}

#[repr(C)]
pub struct WalMethods {
    open: extern "C" fn(
        vfs: *const c_void,
        file: *const c_void,
        wal_name: *const u8,
        no_shm_mode: i32,
        max_size: i64,
        methods: *mut WalMethods,
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
    find_frame: extern "C" fn(wal: *mut Wal, pgno: i32, frame: *mut u32) -> i32,
    read_frame: extern "C" fn(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32,
    db_size: extern "C" fn(wal: *mut Wal) -> u32,
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
        size_after: u32,
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
    exclusive_mode: extern "C" fn(wal: *mut Wal, op: i32) -> i32,
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
    replicator: replicator::Replicator,
}

// Only safe if we consider passing pointers from C safe to Send
unsafe impl Send for WalMethods {}

#[repr(C)]
pub(crate) struct PgHdr {
    page: *const c_void,
    data: *const c_void,
    extra: *const c_void,
    pcache: *const c_void,
    dirty: *const PgHdr,
    pager: *const c_void,
    pgno: i32,
    flags: u16,
}

extern "C" fn open(
    vfs: *const c_void,
    _file: *const c_void,
    wal_name: *const u8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut WalMethods,
    wal: *mut *const Wal,
) -> i32 {
    tracing::debug!("Opening {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name as *const i8)
            .to_str()
            .unwrap()
    });
    let exclusive_mode = if no_shm_mode != 0 {
        WAL_HEAPMEMORY_MODE
    } else {
        WAL_NORMAL_MODE
    };
    let new_wal = Box::into_raw(Box::new(Wal {
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
        exclusive_mode,
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
        wal_methods: methods,
    }));
    unsafe { *wal = new_wal }
    tracing::debug!("Opened WAL at {:?}", new_wal);
    ffi::SQLITE_OK
}

fn get_methods(wal: *mut Wal) -> &'static mut WalMethods {
    unsafe { &mut *(*wal).wal_methods }
}

extern "C" {
    fn libsql_wal_methods_register(wal_methods: *const WalMethods) -> i32;
}

extern "C" fn close(
    wal: *mut Wal,
    _db: *mut c_void,
    _sync_flags: i32,
    _n_buf: i32,
    _z_buf: *mut u8,
) -> i32 {
    let _ = unsafe { Box::from_raw(wal) };
    ffi::SQLITE_OK
}

extern "C" fn limit(_wal: *mut Wal, limit: i64) {
    tracing::debug!("Limit: {}", limit);
}

extern "C" fn begin_read(_wal: *mut Wal, changed: *mut i32) -> i32 {
    tracing::debug!("Read starts");
    unsafe { *changed = 1 }
    ffi::SQLITE_OK
}

extern "C" fn end_read(_wal: *mut Wal) -> i32 {
    tracing::debug!("Read ends");
    ffi::SQLITE_OK
}

extern "C" fn find_frame(wal: *mut Wal, pgno: i32, frame: *mut u32) -> i32 {
    let methods = get_methods(wal);
    let frameno = methods.replicator.find_frame(pgno);
    tracing::debug!("Page {} has frame {}", pgno, frameno);
    unsafe { *frame = frameno }
    ffi::SQLITE_OK
}

extern "C" fn read_frame(wal: *mut Wal, frameno: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let methods = get_methods(wal);
    let frame = methods.replicator.get_frame(frameno);
    tracing::debug!(
        "Frame {}:{} retrieved (n_out={})",
        frameno,
        frame.len(),
        n_out
    );
    unsafe {
        std::ptr::copy(
            frame.as_ptr(),
            p_out,
            std::cmp::min(frame.len(), n_out as usize),
        )
    };
    ffi::SQLITE_OK
}

extern "C" fn db_size(wal: *mut Wal) -> u32 {
    let methods = get_methods(wal);
    methods.replicator.get_number_of_pages()
}

extern "C" fn begin_write(_wal: *mut Wal) -> i32 {
    tracing::debug!("Write starts");
    ffi::SQLITE_OK
}

extern "C" fn end_write(_wal: *mut Wal) -> i32 {
    tracing::debug!("Write ends");
    ffi::SQLITE_OK
}

extern "C" fn undo(
    wal: *mut Wal,
    _func: extern "C" fn(*mut c_void, i32) -> i32,
    _ctx: *mut c_void,
) -> i32 {
    let methods = get_methods(wal);
    methods.replicator.clear_pending();
    ffi::SQLITE_OK
}

extern "C" fn savepoint(_wal: *mut Wal, _wal_data: *mut u32) {
    tracing::debug!("Savepoint called!");
}

extern "C" fn savepoint_undo(_wal: *mut Wal, _wal_data: *mut u32) -> i32 {
    tracing::debug!("Savepoint-undo called!");
    ffi::SQLITE_MISUSE
}

pub(crate) struct PageHdrIter {
    current_ptr: *const PgHdr,
    page_size: usize,
}

impl PageHdrIter {
    fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
        }
    }
}

impl std::iter::Iterator for PageHdrIter {
    type Item = (i32, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.data as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data.to_vec()));
        self.current_ptr = current_hdr.dirty;
        item
    }
}

extern "C" fn frames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: u32,
    is_commit: i32,
    _sync_flags: i32,
) -> i32 {
    let is_commit = is_commit != 0;
    let methods = get_methods(wal);
    unsafe { (*wal).page_size = page_size };
    let pages_iter = PageHdrIter::new(page_headers, page_size as usize);
    methods.replicator.add_pending(pages_iter);
    tracing::debug!(
        "Commit? {:?} {}, size_after = {}",
        wal,
        is_commit,
        size_after
    );
    if is_commit && size_after > 0 {
        methods.replicator.flush_pending_pages();
        methods.replicator.set_number_of_pages(size_after);
    }
    ffi::SQLITE_OK
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
    tracing::debug!("Checkpoint called!");
    ffi::SQLITE_MISUSE
}

extern "C" fn callback(_wal: *mut Wal) -> i32 {
    tracing::debug!("Callback called!");
    ffi::SQLITE_MISUSE
}

fn get_mode(wal: *mut Wal) -> u8 {
    unsafe { (*wal).exclusive_mode }
}

fn set_mode(wal: *mut Wal, mode: u8) {
    unsafe { (*wal).exclusive_mode = mode }
}

extern "C" fn exclusive_mode(wal: *mut Wal, op: i32) -> i32 {
    if op == 0 {
        if get_mode(wal) != WAL_NORMAL_MODE {
            set_mode(wal, WAL_NORMAL_MODE);
            //FIXME: copy locking implementation from wal.c
            // and potentially base it on foundationdb input
        }
        if get_mode(wal) == WAL_NORMAL_MODE {
            0
        } else {
            1
        }
    } else if op > 0 {
        set_mode(wal, WAL_EXCLUSIVE_MODE);
        1
    } else if get_mode(wal) == WAL_NORMAL_MODE {
        0
    } else {
        1
    }
}

extern "C" fn heap_memory(_wal: *mut Wal) -> i32 {
    42
}

extern "C" fn file(_wal: *mut Wal) -> *const c_void {
    tracing::debug!("file() called!");
    std::ptr::null()
}

extern "C" fn db(_wal: *mut Wal, _db: *const c_void) {
    tracing::debug!("db() called!");
}

extern "C" fn pathname_len(orig_len: i32) -> i32 {
    orig_len
}

extern "C" fn get_pathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
}

#[cfg(feature = "fdb")]
impl WalMethods {
    pub(crate) fn new(fdb_config_path: Option<String>) -> anyhow::Result<Self> {
        let vwal_name: *const u8 = "edge_vwal\0".as_ptr();
        let vwal = WalMethods {
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
            replicator: replicator::Replicator::new(fdb_config_path)?,
        };
        Ok(vwal)
    }
}

#[cfg(feature = "fdb")]
pub(crate) fn open_with_virtual_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    vwal_methods: Arc<Mutex<WalMethods>>,
) -> anyhow::Result<super::WalConnection> {
    let mut vwal_methods = vwal_methods.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
    vwal_methods.replicator.load_top_frameno();
    unsafe {
        let mut pdb: *mut ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut ffi::sqlite3 = &mut pdb;
        let register_err = libsql_wal_methods_register(&mut *vwal_methods as *mut WalMethods);
        assert_eq!(register_err, 0);
        let open_err = super::libsql_open(
            path.as_ref().as_os_str().as_bytes().as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            vwal_methods.name,
        );
        assert_eq!(open_err, 0);
        let conn = super::Connection::from_handle(pdb)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        tracing::trace!(
            "Opening a connection with virtual WAL at {}",
            path.as_ref().display()
        );
        Ok(super::WalConnection { inner: conn })
    }
}
