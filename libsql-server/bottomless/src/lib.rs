#![allow(non_snake_case)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(improper_ctypes)]

mod ffi;

pub mod replicator;

use crate::ffi::{libsql_wal_methods, sqlite3_file, sqlite3_vfs, PgHdr, Wal};
use std::ffi::c_void;

// Just heuristics, but should work for ~100% of cases
fn is_regular(vfs: *const sqlite3_vfs) -> bool {
    let vfs = unsafe { std::ffi::CStr::from_ptr((*vfs).zName) }
        .to_str()
        .unwrap_or("[error]");
    tracing::trace!("VFS: {}", vfs);
    vfs.starts_with("unix") || vfs.starts_with("win32")
}

macro_rules! block_on {
    ($runtime:expr, $e:expr) => {
        $runtime.block_on(async { $e.await })
    };
}

fn is_local() -> bool {
    std::env::var("LIBSQL_BOTTOMLESS_LOCAL").map_or(false, |local| {
        local.eq_ignore_ascii_case("true")
            || local.eq_ignore_ascii_case("t")
            || local.eq_ignore_ascii_case("yes")
            || local.eq_ignore_ascii_case("y")
            || local == "1"
    })
}

pub extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    db_file: *mut sqlite3_file,
    wal_name: *const i8,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *mut Wal,
) -> i32 {
    tracing::debug!("Opening WAL {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });

    let orig_methods = unsafe { &*(*methods).underlying_methods };
    let rc = (orig_methods.xOpen)(vfs, db_file, wal_name, no_shm_mode, max_size, methods, wal);
    if rc != ffi::SQLITE_OK {
        return rc;
    }

    if !is_regular(vfs) {
        tracing::error!("Bottomless WAL is currently only supported for regular VFS");
        return ffi::SQLITE_CANTOPEN;
    }

    if is_local() {
        tracing::info!("Running in local-mode only, without any replication");
        return ffi::SQLITE_OK;
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("Failed to initialize async runtime: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    let replicator = block_on!(runtime, replicator::Replicator::new());
    let mut replicator = match replicator {
        Ok(repl) => repl,
        Err(e) => {
            tracing::error!("Failed to initialize replicator: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    let path = unsafe {
        match std::ffi::CStr::from_ptr(wal_name).to_str() {
            Ok(path) if path.len() >= 4 => &path[..path.len() - 4],
            Ok(path) => path,
            Err(e) => {
                tracing::error!("Failed to parse the main database path: {}", e);
                return ffi::SQLITE_CANTOPEN;
            }
        }
    };

    replicator.register_db(path);
    let rc = block_on!(runtime, try_restore(&mut replicator));
    if rc != ffi::SQLITE_OK {
        return rc;
    }

    let context = replicator::Context {
        replicator,
        runtime,
    };
    unsafe { (*(*wal)).replicator_context = Box::leak(Box::new(context)) };

    ffi::SQLITE_OK
}

fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    unsafe { &*((*(*wal).wal_methods).underlying_methods) }
}

fn get_replicator_context(wal: *mut Wal) -> &'static mut replicator::Context {
    unsafe { &mut *((*wal).replicator_context) }
}

pub extern "C" fn xClose(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
) -> i32 {
    tracing::debug!("Closing wal");
    let orig_methods = get_orig_methods(wal);
    if !is_local() {
        let _replicator_box = unsafe { Box::from_raw((*wal).replicator_context) };
    }

    (orig_methods.xClose)(wal, db, sync_flags, n_buf, z_buf)
}

pub extern "C" fn xLimit(wal: *mut Wal, limit: i64) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xLimit)(wal, limit)
}

pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginReadTransaction)(wal, changed)
}

pub extern "C" fn xEndReadTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndReadTransaction)(wal)
}

pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFindFrame)(wal, pgno, frame)
}

pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xReadFrame)(wal, frame, n_out, p_out)
}

pub extern "C" fn xDbSize(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDbSize)(wal)
}

pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xBeginWriteTransaction)(wal)
}

pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xEndWriteTransaction)(wal)
}

pub extern "C" fn xUndo(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = get_orig_methods(wal);
    let rc = (orig_methods.xUndo)(wal, func, ctx);
    if is_local() || rc != ffi::SQLITE_OK {
        return rc;
    }

    let last_valid_frame = unsafe { (*wal).hdr.last_valid_frame };
    let ctx = get_replicator_context(wal);
    tracing::trace!(
        "Undo: rolling back from frame {} to {}",
        ctx.replicator.peek_last_valid_frame(),
        last_valid_frame
    );
    ctx.replicator.rollback_to_frame(last_valid_frame);

    ffi::SQLITE_OK
}

pub extern "C" fn xSavepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xSavepoint)(wal, wal_data)
}

pub extern "C" fn xSavepointUndo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = get_orig_methods(wal);
    let rc = (orig_methods.xSavepointUndo)(wal, wal_data);
    if is_local() || rc != ffi::SQLITE_OK {
        return rc;
    }

    let last_valid_frame = unsafe { *wal_data };
    let ctx = get_replicator_context(wal);
    tracing::trace!(
        "Savepoint: rolling back from frame {} to {}",
        ctx.replicator.peek_last_valid_frame(),
        last_valid_frame
    );
    ctx.replicator.rollback_to_frame(last_valid_frame);

    ffi::SQLITE_OK
}

pub extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *const PgHdr,
    size_after: u32,
    is_commit: i32,
    sync_flags: i32,
) -> i32 {
    let mut last_consistent_frame = 0;
    if !is_local() {
        let ctx = get_replicator_context(wal);
        let last_valid_frame = unsafe { (*wal).hdr.last_valid_frame };
        ctx.replicator.register_last_valid_frame(last_valid_frame);
        // In theory it's enough to set the page size only once, but in practice
        // it's a very cheap operation anyway, and the page is not always known
        // upfront and can change dynamically.
        // FIXME: changing the page size in the middle of operation is *not*
        // supported by bottomless storage.
        if let Err(e) = ctx.replicator.set_page_size(page_size as usize) {
            tracing::error!("{}", e);
            return ffi::SQLITE_IOERR_WRITE;
        }
        for (pgno, data) in ffi::PageHdrIter::new(page_headers, page_size as usize) {
            ctx.replicator.write(pgno, data);
        }

        // TODO: flushing can be done even if is_commit == 0, in order to drain
        // the local cache and free its memory. However, that complicates rollbacks (xUndo),
        // because the flushed-but-not-committed pages should be removed from the remote
        // location. It's not complicated, but potentially costly in terms of latency,
        // so for now it is not yet implemented.
        if is_commit != 0 {
            last_consistent_frame = match block_on!(ctx.runtime, ctx.replicator.flush()) {
                Ok(frame) => frame,
                Err(e) => {
                    tracing::error!("Failed to replicate: {}", e);
                    return ffi::SQLITE_IOERR_WRITE;
                }
            };
        }
    }

    let orig_methods = get_orig_methods(wal);
    let rc = (orig_methods.xFrames)(
        wal,
        page_size,
        page_headers,
        size_after,
        is_commit,
        sync_flags,
    );
    if is_local() || rc != ffi::SQLITE_OK {
        return rc;
    }

    let ctx = get_replicator_context(wal);
    if is_commit != 0 {
        let frame_checksum = unsafe { (*wal).hdr.frame_checksum };

        if let Err(e) = block_on!(
            ctx.runtime,
            ctx.replicator
                .finalize_commit(last_consistent_frame, frame_checksum)
        ) {
            tracing::error!("Failed to finalize replication: {}", e);
            return ffi::SQLITE_IOERR_WRITE;
        }
    }

    ffi::SQLITE_OK
}

extern "C" fn always_wait(_busy_param: *mut c_void) -> i32 {
    std::thread::sleep(std::time::Duration::from_millis(10));
    1
}

#[tracing::instrument(skip(wal, db, busy_handler, busy_arg))]
pub extern "C" fn xCheckpoint(
    wal: *mut Wal,
    db: *mut c_void,
    emode: i32,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    busy_arg: *const c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
    frames_in_wal: *mut i32,
    backfilled_frames: *mut i32,
) -> i32 {
    tracing::trace!("Checkpoint");

    /* In order to avoid partial checkpoints, passive checkpoint
     ** mode is not allowed. Only TRUNCATE checkpoints are accepted,
     ** because these are guaranteed to block writes, copy all WAL pages
     ** back into the main database file and reset the frame number.
     ** In order to make this mechanism work smoothly with the final
     ** checkpoint on WAL close as well as default autocheckpoints,
     ** the mode is unconditionally upgraded to SQLITE_CHECKPOINT_TRUNCATE.
     ** An alternative to consider is to just refuse weaker checkpoints.
     */
    let emode = if emode < ffi::SQLITE_CHECKPOINT_TRUNCATE {
        tracing::trace!("Upgrading checkpoint to TRUNCATE mode");
        ffi::SQLITE_CHECKPOINT_TRUNCATE
    } else {
        emode
    };
    /* If there's no busy handler, let's provide a default one,
     ** since we auto-upgrade the passive checkpoint
     */
    let busy_handler = if (busy_handler as *const c_void).is_null() {
        tracing::trace!("Falling back to the default busy handler - always wait");
        always_wait
    } else {
        busy_handler
    };

    let orig_methods = get_orig_methods(wal);
    let rc = (orig_methods.xCheckpoint)(
        wal,
        db,
        emode,
        busy_handler,
        busy_arg,
        sync_flags,
        n_buf,
        z_buf,
        frames_in_wal,
        backfilled_frames,
    );

    if is_local() || rc != ffi::SQLITE_OK {
        return rc;
    }

    let ctx = get_replicator_context(wal);
    if ctx.replicator.commits_in_current_generation == 0 {
        tracing::debug!("No commits happened in this generation, not snapshotting");
        return ffi::SQLITE_OK;
    }

    ctx.replicator.new_generation();
    tracing::debug!("Snapshotting after checkpoint");
    let result = block_on!(ctx.runtime, ctx.replicator.snapshot_main_db_file());
    if let Err(e) = result {
        tracing::error!(
            "Failed to snapshot the main db file during checkpoint: {}",
            e
        );
        return ffi::SQLITE_IOERR_WRITE;
    }

    ffi::SQLITE_OK
}

pub extern "C" fn xCallback(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xCallback)(wal)
}

pub extern "C" fn xExclusiveMode(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xExclusiveMode)(wal)
}

pub extern "C" fn xHeapMemory(wal: *mut Wal) -> i32 {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xHeapMemory)(wal)
}

pub extern "C" fn xFile(wal: *mut Wal) -> *const c_void {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xFile)(wal)
}

pub extern "C" fn xDb(wal: *mut Wal, db: *const c_void) {
    let orig_methods = get_orig_methods(wal);
    (orig_methods.xDb)(wal, db)
}

pub extern "C" fn xPathnameLen(orig_len: i32) -> i32 {
    orig_len + 4
}

pub extern "C" fn xGetPathname(buf: *mut u8, orig: *const u8, orig_len: i32) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
    unsafe { std::ptr::copy("-wal".as_ptr(), buf.offset(orig_len as isize), 4) }
}

async fn try_restore(replicator: &mut replicator::Replicator) -> i32 {
    match replicator.restore().await {
        Ok(replicator::RestoreAction::None) => (),
        Ok(replicator::RestoreAction::SnapshotMainDbFile) => {
            replicator.new_generation();
            if let Err(e) = replicator.snapshot_main_db_file().await {
                tracing::error!("Failed to snapshot the main db file: {}", e);
                return ffi::SQLITE_CANTOPEN;
            }
            // Restoration process only leaves the local WAL file if it was
            // detected to be newer than its remote counterpart.
            if let Err(e) = replicator.maybe_replicate_wal().await {
                tracing::error!("Failed to replicate local WAL: {}", e);
                return ffi::SQLITE_CANTOPEN;
            }
        }
        Ok(replicator::RestoreAction::ReuseGeneration(gen)) => {
            replicator.set_generation(gen);
        }
        Err(e) => {
            tracing::error!("Failed to restore the database: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    }

    ffi::SQLITE_OK
}

pub extern "C" fn xPreMainDbOpen(_methods: *mut libsql_wal_methods, path: *const i8) -> i32 {
    if is_local() {
        tracing::info!("Running in local-mode only, without any replication");
        return ffi::SQLITE_OK;
    }

    if path.is_null() {
        return ffi::SQLITE_OK;
    }
    let path = unsafe {
        match std::ffi::CStr::from_ptr(path).to_str() {
            Ok(path) => path,
            Err(e) => {
                tracing::error!("Failed to parse the main database path: {}", e);
                return ffi::SQLITE_CANTOPEN;
            }
        }
    };
    tracing::debug!("Main database file {} will be open soon", path);

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("Failed to initialize async runtime: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    let replicator = block_on!(
        runtime,
        replicator::Replicator::create(replicator::Options {
            create_bucket_if_not_exists: true,
            verify_crc: true,
        })
    );
    let mut replicator = match replicator {
        Ok(repl) => repl,
        Err(e) => {
            tracing::error!("Failed to initialize replicator: {}", e);
            return ffi::SQLITE_CANTOPEN;
        }
    };

    replicator.register_db(path);
    block_on!(runtime, try_restore(&mut replicator))
}

#[no_mangle]
pub extern "C" fn bottomless_init() {
    tracing::debug!("bottomless module initialized");
}

#[no_mangle]
pub extern "C" fn bottomless_tracing_init() {
    tracing_subscriber::fmt::init();
}

#[tracing::instrument]
#[no_mangle]
pub extern "C" fn bottomless_methods(
    underlying_methods: *const libsql_wal_methods,
) -> *const libsql_wal_methods {
    let vwal_name: *const u8 = "bottomless\0".as_ptr();

    Box::into_raw(Box::new(libsql_wal_methods {
        iVersion: 1,
        xOpen,
        xClose,
        xLimit,
        xBeginReadTransaction,
        xEndReadTransaction,
        xFindFrame,
        xReadFrame,
        xDbSize,
        xBeginWriteTransaction,
        xEndWriteTransaction,
        xUndo,
        xSavepoint,
        xSavepointUndo,
        xFrames,
        xCheckpoint,
        xCallback,
        xExclusiveMode,
        xHeapMemory,
        snapshot_get_stub: std::ptr::null(),
        snapshot_open_stub: std::ptr::null(),
        snapshot_recover_stub: std::ptr::null(),
        snapshot_check_stub: std::ptr::null(),
        snapshot_unlock_stub: std::ptr::null(),
        framesize_stub: std::ptr::null(),
        xFile,
        write_lock_stub: std::ptr::null(),
        xDb,
        xPathnameLen,
        xGetPathname,
        xPreMainDbOpen,
        name: vwal_name,
        b_uses_shm: 0,
        p_next: std::ptr::null(),
        underlying_methods,
    }))
}

#[cfg(feature = "libsql_linked_statically")]
pub mod static_init {
    use crate::libsql_wal_methods;

    extern "C" {
        fn libsql_wal_methods_find(name: *const std::ffi::c_char) -> *const libsql_wal_methods;
        fn libsql_wal_methods_register(methods: *const libsql_wal_methods) -> i32;
    }

    pub fn register_bottomless_methods() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            crate::bottomless_init();
            let orig_methods = unsafe { libsql_wal_methods_find(std::ptr::null()) };
            if orig_methods.is_null() {}
            let methods = crate::bottomless_methods(orig_methods);
            let rc = unsafe { libsql_wal_methods_register(methods) };
            if rc != crate::ffi::SQLITE_OK {
                let _box = unsafe { Box::from_raw(methods as *mut libsql_wal_methods) };
                tracing::warn!("Failed to instantiate bottomless WAL methods");
            }
        })
    }
}
