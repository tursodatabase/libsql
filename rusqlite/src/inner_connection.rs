use std::ffi::CString;
use std::os::raw::{c_char, c_int};
#[cfg(feature = "load_extension")]
use std::path::Path;
use std::ptr;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::ffi;
use super::{str_for_sqlite, str_to_cstring};
use super::{Connection, InterruptHandle, OpenFlags, Result};
use crate::error::{error_from_handle, error_from_sqlite_code, Error};
use crate::raw_statement::RawStatement;
use crate::statement::Statement;
use crate::unlock_notify;
use crate::version::version_number;

pub struct InnerConnection {
    pub db: *mut ffi::sqlite3,
    // It's unsafe to call `sqlite3_close` while another thread is performing
    // a `sqlite3_interrupt`, and vice versa, so we take this mutex during
    // those functions. This protects a copy of the `db` pointer (which is
    // cleared on closing), however the main copy, `db`, is unprotected.
    // Otherwise, a long running query would prevent calling interrupt, as
    // interrupt would only acquire the lock after the query's completion.
    interrupt_lock: Arc<Mutex<*mut ffi::sqlite3>>,
    #[cfg(feature = "hooks")]
    pub free_commit_hook: Option<unsafe fn(*mut ::std::os::raw::c_void)>,
    #[cfg(feature = "hooks")]
    pub free_rollback_hook: Option<unsafe fn(*mut ::std::os::raw::c_void)>,
    #[cfg(feature = "hooks")]
    pub free_update_hook: Option<unsafe fn(*mut ::std::os::raw::c_void)>,
    owned: bool,
}

impl InnerConnection {
    #[allow(clippy::mutex_atomic)]
    pub unsafe fn new(db: *mut ffi::sqlite3, owned: bool) -> InnerConnection {
        InnerConnection {
            db,
            interrupt_lock: Arc::new(Mutex::new(db)),
            #[cfg(feature = "hooks")]
            free_commit_hook: None,
            #[cfg(feature = "hooks")]
            free_rollback_hook: None,
            #[cfg(feature = "hooks")]
            free_update_hook: None,
            owned,
        }
    }

    pub fn open_with_flags(
        c_path: &CString,
        flags: OpenFlags,
        vfs: Option<&CString>,
    ) -> Result<InnerConnection> {
        #[cfg(not(feature = "bundled"))]
        ensure_valid_sqlite_version();
        ensure_safe_sqlite_threading_mode()?;

        // Replicate the check for sane open flags from SQLite, because the check in
        // SQLite itself wasn't added until version 3.7.3.
        debug_assert_eq!(1 << OpenFlags::SQLITE_OPEN_READ_ONLY.bits, 0x02);
        debug_assert_eq!(1 << OpenFlags::SQLITE_OPEN_READ_WRITE.bits, 0x04);
        debug_assert_eq!(
            1 << (OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE).bits,
            0x40
        );
        if (1 << (flags.bits & 0x7)) & 0x46 == 0 {
            return Err(Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_MISUSE),
                None,
            ));
        }

        let z_vfs = match vfs {
            Some(c_vfs) => c_vfs.as_ptr(),
            None => ptr::null(),
        };

        unsafe {
            let mut db: *mut ffi::sqlite3 = ptr::null_mut();
            let r = ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags.bits(), z_vfs);
            if r != ffi::SQLITE_OK {
                let e = if db.is_null() {
                    error_from_sqlite_code(r, Some(c_path.to_string_lossy().to_string()))
                } else {
                    let mut e = error_from_handle(db, r);
                    if let Error::SqliteFailure(
                        ffi::Error {
                            code: ffi::ErrorCode::CannotOpen,
                            ..
                        },
                        Some(msg),
                    ) = e
                    {
                        e = Error::SqliteFailure(
                            ffi::Error::new(r),
                            Some(format!("{}: {}", msg, c_path.to_string_lossy())),
                        );
                    }
                    ffi::sqlite3_close(db);
                    e
                };

                return Err(e);
            }
            let r = ffi::sqlite3_busy_timeout(db, 5000);
            if r != ffi::SQLITE_OK {
                let e = error_from_handle(db, r);
                ffi::sqlite3_close(db);
                return Err(e);
            }

            // attempt to turn on extended results code; don't fail if we can't.
            ffi::sqlite3_extended_result_codes(db, 1);

            Ok(InnerConnection::new(db, true))
        }
    }

    pub fn db(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    pub fn decode_result(&mut self, code: c_int) -> Result<()> {
        unsafe { InnerConnection::decode_result_raw(self.db(), code) }
    }

    unsafe fn decode_result_raw(db: *mut ffi::sqlite3, code: c_int) -> Result<()> {
        if code == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(error_from_handle(db, code))
        }
    }

    #[allow(clippy::mutex_atomic)]
    pub fn close(&mut self) -> Result<()> {
        if self.db.is_null() {
            return Ok(());
        }
        self.remove_hooks();
        let mut shared_handle = self.interrupt_lock.lock().unwrap();
        assert!(
            !shared_handle.is_null(),
            "Bug: Somehow interrupt_lock was cleared before the DB was closed"
        );
        if !self.owned {
            self.db = ptr::null_mut();
            return Ok(());
        }
        unsafe {
            let r = ffi::sqlite3_close(self.db);
            // Need to use _raw because _guard has a reference out, and
            // decode_result takes &mut self.
            let r = InnerConnection::decode_result_raw(self.db, r);
            if r.is_ok() {
                *shared_handle = ptr::null_mut();
                self.db = ptr::null_mut();
            }
            r
        }
    }

    pub fn get_interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle {
            db_lock: Arc::clone(&self.interrupt_lock),
        }
    }

    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let c_sql = str_to_cstring(sql)?;
        unsafe {
            let r = ffi::sqlite3_exec(
                self.db(),
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            self.decode_result(r)
        }
    }

    #[cfg(feature = "load_extension")]
    pub fn enable_load_extension(&mut self, onoff: c_int) -> Result<()> {
        let r = unsafe { ffi::sqlite3_enable_load_extension(self.db, onoff) };
        self.decode_result(r)
    }

    #[cfg(feature = "load_extension")]
    pub fn load_extension(&self, dylib_path: &Path, entry_point: Option<&str>) -> Result<()> {
        let dylib_str = super::path_to_cstring(dylib_path)?;
        unsafe {
            let mut errmsg: *mut c_char = ptr::null_mut();
            let r = if let Some(entry_point) = entry_point {
                let c_entry = str_to_cstring(entry_point)?;
                ffi::sqlite3_load_extension(
                    self.db,
                    dylib_str.as_ptr(),
                    c_entry.as_ptr(),
                    &mut errmsg,
                )
            } else {
                ffi::sqlite3_load_extension(self.db, dylib_str.as_ptr(), ptr::null(), &mut errmsg)
            };
            if r == ffi::SQLITE_OK {
                Ok(())
            } else {
                let message = super::errmsg_to_string(errmsg);
                ffi::sqlite3_free(errmsg as *mut ::std::os::raw::c_void);
                Err(error_from_sqlite_code(r, Some(message)))
            }
        }
    }

    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db()) }
    }

    pub fn prepare<'a>(&mut self, conn: &'a Connection, sql: &str) -> Result<Statement<'a>> {
        let mut c_stmt = ptr::null_mut();
        let (c_sql, len, _) = str_for_sqlite(sql.as_bytes())?;
        let mut c_tail = ptr::null();
        let r = unsafe {
            if cfg!(feature = "unlock_notify") {
                let mut rc;
                loop {
                    rc = ffi::sqlite3_prepare_v2(
                        self.db(),
                        c_sql,
                        len,
                        &mut c_stmt as *mut *mut ffi::sqlite3_stmt,
                        &mut c_tail as *mut *const c_char,
                    );
                    if !unlock_notify::is_locked(self.db, rc) {
                        break;
                    }
                    rc = unlock_notify::wait_for_unlock_notify(self.db);
                    if rc != ffi::SQLITE_OK {
                        break;
                    }
                }
                rc
            } else {
                ffi::sqlite3_prepare_v2(
                    self.db(),
                    c_sql,
                    len,
                    &mut c_stmt as *mut *mut ffi::sqlite3_stmt,
                    &mut c_tail as *mut *const c_char,
                )
            }
        };
        // If there is an error, *ppStmt is set to NULL.
        self.decode_result(r)?;
        // If the input text contains no SQL (if the input is an empty string or a
        // comment) then *ppStmt is set to NULL.
        let c_stmt: *mut ffi::sqlite3_stmt = c_stmt;
        let c_tail: *const c_char = c_tail;
        // TODO ignore spaces, comments, ... at the end
        let tail = !c_tail.is_null() && unsafe { c_tail != c_sql.offset(len as isize) };
        Ok(Statement::new(conn, unsafe {
            RawStatement::new(c_stmt, tail)
        }))
    }

    pub fn changes(&mut self) -> usize {
        unsafe { ffi::sqlite3_changes(self.db()) as usize }
    }

    pub fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.db()) != 0 }
    }

    #[cfg(feature = "modern_sqlite")] // 3.8.6
    pub fn is_busy(&self) -> bool {
        let db = self.db();
        unsafe {
            let mut stmt = ffi::sqlite3_next_stmt(db, ptr::null_mut());
            while !stmt.is_null() {
                if ffi::sqlite3_stmt_busy(stmt) != 0 {
                    return true;
                }
                stmt = ffi::sqlite3_next_stmt(db, stmt);
            }
        }
        false
    }

    #[cfg(not(feature = "hooks"))]
    fn remove_hooks(&mut self) {}
}

impl Drop for InnerConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        use std::thread::panicking;

        if let Err(e) = self.close() {
            if panicking() {
                eprintln!("Error while closing SQLite connection: {:?}", e);
            } else {
                panic!("Error while closing SQLite connection: {:?}", e);
            }
        }
    }
}

#[cfg(not(feature = "bundled"))]
static SQLITE_VERSION_CHECK: std::sync::Once = std::sync::Once::new();
#[cfg(not(feature = "bundled"))]
pub static BYPASS_VERSION_CHECK: AtomicBool = AtomicBool::new(false);

#[cfg(not(feature = "bundled"))]
fn ensure_valid_sqlite_version() {
    use crate::version::version;

    SQLITE_VERSION_CHECK.call_once(|| {
        let version_number = version_number();

        // Check our hard floor.
        if version_number < 3_006_008 {
            panic!("rusqlite requires SQLite 3.6.8 or newer");
        }

        // Check that the major version number for runtime and buildtime match.
        let buildtime_major = ffi::SQLITE_VERSION_NUMBER / 1_000_000;
        let runtime_major = version_number / 1_000_000;
        if buildtime_major != runtime_major {
            panic!(
                "rusqlite was built against SQLite {} but is running with SQLite {}",
                str::from_utf8(ffi::SQLITE_VERSION).unwrap(),
                version()
            );
        }

        if BYPASS_VERSION_CHECK.load(Ordering::Relaxed) {
            return;
        }

        // Check that the runtime version number is compatible with the version number
        // we found at build-time.
        if version_number < ffi::SQLITE_VERSION_NUMBER {
            panic!(
                "\
rusqlite was built against SQLite {} but the runtime SQLite version is {}. To fix this, either:
* Recompile rusqlite and link against the SQLite version you are using at runtime, or
* Call rusqlite::bypass_sqlite_version_check() prior to your first connection attempt. Doing this
  means you're sure everything will work correctly even though the runtime version is older than
  the version we found at build time.",
                str::from_utf8(ffi::SQLITE_VERSION).unwrap(),
                version()
            );
        }
    });
}

#[cfg(not(any(target_arch = "wasm32")))]
static SQLITE_INIT: std::sync::Once = std::sync::Once::new();

pub static BYPASS_SQLITE_INIT: AtomicBool = AtomicBool::new(false);

// threading mode checks are not necessary (and do not work) on target
// platforms that do not have threading (such as webassembly)
#[cfg(any(target_arch = "wasm32"))]
fn ensure_safe_sqlite_threading_mode() -> Result<()> {
    Ok(())
}

#[cfg(not(any(target_arch = "wasm32")))]
fn ensure_safe_sqlite_threading_mode() -> Result<()> {
    // Ensure SQLite was compiled in thredsafe mode.
    if unsafe { ffi::sqlite3_threadsafe() == 0 } {
        return Err(Error::SqliteSingleThreadedMode);
    }

    // Now we know SQLite is _capable_ of being in Multi-thread of Serialized mode,
    // but it's possible someone configured it to be in Single-thread mode
    // before calling into us. That would mean we're exposing an unsafe API via
    // a safe one (in Rust terminology), which is no good. We have two options
    // to protect against this, depending on the version of SQLite we're linked
    // with:
    //
    // 1. If we're on 3.7.0 or later, we can ask SQLite for a mutex and check for
    //    the magic value 8. This isn't documented, but it's what SQLite
    //    returns for its mutex allocation function in Single-thread mode.
    // 2. If we're prior to SQLite 3.7.0, AFAIK there's no way to check the
    //    threading mode. The check we perform for >= 3.7.0 will segfault.
    //    Instead, we insist on being able to call sqlite3_config and
    //    sqlite3_initialize ourself, ensuring we know the threading
    //    mode. This will fail if someone else has already initialized SQLite
    //    even if they initialized it safely. That's not ideal either, which is
    //    why we expose bypass_sqlite_initialization    above.
    if version_number() >= 3_007_000 {
        const SQLITE_SINGLETHREADED_MUTEX_MAGIC: usize = 8;
        let is_singlethreaded = unsafe {
            let mutex_ptr = ffi::sqlite3_mutex_alloc(0);
            let is_singlethreaded = mutex_ptr as usize == SQLITE_SINGLETHREADED_MUTEX_MAGIC;
            ffi::sqlite3_mutex_free(mutex_ptr);
            is_singlethreaded
        };
        if is_singlethreaded {
            Err(Error::SqliteSingleThreadedMode)
        } else {
            Ok(())
        }
    } else {
        SQLITE_INIT.call_once(|| {
            if BYPASS_SQLITE_INIT.load(Ordering::Relaxed) {
                return;
            }

            unsafe {
                let msg = "\
Could not ensure safe initialization of SQLite.
To fix this, either:
* Upgrade SQLite to at least version 3.7.0
* Ensure that SQLite has been initialized in Multi-thread or Serialized mode and call
  rusqlite::bypass_sqlite_initialization() prior to your first connection attempt.";

                if ffi::sqlite3_config(ffi::SQLITE_CONFIG_MULTITHREAD) != ffi::SQLITE_OK {
                    panic!(msg);
                }
                if ffi::sqlite3_initialize() != ffi::SQLITE_OK {
                    panic!(msg);
                }
            }
        });
        Ok(())
    }
}
