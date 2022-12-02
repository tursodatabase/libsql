#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod postgres;

use anyhow::Result;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::rc::Rc;
use tracing::trace;
use unwrap_or::unwrap_ok_or;

thread_local! {
    static ERRMSG: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_error_message<T: ToString>(e: T) {
    ERRMSG.with(|errmsg| {
        errmsg.replace(Some(CString::new(e.to_string()).unwrap()));
    });
}

macro_rules! define_stub {
    ($name:tt) => {
        #[no_mangle]
        pub extern "C" fn $name() -> c_int {
            let func_name = std::stringify!($name);
            trace!("STUB {}", func_name);
            set_error_message(format!("{} not implemented", func_name));
            SQLITE_ERROR
        }
    };
}

pub type sqlite3_int64 = i64;
pub type sqlite3_uint64 = u64;

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_LOCKED: c_int = 6;
pub const SQLITE_MISUSE: c_int = 21;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;
pub const SQLITE_LOCKED_SHAREDCACHE: c_int = SQLITE_LOCKED | (1 << 8);

pub const SQLITE_TRANSIENT: c_int = -1;

pub const SQLITE_UTF8: c_int = 1;

struct Database {
    conn: RefCell<postgres::Connection>,
}

impl Database {
    fn new(conn: postgres::Connection) -> Self {
        let conn = RefCell::new(conn);
        Self { conn }
    }
}

pub struct sqlite3 {
    inner: Rc<Database>,
}

impl sqlite3 {
    fn connect(addr: &str) -> Result<Self> {
        let mut conn = postgres::Connection::connect(addr)?;
        conn.send_startup()?;
        conn.wait_until_ready()?;
        let inner = Rc::new(Database::new(conn));
        Ok(Self { inner })
    }
}

impl Drop for sqlite3 {
    fn drop(&mut self) {
        trace!("TRACE drop sqlite3");
    }
}

enum StatementState {
    Prepared,
    _Rows,
    _Done,
}

struct Statement {
    parent: Rc<Database>,
    sql: String,
    state: StatementState,
}

impl Statement {
    fn new(parent: Rc<Database>, sql: String) -> Self {
        let state = StatementState::Prepared;
        Self { parent, sql, state }
    }
}
pub struct sqlite3_stmt {
    inner: Statement,
}

impl Drop for sqlite3_stmt {
    fn drop(&mut self) {
        trace!("TRACE drop sqlite3_stmt");
    }
}

/*
 * Library version numbers.
 */

const SQLITE_VERSION_NUMBER: c_int = 3039003;

#[no_mangle]
pub static mut sqlite3_version: *const c_char = b"3.39.3\0" as *const u8 as *const c_char;

#[no_mangle]
pub extern "C" fn sqlite3_libversion() -> *const c_char {
    unsafe { sqlite3_version }
}

#[no_mangle]
pub extern "C" fn sqlite3_libversion_number() -> c_int {
    SQLITE_VERSION_NUMBER
}

/*
 * Initialize the library.
 */

#[no_mangle]
pub extern "C" fn sqlite3_initialize() -> c_int {
    tracing_subscriber::fmt::init();
    trace!("STUB sqlite3_initialize");
    set_error_message("");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_shutdown() -> c_int {
    trace!("STUB sqlite3_shutdown");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_os_init() -> c_int {
    trace!("STUB sqlite3_os_init");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_os_end() -> c_int {
    trace!("STUB sqlite3_os_end");
    SQLITE_OK
}

/*
 * Error codes and messages.
 */

#[no_mangle]
pub extern "C" fn sqlite3_errcode(_db: *mut sqlite3) -> c_int {
    trace!("STUB sqlite3_errcode");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_extended_errcode(_db: *mut sqlite3) -> c_int {
    trace!("STUB sqlite3_extended_errcode");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_errmsg(_db: *mut sqlite3) -> *const c_char {
    trace!("STUB sqlite3_errmsg");
    ERRMSG.with(|errmsg| {
        errmsg
            .borrow()
            .as_ref()
            .map_or_else(std::ptr::null, |v| v.as_ptr())
    })
}

#[no_mangle]
pub extern "C" fn sqlite3_errmsg16(_db: *mut sqlite3) -> *const c_char {
    trace!("STUB sqlite3_errmsg16");
    std::ptr::null()
}

#[no_mangle]
pub extern "C" fn sqlite3_errstr(_err: c_int) -> *const c_char {
    trace!("STUB sqlite3_errstr");
    std::ptr::null()
}

define_stub!(sqlite3_error_offset);

/*
 * Opening a database connection.
 */

#[no_mangle]
pub extern "C" fn sqlite3_open(filename: *const c_char, db: *mut *mut sqlite3) -> c_int {
    trace!("TRACE sqlite3_open");
    let filename = unsafe { CStr::from_ptr(filename) };
    let filename = unwrap_ok_or!(filename.to_str(), e, {
        set_error_message(e);
        return SQLITE_MISUSE;
    });
    unsafe {
        let database = unwrap_ok_or!(sqlite3::connect(filename), e, {
            set_error_message(e);
            return SQLITE_ERROR;
        });
        let ptr = Box::new(database);
        *db = Box::into_raw(ptr);
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_open16(filename: *const c_char, db: *mut *mut sqlite3) -> c_int {
    trace!("TRACE sqlite3_open16");
    let filename = unsafe { CStr::from_ptr(filename) };
    let filename = unwrap_ok_or!(filename.to_str(), e, {
        set_error_message(e);
        return SQLITE_MISUSE;
    });
    unsafe {
        let database = unwrap_ok_or!(sqlite3::connect(filename), e, {
            set_error_message(e);
            return SQLITE_ERROR;
        });
        let ptr = Box::new(database);
        *db = Box::into_raw(ptr);
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_open_v2(
    filename: *const c_char,
    db: *mut *mut sqlite3,
    _flags: c_int,
    _pVfs: *const c_char,
) -> c_int {
    trace!("TRACE sqlite3_open_v2");
    let filename = unsafe { CStr::from_ptr(filename) };
    let filename = unwrap_ok_or!(filename.to_str(), e, {
        set_error_message(e);
        return SQLITE_MISUSE;
    });
    unsafe {
        let database = unwrap_ok_or!(sqlite3::connect(filename), e, {
            set_error_message(e);
            return SQLITE_ERROR;
        });
        let ptr = Box::new(database);
        *db = Box::into_raw(ptr);
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_close(db: *mut sqlite3) -> c_int {
    trace!("TRACE sqlite3_close");
    if db.is_null() {
        return SQLITE_OK;
    }
    let _ = unsafe { Box::from_raw(db) };
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> c_int {
    trace!("TRACE sqlite3_close_v2");
    if db.is_null() {
        return SQLITE_OK;
    }
    let _ = unsafe { Box::from_raw(db) };
    SQLITE_OK
}

/*
 * Prepared statements.
 */

#[no_mangle]
pub extern "C" fn sqlite3_prepare_v2(
    db: *mut sqlite3,
    zSql: *const c_char,
    _nByte: c_int,
    ppStmt: *mut *mut sqlite3_stmt,
    pzTail: *mut *const c_char,
) -> c_int {
    let database = unsafe { (*db).inner.clone() };
    trace!("TRACE sqlite3_prepare_v2");
    let zSql = unsafe { CStr::from_ptr(zSql) };
    let sql = unwrap_ok_or!(zSql.to_str(), _, {
        return SQLITE_ERROR;
    });
    let sql = sql.to_string();
    unsafe {
        let stmt = Statement::new(database, sql);
        let ptr = Box::new(sqlite3_stmt { inner: stmt });
        *ppStmt = Box::into_raw(ptr);
    }
    if !pzTail.is_null() {
        unsafe { *pzTail = "\0".as_ptr() as *const c_char };
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_finalize(_stmt: *mut sqlite3_stmt) -> c_int {
    trace!("STUB sqlite3_finalize");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_reset(_stmt: *mut sqlite3_stmt) -> c_int {
    trace!("STUB sqlite3_reset");
    SQLITE_OK
}

/*
 * SQL evaluation.
 */

#[no_mangle]
pub extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int {
    let stmt = unsafe { &(*stmt).inner };
    trace!("STUB sqlite3_step {}", stmt.sql);
    match stmt.state {
        StatementState::Prepared => {
            let database = stmt.parent.clone();
            let mut conn = database.conn.borrow_mut();
            unwrap_ok_or!(conn.send_simple_query(&stmt.sql), e, {
                set_error_message(e);
                return SQLITE_ERROR;
            });
            unwrap_ok_or!(conn.wait_until_ready(), e, {
                set_error_message(e);
                return SQLITE_ERROR;
            });
            SQLITE_DONE
        }
        StatementState::_Rows => todo!(),
        StatementState::_Done => SQLITE_MISUSE,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: *const (),
    _n: c_int,
    _callback: extern "C" fn(*mut ()),
) -> c_int {
    trace!("STUB sqlite3_bind_blob");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_blob64(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: *const (),
    _n: sqlite3_uint64,
    _callback: extern "C" fn(*mut ()),
) -> c_int {
    trace!("STUB sqlite3_bind_blob64");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_double(_stmt: *mut sqlite3_stmt, _idx: c_int, _value: f32) -> c_int {
    trace!("sqlite3_bind_double");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_int(_stmt: *mut sqlite3_stmt, _idx: c_int, _value: c_int) -> c_int {
    trace!("sqlite3_bind_int");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_int64(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: sqlite3_int64,
) -> c_int {
    trace!("STUB sqlite3_bind_int64");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_null(_stmt: *mut sqlite3_stmt, _idx: c_int) -> c_int {
    trace!("STUB sqlite3_bind_null");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_text(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: *const c_char,
    _n: c_int,
    _callback: extern "C" fn(*mut ()),
) -> c_int {
    trace!("STUB sqlite3_bind_text");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_text16(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: *const c_void,
    _n: c_int,
    _callback: extern "C" fn(*mut ()),
) -> c_int {
    trace!("STUB sqlite3_bind_text64");
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_text64(
    _stmt: *mut sqlite3_stmt,
    _idx: c_int,
    _value: *const c_char,
    _n: sqlite3_uint64,
    _callback: extern "C" fn(*mut ()),
    _encoding: c_char,
) -> c_int {
    trace!("STUB sqlite3_bind_text64");
    SQLITE_OK
}

define_stub!(sqlite3_bind_value);
define_stub!(sqlite3_bind_pointer);
define_stub!(sqlite3_bind_zeroblob);
define_stub!(sqlite3_bind_zeroblob64);

/*
 * Mutexes
 */

define_stub!(sqlite3_mutex_alloc);
define_stub!(sqlite3_mutex_enter);
define_stub!(sqlite3_mutex_free);
define_stub!(sqlite3_mutex_held);
define_stub!(sqlite3_mutex_leave);
define_stub!(sqlite3_mutex_notheld);
define_stub!(sqlite3_mutex_try);

/*
 * Stubs.
 */

define_stub!(sqlite3_aggregate_context);
define_stub!(sqlite3_aggregate_count);
define_stub!(sqlite3_auto_extension);
define_stub!(sqlite3_autovacuum_pages);
define_stub!(sqlite3_backup_finish);
define_stub!(sqlite3_backup_init);
define_stub!(sqlite3_backup_pagecount);
define_stub!(sqlite3_backup_remaining);
define_stub!(sqlite3_backup_step);
define_stub!(sqlite3_bind_parameter_count);
define_stub!(sqlite3_bind_parameter_index);
define_stub!(sqlite3_bind_parameter_name);
define_stub!(sqlite3_blob_bytes);
define_stub!(sqlite3_blob_close);
define_stub!(sqlite3_blob_open);
define_stub!(sqlite3_blob_read);
define_stub!(sqlite3_blob_reopen);
define_stub!(sqlite3_blob_write);
define_stub!(sqlite3_busy_handler);
define_stub!(sqlite3_busy_timeout);
define_stub!(sqlite3_cancel_auto_extension);
define_stub!(sqlite3_changes);
define_stub!(sqlite3_changes64);
define_stub!(sqlite3_clear_bindings);
define_stub!(sqlite3_collation_needed);
define_stub!(sqlite3_collation_needed16);
define_stub!(sqlite3_column_blob);
define_stub!(sqlite3_column_bytes);
define_stub!(sqlite3_column_bytes16);
define_stub!(sqlite3_column_count);
define_stub!(sqlite3_column_database_name);
define_stub!(sqlite3_column_database_name16);
define_stub!(sqlite3_column_decltype);
define_stub!(sqlite3_column_decltype16);
define_stub!(sqlite3_column_double);
define_stub!(sqlite3_column_int);
define_stub!(sqlite3_column_int64);
define_stub!(sqlite3_column_name);
define_stub!(sqlite3_column_name16);
define_stub!(sqlite3_column_origin_name);
define_stub!(sqlite3_column_origin_name16);
define_stub!(sqlite3_column_table_name);
define_stub!(sqlite3_column_table_name16);
define_stub!(sqlite3_column_text);
define_stub!(sqlite3_column_text16);
define_stub!(sqlite3_column_type);
define_stub!(sqlite3_column_value);
define_stub!(sqlite3_commit_hook);
define_stub!(sqlite3_compileoption_get);
define_stub!(sqlite3_compileoption_used);
define_stub!(sqlite3_complete);
define_stub!(sqlite3_complete16);
define_stub!(sqlite3_config);
define_stub!(sqlite3_context_db_handle);
define_stub!(sqlite3_create_collation);
define_stub!(sqlite3_create_collation16);
define_stub!(sqlite3_create_collation_v2);
define_stub!(sqlite3_create_filename);
define_stub!(sqlite3_create_function);
define_stub!(sqlite3_create_function16);
define_stub!(sqlite3_create_function_v2);
define_stub!(sqlite3_create_module);
define_stub!(sqlite3_create_module_v2);
define_stub!(sqlite3_create_window_function);
define_stub!(sqlite3_data_count);
define_stub!(sqlite3_database_file_object);
define_stub!(sqlite3_db_cacheflush);
define_stub!(sqlite3_db_config);
define_stub!(sqlite3_db_filename);
define_stub!(sqlite3_db_handle);
define_stub!(sqlite3_db_mutex);
define_stub!(sqlite3_db_name);
define_stub!(sqlite3_db_readonly);
define_stub!(sqlite3_db_release_memory);
define_stub!(sqlite3_db_status);
define_stub!(sqlite3_declare_vtab);
define_stub!(sqlite3_deserialize);
define_stub!(sqlite3_drop_modules);
define_stub!(sqlite3_enable_load_extension);
define_stub!(sqlite3_enable_shared_cache);
define_stub!(sqlite3_exec);
define_stub!(sqlite3_expanded_sql);
define_stub!(sqlite3_expired);
define_stub!(sqlite3_extended_result_codes);
define_stub!(sqlite3_file_control);
define_stub!(sqlite3_filename_database);
define_stub!(sqlite3_filename_journal);
define_stub!(sqlite3_filename_wal);
define_stub!(sqlite3_free);
define_stub!(sqlite3_free_filename);
define_stub!(sqlite3_free_table);
define_stub!(sqlite3_get_autocommit);
define_stub!(sqlite3_get_auxdata);
define_stub!(sqlite3_get_table);
define_stub!(sqlite3_global_recover);
define_stub!(sqlite3_hard_heap_limit64);
define_stub!(sqlite3_interrupt);
define_stub!(sqlite3_keyword_check);
define_stub!(sqlite3_keyword_count);
define_stub!(sqlite3_keyword_name);
define_stub!(sqlite3_last_insert_rowid);
define_stub!(sqlite3_limit);
define_stub!(sqlite3_load_extension);
define_stub!(sqlite3_log);
define_stub!(sqlite3_malloc);
define_stub!(sqlite3_malloc64);
define_stub!(sqlite3_memory_alarm);
define_stub!(sqlite3_memory_highwater);
define_stub!(sqlite3_memory_used);
define_stub!(sqlite3_mprintf);
define_stub!(sqlite3_msize);
define_stub!(sqlite3_next_stmt);
define_stub!(sqlite3_normalized_sql);
define_stub!(sqlite3_overload_function);
define_stub!(sqlite3_prepare);
define_stub!(sqlite3_prepare16);
define_stub!(sqlite3_prepare16_v2);
define_stub!(sqlite3_prepare16_v3);
define_stub!(sqlite3_prepare_v3);
define_stub!(sqlite3_preupdate_blobwrite);
define_stub!(sqlite3_preupdate_count);
define_stub!(sqlite3_preupdate_depth);
define_stub!(sqlite3_preupdate_hook);
define_stub!(sqlite3_preupdate_new);
define_stub!(sqlite3_preupdate_old);
define_stub!(sqlite3_profile);
define_stub!(sqlite3_progress_handler);
define_stub!(sqlite3_randomness);
define_stub!(sqlite3_realloc);
define_stub!(sqlite3_realloc64);
define_stub!(sqlite3_release_memory);
define_stub!(sqlite3_reset_auto_extension);
define_stub!(sqlite3_result_blob);
define_stub!(sqlite3_result_blob64);
define_stub!(sqlite3_result_double);
define_stub!(sqlite3_result_error);
define_stub!(sqlite3_result_error16);
define_stub!(sqlite3_result_error_code);
define_stub!(sqlite3_result_error_nomem);
define_stub!(sqlite3_result_error_toobig);
define_stub!(sqlite3_result_int);
define_stub!(sqlite3_result_int64);
define_stub!(sqlite3_result_null);
define_stub!(sqlite3_result_pointer);
define_stub!(sqlite3_result_subtype);
define_stub!(sqlite3_result_text);
define_stub!(sqlite3_result_text16);
define_stub!(sqlite3_result_text16be);
define_stub!(sqlite3_result_text16le);
define_stub!(sqlite3_result_text64);
define_stub!(sqlite3_result_value);
define_stub!(sqlite3_result_zeroblob);
define_stub!(sqlite3_result_zeroblob64);
define_stub!(sqlite3_rollback_hook);
define_stub!(sqlite3_serialize);
define_stub!(sqlite3_set_authorizer);
define_stub!(sqlite3_set_auxdata);
define_stub!(sqlite3_set_last_insert_rowid);
define_stub!(sqlite3_sleep);
define_stub!(sqlite3_snapshot_cmp);
define_stub!(sqlite3_snapshot_free);
define_stub!(sqlite3_snapshot_get);
define_stub!(sqlite3_snapshot_open);
define_stub!(sqlite3_snapshot_recover);
define_stub!(sqlite3_snprintf);
define_stub!(sqlite3_soft_heap_limit);
define_stub!(sqlite3_soft_heap_limit64);
define_stub!(sqlite3_sourceid);
define_stub!(sqlite3_sql);
define_stub!(sqlite3_status);
define_stub!(sqlite3_status64);
define_stub!(sqlite3_stmt_busy);
define_stub!(sqlite3_stmt_isexplain);
define_stub!(sqlite3_stmt_readonly);
define_stub!(sqlite3_stmt_scanstatus);
define_stub!(sqlite3_stmt_scanstatus_reset);
define_stub!(sqlite3_stmt_status);
define_stub!(sqlite3_str_append);
define_stub!(sqlite3_str_appendall);
define_stub!(sqlite3_str_appendchar);
define_stub!(sqlite3_str_appendf);
define_stub!(sqlite3_str_errcode);
define_stub!(sqlite3_str_finish);
define_stub!(sqlite3_str_length);
define_stub!(sqlite3_str_new);
define_stub!(sqlite3_str_reset);
define_stub!(sqlite3_str_value);
define_stub!(sqlite3_str_vappendf);
define_stub!(sqlite3_strglob);
define_stub!(sqlite3_stricmp);
define_stub!(sqlite3_strlike);
define_stub!(sqlite3_strnicmp);
define_stub!(sqlite3_system_errno);
define_stub!(sqlite3_table_column_metadata);
define_stub!(sqlite3_test_control);
define_stub!(sqlite3_thread_cleanup);
define_stub!(sqlite3_threadsafe);
define_stub!(sqlite3_total_changes);
define_stub!(sqlite3_total_changes64);
define_stub!(sqlite3_trace);
define_stub!(sqlite3_trace_v2);
define_stub!(sqlite3_transfer_bindings);
define_stub!(sqlite3_txn_state);
define_stub!(sqlite3_unlock_notify);
define_stub!(sqlite3_update_hook);
define_stub!(sqlite3_uri_boolean);
define_stub!(sqlite3_uri_int64);
define_stub!(sqlite3_uri_key);
define_stub!(sqlite3_uri_parameter);
define_stub!(sqlite3_user_data);
define_stub!(sqlite3_value_blob);
define_stub!(sqlite3_value_bytes);
define_stub!(sqlite3_value_bytes16);
define_stub!(sqlite3_value_double);
define_stub!(sqlite3_value_dup);
define_stub!(sqlite3_value_free);
define_stub!(sqlite3_value_frombind);
define_stub!(sqlite3_value_int);
define_stub!(sqlite3_value_int64);
define_stub!(sqlite3_value_nochange);
define_stub!(sqlite3_value_numeric_type);
define_stub!(sqlite3_value_pointer);
define_stub!(sqlite3_value_subtype);
define_stub!(sqlite3_value_text);
define_stub!(sqlite3_value_text16);
define_stub!(sqlite3_value_text16be);
define_stub!(sqlite3_value_text16le);
define_stub!(sqlite3_value_type);
define_stub!(sqlite3_vfs_find);
define_stub!(sqlite3_vfs_register);
define_stub!(sqlite3_vfs_unregister);
define_stub!(sqlite3_vmprintf);
define_stub!(sqlite3_vsnprintf);
define_stub!(sqlite3_vtab_collation);
define_stub!(sqlite3_vtab_config);
define_stub!(sqlite3_vtab_distinct);
define_stub!(sqlite3_vtab_in);
define_stub!(sqlite3_vtab_in_first);
define_stub!(sqlite3_vtab_in_next);
define_stub!(sqlite3_vtab_nochange);
define_stub!(sqlite3_vtab_on_conflict);
define_stub!(sqlite3_vtab_rhs_value);
define_stub!(sqlite3_wal_autocheckpoint);
define_stub!(sqlite3_wal_checkpoint);
define_stub!(sqlite3_wal_checkpoint_v2);
define_stub!(sqlite3_wal_hook);
define_stub!(sqlite3_win32_set_directory);
define_stub!(sqlite3_win32_set_directory16);
define_stub!(sqlite3_win32_set_directory8);
