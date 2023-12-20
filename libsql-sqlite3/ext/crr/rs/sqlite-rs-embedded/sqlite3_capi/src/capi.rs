extern crate alloc;

use core::ffi::{c_char, c_int, c_uchar, c_uint, c_void, CStr};
use core::ptr;

use alloc::borrow::ToOwned;
use alloc::ffi::CString;

pub use crate::bindings::{
    sqlite3, sqlite3_api_routines as api_routines, sqlite3_context as context,
    sqlite3_index_info as index_info,
    sqlite3_index_info_sqlite3_index_constraint as index_constraint,
    sqlite3_index_info_sqlite3_index_constraint_usage as index_constraint_usage,
    sqlite3_module as module, sqlite3_stmt as stmt, sqlite3_uint64 as uint64,
    sqlite3_value as value, sqlite3_vtab as vtab, sqlite3_vtab_cursor as vtab_cursor,
    sqlite_int64 as int64, SQLITE_DETERMINISTIC as DETERMINISTIC, SQLITE_DIRECTONLY as DIRECTONLY,
    SQLITE_INDEX_CONSTRAINT_EQ as INDEX_CONSTRAINT_EQ,
    SQLITE_INDEX_CONSTRAINT_GE as INDEX_CONSTRAINT_GE,
    SQLITE_INDEX_CONSTRAINT_GLOB as INDEX_CONSTRAINT_GLOB,
    SQLITE_INDEX_CONSTRAINT_GT as INDEX_CONSTRAINT_GT,
    SQLITE_INDEX_CONSTRAINT_IS as INDEX_CONSTRAINT_IS,
    SQLITE_INDEX_CONSTRAINT_ISNOT as INDEX_CONSTRAINT_ISNOT,
    SQLITE_INDEX_CONSTRAINT_ISNOTNULL as INDEX_CONSTRAINT_ISNOTNULL,
    SQLITE_INDEX_CONSTRAINT_ISNULL as INDEX_CONSTRAINT_ISNULL,
    SQLITE_INDEX_CONSTRAINT_LE as INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LIKE as INDEX_CONSTRAINT_LIKE,
    SQLITE_INDEX_CONSTRAINT_LT as INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_CONSTRAINT_MATCH as INDEX_CONSTRAINT_MATCH,
    SQLITE_INDEX_CONSTRAINT_NE as INDEX_CONSTRAINT_NE,
    SQLITE_INDEX_CONSTRAINT_REGEXP as INDEX_CONSTRAINT_REGEXP, SQLITE_INNOCUOUS as INNOCUOUS,
    SQLITE_PREPARE_NORMALIZE as PREPARE_NORMALIZE, SQLITE_PREPARE_NO_VTAB as PREPARE_NO_VTAB,
    SQLITE_PREPARE_PERSISTENT as PREPARE_PERSISTENT, SQLITE_UTF8 as UTF8,
};

mod aliased {
    #[cfg(feature = "static")]
    pub use crate::bindings::{
        sqlite3_bind_blob as bind_blob, sqlite3_bind_double as bind_double,
        sqlite3_bind_int as bind_int, sqlite3_bind_int64 as bind_int64,
        sqlite3_bind_null as bind_null, sqlite3_bind_parameter_count as bind_parameter_count,
        sqlite3_bind_parameter_index as bind_parameter_index,
        sqlite3_bind_parameter_name as bind_parameter_name, sqlite3_bind_pointer as bind_pointer,
        sqlite3_bind_text as bind_text, sqlite3_bind_value as bind_value,
        sqlite3_bind_zeroblob as bind_zeroblob, sqlite3_changes64 as changes64,
        sqlite3_clear_bindings as clear_bindings, sqlite3_close as close,
        sqlite3_column_blob as column_blob, sqlite3_column_bytes as column_bytes,
        sqlite3_column_count as column_count, sqlite3_column_decltype as column_decltype,
        sqlite3_column_double as column_double, sqlite3_column_int as column_int,
        sqlite3_column_int64 as column_int64, sqlite3_column_name as column_name,
        sqlite3_column_origin_name as column_origin_name,
        sqlite3_column_table_name as column_table_name, sqlite3_column_text as column_text,
        sqlite3_column_type as column_type, sqlite3_column_value as column_value,
        sqlite3_commit_hook as commit_hook, sqlite3_context_db_handle as context_db_handle,
        sqlite3_create_function_v2 as create_function_v2,
        sqlite3_create_module_v2 as create_module_v2, sqlite3_declare_vtab as declare_vtab,
        sqlite3_errcode as errcode, sqlite3_errmsg as errmsg, sqlite3_exec as exec,
        sqlite3_finalize as finalize, sqlite3_free as free,
        sqlite3_get_autocommit as get_autocommit, sqlite3_get_auxdata as get_auxdata,
        sqlite3_malloc as malloc, sqlite3_malloc64 as malloc64, sqlite3_next_stmt as next_stmt,
        sqlite3_open as open, sqlite3_prepare_v2 as prepare_v2, sqlite3_prepare_v3 as prepare_v3,
        sqlite3_randomness as randomness, sqlite3_reset as reset,
        sqlite3_result_blob as result_blob, sqlite3_result_double as result_double,
        sqlite3_result_error as result_error, sqlite3_result_error_code as result_error_code,
        sqlite3_result_int as result_int, sqlite3_result_int64 as result_int64,
        sqlite3_result_null as result_null, sqlite3_result_pointer as result_pointer,
        sqlite3_result_subtype as result_subtype, sqlite3_result_text as result_text,
        sqlite3_result_value as result_value, sqlite3_set_authorizer as set_authorizer,
        sqlite3_set_auxdata as set_auxdata, sqlite3_shutdown as shutdown, sqlite3_sql as sql,
        sqlite3_step as step, sqlite3_user_data as user_data, sqlite3_value_blob as value_blob,
        sqlite3_value_bytes as value_bytes, sqlite3_value_double as value_double,
        sqlite3_value_int as value_int, sqlite3_value_int64 as value_int64,
        sqlite3_value_pointer as value_pointer, sqlite3_value_subtype as value_subtype,
        sqlite3_value_text as value_text, sqlite3_value_type as value_type,
        sqlite3_vtab_collation as vtab_collation, sqlite3_vtab_config as vtab_config,
        sqlite3_vtab_distinct as vtab_distinct, sqlite3_vtab_nochange as vtab_nochange,
        sqlite3_vtab_on_conflict as vtab_on_conflict,
    };
}

pub enum Destructor {
    TRANSIENT,
    STATIC,
    CUSTOM(xDestroy),
}

#[macro_export]
macro_rules! strlit {
    ($s:expr) => {
        concat!($s, "\0").as_ptr() as *const c_char
    };
}

#[cfg(feature = "static")]
macro_rules! invoke_sqlite {
    ($name:ident, $($arg:expr),*) => {
      aliased::$name($($arg),*)
    };
}

#[cfg(feature = "loadable_extension")]
macro_rules! invoke_sqlite {
  ($name:ident, $($arg:expr),*) => {
    ((*SQLITE3_API).$name.unwrap())($($arg),*)
  }
}

pub extern "C" fn droprust(ptr: *mut c_void) {
    unsafe { invoke_sqlite!(free, ptr as *mut c_void) }
}

#[macro_export]
macro_rules! args {
    ($argc:expr, $argv:expr) => {
        unsafe { ::core::slice::from_raw_parts($argv, $argc as usize) }
    };
}

#[macro_export]
macro_rules! args_mut {
    ($argc:expr, $argv:expr) => {
        unsafe { ::core::slice::from_raw_parts_mut($argv, $argc as usize) }
    };
}

static mut SQLITE3_API: *mut api_routines = ptr::null_mut();

pub fn EXTENSION_INIT2(api: *mut api_routines) {
    unsafe {
        SQLITE3_API = api;
    }
}

pub fn bind_blob(
    stmt: *mut stmt,
    c: c_int,
    blob: *const c_void,
    len: c_int,
    d: Destructor,
) -> c_int {
    unsafe {
        invoke_sqlite!(
            bind_blob,
            stmt,
            c,
            blob,
            len,
            match d {
                Destructor::TRANSIENT => Some(core::mem::transmute(-1_isize)),
                Destructor::STATIC => None,
                Destructor::CUSTOM(f) => Some(f),
            }
        )
    }
}

pub fn changes64(db: *mut sqlite3) -> int64 {
    unsafe { invoke_sqlite!(changes64, db) }
}

pub fn shutdown() -> c_int {
    #[cfg(feature = "static")]
    unsafe {
        aliased::shutdown()
    }

    #[cfg(feature = "loadable_extension")]
    0
}

pub fn bind_int(stmt: *mut stmt, c: c_int, i: c_int) -> c_int {
    unsafe { invoke_sqlite!(bind_int, stmt, c, i) }
}

pub fn bind_int64(stmt: *mut stmt, c: c_int, i: int64) -> c_int {
    unsafe { invoke_sqlite!(bind_int64, stmt, c, i) }
}

pub fn bind_double(stmt: *mut stmt, c: c_int, f: f64) -> c_int {
    unsafe { invoke_sqlite!(bind_double, stmt, c, f) }
}

pub fn bind_null(stmt: *mut stmt, c: c_int) -> c_int {
    unsafe { invoke_sqlite!(bind_null, stmt, c) }
}

pub fn clear_bindings(stmt: *mut stmt) -> c_int {
    unsafe { invoke_sqlite!(clear_bindings, stmt) }
}

pub fn bind_text(
    stmt: *mut stmt,
    c: c_int,
    text: *const c_char,
    len: c_int,
    d: Destructor,
) -> c_int {
    unsafe {
        invoke_sqlite!(
            bind_text,
            stmt,
            c,
            text,
            len,
            match d {
                Destructor::TRANSIENT => Some(core::mem::transmute(-1_isize)),
                Destructor::STATIC => None,
                Destructor::CUSTOM(f) => Some(f),
            }
        )
    }
}

pub fn bind_pointer(stmt: *mut stmt, i: c_int, p: *mut c_void, t: *const c_char) -> c_int {
    unsafe { invoke_sqlite!(bind_pointer, stmt, i, p, t, None) }
}

pub fn bind_value(stmt: *mut stmt, c: c_int, v: *mut value) -> c_int {
    unsafe { invoke_sqlite!(bind_value, stmt, c, v) }
}

pub fn close(db: *mut sqlite3) -> c_int {
    unsafe { invoke_sqlite!(close, db) }
}

pub fn vtab_config(db: *mut sqlite3, options: u32) -> c_int {
    unsafe { invoke_sqlite!(vtab_config, db, options as i32) }
}

pub type xCommitHook = unsafe extern "C" fn(*mut c_void) -> c_int;
pub fn commit_hook(
    db: *mut sqlite3,
    callback: Option<xCommitHook>,
    user_data: *mut c_void,
) -> Option<xCommitHook> {
    unsafe {
        invoke_sqlite!(commit_hook, db, callback, user_data)
            .as_ref()
            .map(|p| core::mem::transmute(p))
    }
}

// pub fn mprintf(format: *const i8, ...) -> *mut c_char {
//     unsafe { ((*SQLITE3_API).mprintf.expect(EXPECT_MESSAGE))(format, args) }
// }

pub fn column_type(stmt: *mut stmt, c: c_int) -> c_int {
    unsafe { invoke_sqlite!(column_type, stmt, c) }
}

pub fn column_count(stmt: *mut stmt) -> c_int {
    unsafe { invoke_sqlite!(column_count, stmt) }
}

pub fn column_text<'a>(stmt: *mut stmt, c: c_int) -> &'a str {
    unsafe {
        let len = column_bytes(stmt, c);
        let bytes = invoke_sqlite!(column_text, stmt, c);
        let slice = core::slice::from_raw_parts(bytes as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    }
}

pub fn column_text_ptr(stmt: *mut stmt, c: c_int) -> *const c_uchar {
    unsafe { invoke_sqlite!(column_text, stmt, c) }
}

pub fn column_blob(stmt: *mut stmt, c: c_int) -> *const c_void {
    unsafe { invoke_sqlite!(column_blob, stmt, c) }
}

pub fn column_bytes(stmt: *mut stmt, c: c_int) -> c_int {
    unsafe { invoke_sqlite!(column_bytes, stmt, c) }
}

pub fn column_value(stmt: *mut stmt, c: c_int) -> *mut value {
    unsafe { invoke_sqlite!(column_value, stmt, c) }
}

pub fn column_double(stmt: *mut stmt, c: c_int) -> f64 {
    unsafe { invoke_sqlite!(column_double, stmt, c) }
}

pub fn column_int(stmt: *mut stmt, c: c_int) -> c_int {
    unsafe { invoke_sqlite!(column_int, stmt, c) }
}

pub fn column_int64(stmt: *mut stmt, c: c_int) -> int64 {
    unsafe { invoke_sqlite!(column_int64, stmt, c) }
}

pub fn column_name(stmt: *mut stmt, c: c_int) -> *const c_char {
    unsafe { invoke_sqlite!(column_name, stmt, c) }
}

pub fn context_db_handle(ctx: *mut context) -> *mut sqlite3 {
    unsafe { invoke_sqlite!(context_db_handle, ctx) }
}

pub type xFunc = unsafe extern "C" fn(*mut context, c_int, *mut *mut value);
pub type xStep = unsafe extern "C" fn(*mut context, c_int, *mut *mut value);
pub type xFinal = unsafe extern "C" fn(*mut context);
pub type xDestroy = unsafe extern "C" fn(*mut c_void);
pub fn create_function_v2(
    db: *mut sqlite3,
    s: *const c_char,
    argc: c_int,
    flags: c_int,
    p_app: *mut c_void,
    x_func: Option<xFunc>,
    x_step: Option<xStep>,
    x_final: Option<xFinal>,
    destroy: Option<xDestroy>,
) -> c_int {
    unsafe {
        invoke_sqlite!(
            create_function_v2,
            db,
            s,
            argc,
            i32::try_from(flags).expect("Invalid flags"),
            p_app,
            x_func,
            x_step,
            x_final,
            destroy
        )
    }
}

pub fn create_module_v2(
    db: *mut sqlite3,
    s: *const c_char,
    module: *const module,
    p_app: *mut c_void,
    destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    unsafe { invoke_sqlite!(create_module_v2, db, s, module, p_app, destroy) }
}

pub fn declare_vtab(db: *mut sqlite3, s: *const c_char) -> c_int {
    unsafe { invoke_sqlite!(declare_vtab, db, s) }
}

#[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
pub fn enable_load_extension(db: *mut sqlite3, onoff: c_int) -> c_int {
    unsafe { crate::bindings::sqlite3_enable_load_extension(db, onoff) }
}

pub fn errcode(db: *mut sqlite3) -> c_int {
    unsafe { invoke_sqlite!(errcode, db) }
}

pub fn errmsg(db: *mut sqlite3) -> CString {
    unsafe { CStr::from_ptr(invoke_sqlite!(errmsg, db)).to_owned() }
}

pub fn exec(db: *mut sqlite3, s: *const c_char) -> c_int {
    unsafe { invoke_sqlite!(exec, db, s, None, ptr::null_mut(), ptr::null_mut()) }
}

pub fn finalize(stmt: *mut stmt) -> c_int {
    unsafe { invoke_sqlite!(finalize, stmt) }
}

#[inline]
pub fn free(ptr: *mut c_void) {
    unsafe { invoke_sqlite!(free, ptr) }
}

pub fn get_auxdata(context: *mut context, n: c_int) -> *mut c_void {
    unsafe { invoke_sqlite!(get_auxdata, context, n) }
}

#[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
pub fn load_extension(
    db: *mut sqlite3,
    zfile: *const c_char,
    zproc: *const c_char,
    pzerr: *mut *mut c_char,
) -> c_int {
    unsafe { crate::bindings::sqlite3_load_extension(db, zfile, zproc, pzerr) }
}

#[inline]
pub fn malloc(size: usize) -> *mut u8 {
    unsafe {
        if usize::BITS == 64 {
            invoke_sqlite!(malloc64, size as uint64) as *mut u8
        } else {
            invoke_sqlite!(malloc, size as c_int) as *mut u8
        }
    }
}

pub fn next_stmt(db: *mut sqlite3, s: *mut stmt) -> *mut stmt {
    unsafe { invoke_sqlite!(next_stmt, db, s) }
}

pub fn open(filename: *const c_char, db: *mut *mut sqlite3) -> c_int {
    unsafe { invoke_sqlite!(open, filename, db) }
}

pub fn prepare_v2(
    db: *mut sqlite3,
    sql: *const c_char,
    n: c_int,
    stmt: *mut *mut stmt,
    leftover: *mut *const c_char,
) -> c_int {
    unsafe { invoke_sqlite!(prepare_v2, db, sql, n, stmt, leftover) }
}

pub fn prepare_v3(
    db: *mut sqlite3,
    sql: *const c_char,
    n: c_int,
    flags: c_uint,
    stmt: *mut *mut stmt,
    leftover: *mut *const c_char,
) -> c_int {
    unsafe { invoke_sqlite!(prepare_v3, db, sql, n, flags, stmt, leftover) }
}

pub fn randomness(len: c_int, blob: *mut c_void) {
    unsafe { invoke_sqlite!(randomness, len, blob) }
}

pub fn result_int(context: *mut context, v: c_int) {
    unsafe { invoke_sqlite!(result_int, context, v) }
}

pub fn result_blob(context: *mut context, b: *const u8, n: c_int, d: Destructor) {
    unsafe {
        invoke_sqlite!(
            result_blob,
            context,
            b as *const c_void,
            n,
            match d {
                Destructor::TRANSIENT => Some(core::mem::transmute(-1_isize)),
                Destructor::STATIC => None,
                Destructor::CUSTOM(f) => Some(f),
            }
        )
    }
}

pub fn result_int64(context: *mut context, v: int64) {
    unsafe { invoke_sqlite!(result_int64, context, v) }
}

pub fn result_double(context: *mut context, f: f64) {
    unsafe { invoke_sqlite!(result_double, context, f) }
}

pub fn result_null(context: *mut context) {
    unsafe { invoke_sqlite!(result_null, context) }
}
pub fn result_pointer(
    context: *mut context,
    pointer: *mut c_void,
    name: *mut c_char,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    unsafe { invoke_sqlite!(result_pointer, context, pointer, name, destructor) }
}

pub fn result_error(context: *mut context, text: *mut c_char, len: c_int) {
    unsafe { invoke_sqlite!(result_error, context, text, len) }
}

pub fn result_error_code(context: *mut context, code: c_int) {
    unsafe { invoke_sqlite!(result_error_code, context, code) }
}

pub fn result_value(ctx: *mut context, v: *mut value) {
    unsafe { invoke_sqlite!(result_value, ctx, v) }
}

// d is our destructor function.
// -- https://dev.to/kgrech/7-ways-to-pass-a-string-between-rust-and-c-4ieb
pub fn result_text(context: *mut context, s: *const c_char, n: c_int, d: Destructor) {
    unsafe {
        invoke_sqlite!(
            result_text,
            context,
            s,
            n,
            match d {
                Destructor::TRANSIENT => Some(core::mem::transmute(-1_isize)),
                Destructor::STATIC => None,
                Destructor::CUSTOM(f) => Some(f),
            }
        )
    }
}

pub fn result_subtype(context: *mut context, subtype: u32) {
    unsafe { invoke_sqlite!(result_subtype, context, subtype) }
}

pub type XAuthorizer = unsafe extern "C" fn(
    user_data: *mut c_void,
    action_code: c_int,
    item_name: *const c_char,
    sub_item_name: *const c_char,
    db_name: *const c_char,
    trigger_view_or_null: *const c_char,
) -> c_int;

pub fn set_authorizer(
    db: *mut sqlite3,
    xAuth: ::core::option::Option<XAuthorizer>,
    user_data: *mut c_void,
) -> c_int {
    unsafe { invoke_sqlite!(set_authorizer, db, xAuth, user_data) }
}

pub fn set_auxdata(
    context: *mut context,
    n: c_int,
    p: *mut c_void,
    d: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    unsafe { invoke_sqlite!(set_auxdata, context, n, p, d) }
}

pub fn sql(s: *mut stmt) -> *const c_char {
    unsafe { invoke_sqlite!(sql, s) }
}

pub fn reset(stmt: *mut stmt) -> c_int {
    unsafe { invoke_sqlite!(reset, stmt) }
}

#[inline]
pub fn step(stmt: *mut stmt) -> c_int {
    unsafe { invoke_sqlite!(step, stmt) }
}

#[inline]
pub fn user_data(ctx: *mut context) -> *mut c_void {
    unsafe { invoke_sqlite!(user_data, ctx) }
}

pub fn value_text<'a>(arg1: *mut value) -> &'a str {
    unsafe {
        let len = value_bytes(arg1);
        let bytes = invoke_sqlite!(value_text, arg1);
        let slice = core::slice::from_raw_parts(bytes as *const u8, len as usize);
        core::str::from_utf8_unchecked(slice)
    }
}

pub fn value_type(value: *mut value) -> c_int {
    unsafe { invoke_sqlite!(value_type, value) }
}

pub fn value_bytes(arg1: *mut value) -> c_int {
    unsafe { invoke_sqlite!(value_bytes, arg1) }
}

pub fn value_blob<'a>(value: *mut value) -> &'a [u8] {
    unsafe {
        let n = value_bytes(value);
        let b = invoke_sqlite!(value_blob, value);
        core::slice::from_raw_parts(b.cast::<u8>(), n as usize)
    }
}

pub fn value_int(arg1: *mut value) -> c_int {
    unsafe { invoke_sqlite!(value_int, arg1) }
}

pub fn value_int64(arg1: *mut value) -> int64 {
    unsafe { invoke_sqlite!(value_int64, arg1) }
}

pub fn value_double(arg1: *mut value) -> f64 {
    unsafe { invoke_sqlite!(value_double, arg1) }
}

pub fn value_pointer(arg1: *mut value, p: *mut c_char) -> *mut c_void {
    unsafe { invoke_sqlite!(value_pointer, arg1, p) }
}

pub fn vtab_distinct(index_info: *mut index_info) -> c_int {
    unsafe { invoke_sqlite!(vtab_distinct, index_info) }
}

pub fn get_autocommit(db: *mut sqlite3) -> c_int {
    unsafe { invoke_sqlite!(get_autocommit, db) }
}
