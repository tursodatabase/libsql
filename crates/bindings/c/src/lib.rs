#![allow(non_camel_case_types)]

mod errors;
mod types;

use errors::libsql_error;
use types::{
    libsql_connection, libsql_connection_t, libsql_database, libsql_database_t, libsql_result,
    libsql_result_t,
};

#[no_mangle]
pub unsafe extern "C" fn libsql_open_ext(url: *const std::ffi::c_char) -> libsql_database_t {
    let url = unsafe { std::ffi::CStr::from_ptr(url) };
    let url = match url.to_str() {
        Ok(url) => url,
        Err(_) => {
            return libsql_database_t::null();
        }
    };
    let db = libsql::Database::open(url);
    let db = Box::leak(Box::new(libsql_database { db }));
    libsql_database_t::from(db)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_close(db: libsql_database_t) {
    if db.is_null() {
        return;
    }
    let db = unsafe { Box::from_raw(db.get_ref_mut()) };
    db.close();
}

#[no_mangle]
pub unsafe extern "C" fn libsql_connect(db: libsql_database_t) -> libsql_connection_t {
    let conn = libsql::Connection {};
    let conn = Box::leak(Box::new(libsql_connection { conn }));
    libsql_connection_t::from(conn)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_disconnect(conn: libsql_connection_t) {
    if conn.is_null() {
        return;
    }
    let conn = unsafe { Box::from_raw(conn.get_ref_mut()) };
    conn.disconnect();
}

#[no_mangle]
pub unsafe extern "C" fn libsql_execute(
    _conn: libsql_connection_t,
    _sql: *const std::ffi::c_char,
) -> libsql_result_t {
    let result = libsql::Result {};
    let result = Box::leak(Box::new(libsql_result { result }));
    libsql_result_t::from(result)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_wait_result(_res: libsql_result_t) {}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_result(res: libsql_result_t) {
    if res.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(res.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_row_count(res: libsql_result_t) -> std::ffi::c_int {
    let res = res.get_ref();
    res.row_count()
}

#[no_mangle]
pub unsafe extern "C" fn libsql_column_count(res: libsql_result_t) -> std::ffi::c_int {
    let res = res.get_ref();
    res.column_count()
}

#[no_mangle]
pub unsafe extern "C" fn libsql_value_text(
    _res: libsql_result_t,
    _row: std::ffi::c_int,
    _col: std::ffi::c_int,
) -> *const std::ffi::c_char {
    todo!();
}
