#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

mod errors;
mod types;

use types::{
    libsql_connection, libsql_connection_t, libsql_database, libsql_database_t, libsql_rows,
    libsql_rows_future, libsql_rows_future_t, libsql_rows_t,
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
    let db = libsql::Database::open(url.to_string()).unwrap();
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
    let db = db.get_ref();
    let conn = match db.connect() {
        Ok(conn) => conn,
        Err(err) => {
            println!("error: {}", err);
            return libsql_connection_t::null();
        }
    };
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
pub unsafe extern "C" fn libsql_execute(conn: libsql_connection_t, sql: *const std::ffi::c_char) {
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) };
    let sql = match sql.to_str() {
        Ok(sql) => sql,
        Err(_) => {
            todo!("bad string");
        }
    };
    let conn = conn.get_ref();
    conn.execute(sql.to_string(), ()).unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_rows(res: libsql_rows_t) {
    if res.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(res.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_execute_async(
    conn: libsql_connection_t,
    sql: *const std::ffi::c_char,
) -> libsql_rows_future_t {
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) };
    let sql = match sql.to_str() {
        Ok(sql) => sql,
        Err(_) => {
            todo!("bad string");
        }
    };
    let conn = conn.get_ref();
    let result = conn.execute_async(sql.to_string(), ());
    let result = Box::leak(Box::new(libsql_rows_future { result }));
    libsql_rows_future_t::from(result)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_rows_future(res: libsql_rows_future_t) {
    if res.is_null() {
        return;
    }
    let mut res = unsafe { Box::from_raw(res.get_ref_mut()) };
    res.wait().unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn libsql_wait_result(res: libsql_rows_future_t) {
    let res = res.get_ref_mut();
    res.wait().unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn libsql_column_count(res: libsql_rows_t) -> std::ffi::c_int {
    let res = res.get_ref();
    res.column_count()
}

#[no_mangle]
pub unsafe extern "C" fn libsql_value_text(
    _res: libsql_rows_t,
    _row: std::ffi::c_int,
    _col: std::ffi::c_int,
) -> *const std::ffi::c_char {
    todo!();
}
