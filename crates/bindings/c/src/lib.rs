#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

mod errors;
mod types;

use types::{
    blob, libsql_connection, libsql_connection_t, libsql_database, libsql_database_t, libsql_row,
    libsql_row_t, libsql_rows, libsql_rows_future, libsql_rows_future_t, libsql_rows_t,
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
pub unsafe extern "C" fn libsql_execute(
    conn: libsql_connection_t,
    sql: *const std::ffi::c_char,
) -> libsql_rows_t {
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) };
    let sql = match sql.to_str() {
        Ok(sql) => sql,
        Err(_) => {
            todo!("bad string");
        }
    };
    let conn = conn.get_ref();
    let rows = conn.execute(sql.to_string(), ()).unwrap();
    match rows {
        Some(rows) => {
            let rows = Box::leak(Box::new(libsql_rows { result: rows }));
            libsql_rows_t::from(rows)
        }
        None => libsql_rows_t::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_rows(res: libsql_rows_t) {
    if res.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(res.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_execute_async<'a>(
    conn: &'a libsql_connection_t,
    sql: *const std::ffi::c_char,
) -> libsql_rows_future_t<'a> {
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
pub unsafe extern "C" fn libsql_column_name(
    res: libsql_rows_t,
    col: std::ffi::c_int,
) -> *const std::ffi::c_char {
    let res = res.get_ref();
    if col >= res.column_count() {
        return std::ptr::null();
    }
    let name = res.column_name(col);
    match std::ffi::CString::new(name) {
        Ok(name) => name.into_raw(),
        Err(_) => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_column_type(
    res: libsql_rows_t,
    col: std::ffi::c_int,
) -> std::ffi::c_int {
    let res = res.get_ref();
    if col >= res.column_count() {
        return -1;
    }
    res.column_type(col)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_next_row(res: libsql_rows_t) -> libsql_row_t {
    if res.is_null() {
        return libsql_row_t::null();
    }
    let res = res.get_ref();
    match res.next() {
        Ok(Some(row)) => {
            let row = Box::leak(Box::new(libsql_row { result: row }));
            libsql_row_t::from(row)
        }
        _ => libsql_row_t::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_row(res: libsql_row_t) {
    if res.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(res.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_string(
    res: libsql_row_t,
    col: std::ffi::c_int,
) -> *const std::ffi::c_char {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Text(s)) => match std::ffi::CString::new(s) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null(),
        },
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_string(ptr: *const std::ffi::c_char) {
    if !ptr.is_null() {
        let _ = unsafe { std::ffi::CString::from_raw(ptr as *mut _) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_int(
    res: libsql_row_t,
    col: std::ffi::c_int,
) -> std::ffi::c_longlong {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Integer(i)) => i,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_float(
    res: libsql_row_t,
    col: std::ffi::c_int,
) -> std::ffi::c_double {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Real(f)) => f,
        _ => 0.0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_blob(res: libsql_row_t, col: std::ffi::c_int) -> blob {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Blob(v)) => {
            let len: i32 = v.len().try_into().unwrap();
            let buf = v.into_boxed_slice();
            let data = buf.as_ptr();
            std::mem::forget(buf);
            blob {
                ptr: data as *const i8,
                len,
            }
        }
        _ => blob {
            ptr: std::ptr::null(),
            len: 0,
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_blob(b: blob) {
    if !b.ptr.is_null() {
        let ptr =
            unsafe { std::slice::from_raw_parts_mut(b.ptr as *mut i8, b.len.try_into().unwrap()) };
        let _ = unsafe { Box::from_raw(ptr) };
    }
}
