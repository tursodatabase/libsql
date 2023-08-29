#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]
#[macro_use]
extern crate lazy_static;

mod types;

use tokio::runtime::Runtime;
use types::{
    blob, libsql_connection, libsql_connection_t, libsql_database, libsql_database_t, libsql_row,
    libsql_row_t, libsql_rows, libsql_rows_future_t, libsql_rows_t,
};

lazy_static! {
    static ref RT: Runtime = tokio::runtime::Runtime::new().unwrap();
}

fn translate_string(s: String) -> *const std::ffi::c_char {
    match std::ffi::CString::new(s) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null(),
    }
}

unsafe fn set_err_msg(msg: String, output: *mut *const std::ffi::c_char) {
    if !output.is_null() {
        *output = translate_string(msg);
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_sync(
    db: libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let db = db.get_ref();
    match RT.block_on(db.sync()) {
        Ok(_) => 0,
        Err(e) => {
            set_err_msg(
                format!("Error syncing database: {}", e.to_string()),
                out_err_msg,
            );
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_sync(
    db_path: *const std::ffi::c_char,
    primary_url: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let db_path = unsafe { std::ffi::CStr::from_ptr(db_path) };
    let db_path = match db_path.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {}", e.to_string()), out_err_msg);
            return 1;
        }
    };
    let primary_url = unsafe { std::ffi::CStr::from_ptr(primary_url) };
    let primary_url = match primary_url.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {}", e.to_string()), out_err_msg);
            return 1;
        }
    };
    match RT.block_on(libsql::v2::Database::open_with_sync(
        db_path.to_string(),
        primary_url,
        "",
    )) {
        Ok(db) => {
            let db = Box::leak(Box::new(libsql_database { db }));
            *out_db = libsql_database_t::from(db);
            0
        }
        Err(e) => {
            set_err_msg(
                format!(
                    "Error opening db path {}, primary url {}: {}",
                    db_path.to_string(),
                    primary_url.to_string(),
                    e.to_string()
                ),
                out_err_msg,
            );
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_ext(
    url: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let url = unsafe { std::ffi::CStr::from_ptr(url) };
    let url = match url.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {}", e.to_string()), out_err_msg);
            return 1;
        }
    };
    match libsql::v2::Database::open(url.to_string()) {
        Ok(db) => {
            let db = Box::leak(Box::new(libsql_database { db }));
            *out_db = libsql_database_t::from(db);
            0
        }
        Err(e) => {
            set_err_msg(
                format!("Error opening URL {}: {}", url.to_string(), e.to_string()),
                out_err_msg,
            );
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_close(db: libsql_database_t) {
    if db.is_null() {
        return;
    }
    let _db = unsafe { Box::from_raw(db.get_ref_mut()) };
    // TODO close db
}

#[no_mangle]
pub unsafe extern "C" fn libsql_connect(
    db: libsql_database_t,
    out_conn: *mut libsql_connection_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let db = db.get_ref();
    let conn = match RT.block_on(db.connect()) {
        Ok(conn) => conn,
        Err(err) => {
            set_err_msg(format!("Unable to connect: {}", err), out_err_msg);
            return 1;
        }
    };
    let conn = Box::leak(Box::new(libsql_connection { conn }));
    *out_conn = libsql_connection_t::from(conn);
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_disconnect(conn: libsql_connection_t) {
    if conn.is_null() {
        return;
    }
    let _conn = unsafe { Box::from_raw(conn.get_ref_mut()) };
    // TODO close conn
}

#[no_mangle]
pub unsafe extern "C" fn libsql_execute(
    conn: libsql_connection_t,
    sql: *const std::ffi::c_char,
    out_rows: *mut libsql_rows_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let sql = unsafe { std::ffi::CStr::from_ptr(sql) };
    let sql = match sql.to_str() {
        Ok(sql) => sql,
        Err(e) => {
            set_err_msg(format!("Wrong SQL: {}", e), out_err_msg);
            return 1;
        }
    };
    let conn = conn.get_ref();
    match RT.block_on(conn.query(sql, ())) {
        Ok(rows) => {
            let rows = Box::leak(Box::new(libsql_rows { result: rows }));
            *out_rows = libsql_rows_t::from(rows);
        }
        Err(e) => {
            set_err_msg(format!("Error executing statement: {}", e), out_err_msg);
            return 1;
        }
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_rows(res: libsql_rows_t) {
    if res.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(res.get_ref_mut()) };
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
    out_name: *mut *const std::ffi::c_char,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    if col >= res.column_count() {
        set_err_msg(
            format!(
                "Column index too big - got index {} with {} columns",
                col,
                res.column_count()
            ),
            out_err_msg,
        );
        return 1;
    }
    let name = res
        .column_name(col)
        .expect("Column should have valid index");
    match std::ffi::CString::new(name) {
        Ok(name) => {
            *out_name = name.into_raw();
            0
        }
        Err(e) => {
            set_err_msg(format!("Invalid name: {}", e), out_err_msg);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_column_type(
    res: libsql_rows_t,
    col: std::ffi::c_int,
    out_type: *mut std::ffi::c_int,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    if col >= res.column_count() {
        set_err_msg(
            format!(
                "Column index too big - got index {} with {} columns",
                col,
                res.column_count()
            ),
            out_err_msg,
        );
        return 1;
    }
    match res.column_type(col) {
        Ok(t) => {
            *out_type = t as i32;
        }
        Err(e) => {
            set_err_msg(format!("Invalid type: {}", e), out_err_msg);
            return 2;
        }
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_next_row(
    res: libsql_rows_t,
    out_row: *mut libsql_row_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if res.is_null() {
        *out_row = libsql_row_t::null();
        return 0;
    }
    let res = res.get_ref_mut();
    match res.next() {
        Ok(Some(row)) => {
            let row = Box::leak(Box::new(libsql_row { result: row }));
            *out_row = libsql_row_t::from(row);
            0
        }
        Ok(None) => {
            *out_row = libsql_row_t::null();
            0
        }
        Err(e) => {
            *out_row = libsql_row_t::null();
            set_err_msg(format!("Error fetching next row: {}", e), out_err_msg);
            1
        }
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
    out_value: *mut *const std::ffi::c_char,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Text(s)) => {
            *out_value = translate_string(s);
            0
        }
        Ok(_) => {
            set_err_msg(format!("Value not a string"), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {}", e), out_err_msg);
            2
        }
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
    out_value: *mut std::ffi::c_longlong,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Integer(i)) => {
            *out_value = i;
            0
        }
        Ok(_) => {
            set_err_msg(format!("Value not an integer"), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {}", e), out_err_msg);
            2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_float(
    res: libsql_row_t,
    col: std::ffi::c_int,
    out_value: *mut std::ffi::c_double,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Real(f)) => {
            *out_value = f;
            0
        }
        Ok(_) => {
            set_err_msg(format!("Value not a float"), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {}", e), out_err_msg);
            2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_get_blob(
    res: libsql_row_t,
    col: std::ffi::c_int,
    out_blob: *mut blob,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let res = res.get_ref();
    match res.get_value(col) {
        Ok(libsql::params::Value::Blob(v)) => {
            let len: i32 = v.len().try_into().unwrap();
            let buf = v.into_boxed_slice();
            let data = buf.as_ptr();
            std::mem::forget(buf);
            *out_blob = blob {
                ptr: data as *const std::ffi::c_char,
                len,
            };
            0
        }
        Ok(_) => {
            set_err_msg(format!("Value not a float"), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {}", e), out_err_msg);
            2
        }
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
