#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]
#[macro_use]
extern crate lazy_static;

mod types;

use crate::types::libsql_config;
use libsql::{errors, LoadExtensionGuard};
use tokio::runtime::Runtime;
use types::{
    blob, libsql_connection, libsql_connection_t, libsql_database, libsql_database_t, libsql_row,
    libsql_row_t, libsql_rows, libsql_rows_future_t, libsql_rows_t, libsql_stmt, libsql_stmt_t,
    replicated, stmt,
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
            set_err_msg(format!("Error syncing database: {e}"), out_err_msg);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_sync2(
    db: libsql_database_t,
    out_replicated: *mut replicated,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let db = db.get_ref();
    match RT.block_on(db.sync()) {
        Ok(replicated) => {
            if !out_replicated.is_null() {
                (*out_replicated).frame_no = replicated.frame_no().unwrap_or(0) as i32;
                (*out_replicated).frames_synced = replicated.frames_synced() as i32;
            }

            0
        }
        Err(e) => {
            set_err_msg(format!("Error syncing database: {e}"), out_err_msg);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_sync(
    db_path: *const std::ffi::c_char,
    primary_url: *const std::ffi::c_char,
    auth_token: *const std::ffi::c_char,
    read_your_writes: std::ffi::c_char,
    encryption_key: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let config = libsql_config {
        db_path,
        primary_url,
        auth_token,
        read_your_writes,
        encryption_key,
        sync_interval: 0,
        with_webpki: 0,
    };
    libsql_open_sync_with_config(config, out_db, out_err_msg)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_sync_with_webpki(
    db_path: *const std::ffi::c_char,
    primary_url: *const std::ffi::c_char,
    auth_token: *const std::ffi::c_char,
    read_your_writes: std::ffi::c_char,
    encryption_key: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let config = libsql_config {
        db_path,
        primary_url,
        auth_token,
        read_your_writes,
        encryption_key,
        sync_interval: 0,
        with_webpki: 1,
    };
    libsql_open_sync_with_config(config, out_db, out_err_msg)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_sync_with_config(
    config: libsql_config,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let db_path = unsafe { std::ffi::CStr::from_ptr(config.db_path) };
    let db_path = match db_path.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {e}"), out_err_msg);
            return 1;
        }
    };
    let primary_url = unsafe { std::ffi::CStr::from_ptr(config.primary_url) };
    let primary_url = match primary_url.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {e}"), out_err_msg);
            return 2;
        }
    };
    let auth_token = unsafe { std::ffi::CStr::from_ptr(config.auth_token) };
    let auth_token = match auth_token.to_str() {
        Ok(token) => token,
        Err(e) => {
            set_err_msg(format!("Wrong Auth Token: {e}"), out_err_msg);
            return 3;
        }
    };
    let mut builder = libsql::Builder::new_remote_replica(
        db_path,
        primary_url.to_string(),
        auth_token.to_string(),
    );
    if config.with_webpki != 0 {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        builder = builder.connector(https);
    }
    if config.sync_interval > 0 {
        let interval = match config.sync_interval.try_into() {
            Ok(d) => d,
            Err(e) => {
                set_err_msg(format!("Wrong periodic sync interval: {e}"), out_err_msg);
                return 4;
            }
        };
        builder = builder.sync_interval(std::time::Duration::from_secs(interval));
    }
    builder = builder.read_your_writes(config.read_your_writes != 0);
    if !config.encryption_key.is_null() {
        let key = unsafe { std::ffi::CStr::from_ptr(config.encryption_key) };
        let key = match key.to_str() {
            Ok(k) => k,
            Err(e) => {
                set_err_msg(format!("Wrong encryption key: {e}"), out_err_msg);
                return 5;
            }
        };
        let key = bytes::Bytes::copy_from_slice(key.as_bytes());
        let config = libsql::EncryptionConfig::new(libsql::Cipher::Aes256Cbc, key);
        builder = builder.encryption_config(config)
    };
    match RT.block_on(builder.build()) {
        Ok(db) => {
            let db = Box::leak(Box::new(libsql_database { db }));
            *out_db = libsql_database_t::from(db);
            0
        }
        Err(e) => {
            set_err_msg(
                format!("Error opening db path {db_path}, primary url {primary_url}: {e}"),
                out_err_msg,
            );
            6
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_ext(
    url: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    libsql_open_file(url, out_db, out_err_msg)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_file(
    url: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let url = unsafe { std::ffi::CStr::from_ptr(url) };
    let url = match url.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {e}"), out_err_msg);
            return 1;
        }
    };
    match RT.block_on(libsql::Builder::new_local(url).build()) {
        Ok(db) => {
            let db = Box::leak(Box::new(libsql_database { db }));
            *out_db = libsql_database_t::from(db);
            0
        }
        Err(e) => {
            set_err_msg(format!("Error opening URL {url}: {e}"), out_err_msg);
            1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_remote(
    url: *const std::ffi::c_char,
    auth_token: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    libsql_open_remote_internal(url, auth_token, false, out_db, out_err_msg)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_open_remote_with_webpki(
    url: *const std::ffi::c_char,
    auth_token: *const std::ffi::c_char,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    libsql_open_remote_internal(url, auth_token, true, out_db, out_err_msg)
}

unsafe fn libsql_open_remote_internal(
    url: *const std::ffi::c_char,
    auth_token: *const std::ffi::c_char,
    with_webpki: bool,
    out_db: *mut libsql_database_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let url = unsafe { std::ffi::CStr::from_ptr(url) };
    let url = match url.to_str() {
        Ok(url) => url,
        Err(e) => {
            set_err_msg(format!("Wrong URL: {e}"), out_err_msg);
            return 1;
        }
    };
    let auth_token = unsafe { std::ffi::CStr::from_ptr(auth_token) };
    let auth_token = match auth_token.to_str() {
        Ok(token) => token,
        Err(e) => {
            set_err_msg(format!("Wrong Auth Token: {e}"), out_err_msg);
            return 2;
        }
    };
    let mut builder = libsql::Builder::new_remote(url.to_string(), auth_token.to_string());
    if with_webpki {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        builder = builder.connector(https);
    }
    match RT.block_on(builder.build()) {
        Ok(db) => {
            let db = Box::leak(Box::new(libsql_database { db }));
            *out_db = libsql_database_t::from(db);
            0
        }
        Err(e) => {
            set_err_msg(format!("Error opening URL {url}: {e}"), out_err_msg);
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
    let conn = match db.connect() {
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
pub unsafe extern "C" fn libsql_load_extension(
    conn: libsql_connection_t,
    path: *const std::ffi::c_char,
    entry_point: *const std::ffi::c_char,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if path.is_null() {
        set_err_msg("Null path".to_string(), out_err_msg);
        return 1;
    }
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = match path.to_str() {
        Ok(path) => path,
        Err(e) => {
            set_err_msg(format!("Wrong path: {}", e), out_err_msg);
            return 2;
        }
    };
    let mut entry_point_option = None;
    if !entry_point.is_null() {
        let entry_point = unsafe { std::ffi::CStr::from_ptr(entry_point) };
        entry_point_option = match entry_point.to_str() {
            Ok(entry_point) => Some(entry_point),
            Err(e) => {
                set_err_msg(format!("Wrong entry point: {}", e), out_err_msg);
                return 4;
            }
        };
    }
    if conn.is_null() {
        set_err_msg("Null connection".to_string(), out_err_msg);
        return 5;
    }
    let conn = conn.get_ref();
    match RT.block_on(async move {
        let _guard = LoadExtensionGuard::new(conn)?;
        conn.load_extension(path, entry_point_option)?;
        Ok::<(), errors::Error>(())
    }) {
        Ok(()) => {}
        Err(e) => {
            set_err_msg(format!("Error loading extension: {}", e), out_err_msg);
            return 6;
        }
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_reset(
    conn: libsql_connection_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if conn.is_null() {
        set_err_msg("Null connection".to_string(), out_err_msg);
        return 1;
    }
    let conn = conn.get_ref();
    RT.block_on(conn.reset());
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_disconnect(conn: libsql_connection_t) {
    if conn.is_null() {
        return;
    }
    let conn = unsafe { Box::from_raw(conn.get_ref_mut()) };
    RT.spawn_blocking(|| {
        drop(conn);
    });
}

#[no_mangle]
pub unsafe extern "C" fn libsql_prepare(
    conn: libsql_connection_t,
    sql: *const std::ffi::c_char,
    out_stmt: *mut libsql_stmt_t,
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
    if conn.is_null() {
        set_err_msg("Null connection".to_string(), out_err_msg);
        return 2;
    }
    let conn = conn.get_ref();
    match RT.block_on(conn.prepare(sql)) {
        Ok(stmt) => {
            let stmt = Box::leak(Box::new(libsql_stmt {
                stmt: stmt {
                    stmt,
                    params: vec![],
                },
            }));
            *out_stmt = libsql_stmt_t::from(stmt);
        }
        Err(e) => {
            set_err_msg(format!("Error preparing statement: {}", e), out_err_msg);
            return 3;
        }
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_bind_int(
    stmt: libsql_stmt_t,
    idx: std::ffi::c_int,
    value: std::ffi::c_longlong,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let idx: usize = match idx.try_into() {
        Ok(x) => x,
        Err(e) => {
            set_err_msg(format!("Wrong param index: {}", e), out_err_msg);
            return 1;
        }
    };
    let stmt = stmt.get_ref_mut();
    if stmt.params.len() < idx {
        stmt.params.resize(idx, libsql::Value::Null);
    }
    stmt.params[idx - 1] = value.into();
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_bind_float(
    stmt: libsql_stmt_t,
    idx: std::ffi::c_int,
    value: std::ffi::c_double,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let idx: usize = match idx.try_into() {
        Ok(x) => x,
        Err(e) => {
            set_err_msg(format!("Wrong param index: {}", e), out_err_msg);
            return 1;
        }
    };
    let stmt = stmt.get_ref_mut();
    if stmt.params.len() < idx {
        stmt.params.resize(idx, libsql::Value::Null);
    }
    stmt.params[idx - 1] = value.into();
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_bind_null(
    stmt: libsql_stmt_t,
    idx: std::ffi::c_int,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let idx: usize = match idx.try_into() {
        Ok(x) => x,
        Err(e) => {
            set_err_msg(format!("Wrong param index: {}", e), out_err_msg);
            return 1;
        }
    };
    let stmt = stmt.get_ref_mut();
    if stmt.params.len() < idx {
        stmt.params.resize(idx, libsql::Value::Null);
    }
    stmt.params[idx - 1] = libsql::Value::Null;
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_bind_string(
    stmt: libsql_stmt_t,
    idx: std::ffi::c_int,
    value: *const std::ffi::c_char,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let idx: usize = match idx.try_into() {
        Ok(x) => x,
        Err(e) => {
            set_err_msg(format!("Wrong param index: {}", e), out_err_msg);
            return 1;
        }
    };
    let value = unsafe { std::ffi::CStr::from_ptr(value) };
    let value = match value.to_str() {
        Ok(v) => v,
        Err(e) => {
            set_err_msg(format!("Wrong param value: {}", e), out_err_msg);
            return 2;
        }
    };
    let stmt = stmt.get_ref_mut();
    if stmt.params.len() < idx {
        stmt.params.resize(idx, libsql::Value::Null);
    }
    stmt.params[idx - 1] = value.to_string().into();
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_bind_blob(
    stmt: libsql_stmt_t,
    idx: std::ffi::c_int,
    value: *const std::ffi::c_uchar,
    value_len: std::ffi::c_int,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let idx: usize = match idx.try_into() {
        Ok(x) => x,
        Err(e) => {
            set_err_msg(format!("Wrong param index: {}", e), out_err_msg);
            return 1;
        }
    };
    let value_len: usize = match value_len.try_into() {
        Ok(v) => v,
        Err(e) => {
            set_err_msg(format!("Wrong param value len: {}", e), out_err_msg);
            return 2;
        }
    };
    let value = unsafe { core::slice::from_raw_parts(value, value_len) };
    let value = Vec::from(value);
    let stmt = stmt.get_ref_mut();
    if stmt.params.len() < idx {
        stmt.params.resize(idx, libsql::Value::Null);
    }
    stmt.params[idx - 1] = value.into();
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_query_stmt(
    stmt: libsql_stmt_t,
    out_rows: *mut libsql_rows_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if stmt.is_null() {
        set_err_msg("Null statement".to_string(), out_err_msg);
        return 1;
    }
    let stmt = stmt.get_ref_mut();
    match RT.block_on(stmt.stmt.query(stmt.params.clone())) {
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
pub unsafe extern "C" fn libsql_execute_stmt(
    stmt: libsql_stmt_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if stmt.is_null() {
        set_err_msg("Null statement".to_string(), out_err_msg);
        return 1;
    }
    let stmt = stmt.get_ref_mut();
    match RT.block_on(stmt.stmt.execute(stmt.params.clone())) {
        Ok(_) => 0,
        Err(e) => {
            set_err_msg(format!("Error executing statement: {}", e), out_err_msg);
            2
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn libsql_reset_stmt(
    stmt: libsql_stmt_t,
    out_err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    if stmt.is_null() {
        set_err_msg("Null statement".to_string(), out_err_msg);
        return 1;
    }
    let stmt = stmt.get_ref_mut();
    stmt.params.clear();
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_free_stmt(stmt: libsql_stmt_t) {
    if stmt.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(stmt.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_query(
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
pub unsafe extern "C" fn libsql_execute(
    conn: libsql_connection_t,
    sql: *const std::ffi::c_char,
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
    match RT.block_on(conn.execute(sql, ())) {
        Ok(_) => 0,
        Err(e) => {
            set_err_msg(format!("Error executing statement: {}", e), out_err_msg);
            2
        }
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
    row: libsql_row_t,
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
    let row = row.get_ref();
    match row.get_value(col) {
        Ok(libsql::Value::Null) => {
            *out_type = types::LIBSQL_NULL as i32;
        }
        Ok(libsql::Value::Text(_)) => {
            *out_type = types::LIBSQL_TEXT as i32;
        }
        Ok(libsql::Value::Integer(_)) => {
            *out_type = types::LIBSQL_INT as i32;
        }
        Ok(libsql::Value::Real(_)) => {
            *out_type = types::LIBSQL_FLOAT as i32;
        }
        Ok(libsql::Value::Blob(_)) => {
            *out_type = types::LIBSQL_BLOB as i32;
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {e}"), out_err_msg);
            return 2;
        }
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn libsql_changes(conn: libsql_connection_t) -> u64 {
    let conn = conn.get_ref();
    conn.changes()
}

#[no_mangle]
pub unsafe extern "C" fn libsql_last_insert_rowid(conn: libsql_connection_t) -> i64 {
    let conn = conn.get_ref();
    conn.last_insert_rowid()
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
    let rows = res.get_ref_mut();
    let res = RT.block_on(rows.next());
    match res {
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
        Ok(libsql::Value::Text(s)) => {
            *out_value = translate_string(s);
            0
        }
        Ok(_) => {
            set_err_msg("Value not a string".into(), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {e}"), out_err_msg);
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
        Ok(libsql::Value::Integer(i)) => {
            *out_value = i;
            0
        }
        Ok(_) => {
            set_err_msg("Value not an integer".into(), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {e}"), out_err_msg);
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
        Ok(libsql::Value::Real(f)) => {
            *out_value = f;
            0
        }
        Ok(_) => {
            set_err_msg("Value not a float".into(), out_err_msg);
            1
        }
        Err(e) => {
            set_err_msg(format!("Error fetching value: {e}"), out_err_msg);
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
        Ok(libsql::Value::Blob(v)) => {
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
            set_err_msg("Value not a float".into(), out_err_msg);
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
