#![cfg_attr(not(test), no_std)]
#![feature(vec_into_raw_parts)]

mod alter;
mod automigrate;
mod backfill;
mod bootstrap;
mod c;
mod changes_vtab;
mod changes_vtab_read;
mod changes_vtab_write;
mod compare_values;
mod consts;
mod create_cl_set_vtab;
mod is_crr;
mod pack_columns;
mod stmt_cache;
mod teardown;
mod triggers;
mod unpack_columns_vtab;
mod util;

use core::{ffi::c_char, slice};
extern crate alloc;
use alloc::vec::Vec;
pub use automigrate::*;
pub use backfill::*;
use core::ffi::{c_int, CStr};
pub use is_crr::*;
use pack_columns::crsql_pack_columns;
pub use pack_columns::unpack_columns;
pub use pack_columns::ColumnValue;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};
pub use teardown::*;

pub extern "C" fn crsql_as_table(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = sqlite::args!(argc, argv);
    let db = ctx.db_handle();
    let table = args[0].text();

    if let Err(_) = db.exec_safe("SAVEPOINT as_table;") {
        ctx.result_error("failed to start as_table savepoint");
        return;
    }

    if let Err(_) = crsql_as_table_impl(db, table) {
        ctx.result_error("failed to downgrade the crr");
        if let Err(_) = db.exec_safe("ROLLBACK TO as_table;") {
            // fine.
        }
        return;
    }

    if let Err(_) = db.exec_safe("RELEASE as_table;") {
        // fine
    }
}

fn crsql_as_table_impl(db: *mut sqlite::sqlite3, table: &str) -> Result<ResultCode, ResultCode> {
    remove_crr_clock_table_if_exists(db, table)?;
    remove_crr_triggers_if_exist(db, table)
}

#[no_mangle]
pub extern "C" fn sqlite3_crsqlcore_init(
    db: *mut sqlite::sqlite3,
    _err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> c_int {
    sqlite::EXTENSION_INIT2(api);

    let rc = db
        .create_function_v2(
            "crsql_automigrate",
            -1,
            sqlite::UTF8,
            None,
            Some(crsql_automigrate),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return rc as c_int;
    }

    let rc = db
        .create_function_v2(
            "crsql_pack_columns",
            -1,
            sqlite::UTF8,
            None,
            Some(crsql_pack_columns),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return rc as c_int;
    }

    let rc = db
        .create_function_v2(
            "crsql_as_table",
            1,
            sqlite::UTF8,
            None,
            Some(crsql_as_table),
            None,
            None,
            None,
        )
        .unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return rc as c_int;
    }

    let rc = unpack_columns_vtab::create_module(db).unwrap_or(sqlite::ResultCode::ERROR);
    if rc != ResultCode::OK {
        return rc as c_int;
    }
    let rc = create_cl_set_vtab::create_module(db).unwrap_or(ResultCode::ERROR);
    return rc as c_int;
}

#[no_mangle]
pub extern "C" fn crsql_backfill_table(
    db: *mut sqlite::sqlite3,
    table: *const c_char,
    pk_cols: *const *const c_char,
    pk_cols_len: c_int,
    non_pk_cols: *const *const c_char,
    non_pk_cols_len: c_int,
    is_commit_alter: c_int,
    no_tx: c_int,
) -> c_int {
    let table = unsafe { CStr::from_ptr(table).to_str() };
    let pk_cols = unsafe {
        let parts = slice::from_raw_parts(pk_cols, pk_cols_len as usize);
        parts
            .iter()
            .map(|&p| CStr::from_ptr(p).to_str())
            .collect::<Result<Vec<_>, _>>()
    };
    let non_pk_cols = unsafe {
        let parts = slice::from_raw_parts(non_pk_cols, non_pk_cols_len as usize);
        parts
            .iter()
            .map(|&p| CStr::from_ptr(p).to_str())
            .collect::<Result<Vec<_>, _>>()
    };

    let result = match (table, pk_cols, non_pk_cols) {
        (Ok(table), Ok(pk_cols), Ok(non_pk_cols)) => backfill_table(
            db,
            table,
            pk_cols,
            non_pk_cols,
            is_commit_alter != 0,
            no_tx != 0,
        ),
        _ => Err(ResultCode::ERROR),
    };

    match result {
        Ok(result) => result as c_int,
        Err(result) => result as c_int,
    }
}

#[no_mangle]
pub extern "C" fn crsql_remove_crr_triggers_if_exist(
    db: *mut sqlite::sqlite3,
    table: *const c_char,
) -> c_int {
    if let Ok(table) = unsafe { CStr::from_ptr(table).to_str() } {
        let result = remove_crr_triggers_if_exist(db, table);
        match result {
            Ok(result) => result as c_int,
            Err(result) => result as c_int,
        }
    } else {
        ResultCode::NOMEM as c_int
    }
}

#[no_mangle]
pub extern "C" fn crsql_is_crr(db: *mut sqlite::sqlite3, table: *const c_char) -> c_int {
    if let Ok(table) = unsafe { CStr::from_ptr(table).to_str() } {
        match is_crr(db, table) {
            Ok(b) => {
                if b {
                    1
                } else {
                    0
                }
            }
            Err(c) => (c as c_int) * -1,
        }
    } else {
        (ResultCode::NOMEM as c_int) * -1
    }
}
