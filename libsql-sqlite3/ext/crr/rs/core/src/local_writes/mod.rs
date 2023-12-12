use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;

use crate::alloc::string::ToString;
use crate::c::crsql_ExtData;
use crate::stmt_cache::reset_cached_stmt;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use sqlite::sqlite3;
use sqlite::{Context, ManagedStmt, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, ColumnInfo, TableInfo};

pub mod after_delete;
pub mod after_insert;
pub mod after_update;

fn trigger_fn_preamble<F>(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    f: F,
) -> Result<ResultCode, String>
where
    F: Fn(&TableInfo, &[*mut sqlite::value], *mut crsql_ExtData) -> Result<ResultCode, String>,
{
    if argc < 1 {
        return Err("expected at least 1 argument".to_string());
    }

    let values = sqlite::args!(argc, argv);
    let ext_data = sqlite::user_data(ctx) as *mut crsql_ExtData;
    let mut inner_err: *mut c_char = core::ptr::null_mut();
    let outer_err: *mut *mut c_char = &mut inner_err;

    let rc = crsql_ensure_table_infos_are_up_to_date(ctx.db_handle(), ext_data, outer_err);
    if rc != ResultCode::OK as c_int {
        return Err(format!(
            "failed to ensure table infos are up to date: {}",
            rc
        ));
    }

    let table_infos =
        unsafe { ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>)) };
    let table_name = values[0].text();
    let table_info = match table_infos.iter().find(|t| &(t.tbl_name) == table_name) {
        Some(t) => t,
        None => {
            return Err(format!("table {} not found", table_name));
        }
    };

    f(table_info, &values, ext_data)
}

fn step_trigger_stmt(stmt: &ManagedStmt) -> Result<ResultCode, String> {
    match stmt.step() {
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(stmt.stmt)
                .or_else(|_e| Err("done -- unable to reset cached trigger stmt"))?;
            Ok(ResultCode::OK)
        }
        Ok(code) | Err(code) => {
            reset_cached_stmt(stmt.stmt)
                .or_else(|_e| Err("error -- unable to reset cached trigger stmt"))?;
            Err(format!(
                "unexpected result code from tigger_stmt.step: {}",
                code
            ))
        }
    }
}

fn mark_new_pk_row_created(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key_new: sqlite::int64,
    db_version: i64,
    seq: i32,
) -> Result<ResultCode, String> {
    let mark_locally_created_stmt_ref = tbl_info
        .get_mark_locally_created_stmt(db)
        .or_else(|_e| Err("failed to get mark_locally_created_stmt"))?;
    let mark_locally_created_stmt = mark_locally_created_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;

    mark_locally_created_stmt
        .bind_int64(1, key_new)
        .and_then(|_| mark_locally_created_stmt.bind_int64(2, db_version))
        .and_then(|_| mark_locally_created_stmt.bind_int(3, seq))
        .and_then(|_| mark_locally_created_stmt.bind_int64(4, db_version))
        .and_then(|_| mark_locally_created_stmt.bind_int(5, seq))
        .or_else(|_| Err("failed binding to mark_locally_created_stmt"))?;
    step_trigger_stmt(mark_locally_created_stmt)
}

fn bump_seq(ext_data: *mut crsql_ExtData) -> c_int {
    unsafe {
        (*ext_data).seq += 1;
        (*ext_data).seq - 1
    }
}

#[allow(non_snake_case)]
fn mark_locally_updated(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    col_info: &ColumnInfo,
    db_version: sqlite::int64,
    seq: i32,
) -> Result<ResultCode, String> {
    let mark_locally_updated_stmt_ref = tbl_info
        .get_mark_locally_updated_stmt(db)
        .or_else(|_e| Err("failed to get mark_locally_updated_stmt"))?;
    let mark_locally_updated_stmt = mark_locally_updated_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;

    mark_locally_updated_stmt
        .bind_int64(1, new_key)
        .and_then(|_| {
            mark_locally_updated_stmt.bind_text(2, &col_info.name, sqlite::Destructor::STATIC)
        })
        .and_then(|_| mark_locally_updated_stmt.bind_int64(3, db_version))
        .and_then(|_| mark_locally_updated_stmt.bind_int(4, seq))
        .and_then(|_| mark_locally_updated_stmt.bind_int64(5, db_version))
        .and_then(|_| mark_locally_updated_stmt.bind_int(6, seq))
        .or_else(|_| Err("failed binding to mark_locally_updated_stmt"))?;
    step_trigger_stmt(mark_locally_updated_stmt)
}
