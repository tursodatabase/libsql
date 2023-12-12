use alloc::string::String;
use core::ffi::c_int;
use sqlite::sqlite3;
use sqlite::value;
use sqlite::Context;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;

use crate::{c::crsql_ExtData, tableinfo::TableInfo};

use super::bump_seq;
use super::trigger_fn_preamble;

/**
 * crsql_after_delete("table", old_pk_values...)
 */
pub unsafe extern "C" fn x_crsql_after_delete(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let result = trigger_fn_preamble(ctx, argc, argv, |table_info, values, ext_data| {
        after_delete(ctx.db_handle(), ext_data, table_info, &values[1..])
    });

    match result {
        Ok(_) => {
            ctx.result_int64(0);
        }
        Err(msg) => {
            ctx.result_error(&msg);
        }
    }
}

fn after_delete(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    pks_old: &[*mut value],
) -> Result<ResultCode, String> {
    let db_version = crate::db_version::next_db_version(db, ext_data, None)?;
    let seq = bump_seq(ext_data);
    let key = tbl_info
        .get_or_create_key_via_raw_values(db, pks_old)
        .or_else(|_| Err("failed geteting or creating lookaside key"))?;

    let mark_locally_deleted_stmt_ref = tbl_info
        .get_mark_locally_deleted_stmt(db)
        .or_else(|_e| Err("failed to get mark_locally_deleted_stmt"))?;
    let mark_locally_deleted_stmt = mark_locally_deleted_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;
    mark_locally_deleted_stmt
        .bind_int64(1, key)
        .and_then(|_| mark_locally_deleted_stmt.bind_int64(2, db_version))
        .and_then(|_| mark_locally_deleted_stmt.bind_int(3, seq))
        .and_then(|_| mark_locally_deleted_stmt.bind_int64(4, db_version))
        .and_then(|_| mark_locally_deleted_stmt.bind_int(5, seq))
        .or_else(|_| Err("failed binding to mark locally deleted stmt"))?;
    super::step_trigger_stmt(mark_locally_deleted_stmt)?;

    // now actually delete the row metadata
    let drop_clocks_stmt_ref = tbl_info
        .get_merge_delete_drop_clocks_stmt(db)
        .or_else(|_e| Err("failed to get mark_locally_deleted_stmt"))?;
    let drop_clocks_stmt = drop_clocks_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;

    drop_clocks_stmt
        .bind_int64(1, key)
        .or_else(|_e| Err("failed to bind pks to drop_clocks_stmt"))?;
    super::step_trigger_stmt(drop_clocks_stmt)
}
