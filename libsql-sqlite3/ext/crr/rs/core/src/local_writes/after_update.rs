use core::ffi::c_int;

use alloc::format;
use alloc::string::String;
use sqlite::{sqlite3, value, Context, ResultCode};
use sqlite_nostd as sqlite;

use crate::compare_values::crsql_compare_sqlite_values;
use crate::{c::crsql_ExtData, tableinfo::TableInfo};

use super::trigger_fn_preamble;

pub unsafe extern "C" fn x_crsql_after_update(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let result = trigger_fn_preamble(ctx, argc, argv, |table_info, values, ext_data| {
        let (pks_new, pks_old, non_pks_new, non_pks_old) =
            partition_values(values, 1, table_info.pks.len(), table_info.non_pks.len())?;

        after_update(
            ctx.db_handle(),
            ext_data,
            table_info,
            pks_new,
            pks_old,
            non_pks_new,
            non_pks_old,
        )
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

fn partition_values<T>(
    values: &[T],
    offset: usize,
    num_pks: usize,
    num_non_pks: usize,
) -> Result<(&[T], &[T], &[T], &[T]), String> {
    let expected_len = offset + num_pks * 2 + num_non_pks * 2;
    if values.len() != expected_len {
        return Err(format!(
            "expected {} values, got {}",
            expected_len,
            values.len()
        ));
    }
    Ok((
        &values[offset..num_pks + offset],
        &values[num_pks + offset..num_pks * 2 + offset],
        &values[num_pks * 2 + offset..num_pks * 2 + num_non_pks + offset],
        &values[num_pks * 2 + num_non_pks + offset..],
    ))
}

fn after_update(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    pks_new: &[*mut value],
    pks_old: &[*mut value],
    non_pks_new: &[*mut value],
    non_pks_old: &[*mut value],
) -> Result<ResultCode, String> {
    let next_db_version = crate::db_version::next_db_version(db, ext_data, None)?;
    let new_key = tbl_info
        .get_or_create_key_via_raw_values(db, pks_new)
        .or_else(|_| Err("failed geteting or creating lookaside key"))?;

    // Changing a primary key column to a new value is the same thing as deleting the row
    // previously identified by the primary key.
    if crate::compare_values::any_value_changed(pks_new, pks_old)? {
        let old_key = tbl_info
            .get_or_create_key_via_raw_values(db, pks_old)
            .or_else(|_| Err("failed geteting or creating lookaside key"))?;
        let next_seq = super::bump_seq(ext_data);
        // Record the delete of the row identified by the old primary keys
        after_update__mark_old_pk_row_deleted(db, tbl_info, old_key, next_db_version, next_seq)?;
        // TODO: each non sentinel needs a unique seq on the move?
        after_update__move_non_sentinels(db, tbl_info, new_key, old_key)?;
        // Record a create of the row identified by the new primary keys
        // if no rows were moved. This is related to the optimization to not save
        // sentinels unless required.
        // if db.changes64() == 0 { <-- an optimization if we can get to it. we'd need to know to increment causal length.
        // so we can get to this when CL is stored in the lookaside.
        let next_seq = super::bump_seq(ext_data);
        super::mark_new_pk_row_created(db, tbl_info, new_key, next_db_version, next_seq)?;
        // }
    }

    // now for each non_pk_col we need to do an insert
    // where new value is not old value
    for ((new, old), col_info) in non_pks_new
        .iter()
        .zip(non_pks_old.iter())
        .zip(tbl_info.non_pks.iter())
    {
        if crsql_compare_sqlite_values(*new, *old) != 0 {
            let next_seq = super::bump_seq(ext_data);
            // we had a difference in new and old values
            // we need to track crdt metadata
            super::mark_locally_updated(
                db,
                tbl_info,
                new_key,
                col_info,
                next_db_version,
                next_seq,
            )?;
        }
    }

    Ok(ResultCode::OK)
}

#[allow(non_snake_case)]
fn after_update__mark_old_pk_row_deleted(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    old_key: sqlite::int64,
    db_version: sqlite::int64,
    seq: i32,
) -> Result<ResultCode, String> {
    let mark_locally_deleted_stmt_ref = tbl_info
        .get_mark_locally_deleted_stmt(db)
        .or_else(|_e| Err("failed to get mark_locally_deleted_stmt"))?;
    let mark_locally_deleted_stmt = mark_locally_deleted_stmt_ref
        .as_ref()
        .ok_or("Failed to deref sentinel stmt")?;
    mark_locally_deleted_stmt
        .bind_int64(1, old_key)
        .and_then(|_| mark_locally_deleted_stmt.bind_int64(2, db_version))
        .and_then(|_| mark_locally_deleted_stmt.bind_int(3, seq))
        .and_then(|_| mark_locally_deleted_stmt.bind_int64(4, db_version))
        .and_then(|_| mark_locally_deleted_stmt.bind_int(5, seq))
        .or_else(|_| Err("failed binding to mark_locally_deleted_stmt"))?;
    super::step_trigger_stmt(mark_locally_deleted_stmt)
}

// TODO: in the future we can keep sentinel information in the lookaside
#[allow(non_snake_case)]
fn after_update__move_non_sentinels(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    new_key: sqlite::int64,
    old_key: sqlite::int64,
) -> Result<ResultCode, String> {
    let move_non_sentinels_stmt_ref = tbl_info
        .get_move_non_sentinels_stmt(db)
        .or_else(|_| Err("failed to get move_non_sentinels_stmt"))?;
    let move_non_sentinels_stmt = move_non_sentinels_stmt_ref
        .as_ref()
        .ok_or("Failed to deref move_non_sentinels_stmt")?;

    move_non_sentinels_stmt
        // set things to new key
        .bind_int64(1, new_key)
        // where they have the old key
        .and_then(|_| move_non_sentinels_stmt.bind_int64(2, old_key))
        .or_else(|_| Err("failed to bind pks to move_non_sentinels_stmt"))?;
    super::step_trigger_stmt(move_non_sentinels_stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_values() {
        let values1 = vec!["tbl", "pk.new", "pk.old", "c.new", "c.old"];
        let values2 = vec!["tbl", "pk.new", "pk.old"];
        let values3 = vec!["tbl", "pk1.new", "pk2.new", "pk1.old", "pk2.old"];
        let values4 = vec![
            "tbl", "pk1.new", "pk2.new", "pk1.old", "pk2.old", "c.new", "d.new", "c.old", "d.old",
        ];

        assert_eq!(
            partition_values(&values1, 1, 1, 1),
            Ok((
                &["pk.new"] as &[&str],
                &["pk.old"] as &[&str],
                &["c.new"] as &[&str],
                &["c.old"] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values2, 1, 1, 0),
            Ok((
                &["pk.new"] as &[&str],
                &["pk.old"] as &[&str],
                &[] as &[&str],
                &[] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values3, 1, 2, 0),
            Ok((
                &["pk1.new", "pk2.new"] as &[&str],
                &["pk1.old", "pk2.old"] as &[&str],
                &[] as &[&str],
                &[] as &[&str]
            ))
        );
        assert_eq!(
            partition_values(&values4, 1, 2, 2),
            Ok((
                &["pk1.new", "pk2.new"] as &[&str],
                &["pk1.old", "pk2.old"] as &[&str],
                &["c.new", "d.new"] as &[&str],
                &["c.old", "d.old"] as &[&str]
            ))
        );
    }
}
