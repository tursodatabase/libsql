use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::format;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int};
use core::mem;
use sqlite::Stmt;
use sqlite_nostd as sqlite;
use sqlite_nostd::{sqlite3, ResultCode, Value};

use crate::c::crsql_ExtData;
use crate::c::{crsql_Changes_vtab, CrsqlChangesColumn};
use crate::compare_values::crsql_compare_sqlite_values;
use crate::pack_columns::bind_package_to_stmt;
use crate::pack_columns::{unpack_columns, ColumnValue};
use crate::stmt_cache::reset_cached_stmt;
use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, TableInfo};
use crate::util::slab_rowid;

/**
 * did_cid_win does not take into account the causal length.
 * The expectation is that all cuasal length concerns have already been handle
 * via:
 * - early return because insert_cl < local_cl
 * - automatic win because insert_cl > local_cl
 * - come here to did_cid_win iff insert_cl = local_cl
 */
fn did_cid_win(
    db: *mut sqlite3,
    insert_tbl: &str,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    insert_val: *mut sqlite::value,
    col_name: &str,
    col_version: sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<bool, ResultCode> {
    let col_vrsn_stmt_ref = tbl_info.get_col_version_stmt(db)?;
    let col_vrsn_stmt = col_vrsn_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = col_vrsn_stmt.bind_int64(1, key);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_vrsn_stmt.stmt)?;
        return Err(rc);
    }
    if let Err(rc) = col_vrsn_stmt.bind_text(2, col_name, sqlite::Destructor::STATIC) {
        reset_cached_stmt(col_vrsn_stmt.stmt)?;
        return Err(rc);
    }

    match col_vrsn_stmt.step() {
        Ok(ResultCode::ROW) => {
            let local_version = col_vrsn_stmt.column_int64(0);
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            // causal lengths are the same. Fall back to original algorithm.
            if col_version > local_version {
                return Ok(true);
            } else if col_version < local_version {
                return Ok(false);
            }
        }
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            // no rows returned
            // of course the incoming change wins if there's nothing there locally.
            return Ok(true);
        }
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(col_vrsn_stmt.stmt)?;
            let err = CString::new("Bad return code when selecting local column version")?;
            unsafe { *errmsg = err.into_raw() };
            return Err(rc);
        }
    }

    // versions are equal
    // need to compare values
    let col_val_stmt_ref = tbl_info.get_col_value_stmt(db, col_name)?;
    let col_val_stmt = col_val_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = bind_package_to_stmt(col_val_stmt.stmt, &unpacked_pks, 0);
    if let Err(rc) = bind_result {
        reset_cached_stmt(col_val_stmt.stmt)?;
        return Err(rc);
    }

    let step_result = col_val_stmt.step();
    match step_result {
        Ok(ResultCode::ROW) => {
            let local_value = col_val_stmt.column_value(0)?;
            let ret = crsql_compare_sqlite_values(insert_val, local_value);
            reset_cached_stmt(col_val_stmt.stmt)?;
            // value won, take value
            // if values are the same (ret == 0) then we return false and do not take the update
            return Ok(ret > 0);
        }
        _ => {
            // ResultCode::DONE would happen if clock values exist but actual values are missing.
            // should we just allow the insert anyway?
            reset_cached_stmt(col_val_stmt.stmt)?;
            let err = CString::new(format!(
                "could not find row to merge with for tbl {}",
                insert_tbl
            ))?;
            unsafe { *errmsg = err.into_raw() };
            return Err(ResultCode::ERROR);
        }
    }
}

fn set_winner_clock(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    insert_col_name: &str,
    insert_col_vrsn: sqlite::int64,
    insert_db_vrsn: sqlite::int64,
    insert_site_id: &[u8],
    insert_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    // set the site_id ordinal
    // get the returned ordinal
    // use that in place of insert_site_id in the metadata table(s)

    // on changes read, join to gather the proper site id.
    let ordinal = unsafe {
        if insert_site_id.is_empty() {
            None
        } else {
            (*ext_data).pSelectSiteIdOrdinalStmt.bind_blob(
                1,
                insert_site_id,
                sqlite::Destructor::STATIC,
            )?;
            let rc = (*ext_data).pSelectSiteIdOrdinalStmt.step()?;
            if rc == ResultCode::ROW {
                let ordinal = (*ext_data).pSelectSiteIdOrdinalStmt.column_int64(0);
                (*ext_data).pSelectSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSelectSiteIdOrdinalStmt.reset()?;

                Some(ordinal)
            } else {
                (*ext_data).pSelectSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSelectSiteIdOrdinalStmt.reset()?;
                // site id had no ordinal yet.
                // set one and return the ordinal.
                (*ext_data).pSetSiteIdOrdinalStmt.bind_blob(
                    1,
                    insert_site_id,
                    sqlite::Destructor::STATIC,
                )?;
                let rc = (*ext_data).pSetSiteIdOrdinalStmt.step()?;
                if rc == ResultCode::DONE {
                    (*ext_data).pSetSiteIdOrdinalStmt.clear_bindings()?;
                    (*ext_data).pSetSiteIdOrdinalStmt.reset()?;
                    return Err(ResultCode::ABORT);
                }
                let ordinal = (*ext_data).pSetSiteIdOrdinalStmt.column_int64(0);
                (*ext_data).pSetSiteIdOrdinalStmt.clear_bindings()?;
                (*ext_data).pSetSiteIdOrdinalStmt.reset()?;
                Some(ordinal)
            }
        }
    };

    let set_stmt_ref = tbl_info.get_set_winner_clock_stmt(db)?;
    let set_stmt = set_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = set_stmt.bind_int64(1, key);
    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt.stmt)?;
        return Err(rc);
    }
    let bind_result = set_stmt
        .bind_text(2, insert_col_name, sqlite::Destructor::STATIC)
        .and_then(|_| set_stmt.bind_int64(3, insert_col_vrsn))
        .and_then(|_| set_stmt.bind_int64(4, insert_db_vrsn))
        .and_then(|_| set_stmt.bind_int64(5, insert_seq))
        .and_then(|_| match ordinal {
            Some(ordinal) => set_stmt.bind_int64(6, ordinal),
            None => set_stmt.bind_null(6),
        });

    if let Err(rc) = bind_result {
        reset_cached_stmt(set_stmt.stmt)?;
        return Err(rc);
    }

    match set_stmt.step() {
        Ok(ResultCode::ROW) => {
            let rowid = set_stmt.column_int64(0);
            reset_cached_stmt(set_stmt.stmt)?;
            Ok(rowid)
        }
        _ => {
            reset_cached_stmt(set_stmt.stmt)?;
            Err(ResultCode::ERROR)
        }
    }
}

fn merge_sentinel_only_insert(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    remote_col_vrsn: sqlite::int64,
    remote_db_vsn: sqlite::int64,
    remote_site_id: &[u8],
    remote_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let merge_stmt_ref = tbl_info.get_merge_pk_only_insert_stmt(db)?;
    let merge_stmt = merge_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = bind_package_to_stmt(merge_stmt.stmt, unpacked_pks, 0);
    if let Err(rc) = rc {
        reset_cached_stmt(merge_stmt.stmt)?;
        return Err(rc);
    }
    let rc = unsafe {
        (*ext_data)
            .pSetSyncBitStmt
            .step()
            .and_then(|_| (*ext_data).pSetSyncBitStmt.reset())
            .and_then(|_| merge_stmt.step())
    };

    // TODO: report err?
    let _ = reset_cached_stmt(merge_stmt.stmt);

    let sync_rc = unsafe {
        (*ext_data)
            .pClearSyncBitStmt
            .step()
            .and_then(|_| (*ext_data).pClearSyncBitStmt.reset())
    };

    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }
    if let Err(rc) = rc {
        return Err(rc);
    }

    if let Ok(_) = rc {
        zero_clocks_on_resurrect(db, tbl_info, key, remote_db_vsn)?;
        return set_winner_clock(
            db,
            ext_data,
            tbl_info,
            key,
            crate::c::INSERT_SENTINEL,
            remote_col_vrsn,
            remote_db_vsn,
            remote_site_id,
            remote_seq,
        );
    }

    Ok(-1)
}

fn zero_clocks_on_resurrect(
    db: *mut sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
    insert_db_vrsn: sqlite::int64,
) -> Result<ResultCode, ResultCode> {
    let zero_stmt_ref = tbl_info.get_zero_clocks_on_resurrect_stmt(db)?;
    let zero_stmt = zero_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let ret = zero_stmt
        .bind_int64(1, insert_db_vrsn)
        .and_then(|_| zero_stmt.bind_int64(2, key))
        .and_then(|_| zero_stmt.step());
    reset_cached_stmt(zero_stmt.stmt)?;
    return ret;
}

unsafe fn merge_delete(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
    tbl_info: &TableInfo,
    unpacked_pks: &Vec<ColumnValue>,
    key: sqlite::int64,
    remote_col_vrsn: sqlite::int64,
    remote_db_vrsn: sqlite::int64,
    remote_site_id: &[u8],
    remote_seq: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let delete_stmt_ref = tbl_info.get_merge_delete_stmt(db)?;
    let delete_stmt = delete_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    if let Err(rc) = bind_package_to_stmt(delete_stmt.stmt, unpacked_pks, 0) {
        reset_cached_stmt(delete_stmt.stmt)?;
        return Err(rc);
    }
    let rc = (*ext_data)
        .pSetSyncBitStmt
        .step()
        .and_then(|_| (*ext_data).pSetSyncBitStmt.reset())
        .and_then(|_| delete_stmt.step());

    reset_cached_stmt(delete_stmt.stmt)?;

    let sync_rc = (*ext_data)
        .pClearSyncBitStmt
        .step()
        .and_then(|_| (*ext_data).pClearSyncBitStmt.reset());

    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }
    if let Err(rc) = rc {
        return Err(rc);
    }

    let ret = set_winner_clock(
        db,
        ext_data,
        tbl_info,
        key,
        crate::c::DELETE_SENTINEL,
        remote_col_vrsn,
        remote_db_vrsn,
        remote_site_id,
        remote_seq,
    )?;

    // Drop clocks _after_ setting the winner clock so we don't lose track of the max db_version!!
    // This must never come before `set_winner_clock`
    let drop_clocks_stmt_ref = tbl_info.get_merge_delete_drop_clocks_stmt(db)?;
    let drop_clocks_stmt = drop_clocks_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = drop_clocks_stmt
        .bind_int64(1, key)
        .and_then(|_| drop_clocks_stmt.step());
    reset_cached_stmt(drop_clocks_stmt.stmt)?;
    rc?;

    return Ok(ret);
}

#[no_mangle]
pub unsafe extern "C" fn crsql_merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> c_int {
    match merge_insert(vtab, argc, argv, rowid, errmsg) {
        Err(rc) | Ok(rc) => rc as c_int,
    }
}

fn get_local_cl(
    db: *mut sqlite::sqlite3,
    tbl_info: &TableInfo,
    key: sqlite::int64,
) -> Result<sqlite::int64, ResultCode> {
    let local_cl_stmt_ref = tbl_info.get_local_cl_stmt(db)?;
    let local_cl_stmt = local_cl_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let rc = local_cl_stmt.bind_int64(1, key);
    if let Err(rc) = rc {
        reset_cached_stmt(local_cl_stmt.stmt)?;
        return Err(rc);
    }
    let rc = local_cl_stmt.bind_int64(2, key);
    if let Err(rc) = rc {
        reset_cached_stmt(local_cl_stmt.stmt)?;
        return Err(rc);
    }

    let step_result = local_cl_stmt.step();
    match step_result {
        Ok(ResultCode::ROW) => {
            let ret = local_cl_stmt.column_int64(0);
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Ok(ret)
        }
        Ok(ResultCode::DONE) => {
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Ok(0)
        }
        Ok(rc) | Err(rc) => {
            reset_cached_stmt(local_cl_stmt.stmt)?;
            Err(rc)
        }
    }
}

unsafe fn merge_insert(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    rowid: *mut sqlite::int64,
    errmsg: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let tab = vtab.cast::<crsql_Changes_vtab>();
    let db = (*tab).db;

    let rc = crsql_ensure_table_infos_are_up_to_date(db, (*tab).pExtData, errmsg);
    if rc != ResultCode::OK as i32 {
        let err = CString::new("Failed to update CRR table information")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let args = sqlite::args!(argc, argv);
    let insert_tbl = args[2 + CrsqlChangesColumn::Tbl as usize];
    if insert_tbl.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - table name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_tbl = insert_tbl.text();
    let insert_pks = args[2 + CrsqlChangesColumn::Pk as usize];
    let insert_col = args[2 + CrsqlChangesColumn::Cid as usize];
    if insert_col.bytes() > crate::consts::MAX_TBL_NAME_LEN {
        let err = CString::new("crsql - column name exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_col = insert_col.text();
    let insert_val = args[2 + CrsqlChangesColumn::Cval as usize];
    let insert_col_vrsn = args[2 + CrsqlChangesColumn::ColVrsn as usize].int64();
    let insert_db_vrsn = args[2 + CrsqlChangesColumn::DbVrsn as usize].int64();
    let insert_site_id = args[2 + CrsqlChangesColumn::SiteId as usize];
    let insert_cl = args[2 + CrsqlChangesColumn::Cl as usize].int64();
    let insert_seq = args[2 + CrsqlChangesColumn::Seq as usize].int64();

    if insert_site_id.bytes() > crate::consts::SITE_ID_LEN {
        let err = CString::new("crsql - site id exceeded max length")?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }

    let insert_site_id = insert_site_id.blob();
    let tbl_infos = mem::ManuallyDrop::new(Box::from_raw(
        (*(*tab).pExtData).tableInfos as *mut Vec<TableInfo>,
    ));
    // TODO: will this work given `insert_tbl` is null termed?
    let tbl_info_index = tbl_infos.iter().position(|x| x.tbl_name == insert_tbl);

    if tbl_info_index.is_none() {
        let err = CString::new(format!(
            "crsql - could not find the schema information for table {}",
            insert_tbl
        ))?;
        *errmsg = err.into_raw();
        return Err(ResultCode::ERROR);
    }
    // TODO: technically safe since we checked `is_none` but this should be more idiomatic
    let tbl_info_index = tbl_info_index.unwrap();

    let tbl_info = &tbl_infos[tbl_info_index];
    let unpacked_pks = unpack_columns(insert_pks.blob())?;

    // Get or create key as the first thing we do.
    // We'll need the key for all later operations.
    let key = tbl_info.get_or_create_key(db, &unpacked_pks)?;

    let local_cl = get_local_cl(db, &tbl_info, key)?;

    // We can ignore all updates from older causal lengths.
    // They won't win at anything.
    if insert_cl < local_cl {
        return Ok(ResultCode::OK);
    }

    let is_delete = insert_cl % 2 == 0;
    // Resurrect or update to latest cl.
    // The current node might have missed the delete preceeding this causal length
    // in out-of-order delivery setups but we still call it a resurrect as special
    // handling needs to happen in the "alive -> missed_delete -> alive" case.
    let needs_resurrect = insert_cl > local_cl && insert_cl % 2 == 1;
    let row_exists_locally = local_cl != 0;
    let is_sentinel_only = crate::c::INSERT_SENTINEL == insert_col;

    if is_delete {
        // We got a delete event but we've already processed a delete at that version.
        // Just bail.
        if insert_cl == local_cl {
            return Ok(ResultCode::OK);
        }
        // else, it is a delete and the cl is > than ours. Drop the row.
        let merge_result = merge_delete(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                (*(*tab).pExtData).rowsImpacted += 1;
                *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
                return Ok(ResultCode::OK);
            }
        }
    }

    /*
    || crsql_columnExists(
            // TODO: only safe because we _know_ this is actually a cstr
            insert_col.as_ptr() as *const c_char,
            (*tbl_info).nonPks,
            (*tbl_info).nonPksLen,
        ) == 0
     */
    if is_sentinel_only {
        // If it is a sentinel but the local_cl already matches, nothing to do
        // as the local sentinel already has the same data!
        if insert_cl == local_cl {
            return Ok(ResultCode::OK);
        }
        let merge_result = merge_sentinel_only_insert(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_col_vrsn,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        );
        match merge_result {
            Err(rc) => {
                return Err(rc);
            }
            Ok(inner_rowid) => {
                // a success & rowid of -1 means the merge was a no-op
                if inner_rowid != -1 {
                    (*(*tab).pExtData).rowsImpacted += 1;
                    *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
                    return Ok(ResultCode::OK);
                } else {
                    return Ok(ResultCode::OK);
                }
            }
        }
    }

    // we got a causal length which would resurrect the row.
    // In an in-order delivery situation then `sentinel_only` would have already resurrected the row
    // In out-of-order delivery, we need to resurrect the row as soon as we get a value
    // which should resurrect the row. I.e., don't wait on the sentinel value to resurrect the row!
    // If the row does not exist locally and the insert_cl is > 1 then we need to create a sentinel to record the insert cl.
    // Not doing so will cause us to assume a cl of 1.
    if needs_resurrect && (row_exists_locally || (!row_exists_locally && insert_cl > 1)) {
        // this should work -- same as `merge_sentinel_only_insert` except we're not done once we do it
        // and the version to set to is the cl not col_vrsn of current insert
        merge_sentinel_only_insert(
            db,
            (*tab).pExtData,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_cl,
            insert_db_vrsn,
            insert_site_id,
            insert_seq,
        )?;
        (*(*tab).pExtData).rowsImpacted += 1;
    }

    // we can short-circuit via needs_resurrect
    // given the greater cl automatically means a win.
    // or if we realize that the row does not exist locally at all.
    let does_cid_win = needs_resurrect
        || !row_exists_locally
        || did_cid_win(
            db,
            insert_tbl,
            &tbl_info,
            &unpacked_pks,
            key,
            insert_val,
            insert_col,
            insert_col_vrsn,
            errmsg,
        )?;

    if does_cid_win == false {
        // doesCidWin == 0? compared against our clocks, nothing wins. OK and
        // Done.
        return Ok(ResultCode::OK);
    }

    // TODO: this is all almost identical between all three merge cases!
    let merge_stmt_ref = tbl_info.get_merge_insert_stmt(db, insert_col)?;
    let merge_stmt = merge_stmt_ref.as_ref().ok_or(ResultCode::ERROR)?;

    let bind_result = bind_package_to_stmt(merge_stmt.stmt, &unpacked_pks, 0)
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 1, insert_val))
        .and_then(|_| merge_stmt.bind_value(unpacked_pks.len() as i32 + 2, insert_val));
    if let Err(rc) = bind_result {
        reset_cached_stmt(merge_stmt.stmt)?;
        return Err(rc);
    }

    let rc = (*(*tab).pExtData)
        .pSetSyncBitStmt
        .step()
        .and_then(|_| (*(*tab).pExtData).pSetSyncBitStmt.reset())
        .and_then(|_| merge_stmt.step());

    reset_cached_stmt(merge_stmt.stmt)?;

    let sync_rc = (*(*tab).pExtData)
        .pClearSyncBitStmt
        .step()
        .and_then(|_| (*(*tab).pExtData).pClearSyncBitStmt.reset());

    if let Err(rc) = rc {
        return Err(rc);
    }
    if let Err(sync_rc) = sync_rc {
        return Err(sync_rc);
    }

    let merge_result = set_winner_clock(
        db,
        (*tab).pExtData,
        &tbl_info,
        key,
        insert_col,
        insert_col_vrsn,
        insert_db_vrsn,
        insert_site_id,
        insert_seq,
    );
    match merge_result {
        Err(rc) => {
            return Err(rc);
        }
        Ok(inner_rowid) => {
            (*(*tab).pExtData).rowsImpacted += 1;
            *rowid = slab_rowid(tbl_info_index as i32, inner_rowid);
            return Ok(ResultCode::OK);
        }
    }
}
