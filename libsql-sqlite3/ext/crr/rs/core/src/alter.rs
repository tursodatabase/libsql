// Not yet fully migrated from `crsqlite.c`

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int, CStr};
use core::mem;
#[cfg(not(feature = "std"))]
use num_traits::FromPrimitive;
use sqlite_nostd::{sqlite3, Connection, ResultCode, StrRef};

use crate::c::crsql_ExtData;
use crate::db_version::fill_db_version_if_needed;
use crate::tableinfo::{crsql_ensure_table_infos_are_up_to_date, TableInfo};

#[no_mangle]
pub unsafe extern "C" fn crsql_compact_post_alter(
    db: *mut sqlite3,
    tbl_name: *const c_char,
    ext_data: *mut crsql_ExtData,
    errmsg: *mut *mut c_char,
) -> c_int {
    match compact_post_alter(db, tbl_name, ext_data, errmsg) {
        Ok(rc) | Err(rc) => rc as c_int,
    }
}

unsafe fn compact_post_alter(
    db: *mut sqlite3,
    tbl_name: *const c_char,
    ext_data: *mut crsql_ExtData,
    errmsg: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let tbl_name_str = CStr::from_ptr(tbl_name).to_str()?;
    fill_db_version_if_needed(db, ext_data).or_else(|msg| {
        errmsg.set(&msg);
        Err(ResultCode::ERROR)
    })?;
    let current_db_version = (*ext_data).dbVersion;

    // If primary key columns change (in the schema)
    // We need to drop, re-create and backfill
    // the clock table.
    // A change in pk columns means a change in all identities
    // of all rows.
    // We can determine this by comparing unique index on lookaside table vs
    // pks on source table
    let stmt = db.prepare_v2(&format!(
        "SELECT count(name) FROM (
        SELECT name FROM pragma_table_info('{table_name}')
          WHERE pk > 0 AND name NOT IN
            (SELECT name FROM pragma_index_info('{table_name}__crsql_pks_pks'))
          UNION SELECT name FROM pragma_index_info('{table_name}__crsql_pks_pks') WHERE name NOT IN 
            (SELECT name FROM pragma_table_info('{table_name}') WHERE pk > 0) AND name != 'col_name'
        );",
        table_name = crate::util::escape_ident_as_value(tbl_name_str),
    ))?;
    stmt.step()?;

    let pk_diff = stmt.column_int(0);
    // immediately drop stmt, otherwise clock table is considered locked.
    drop(stmt);

    if pk_diff > 0 {
        // drop the clock table so we can re-create it
        db.exec_safe(&format!(
            "DROP TABLE \"{table_name}__crsql_clock\";
             DROP TABLE \"{table_name}__crsql_pks\";",
            table_name = crate::util::escape_ident(tbl_name_str),
        ))?;
    } else {
        // clock table is still relevant but needs compacting
        // in case columns were removed during the migration

        // First delete entries that no longer have a column
        let sql = format!(
            "DELETE FROM \"{tbl_name_ident}__crsql_clock\" WHERE \"col_name\" NOT IN (
              SELECT name FROM pragma_table_info('{tbl_name_val}') UNION SELECT '{cl_sentinel}'
            )",
            tbl_name_ident = crate::util::escape_ident(tbl_name_str),
            tbl_name_val = crate::util::escape_ident_as_value(tbl_name_str),
            cl_sentinel = crate::c::DELETE_SENTINEL,
        );
        db.exec_safe(&sql)?;

        // Next delete entries that no longer have a row but keeping tombstones
        // TODO: if we move the sentinel metadata to the lookaside this becomes much simpler
        let mut sql = String::from(
            format!(
              "DELETE FROM \"{tbl_name}__crsql_clock\" WHERE (col_name != '-1' OR (col_name = '-1' AND col_version % 2 != 0))
              AND NOT EXISTS (SELECT 1 FROM \"{tbl_name}\" JOIN \"{tbl_name}__crsql_pks\" ON ",
              tbl_name = crate::util::escape_ident(tbl_name_str),
            ),
        );
        let c_rc = crsql_ensure_table_infos_are_up_to_date(db, ext_data, errmsg);
        if c_rc != ResultCode::OK as c_int {
            if let Some(rc) = ResultCode::from_i32(c_rc) {
                return Err(rc);
            }
            return Err(ResultCode::ERROR);
        }
        let table_infos =
            mem::ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>));
        let table_info = table_infos.iter().find(|x| x.tbl_name == tbl_name_str);
        if table_info.is_none() {
            return Err(ResultCode::ERROR);
        }
        // TODO: safe since we checked above but make more idiomatic
        let table_info = table_info.unwrap();

        // for each pk col, append \"%w\".\"%w\" = \"%w__crsql_pks\".\"%w\"
        // to the where clause then close the statement.
        for (i, col) in table_info.pks.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }

            sql.push_str(&format!(
                "\"{tbl_name}\".\"{col_name}\" = \"{tbl_name}__crsql_pks\".\"{col_name}\"",
                tbl_name = crate::util::escape_ident(tbl_name_str),
                col_name = &col.name,
            ));
        }
        sql.push_str(
          &format!(
            " WHERE \"{tbl_name}__crsql_clock\".key = \"{tbl_name}__crsql_pks\".__crsql_key LIMIT 1)",
            tbl_name = crate::util::escape_ident(tbl_name_str)
          )
        );
        db.exec_safe(&sql)?;

        // now delete pk lookasides that no longer map to anything in the clock tables
        let sql = format!(
            "DELETE FROM \"{tbl_name}__crsql_pks\" WHERE __crsql_key NOT IN (
        SELECT key FROM \"{tbl_name}__crsql_clock\"
      )",
            tbl_name = crate::util::escape_ident(tbl_name_str),
        );
        db.exec_safe(&sql)?;
    }

    let stmt = db.prepare_v2(
        "INSERT OR REPLACE INTO crsql_master (key, value) VALUES ('pre_compact_dbversion', ?)",
    )?;
    stmt.bind_int64(1, current_db_version)?;
    stmt.step()?;
    Ok(ResultCode::OK)
}
