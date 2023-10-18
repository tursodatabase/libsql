// Not yet fully migrated from `crsqlite.c`

use core::ffi::{c_char, c_int, CStr};

use alloc::format;
use alloc::string::String;
use core::slice;
#[cfg(not(feature = "std"))]
use num_traits::FromPrimitive;
use sqlite_nostd as sqlite;
use sqlite_nostd::{sqlite3, Connection, ResultCode};

use crate::c::{
    crsql_ExtData, crsql_ensureTableInfosAreUpToDate, crsql_findTableInfo, crsql_getDbVersion,
};

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
    let c_rc = crsql_getDbVersion(db, ext_data, errmsg);
    if c_rc != ResultCode::OK as c_int {
        if let Some(rc) = ResultCode::from_i32(c_rc) {
            return Err(rc);
        }
        return Err(ResultCode::ERROR);
    }
    let current_db_version = (*ext_data).dbVersion;

    // If primary key columns change (in the schema)
    // We need to drop, re-create and backfill
    // the clock table.
    // A change in pk columns means a change in all identities
    // of all rows.
    // We can determine this by comparing pks on clock table vs
    // pks on source table
    let stmt = db.prepare_v2(&format!(
        "SELECT count(name) FROM (
        SELECT name FROM pragma_table_info('{table_name}')
          WHERE pk > 0 AND name NOT IN
            (SELECT name FROM pragma_table_info('{table_name}__crsql_clock') WHERE pk > 0)
          UNION SELECT name FROM pragma_table_info('{table_name}__crsql_clock') WHERE pk > 0 AND name NOT IN 
            (SELECT name FROM pragma_table_info('{table_name}') WHERE pk > 0) AND name != '__crsql_col_name'
        );",
        table_name = crate::util::escape_ident_as_value(tbl_name_str),
    ))?;
    stmt.step()?;

    let pk_diff = stmt.column_int(0)?;
    // immediately drop stmt, otherwise clock table is considered locked.
    drop(stmt);

    if pk_diff > 0 {
        // drop the clock table so we can re-create it
        db.exec_safe(&format!(
            "DROP TABLE \"{table_name}__crsql_clock\"",
            table_name = crate::util::escape_ident(tbl_name_str),
        ))?;
    } else {
        // clock table is still relevant but needs compacting
        // in case columns were removed during the migration

        // First delete entries that no longer have a column
        let sql = format!(
            "DELETE FROM \"{tbl_name_ident}__crsql_clock\" WHERE \"__crsql_col_name\" NOT IN (
              SELECT name FROM pragma_table_info('{tbl_name_val}') UNION SELECT '{cl_sentinel}'
            )",
            tbl_name_ident = crate::util::escape_ident(tbl_name_str),
            tbl_name_val = crate::util::escape_ident_as_value(tbl_name_str),
            cl_sentinel = crate::c::DELETE_SENTINEL,
        );
        db.exec_safe(&sql)?;

        // Next delete entries that no longer have a row
        let mut sql = String::from(
            format!(
              "DELETE FROM \"{tbl_name}__crsql_clock\" WHERE (__crsql_col_name != '-1' OR (__crsql_col_name = '-1' AND __crsql_col_version % 2 != 0)) AND NOT EXISTS (SELECT 1 FROM \"{tbl_name}\" WHERE ",
              tbl_name = crate::util::escape_ident(tbl_name_str),
            ),
        );
        let c_rc = crsql_ensureTableInfosAreUpToDate(db, ext_data, errmsg);
        if c_rc != ResultCode::OK as c_int {
            if let Some(rc) = ResultCode::from_i32(c_rc) {
                return Err(rc);
            }
            return Err(ResultCode::ERROR);
        }
        let table_info = crsql_findTableInfo(
            (*ext_data).zpTableInfos,
            (*ext_data).tableInfosLen,
            tbl_name,
        );
        if table_info.is_null() {
            return Err(ResultCode::ERROR);
        }

        // for each pk col, append \"%w\".\"%w\" = \"%w__crsql_clock\".\"%w\"
        // to the where clause then close the statement.
        let pk_cols = sqlite::args!((*table_info).pksLen, (*table_info).pks);
        for (i, col) in pk_cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            let col_name_str = CStr::from_ptr((*col).name).to_str()?;
            sql.push_str(&format!(
                "\"{tbl_name}\".\"{col_name}\" = \"{tbl_name}__crsql_clock\".\"{col_name}\"",
                tbl_name = tbl_name_str,
                col_name = col_name_str,
            ));
        }
        sql.push_str(" LIMIT 1)");
        db.exec_safe(&sql)?;
    }

    let stmt = db.prepare_v2(
        "INSERT OR REPLACE INTO crsql_master (key, value) VALUES ('pre_compact_dbversion', ?)",
    )?;
    stmt.bind_int64(1, current_db_version)?;
    stmt.step()?;
    Ok(ResultCode::OK)
}
