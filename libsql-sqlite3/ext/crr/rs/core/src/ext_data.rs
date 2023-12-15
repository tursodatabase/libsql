extern crate alloc;
use crate::alloc::string::ToString;
use alloc::vec;
use core::ffi::c_int;
use core::ptr::null_mut;

use sqlite::{sqlite3, Connection, ResultCode, Stmt};
use sqlite_nostd as sqlite;

use crate::{c::crsql_ExtData, util::get_db_version_union_query};

#[no_mangle]
pub extern "C" fn crsql_recreate_db_version_stmt(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
) -> c_int {
    match recreate_db_version_stmt(db, ext_data) {
        Ok(ResultCode::DONE) => -1, // negative 1 means no clock tables exist and there is nothing to fetch
        Ok(rc) | Err(rc) => rc as c_int,
    }
}

pub fn recreate_db_version_stmt(
    db: *mut sqlite3,
    ext_data: *mut crsql_ExtData,
) -> Result<ResultCode, ResultCode> {
    let clock_tables_stmt = unsafe { (*ext_data).pSelectClockTablesStmt };
    let db_version_stmt = unsafe { (*ext_data).pDbVersionStmt };

    db_version_stmt.finalize()?;
    unsafe {
        (*ext_data).pDbVersionStmt = null_mut();
    }

    let mut clock_tbl_names = vec![];
    loop {
        match clock_tables_stmt.step() {
            Ok(ResultCode::DONE) => {
                clock_tables_stmt.reset()?;
                if clock_tbl_names.len() == 0 {
                    return Ok(ResultCode::DONE);
                }
                break;
            }
            Ok(ResultCode::ROW) => {
                clock_tbl_names.push(clock_tables_stmt.column_text(0).to_string());
            }
            Ok(rc) | Err(rc) => {
                clock_tables_stmt.reset()?;
                return Err(rc);
            }
        }
    }

    let union = get_db_version_union_query(&clock_tbl_names);

    let db_version_stmt = db.prepare_v3(&union, sqlite::PREPARE_PERSISTENT)?;
    unsafe {
        (*ext_data).pDbVersionStmt = db_version_stmt.into_raw();
    }

    Ok(ResultCode::OK)
}
