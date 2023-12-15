extern crate alloc;
use alloc::vec::Vec;
use core::mem::ManuallyDrop;

use alloc::boxed::Box;
use sqlite::Stmt;
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

use crate::c::crsql_ExtData;
use crate::tableinfo::TableInfo;

// Finalize prepared statements attached to table infos.
// Do not drop the table infos.
// We do this explicitly since `drop` cannot return an error and we want to
// return the error / not panic.
#[no_mangle]
pub extern "C" fn crsql_clear_stmt_cache(ext_data: *mut crsql_ExtData) {
    let tbl_infos =
        unsafe { ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>)) };
    for tbl_info in tbl_infos.iter() {
        // TODO: return an error.
        let _ = tbl_info.clear_stmts();
    }
}

pub fn reset_cached_stmt(stmt: *mut sqlite::stmt) -> Result<ResultCode, ResultCode> {
    if stmt.is_null() {
        return Ok(ResultCode::OK);
    }
    stmt.clear_bindings()?;
    stmt.reset()
}
