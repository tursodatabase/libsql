extern crate alloc;
use core::ffi::c_void;
use core::mem::forget;
use core::ptr::null_mut;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use sqlite::Stmt;
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

use crate::c::crsql_ExtData;

// port the stmt cache so we can
// - start removing some unsafe code
// - remove uthash and just use rust btreemap
pub enum CachedStmtType {
    SetWinnerClock = 0,
    GetLocalCl = 1,
    GetColVersion = 2,
    // can we one day delete this and use site id for ties?
    // if we do, how does that impact the backup and restore story?
    // e.g., restoring a database snapshot on a new machine with a new siteid but
    // bootstrapped from a backup?
    // If we track that "we've seen this restored node since the backup point with the old site_id"
    // then site_id comparisons could change merge results after restore for nodes that
    // have different "seen since" records for the old site_id.
    GetCurrValue = 3,
    MergePkOnlyInsert = 4,
    MergeDelete = 5,
    MergeInsert = 6,
    RowPatchData = 7,
    // We zero clocks, rather than going to 1, because
    // the current values should be totally ignored at all sites.
    // This is because the current values would not exist had the current node
    // processed the intervening delete.
    // This also means that col_version is not always >= 1. A resurrected column,
    // which missed a delete event, will have a 0 version.
    ZeroClocksOnResurrect = 8,
    MergeDeleteDropClocks = 9,
}

#[no_mangle]
pub extern "C" fn crsql_init_stmt_cache(ext_data: *mut crsql_ExtData) {
    let map: BTreeMap<String, *mut sqlite::stmt> = BTreeMap::new();
    unsafe {
        (*ext_data).pStmtCache = Box::into_raw(Box::new(map)) as *mut c_void;
    }
}

#[no_mangle]
pub extern "C" fn crsql_clear_stmt_cache(ext_data: *mut crsql_ExtData) {
    if unsafe { (*ext_data).pStmtCache.is_null() } {
        return;
    }
    let map: Box<BTreeMap<String, *mut sqlite::stmt>> = unsafe {
        Box::from_raw((*ext_data).pStmtCache as *mut BTreeMap<String, *mut sqlite::stmt>)
    };
    for (_key, stmt) in map.iter() {
        let _ = stmt.finalize();
    }
    unsafe {
        (*ext_data).pStmtCache = null_mut();
    }
}

pub fn get_cache_key(
    stmt_type: CachedStmtType,
    tbl_name: &str,
    col_name: Option<&str>,
) -> Result<String, ResultCode> {
    match stmt_type {
        CachedStmtType::SetWinnerClock
        | CachedStmtType::GetLocalCl
        | CachedStmtType::GetColVersion
        | CachedStmtType::MergePkOnlyInsert
        | CachedStmtType::MergeDelete
        | CachedStmtType::ZeroClocksOnResurrect
        | CachedStmtType::MergeDeleteDropClocks => {
            if col_name.is_some() {
                // col name should not be specified for these cases
                return Err(ResultCode::MISUSE);
            }
            Ok(format!(
                "{stmt_type}_{tbl_name}",
                stmt_type = (stmt_type as i32).to_string(),
                tbl_name = tbl_name
            ))
        }
        CachedStmtType::GetCurrValue
        | CachedStmtType::MergeInsert
        | CachedStmtType::RowPatchData => {
            if let Some(col_name) = col_name {
                Ok(format!(
                    "{stmt_type}_{tbl_name}_{col_name}",
                    stmt_type = (stmt_type as i32).to_string(),
                    tbl_name = tbl_name,
                    col_name = col_name
                ))
            } else {
                // col_name must be specified in this case
                Err(ResultCode::MISUSE)
            }
        }
    }
}

pub fn set_cached_stmt(ext_data: *mut crsql_ExtData, key: String, stmt: *mut sqlite::stmt) {
    // give ownership of the key to C
    let mut map: Box<BTreeMap<String, *mut sqlite::stmt>> = unsafe {
        Box::from_raw((*ext_data).pStmtCache as *mut BTreeMap<String, *mut sqlite::stmt>)
    };
    map.insert(key, stmt);
    // C owns this memory.
    forget(map);
}

pub fn get_cached_stmt(ext_data: *mut crsql_ExtData, key: &String) -> Option<*mut sqlite::stmt> {
    let map: Box<BTreeMap<String, *mut sqlite::stmt>> = unsafe {
        Box::from_raw((*ext_data).pStmtCache as *mut BTreeMap<String, *mut sqlite::stmt>)
    };
    let ret = map.get(key).copied();
    // C owns this memory
    forget(map);
    return ret;
}

pub fn reset_cached_stmt(stmt: *mut sqlite::stmt) -> Result<ResultCode, ResultCode> {
    if stmt.is_null() {
        return Ok(ResultCode::OK);
    }
    stmt.clear_bindings()?;
    stmt.reset()
}
