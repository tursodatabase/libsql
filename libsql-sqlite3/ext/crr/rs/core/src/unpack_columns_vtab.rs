extern crate alloc;

use core::ffi::{c_char, c_int, c_void};
use core::slice;

use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::format;
use alloc::vec::Vec;
use sqlite::{Connection, Context, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

use crate::{unpack_columns, ColumnValue};

#[derive(Debug)]
enum Columns {
    CELL = 0,
    PACKAGE = 1,
}

extern "C" fn connect(
    db: *mut sqlite::sqlite3,
    _aux: *mut c_void,
    _argc: c_int,
    _argv: *const *const c_char,
    vtab: *mut *mut sqlite::vtab,
    _err: *mut *mut c_char,
) -> c_int {
    // TODO: more ergonomic rust binding for this
    if let Err(rc) = sqlite::declare_vtab(db, "CREATE TABLE x(cell ANY, package BLOB hidden);") {
        return rc as c_int;
    }

    unsafe {
        // TODO: more ergonomic rust bindings
        *vtab = Box::into_raw(Box::new(sqlite::vtab {
            nRef: 0,
            pModule: core::ptr::null(),
            pLibsqlModule: core::ptr::null(),
            zErrMsg: core::ptr::null_mut(),
        }));
        let _ = sqlite::vtab_config(db, sqlite::INNOCUOUS);
    }
    ResultCode::OK as c_int
}

extern "C" fn disconnect(vtab: *mut sqlite::vtab) -> c_int {
    unsafe {
        drop(Box::from_raw(vtab));
    }
    ResultCode::OK as c_int
}

extern "C" fn best_index(vtab: *mut sqlite::vtab, index_info: *mut sqlite::index_info) -> c_int {
    // TODO: better bindings to create this slice for the user
    let constraints = unsafe {
        slice::from_raw_parts_mut(
            (*index_info).aConstraint,
            (*index_info).nConstraint as usize,
        )
    };
    let constraint_usage = unsafe {
        slice::from_raw_parts_mut(
            (*index_info).aConstraintUsage,
            (*index_info).nConstraint as usize,
        )
    };

    for (i, constraint) in constraints.iter().enumerate() {
        if constraint.usable == 0 {
            continue;
        }
        if constraint.iColumn != Columns::PACKAGE as i32 {
            unsafe {
                (*vtab).zErrMsg = CString::new(format!(
                    "no package column specified. Got {:?} instead",
                    Columns::PACKAGE
                ))
                .map_or(core::ptr::null_mut(), |f| f.into_raw());
            }
            return ResultCode::MISUSE as c_int;
        } else {
            constraint_usage[i].argvIndex = 1;
            constraint_usage[i].omit = 1;
        }
    }

    ResultCode::OK as c_int
}

#[repr(C)]
struct Cursor {
    base: sqlite::vtab_cursor,
    crsr: usize,
    unpacked: Option<Vec<ColumnValue>>,
}

extern "C" fn open(_vtab: *mut sqlite::vtab, cursor: *mut *mut sqlite::vtab_cursor) -> c_int {
    unsafe {
        let boxed = Box::new(Cursor {
            base: sqlite::vtab_cursor {
                pVtab: core::ptr::null_mut(),
            },
            crsr: 0,
            unpacked: None,
        });
        let raw_cursor = Box::into_raw(boxed);
        *cursor = raw_cursor.cast::<sqlite::vtab_cursor>();
    }

    ResultCode::OK as c_int
}

extern "C" fn close(cursor: *mut sqlite::vtab_cursor) -> c_int {
    let crsr = cursor.cast::<Cursor>();
    unsafe {
        drop(Box::from_raw(crsr));
    }
    ResultCode::OK as c_int
}

extern "C" fn filter(
    cursor: *mut sqlite::vtab_cursor,
    _idx_num: c_int,
    _idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) -> c_int {
    // pull out package arg as set up by xBestIndex (should always be argv0)
    // stick into cursor
    let args = sqlite::args!(argc, argv);
    if args.len() < 1 {
        unsafe {
            (*(*cursor).pVtab).zErrMsg = CString::new("Zero args passed to filter")
                .map_or(core::ptr::null_mut(), |f| f.into_raw());
        }
        return ResultCode::MISUSE as c_int;
    }

    let crsr = cursor.cast::<Cursor>();
    unsafe {
        if let Ok(cols) = unpack_columns(args[0].blob()) {
            (*crsr).unpacked = Some(cols);
            (*crsr).crsr = 0;
        } else {
            return ResultCode::ERROR as c_int;
        }
    }

    ResultCode::OK as c_int
}

extern "C" fn next(cursor: *mut sqlite::vtab_cursor) -> c_int {
    // go so long as crsr < unpacked.len
    // if crsr == unpacked.len continue
    // else, return done
    let crsr = cursor.cast::<Cursor>();
    unsafe {
        (*crsr).crsr += 1;
    }
    ResultCode::OK as c_int
}

extern "C" fn eof(cursor: *mut sqlite::vtab_cursor) -> c_int {
    // crsr >= unpacked.len
    let crsr = cursor.cast::<Cursor>();
    unsafe {
        match &(*crsr).unpacked {
            Some(cols) => {
                if (*crsr).crsr >= cols.len() {
                    1
                } else {
                    0
                }
            }
            None => 1,
        }
    }
}

extern "C" fn column(
    cursor: *mut sqlite::vtab_cursor,
    ctx: *mut sqlite::context,
    col_num: c_int,
) -> c_int {
    let crsr = cursor.cast::<Cursor>();
    if col_num == Columns::CELL as i32 {
        unsafe {
            if let Some(cols) = &(*crsr).unpacked {
                let col_value = &cols[(*crsr).crsr];
                match col_value {
                    ColumnValue::Blob(b) => {
                        ctx.result_blob_static(b);
                    }
                    ColumnValue::Float(f) => {
                        ctx.result_double(*f);
                    }
                    ColumnValue::Integer(i) => {
                        ctx.result_int64(*i);
                    }
                    ColumnValue::Null => {
                        ctx.result_null();
                    }
                    ColumnValue::Text(t) => {
                        ctx.result_text_static(t);
                    }
                }
                ResultCode::OK as c_int
            } else {
                (*(*cursor).pVtab).zErrMsg = CString::new("No columns to unpack!")
                    .map_or(core::ptr::null_mut(), |f| f.into_raw());
                ResultCode::ABORT as c_int
            }
        }
    } else {
        unsafe {
            (*(*cursor).pVtab).zErrMsg =
                CString::new(format!("Selected a column besides cell! {}", col_num))
                    .map_or(core::ptr::null_mut(), |f| f.into_raw());
        }
        ResultCode::MISUSE as c_int
    }
}

extern "C" fn rowid(cursor: *mut sqlite::vtab_cursor, row_id: *mut sqlite::int64) -> c_int {
    let crsr = cursor.cast::<Cursor>();
    unsafe { *row_id = (*crsr).crsr as i64 }
    ResultCode::OK as c_int
}

static MODULE: sqlite_nostd::module = sqlite_nostd::module {
    iVersion: 0,
    xCreate: None,
    xConnect: Some(connect),
    xBestIndex: Some(best_index),
    xDisconnect: Some(disconnect),
    xDestroy: None,
    xOpen: Some(open),
    xClose: Some(close),
    xFilter: Some(filter),
    xNext: Some(next),
    xEof: Some(eof),
    xColumn: Some(column),
    xRowid: Some(rowid),
    xUpdate: None,
    xBegin: None,
    xSync: None,
    xCommit: None,
    xRollback: None,
    xFindFunction: None,
    xRename: None,
    xSavepoint: None,
    xRelease: None,
    xRollbackTo: None,
    xShadowName: None,
    xIntegrity: None,
};

/**
 * CREATE TABLE [x] (cell, package HIDDEN);
 * SELECT cell FROM crsql_unpack_columns WHERE package = ___;
 */
pub fn create_module(db: *mut sqlite::sqlite3) -> Result<ResultCode, ResultCode> {
    db.create_module_v2("crsql_unpack_columns", &MODULE, None, None)?;

    Ok(ResultCode::OK)
}
