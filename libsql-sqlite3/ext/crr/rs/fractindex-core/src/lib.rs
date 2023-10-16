#![cfg_attr(not(test), no_std)]
#![allow(non_upper_case_globals)]
#![feature(core_intrinsics)]

mod as_ordered;
mod fractindex;
mod fractindex_view;
mod util;

use core::ffi::{c_char, c_int};
use core::slice;
pub use fractindex::*;
use fractindex_view::fix_conflict_return_old_key;
use sqlite::args;
use sqlite::ColumnType;
use sqlite::Connection;
use sqlite::ResultCode;
use sqlite::{Context, Value};
use sqlite_nostd as sqlite;
extern crate alloc;

pub extern "C" fn crsql_fract_as_ordered(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = args!(argc, argv);
    // decode the args, call as_ordered
    if args.len() < 2 {
        ctx.result_error(
            "Must provide at least 2 arguments -- the table name and the column to order by",
        );
        return;
    }

    let db = ctx.db_handle();
    let table = args[0].text();
    let collection_columns = &args[2..];
    as_ordered::as_ordered(ctx, db, table, args[1], collection_columns);
}

pub extern "C" fn crsql_fract_key_between(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = args!(argc, argv);

    let left = args[0];
    let right = args[1];

    let left = if left.value_type() == ColumnType::Null {
        None
    } else {
        Some(left.text())
    };

    let right = if right.value_type() == ColumnType::Null {
        None
    } else {
        Some(right.text())
    };

    let result = key_between(left, right);

    match result {
        Ok(Some(r)) => ctx.result_text_transient(&r),
        Ok(None) => ctx.result_null(),
        Err(r) => ctx.result_error(r),
    }
}

pub extern "C" fn crsql_fract_fix_conflict_return_old_key(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = args!(argc, argv);

    // process args
    // fix_conflict_return_old_key();
    if args.len() < 4 {
        ctx.result_error("Too few arguments to fix_conflict_return_old_key");
        return;
    }
    let table = args[0];
    let order_col = args[1];

    let collection_columns: &[*mut sqlite_nostd::value] = pull_collection_column_names(2, args);
    // 2 is where we started, + how many collection columns + 1 for the separator (-1)
    let next_index = 2 + collection_columns.len() + 1;
    // from next_index we'll read in primary key names and values

    let primary_key_and_value_count = args.len() - next_index;
    if primary_key_and_value_count <= 0 || primary_key_and_value_count % 2 != 0 {
        ctx.result_error("Incorrect number of primary keys and values provided. Must have at least 1 primary key.");
        return;
    }

    let primary_key_count = primary_key_and_value_count / 2;
    let pk_names = &args[next_index..next_index + primary_key_count];
    let pk_values =
        &args[next_index + primary_key_count..next_index + primary_key_count + primary_key_count];

    if let Err(_) = fix_conflict_return_old_key(
        ctx,
        table.text(),
        order_col,
        collection_columns,
        pk_names,
        pk_values,
    ) {
        ctx.result_error("Failed fixing up ordering conflicts on insert");
    }

    return;
}

fn pull_collection_column_names(
    from: usize,
    args: &[*mut sqlite_nostd::value],
) -> &[*mut sqlite_nostd::value] {
    let mut i = from;
    while i < args.len() {
        let next = args[i];
        if next.value_type() == ColumnType::Integer {
            break;
        }
        i += 1;
    }

    return &args[from..i];
}

#[no_mangle]
pub extern "C" fn sqlite3_crsqlfractionalindex_init(
    db: *mut sqlite::sqlite3,
    _err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> c_int {
    sqlite::EXTENSION_INIT2(api);

    if let Err(rc) = db.create_function_v2(
        "crsql_fract_as_ordered",
        -1,
        sqlite::UTF8 | sqlite::DIRECTONLY,
        None,
        Some(crsql_fract_as_ordered),
        None,
        None,
        None,
    ) {
        return rc as c_int;
    }

    if let Err(rc) = db.create_function_v2(
        "crsql_fract_key_between",
        2,
        sqlite::UTF8 | sqlite::DETERMINISTIC | sqlite::INNOCUOUS,
        None,
        Some(crsql_fract_key_between),
        None,
        None,
        None,
    ) {
        return rc as c_int;
    }

    if let Err(rc) = db.create_function_v2(
        "crsql_fract_fix_conflict_return_old_key",
        -1,
        sqlite::UTF8,
        None,
        Some(crsql_fract_fix_conflict_return_old_key),
        None,
        None,
        None,
    ) {
        return rc as c_int;
    }

    ResultCode::OK as c_int
}
