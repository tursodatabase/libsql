extern crate alloc;
use alloc::{ffi::CString, string::String};
use core::ffi::c_char;
use crsql_bundle::test_exports;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

fn make_site() -> *mut c_char {
    let inner_ptr: *mut c_char = CString::new("0000000000000000").unwrap().into_raw();
    inner_ptr
}

fn test_fetch_db_version_from_storage() -> Result<ResultCode, String> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    let raw_db = db.db;
    let ext_data = unsafe { test_exports::c::crsql_newExtData(raw_db, make_site()) };

    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // no clock tables, no version.
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // this was a bug where calling twice on a fresh db would fail the second
    // time.
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // should still return same data on a subsequent call with no schema
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // create some schemas
    db.exec_safe("CREATE TABLE foo (a primary key not null, b);")
        .expect("made foo");
    db.exec_safe("SELECT crsql_as_crr('foo');")
        .expect("made foo crr");
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    // still v0 since no rows are inserted
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    // version is bumped due to insert
    db.exec_safe("INSERT INTO foo (a, b) VALUES (1, 2);")
        .expect("inserted");
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(1, unsafe { (*ext_data).dbVersion });

    db.exec_safe("CREATE TABLE bar (a primary key not null, b);")
        .expect("created bar");
    db.exec_safe("SELECT crsql_as_crr('bar');")
        .expect("bar as crr");
    db.exec_safe("INSERT INTO bar VALUES (1, 2)")
        .expect("inserted into bar");
    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(2, unsafe { (*ext_data).dbVersion });

    test_exports::db_version::fetch_db_version_from_storage(raw_db, ext_data)?;
    assert_eq!(2, unsafe { (*ext_data).dbVersion });

    unsafe {
        test_exports::c::crsql_freeExtData(ext_data);
    };

    Ok(ResultCode::OK)
}

fn test_next_db_version() -> Result<(), String> {
    let c = crate::opendb().expect("db opened");
    let db = &c.db;
    let raw_db = db.db;
    let ext_data = unsafe { test_exports::c::crsql_newExtData(raw_db, make_site()) };

    // is current + 1
    // doesn't bump forward on successive calls
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data, None)?
    );
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data, None)?
    );
    // doesn't roll back with new provideds
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data, Some(-1))?
    );
    assert_eq!(
        1,
        test_exports::db_version::next_db_version(raw_db, ext_data, Some(0))?
    );
    // sets to max of current and provided
    assert_eq!(
        3,
        test_exports::db_version::next_db_version(raw_db, ext_data, Some(3))?
    );
    assert_eq!(
        3,
        test_exports::db_version::next_db_version(raw_db, ext_data, Some(2))?
    );

    // existing db version not touched
    assert_eq!(0, unsafe { (*ext_data).dbVersion });

    unsafe {
        test_exports::c::crsql_freeExtData(ext_data);
    };
    Ok(())
}

pub fn run_suite() -> Result<(), String> {
    test_fetch_db_version_from_storage()?;
    test_next_db_version()?;
    Ok(())
}
