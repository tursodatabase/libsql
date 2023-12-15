extern crate alloc;
use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::{ffi::c_char, mem};
use crsql_bundle::test_exports;
use crsql_bundle::test_exports::tableinfo::TableInfo;
use sqlite::Connection;
use sqlite_nostd as sqlite;

// Unfortunate circumstance that we still have some C code that requires this argument
fn make_err_ptr() -> *mut *mut c_char {
    let boxed = Box::new(core::ptr::null_mut() as *mut c_char);
    return Box::into_raw(boxed);
}

fn drop_err_ptr(err: *mut *mut c_char) {
    unsafe {
        let ptr = Box::from_raw(err);
        if ptr.is_null() {
            return;
        }
        let _ = CString::from_raw(*ptr);
    }
}

fn make_site() -> *mut c_char {
    let inner_ptr: *mut c_char = CString::new("0000000000000000").unwrap().into_raw();
    inner_ptr
}

fn test_ensure_table_infos_are_up_to_date() {
    let db = crate::opendb().expect("Opened DB");
    let c = &db.db;
    let raw_db = db.db.db;
    let err = make_err_ptr();

    // manually create some clock tables w/o using the extension
    // pull table info and ensure it is what we expect
    c.exec_safe("CREATE TABLE foo (a PRIMARY KEY NOT NULL, b);")
        .expect("made foo");
    c.exec_safe(
        "CREATE TABLE foo__crsql_clock (
      id,
      col_name,
      col_version,
      db_version,
      site_id,
      seq
    )",
    )
    .expect("made foo clock");

    let ext_data = unsafe { test_exports::c::crsql_newExtData(raw_db, make_site()) };
    test_exports::tableinfo::crsql_ensure_table_infos_are_up_to_date(raw_db, ext_data, err);

    let mut table_infos = unsafe {
        mem::ManuallyDrop::new(Box::from_raw((*ext_data).tableInfos as *mut Vec<TableInfo>))
    };

    assert_eq!(table_infos.len(), 1);
    assert_eq!(table_infos[0].tbl_name, "foo");

    // we're going to change table infos so we can check that it does not get filled again since no schema changes happened
    table_infos[0].tbl_name = "bar".to_string();

    unsafe {
        (*ext_data).updatedTableInfosThisTx = 0;
    }
    test_exports::tableinfo::crsql_ensure_table_infos_are_up_to_date(raw_db, ext_data, err);

    assert_eq!(table_infos.len(), 1);
    assert_eq!(table_infos[0].tbl_name, "bar");

    c.exec_safe("CREATE TABLE boo (a PRIMARY KEY NOT NULL, b);")
        .expect("made boo");
    c.exec_safe(
        "CREATE TABLE boo__crsql_clock (
      id,
      col_name,
      col_version,
      db_version,
      site_id,
      seq
    )",
    )
    .expect("made boo clock");

    unsafe {
        (*ext_data).updatedTableInfosThisTx = 0;
    }
    test_exports::tableinfo::crsql_ensure_table_infos_are_up_to_date(raw_db, ext_data, err);

    assert_eq!(table_infos.len(), 2);
    assert_eq!(table_infos[0].tbl_name, "foo");
    assert_eq!(table_infos[1].tbl_name, "boo");

    c.exec_safe("DROP TABLE foo").expect("dropped foo");
    c.exec_safe("DROP TABLE boo").expect("dropped boo");
    c.exec_safe("DROP TABLE boo__crsql_clock")
        .expect("dropped boo");
    c.exec_safe("DROP TABLE foo__crsql_clock")
        .expect("dropped boo");

    unsafe {
        (*ext_data).updatedTableInfosThisTx = 0;
    }
    test_exports::tableinfo::crsql_ensure_table_infos_are_up_to_date(raw_db, ext_data, err);
    drop_err_ptr(err);

    assert_eq!(table_infos.len(), 0);

    unsafe {
        test_exports::c::crsql_freeExtData(ext_data);
    };
}

fn test_pull_table_info() {
    let db = crate::opendb().expect("Opened DB");
    let c = &db.db;
    let raw_db = db.db.db;
    let err = make_err_ptr();
    // test that we receive the expected values in column info and such.
    // pks are ordered
    // pks and non pks split
    // cids filled

    c.exec_safe(
        "CREATE TABLE foo (a INTEGER PRIMARY KEY NOT NULL, b TEXT NOT NULL, c NUMBER, d FLOAT, e);",
    )
    .expect("made foo");

    let tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "foo", err)
        .expect("pulled table info for foo");
    assert_eq!(tbl_info.pks.len(), 1);
    assert_eq!(tbl_info.pks[0].name, "a");
    assert_eq!(tbl_info.pks[0].cid, 0);
    assert_eq!(tbl_info.pks[0].pk, 1);
    assert_eq!(tbl_info.non_pks.len(), 4);
    assert_eq!(tbl_info.non_pks[0].name, "b");
    assert_eq!(tbl_info.non_pks[0].cid, 1);
    assert_eq!(tbl_info.non_pks[1].name, "c");
    assert_eq!(tbl_info.non_pks[1].cid, 2);
    assert_eq!(tbl_info.non_pks[2].name, "d");
    assert_eq!(tbl_info.non_pks[2].cid, 3);
    assert_eq!(tbl_info.non_pks[3].name, "e");
    assert_eq!(tbl_info.non_pks[3].cid, 4);

    c.exec_safe("CREATE TABLE boo (a INTEGER, b TEXT NOT NULL, c NUMBER NOT NULL, d FLOAT NOT NULL, e NOT NULL, PRIMARY KEY(b, c, d, e));")
        .expect("made boo");
    let tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "boo", err)
        .expect("pulled table info for boo");
    assert_eq!(tbl_info.pks.len(), 4);
    assert_eq!(tbl_info.pks[0].name, "b");
    assert_eq!(tbl_info.pks[0].cid, 1);
    assert_eq!(tbl_info.pks[0].pk, 1);
    assert_eq!(tbl_info.pks[1].name, "c");
    assert_eq!(tbl_info.pks[1].cid, 2);
    assert_eq!(tbl_info.pks[1].pk, 2);
    assert_eq!(tbl_info.pks[2].name, "d");
    assert_eq!(tbl_info.pks[2].cid, 3);
    assert_eq!(tbl_info.pks[2].pk, 3);
    assert_eq!(tbl_info.pks[3].name, "e");
    assert_eq!(tbl_info.pks[3].cid, 4);
    assert_eq!(tbl_info.pks[3].pk, 4);
    assert_eq!(tbl_info.non_pks.len(), 1);
    assert_eq!(tbl_info.non_pks[0].name, "a");
    assert_eq!(tbl_info.non_pks[0].cid, 0);
    assert_eq!(tbl_info.non_pks[0].pk, 0);
    drop_err_ptr(err);
}

fn test_is_table_compatible() {
    let db = crate::opendb().expect("Opened DB");
    let c = &db.db;
    let raw_db = db.db.db;
    let err = make_err_ptr();
    // convert the commented out test below into a format that resembles the tests above
    // and then run it

    // no pks
    c.exec_safe("CREATE TABLE foo (a);").expect("made foo");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "foo", err)
        .expect("checked if foo is compatible");
    assert_eq!(is_compatible, false);

    // pks
    c.exec_safe("CREATE TABLE bar (a PRIMARY KEY NOT NULL);")
        .expect("made bar");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "bar", err)
        .expect("checked if bar is compatible");
    assert_eq!(is_compatible, true);

    // nullable pks
    c.exec_safe("CREATE TABLE bal (a PRIMARY KEY);")
        .expect("made bal");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "bal", err)
        .expect("checked if bal is compatible");
    assert_eq!(is_compatible, false);

    // nullable composite pks
    c.exec_safe("CREATE TABLE baf (a NOT NULL, b, PRIMARY KEY(a, b));")
        .expect("made baf");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "baf", err)
        .expect("checked if baf is compatible");
    assert_eq!(is_compatible, false);

    // pks + other non unique indices
    c.exec_safe("CREATE TABLE baz (a PRIMARY KEY NOT NULL, b);")
        .expect("made baz");
    c.exec_safe("CREATE INDEX bar_i ON baz (b);")
        .expect("made index");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "baz", err)
        .expect("checked if baz is compatible");
    assert_eq!(is_compatible, true);

    // pks + other unique indices
    c.exec_safe("CREATE TABLE booz (a PRIMARY KEY NOT NULL, b);")
        .expect("made booz");
    c.exec_safe("CREATE UNIQUE INDEX booz_b ON booz (b);")
        .expect("made index");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "booz", err)
        .expect("checked if booz is compatible");
    assert_eq!(is_compatible, false);

    // not null and no dflt
    c.exec_safe("CREATE TABLE buzz (a PRIMARY KEY NOT NULL, b NOT NULL);")
        .expect("made buzz");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "buzz", err)
        .expect("checked if buzz is compatible");
    assert_eq!(is_compatible, false);

    // not null and dflt
    c.exec_safe("CREATE TABLE boom (a PRIMARY KEY NOT NULL, b NOT NULL DEFAULT 1);")
        .expect("made boom");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "boom", err)
        .expect("checked if boom is compatible");
    assert_eq!(is_compatible, true);

    // fk constraint
    c.exec_safe("CREATE TABLE zoom (a PRIMARY KEY NOT NULL, b, FOREIGN KEY(b) REFERENCES foo(a));")
        .expect("made zoom");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "zoom", err)
        .expect("checked if zoom is compatible");
    assert_eq!(is_compatible, false);

    // strict mode should be ok
    c.exec_safe("CREATE TABLE atable (\"id\" TEXT PRIMARY KEY) STRICT;")
        .expect("made atable");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "atable", err)
        .expect("checked if atable is compatible");
    assert_eq!(is_compatible, true);

    // no autoincrement
    c.exec_safe("CREATE TABLE woom (a integer primary key autoincrement not null);")
        .expect("made woom");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "woom", err)
        .expect("checked if woom is compatible");
    assert_eq!(is_compatible, false);

    // aliased rowid
    c.exec_safe("CREATE TABLE loom (a integer primary key not null);")
        .expect("made loom");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "loom", err)
        .expect("checked if loom is compatible");
    assert_eq!(is_compatible, true);

    c.exec_safe("CREATE TABLE atable2 (\"id\" TEXT PRIMARY KEY NOT NULL, x TEXT) STRICT;")
        .expect("made atable2");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "atable2", err)
        .expect("checked if atable2 is compatible");
    assert_eq!(is_compatible, true);

    c.exec_safe(
        "CREATE TABLE ydoc (\
        doc_id TEXT NOT NULL,\
        yhash BLOB NOT NULL,\
        yval BLOB,\
        primary key (doc_id, yhash)\
      ) STRICT;",
    )
    .expect("made ydoc");
    let is_compatible = test_exports::tableinfo::is_table_compatible(raw_db, "atable2", err)
        .expect("checked if atable2 is compatible");
    assert_eq!(is_compatible, true);
    drop_err_ptr(err);
}

fn test_create_clock_table_from_table_info() {
    let db = crate::opendb().expect("Opened DB");
    let c = &db.db;
    let raw_db = db.db.db;
    let err = make_err_ptr();

    c.exec_safe("CREATE TABLE foo (a not null, b not null, primary key (a, b));")
        .expect("made foo");
    c.exec_safe("CREATE TABLE bar (a primary key not null);")
        .expect("made bar");
    c.exec_safe("CREATE TABLE baz (a primary key not null, b);")
        .expect("made baz");
    c.exec_safe("CREATE TABLE boo (a primary key not null, b, c);")
        .expect("made boo");

    let foo_tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "foo", err)
        .expect("pulled table info for foo");
    let bar_tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "bar", err)
        .expect("pulled table info for bar");
    let baz_tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "baz", err)
        .expect("pulled table info for baz");
    let boo_tbl_info = test_exports::tableinfo::pull_table_info(raw_db, "boo", err)
        .expect("pulled table info for boo");

    test_exports::bootstrap::create_clock_table(raw_db, &foo_tbl_info, err)
        .expect("created clock table for foo");
    test_exports::bootstrap::create_clock_table(raw_db, &bar_tbl_info, err)
        .expect("created clock table for bar");
    test_exports::bootstrap::create_clock_table(raw_db, &baz_tbl_info, err)
        .expect("created clock table for baz");
    test_exports::bootstrap::create_clock_table(raw_db, &boo_tbl_info, err)
        .expect("created clock table for boo");

    drop_err_ptr(err);
    // todo: Check that clock tables have expected schema(s)
}

fn test_leak_condition() {
    // updating schemas prepares stements
    // re-pulling table infos should finalize those statements
    let c1w = crate::opendb_file("test_leak_condition").expect("Opened DB");
    let c2w = crate::opendb_file("test_leak_condition").expect("Opened DB");

    let c1 = &c1w.db;
    let c2 = &c2w.db;

    c1.exec_safe(
        "DROP TABLE IF EXISTS foo;
        DROP TABLE IF EXISTS bar;
        VACUUM;",
    )
    .expect("reset db");

    c1.exec_safe("CREATE TABLE foo (a not null, b not null, primary key (a, b));")
        .expect("made foo");
    c1.exec_safe("SELECT crsql_as_crr('foo')")
        .expect("made foo a crr");
    c1.exec_safe("INSERT INTO foo VALUES (1, 2)")
        .expect("inserted into foo");
    c1.exec_safe("UPDATE FOO set b = 3").expect("updated foo");
    c2.exec_safe("INSERT INTO foo VALUES (2, 3)")
        .expect("inserted into foo");
    c2.exec_safe("CREATE TABLE bar (a)").expect("created bar");
    c1.exec_safe("INSERT INTO foo VALUES (3, 4)")
        .expect("inserted into foo");
    c2.exec_safe("INSERT INTO foo VALUES (4, 5)")
        .expect("inserted into foo");
}

pub fn run_suite() {
    test_ensure_table_infos_are_up_to_date();
    test_pull_table_info();
    test_is_table_compatible();
    test_create_clock_table_from_table_info();
    test_leak_condition();
}
