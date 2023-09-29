use core::mem::forget;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

integration_utils::counter_setup!(2);

#[test]
fn crr_to_table() {
    crr_to_table_impl().unwrap();
    decrement_counter();
}

fn crr_to_table_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    db.db.exec_safe("CREATE TABLE foo (a primary key, b);")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    db.db.exec_safe("SELECT crsql_as_table('foo');")?;
    let stmt = db
        .db
        .prepare_v2("SELECT count(*) FROM sqlite_master WHERE name LIKE 'foo__%'")?;
    stmt.step()?;
    let count = stmt.column_int(0)?;
    assert!(count == 0);
    Ok(())
}

#[test]
fn statements_finalized_on_connection_close() {
    let db_wrapped = integration_utils::opendb().expect("opened db");
    let db = &db_wrapped.db;
    db.exec_safe("CREATE TABLE foo (a primary key not null, b);")
        .expect("created foo");
    db.exec_safe("SELECT crsql_as_crr('foo');")
        .expect("created crr");

    let db_ptr = db.db;
    // forget the db. We'll close it ourself
    forget(db);
    // forget so we don't run the `crsql_finalize` routine -- the close hook should now do that for us.
    forget(db_wrapped);
    let rc = sqlite::close(db_ptr);
    assert!(rc == 0);
    decrement_counter();
}
