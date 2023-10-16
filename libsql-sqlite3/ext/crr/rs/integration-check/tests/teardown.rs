use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

integration_utils::counter_setup!(1);

#[test]
fn tear_down() {
    tear_down_impl().unwrap();
    decrement_counter();
}

fn tear_down_impl() -> Result<(), ResultCode> {
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
