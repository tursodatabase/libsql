extern crate crsql_bundle;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

fn tear_down() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db
        .exec_safe("CREATE TABLE foo (a primary key not null, b);")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    db.db.exec_safe("SELECT crsql_as_table('foo');")?;
    let stmt = db
        .db
        .prepare_v2("SELECT count(*) FROM sqlite_master WHERE name LIKE 'foo__%'")?;
    stmt.step()?;
    let count = stmt.column_int(0);
    assert!(count == 0);
    Ok(())
}

pub fn run_suite() -> Result<(), ResultCode> {
    tear_down()
}
