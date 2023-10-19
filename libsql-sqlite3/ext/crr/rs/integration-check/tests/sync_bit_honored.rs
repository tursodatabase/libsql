use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

#[test]
fn sync_bit_honored() {
    sync_bit_honored_impl().unwrap();
}

// If sync bit is on, nothing gets written to clock tables for that connection.
fn sync_bit_honored_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let conn = &db.db;
    conn.exec_safe("CREATE TABLE foo (a primary key, b);")?;
    conn.exec_safe("SELECT crsql_as_crr('foo');")?;
    conn.exec_safe("SELECT crsql_internal_sync_bit(1)")?;
    conn.exec_safe("INSERT INTO foo VALUES (1, 2);")?;
    conn.exec_safe("UPDATE foo SET b = 5 WHERE a = 1;")?;
    conn.exec_safe("INSERT INTO foo VALUES (2, 2);")?;
    conn.exec_safe("DELETE FROM foo WHERE a = 2;")?;

    let stmt = conn.prepare_v2("SELECT 1 FROM foo__crsql_clock")?;
    let result = stmt.step()?;
    assert!(result == ResultCode::DONE);

    Ok(())
}
