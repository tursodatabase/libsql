use sqlite::{Connection, ManagedConnection, ResultCode};
use sqlite_nostd as sqlite;

/*
Test:
- create crr
- destroy crr
- use crr that was created
- create if not exist vtab
-
*/

#[test]
fn create_crr_via_vtab() {
    create_crr_via_vtab_impl().unwrap();
}

fn create_crr_via_vtab_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let conn = &db.db;

    conn.exec_safe("CREATE VIRTUAL TABLE foo_schema USING CLSet (a primary key, b);")?;
    conn.exec_safe("INSERT INTO foo VALUES (1, 2);")?;
    let stmt = conn.prepare_v2("SELECT count(*) FROM crsql_changes")?;
    stmt.step()?;
    let count = stmt.column_int(0)?;
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn destroy_crr_via_vtab() {
    destroy_crr_via_vtab_impl().unwrap();
}

fn destroy_crr_via_vtab_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let conn = &db.db;

    conn.exec_safe("CREATE VIRTUAL TABLE foo_schema USING CLSet (a primary key, b);")?;
    conn.exec_safe("DROP TABLE foo_schema")?;
    let stmt = conn.prepare_v2("SELECT count(*) FROM sqlite_master WHERE name LIKE '%foo%'")?;
    stmt.step()?;
    let count = stmt.column_int(0)?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn create_invalid_crr() {
    create_invalid_crr_impl().unwrap();
}

fn create_invalid_crr_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let conn = &db.db;

    let result = conn.exec_safe("CREATE VIRTUAL TABLE foo_schema USING CLSet (a, b);");
    assert_eq!(result, Err(ResultCode::ERROR));
    let msg = conn.errmsg().unwrap();
    assert_eq!(
        msg,
        "Table foo has no primary key. CRRs must have a primary key"
    );
    Ok(())
}

#[test]
fn create_if_not_exists() {
    create_if_not_exists_impl().unwrap();
}

fn create_if_not_exists_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let conn = &db.db;

    conn.exec_safe(
        "CREATE VIRTUAL TABLE IF NOT EXISTS foo_schema USING CLSet (a primary key, b);",
    )?;
    conn.exec_safe("INSERT INTO foo VALUES (1, 2);")?;
    let stmt = conn.prepare_v2("SELECT count(*) FROM crsql_changes")?;
    stmt.step()?;
    let count = stmt.column_int(0)?;
    assert_eq!(count, 1);
    drop(stmt);
    // second create is a no-op
    conn.exec_safe(
        "CREATE VIRTUAL TABLE IF NOT EXISTS foo_schema USING CLSet (a primary key, b);",
    )?;
    let stmt = conn.prepare_v2("SELECT count(*) FROM crsql_changes")?;
    stmt.step()?;
    let count = stmt.column_int(0)?;
    assert_eq!(count, 1);
    Ok(())
}

// and later migration tests
// UPDATE foo SET schema = '...';
// INSERT INTO foo (alter) VALUES ('...');
// and auto-migrate tests for whole schema.
// auto-migrate would...
// - re-write `create vtab` things as `update foo set schema = ...` where those vtabs did not exist.
