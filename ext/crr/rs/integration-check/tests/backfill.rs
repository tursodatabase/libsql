// Test that we can backfill old tables
// the bulk of these tests have been moved to the python code
// given integration tests are much more easily written in python
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

// TODO: auto-calculate starting number
integration_utils::counter_setup!(4);

#[test]
fn new_empty_table() {
    new_empty_table_impl().unwrap();
    decrement_counter();
}

#[test]
fn new_nonempty_table() {
    new_nonempty_table_impl(false).unwrap();
    decrement_counter();
}

#[test]
fn reapplied_empty_table() {
    reapplied_empty_table_impl().unwrap();
    decrement_counter();
}

#[test]
fn reapplied_nonempty_table_with_newdata() {
    new_nonempty_table_impl(true).unwrap();
    decrement_counter();
}

fn new_empty_table_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    // Just testing that we can execute these statements without error
    db.db
        .exec_safe("CREATE TABLE foo (id PRIMARY KEY, name);")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    db.db.exec_safe("SELECT * FROM foo__crsql_clock;")?;
    Ok(())
}

fn new_nonempty_table_impl(apply_twice: bool) -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    db.db
        .exec_safe("CREATE TABLE foo (id PRIMARY KEY, name);")?;
    db.db
        .exec_safe("INSERT INTO foo VALUES (1, 'one'), (2, 'two');")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    let stmt = db.db.prepare_v2("SELECT * FROM foo__crsql_clock;")?;
    if apply_twice {
        db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    }

    let mut cnt = 0;
    while stmt.step()? == ResultCode::ROW {
        cnt = cnt + 1;
        assert_eq!(stmt.column_int64(0)?, cnt); // pk
        assert_eq!(stmt.column_text(1)?, "name"); // col name
        assert_eq!(stmt.column_int64(2)?, 1); // col version
        assert_eq!(stmt.column_int64(3)?, 1); // db version
    }
    assert_eq!(cnt, 2);

    // select from crsql_changes too
    let stmt = db.db.prepare_v2(
        "SELECT [table], [pk], [cid], [val], [col_version], [db_version] FROM crsql_changes;",
    )?;
    let mut cnt = 0;
    while stmt.step().unwrap() == ResultCode::ROW {
        cnt = cnt + 1;
        if cnt == 1 {
            assert_eq!(stmt.column_blob(1)?, [1, 9, 1]); // pk
            assert_eq!(stmt.column_text(3)?, "one"); // col value
        } else {
            assert_eq!(stmt.column_blob(1)?, [1, 9, 2]); // pk
            assert_eq!(stmt.column_text(3)?, "two"); // col value
        }
        assert_eq!(stmt.column_text(0)?, "foo"); // table name
        assert_eq!(stmt.column_text(2)?, "name"); // col name
        assert_eq!(stmt.column_int64(4)?, 1); // col version
        assert_eq!(stmt.column_int64(5)?, 1); // db version
    }
    assert_eq!(cnt, 2);
    Ok(())
}

fn reapplied_empty_table_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    // Just testing that we can execute these statements without error
    db.db
        .exec_safe("CREATE TABLE foo (id PRIMARY KEY, name);")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    db.db.exec_safe("SELECT * FROM foo__crsql_clock;")?;
    db.db.exec_safe("SELECT crsql_as_crr('foo');")?;
    db.db.exec_safe("SELECT * FROM foo__crsql_clock;")?;
    Ok(())
}
