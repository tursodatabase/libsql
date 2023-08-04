use rusqlite::Connection;

#[test]
fn test_update_column_check() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t(id)", ()).unwrap();
    conn.execute("INSERT INTO t VALUES (10)", ()).unwrap();
    conn.execute("ALTER TABLE t UPDATE COLUMN id TO id CHECK(id < 5)", ())
        .unwrap();
    assert!(conn.execute("INSERT INTO t VALUES (10)", ()).is_err());
    assert!(conn.execute("INSERT INTO t VALUES (4)", ()).is_ok());
    assert!(conn
        .execute("UPDATE t SET id = 10 WHERE id = 10", ())
        .is_err());
    assert!(conn
        .execute("UPDATE t SET id = 4 WHERE id = 10", ())
        .is_ok());
    conn.execute("ALTER TABLE t UPDATE COLUMN id TO id", ())
        .unwrap();
    assert!(conn.execute("INSERT INTO t VALUES (10)", ()).is_ok());
}

#[test]
fn test_update_default_constraint() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t(id)", ()).unwrap();
    assert!(conn.execute("INSERT INTO t VALUES (10)", ()).is_ok());

    let row: Result<i64, _> = conn.query_row("SELECT id FROM t", [], |row| row.get(0));
    assert_eq!(row.unwrap(), 10);

    assert!(conn.execute("INSERT INTO t DEFAULT VALUES", ()).is_ok());
    assert!(conn
        .query_row("SELECT id FROM t WHERE id = 42", (), |_| Ok(()))
        .is_err());

    conn.execute("ALTER TABLE t UPDATE COLUMN id TO id DEFAULT 42", ())
        .unwrap();
    assert!(conn.execute("INSERT INTO t DEFAULT VALUES", ()).is_ok());
    let row: Result<i64, _> =
        conn.query_row("SELECT id FROM t WHERE id = 42", [], |row| row.get(0));
    assert_eq!(row.unwrap(), 42);
}

#[test]
fn test_update_not_null_constraint() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t(id)", ()).unwrap();
    assert!(conn.execute("INSERT INTO t VALUES (NULL)", ()).is_ok());
    conn.execute("ALTER TABLE t UPDATE COLUMN id TO id INT NOT NULL", ())
        .unwrap();
    assert!(conn.execute("INSERT INTO t VALUES (NULL)", ()).is_err());
    assert!(conn.execute("INSERT INTO t DEFAULT VALUES", ()).is_err());
}

#[test]
fn test_update_references_foreign_key() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t1(id int primary key)", ())
        .unwrap();
    conn.execute("CREATE TABLE t2(id int primary key, v text, t1_id)", ())
        .unwrap();
    conn.execute("INSERT INTO t1 VALUES (1)", ()).unwrap();
    conn.execute(
        "ALTER TABLE t2 UPDATE COLUMN t1_id TO t1_id REFERENCES t1(id)",
        (),
    )
    .unwrap();
    // Inserting a row with a non-existent foreign key is ok, because those are not validated by default
    assert!(conn
        .execute("INSERT INTO t2 VALUES (1, 'a', 42)", ())
        .is_ok());
    // Now they should be validated
    conn.execute("PRAGMA foreign_keys = ON", ()).unwrap();
    assert!(conn
        .execute("INSERT INTO t2 VALUES (2, 'b', 42)", ())
        .is_err());
    assert!(conn
        .execute("INSERT INTO t2 VALUES (2, 'b', 1)", ())
        .is_ok());
    assert!(conn
        .execute("UPDATE t1 SET t1_id = 42 WHERE t1_id = 1", ())
        .is_err());
    conn.execute("ALTER TABLE t2 UPDATE COLUMN t1_id TO t1_id", ())
        .unwrap();
    // It's again ok to insert a row with a non-existent foreign key
    assert!(conn
        .execute("INSERT INTO t2 VALUES (3, 'c', 42)", ())
        .is_ok());
}
