use libsql_wal::test::TestEnv;

#[test]
fn transaction_rollback() {
    let env = TestEnv::new();
    let mut conn1 = env.open_conn("test");
    let conn2 = env.open_conn("test");

    let tx = conn1.transaction().unwrap();
    tx.execute("create table test (x)", ()).unwrap();

    assert!(conn2
        .query_row("select count(0) from test", (), |_| { Ok(()) })
        .is_err());

    tx.execute("insert into test values (42)", ()).unwrap();
    tx.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 1);
        Ok(())
    })
    .unwrap();

    assert!(conn2
        .query_row("select count(0) from test", (), |_| { Ok(()) })
        .is_err());

    tx.rollback().unwrap();

    assert!(conn1
        .query_row("select count(0) from test", (), |_| { Ok(()) })
        .is_err());

    assert!(conn2
        .query_row("select count(0) from test", (), |_| { Ok(()) })
        .is_err());

    conn1.execute("create table test (c)", ()).unwrap();

    conn1
        .query_row("select count(0) from test", (), |r| {
            assert_eq!(r.get::<_, usize>(0).unwrap(), 0);
            Ok(())
        })
        .unwrap();
    conn2
        .query_row("select count(0) from test", (), |r| {
            assert_eq!(r.get::<_, usize>(0).unwrap(), 0);
            Ok(())
        })
        .unwrap();
}

#[test]
fn transaction_savepoints() {
    let env = TestEnv::new();
    let mut conn = env.open_conn("test");

    let mut tx = conn.transaction().unwrap();
    tx.execute("create table test (x)", ()).unwrap();

    let mut s1 = tx.savepoint().unwrap();
    s1.execute("insert into test values (42)", ()).unwrap();
    s1.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 1);
        Ok(())
    })
    .unwrap();

    let mut s2 = s1.savepoint().unwrap();
    s2.execute("insert into test values (42)", ()).unwrap();
    s2.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 2);
        Ok(())
    })
    .unwrap();

    s2.rollback().unwrap();
    drop(s2);

    s1.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 1);
        Ok(())
    })
    .unwrap();

    s1.rollback().unwrap();
    drop(s1);

    tx.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 0);
        Ok(())
    })
    .unwrap();

    tx.commit().unwrap();

    conn.query_row("select count(0) from test", (), |r| {
        assert_eq!(r.get::<_, usize>(0).unwrap(), 0);
        Ok(())
    })
    .unwrap();
}
