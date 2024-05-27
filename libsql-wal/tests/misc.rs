use std::{path::Path, sync::Arc};

use libsql_sys::rusqlite::OpenFlags;
use libsql_wal::{name::NamespaceName, registry::WalRegistry, wal::LibsqlWalManager};
use tempfile::tempdir;

#[test]
fn transaction_rollback() {
    let tmp = tempdir().unwrap();
    let resolver = |path: &Path| {
        let name = path.file_name().unwrap().to_str().unwrap();
        NamespaceName::from_string(name.to_string())
    };
    let registry = Arc::new(WalRegistry::new(tmp.path().join("test/wals"), resolver).unwrap());
    let wal_manager = LibsqlWalManager {
        registry: registry.clone(),
        next_conn_id: Default::default(),
    };

    let mut conn1 = libsql_sys::Connection::open(
        tmp.path().join("test/data").clone(),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

    let conn2 = libsql_sys::Connection::open(
        tmp.path().join("test/data").clone(),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

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
    let tmp = tempdir().unwrap();
    let resolver = |path: &Path| {
        let name = path.file_name().unwrap().to_str().unwrap();
        NamespaceName::from_string(name.to_string())
    };
    let registry = Arc::new(WalRegistry::new(tmp.path().join("test/wals"), resolver).unwrap());
    let wal_manager = LibsqlWalManager {
        registry: registry.clone(),
        next_conn_id: Default::default(),
    };

    let mut conn = libsql_sys::Connection::open(
        tmp.path().join("test/data").clone(),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

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
