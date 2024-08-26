use std::path::Path;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::{self, OpenFlags};
use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager, Wal};
use libsql_sys::Connection;
use libsql_wal::io::StdIO;
use libsql_wal::storage::NoStorage;
use libsql_wal::wal::LibsqlWal;
use libsql_wal::{registry::WalRegistry, wal::LibsqlWalManager};
use tempfile::tempdir;

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

pub fn criterion_benchmark(c: &mut Criterion) {
    with_libsql_conn(|conn| {
        c.bench_function("libsql random inserts", |b| {
            bench_random_inserts(conn, b);
        });
    });

    with_sqlite_conn(|conn| {
        c.bench_function("sqlite3 random inserts", |b| {
            bench_random_inserts(conn, b);
        });
    });

    with_sqlite_conn(|conn| {
        prepare_for_random_reads(conn);
        c.bench_function("sqlite3 random reads", |b| {
            bench_random_reads(conn, b);
        });
    });

    with_libsql_conn(|conn| {
        prepare_for_random_reads(conn);
        c.bench_function("libsql random reads", |b| {
            bench_random_reads(conn, b);
        });
    });
}

fn prepare_for_random_reads<W: Wal>(conn: &mut Connection<W>) {
    let _ = conn.execute(
        "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB(16), c BLOB(16), d BLOB(400));",
        (),
    );
    let _ = conn.execute("CREATE INDEX i1 ON t1(b);", ());
    let _ = conn.execute("CREATE INDEX i2 ON t1(c);", ());
    for _ in 0..20_000 {
        random_inserts(conn);
    }
}

fn with_libsql_conn(f: impl FnOnce(&mut Connection<LibsqlWal<StdIO>>)) {
    let tmp = tempdir().unwrap();
    let resolver = |_: &Path| NamespaceName::from_string("test".into());

    let (sender, _) = tokio::sync::mpsc::channel(12);
    let registry =
        Arc::new(WalRegistry::new(tmp.path().join("wals"), NoStorage.into(), sender).unwrap());
    let wal_manager = LibsqlWalManager::new(registry.clone(), Arc::new(resolver));

    let mut conn = libsql_sys::Connection::open(
        tmp.path().join("data"),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

    f(&mut conn)
}

fn with_sqlite_conn(f: impl FnOnce(&mut Connection<Sqlite3Wal>)) {
    let tmp = tempdir().unwrap();
    let mut conn = libsql_sys::Connection::open(
        tmp.path().join("data"),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        Sqlite3WalManager::default(),
        100000,
        None,
    )
    .unwrap();

    f(&mut conn)
}

fn bench_random_reads<W: Wal>(conn: &mut Connection<W>, bencher: &mut Bencher<'_>) {
    bencher.iter(|| random_read(conn));
}

fn bench_random_inserts<W: Wal>(conn: &mut Connection<W>, bencher: &mut Bencher<'_>) {
    let _ = conn.execute(
        "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB(16), c BLOB(16), d BLOB(400));",
        (),
    );
    let _ = conn.execute("CREATE INDEX i1 ON t1(b);", ());
    let _ = conn.execute("CREATE INDEX i2 ON t1(c);", ());
    bencher.iter(|| random_inserts(conn));
}

fn random_inserts<W: Wal>(conn: &mut Connection<W>) {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    // println!("write_acquired: {:?}", before.elapsed().as_micros());
    tx.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()).unwrap();
    tx.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()).unwrap();
    tx.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()).unwrap();
    tx.commit().unwrap();
}

fn random_read<W: Wal>(conn: &mut Connection<W>) {
    let tx = conn.transaction().unwrap();
    // println!("write_acquired: {:?}", before.elapsed().as_micros());
    let mut stmt = tx
        .prepare("SELECT * FROM t1 WHERE a>abs((random()%5000000)) LIMIT 10;")
        .unwrap();
    stmt.query(()).unwrap().mapped(|_r| Ok(())).count();
    stmt.query(()).unwrap().mapped(|_r| Ok(())).count();
    stmt.query(()).unwrap().mapped(|_r| Ok(())).count();
    drop(stmt);
    tx.commit().unwrap();
}
