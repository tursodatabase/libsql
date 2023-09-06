use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use libsql::{Database, Params};
use pprof::criterion::{Output, PProfProfiler};
use tokio::runtime;

fn bench_db() -> Database {
    Database::open(":memory:").unwrap()
}

fn bench(c: &mut Criterion) {
    let rt = runtime::Builder::new_current_thread().build().unwrap();

    let mut group = c.benchmark_group("libsql");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = rt.block_on(db.connect()).unwrap();

    group.bench_function("select 1", |b| {
        b.to_async(&rt).iter(|| async {
            let mut rows = conn.query("SELECT 1", ()).await.unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
        });
    });

    let stmt = rt.block_on(conn.prepare("SELECT 1")).unwrap();
    group.bench_function("select 1 (prepared)", |b| {
        b.to_async(&rt).iter(|| async {
            let mut rows = stmt.query(&Params::None).await.unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
            stmt.reset();
        });
    });

    rt.block_on(conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ()))
        .unwrap();
    for _ in 0..1000 {
        rt.block_on(conn.execute("INSERT INTO users (name) VALUES ('FOO')", ()))
            .unwrap();
    }

    let stmt = rt
        .block_on(conn.prepare("SELECT * FROM users LIMIT 1"))
        .unwrap();
    group.bench_function("SELECT * FROM users LIMIT 1", |b| {
        b.to_async(&rt).iter(|| async {
            let mut rows = stmt.query(&Params::None).await.unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
            stmt.reset();
        });
    });

    let stmt = rt
        .block_on(conn.prepare("SELECT * FROM users LIMIT 100"))
        .unwrap();
    group.bench_function("SELECT * FROM users LIMIT 100", |b| {
        b.to_async(&rt).iter(|| async {
            let mut rows = stmt.query(&Params::None).await.unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
            stmt.reset();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
