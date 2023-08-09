use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use libsql::{Database, Params};
use pprof::criterion::{Output, PProfProfiler};

fn bench_db() -> Database {
    Database::open(":memory:").unwrap()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("libsql");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = db.connect().unwrap();

    group.bench_function("select 1", |b| {
        b.iter(|| {
            let rows = conn.query("SELECT 1", ()).unwrap().unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
        });
    });

    let stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("select 1 (prepared)", |b| {
        b.iter(|| {
            let rows = stmt.query(&Params::None).unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
            stmt.reset();
        });
    });

    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .unwrap();
    for _ in 0..1000 {
        conn.execute("INSERT INTO users (name) VALUES ('FOO')", ())
            .unwrap();
    }

    let stmt = conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
    group.bench_function("SELECT * FROM users LIMIT 1", |b| {
        b.iter(|| {
            let rows = stmt.query(&Params::None).unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
            stmt.reset();
        });
    });

    let stmt = conn.prepare("SELECT * FROM users LIMIT 100").unwrap();
    group.bench_function("SELECT * FROM users LIMIT 100", |b| {
        b.iter(|| {
            let rows = stmt.query(&Params::None).unwrap();
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
