use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use libsql_core::Database;
use pprof::criterion::{Output, PProfProfiler};

fn bench_db() -> Database {
    Database::open(":memory:")
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("libsql");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = db.connect().unwrap();
    group.bench_function("select 1", |b| {
        b.iter(|| {
            let rows = conn.execute("SELECT 1").unwrap().unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
        });
    });

    let stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("select 1 (prepared)", |b| {
        b.iter(|| {
            let rows = stmt.execute().unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i32>(0).unwrap(), 1);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
