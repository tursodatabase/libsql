use bencher::{benchmark_group, benchmark_main, Bencher};
use rusqlite::Connection;

fn bench_execute(b: &mut Bencher) {
    let db = Connection::open_in_memory().unwrap();
    let sql = "PRAGMA user_version=1";
    b.iter(|| db.execute(sql, []).unwrap());
}

fn bench_execute_batch(b: &mut Bencher) {
    let db = Connection::open_in_memory().unwrap();
    let sql = "PRAGMA user_version=1";
    b.iter(|| db.execute_batch(sql).unwrap());
}

benchmark_group!(exec_benches, bench_execute, bench_execute_batch);
benchmark_main!(exec_benches);
