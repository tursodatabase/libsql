#![feature(test)]
extern crate test;

extern crate rusqlite;

use rusqlite::Connection;
use test::Bencher;

#[bench]
fn bench_no_cache(b: &mut Bencher) {
    let db = Connection::open_in_memory().unwrap();
    db.set_prepared_statement_cache_capacity(0);
    let sql = "SELECT 1, 'test', 3.14 UNION SELECT 2, 'exp', 2.71";
    b.iter(|| db.prepare(sql).unwrap());
}

#[bench]
fn bench_cache(b: &mut Bencher) {
    let db = Connection::open_in_memory().unwrap();
    let sql = "SELECT 1, 'test', 3.14 UNION SELECT 2, 'exp', 2.71";
    b.iter(|| db.prepare_cached(sql).unwrap());
}
