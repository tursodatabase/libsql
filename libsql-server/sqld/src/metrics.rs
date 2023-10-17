#![allow(dead_code)]
use metrics::{
    describe_counter, describe_gauge, describe_histogram, register_counter, register_gauge,
    register_histogram, Counter, Gauge, Histogram,
};
use once_cell::sync::Lazy;

pub static WRITE_QUERY_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "writes_count";
    describe_counter!(NAME, "number of write statements");
    register_counter!(NAME)
});
pub static READ_QUERY_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "read_count";
    describe_counter!(NAME, "number of read statements");
    register_counter!(NAME)
});
pub static REQUESTS_PROXIED: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "requests_proxied";
    describe_counter!(NAME, "number of proxied requests");
    register_counter!(NAME)
});
pub static CONCCURENT_CONNECTIONS_COUNT: Lazy<Gauge> = Lazy::new(|| {
    const NAME: &str = "conccurent_connections";
    describe_gauge!(NAME, "number of conccurent connections");
    register_gauge!(NAME)
});
pub static NAMESPACE_LOAD_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "namespace_load_latency";
    describe_histogram!(NAME, "latency is us when loading a namespace");
    register_histogram!(NAME)
});
pub static CONNECTION_CREATE_TIME: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "connection_create_time";
    describe_histogram!(NAME, "time to create a connection");
    register_histogram!(NAME)
});
pub static CONNECTION_ALIVE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "connection_alive_duration";
    describe_histogram!(NAME, "duration for which a connection was kept alive");
    register_histogram!(NAME)
});
pub static WRITE_TXN_DURATION: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "write_txn_duration";
    describe_histogram!(NAME, "duration for which a write transaction was kept open");
    register_histogram!(NAME)
});
