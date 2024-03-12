#![allow(dead_code)]
use metrics::{
    describe_counter, describe_gauge, describe_histogram, register_counter, register_gauge,
    register_histogram, Counter, Gauge, Histogram,
};
use once_cell::sync::Lazy;

pub static CLIENT_VERSION: &str = "libsql_client_version";

pub static WRITE_QUERY_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_writes_count";
    describe_counter!(NAME, "number of write statements");
    register_counter!(NAME)
});
pub static READ_QUERY_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_reads_count";
    describe_counter!(NAME, "number of read statements");
    register_counter!(NAME)
});
pub static REQUESTS_PROXIED: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_requests_proxied";
    describe_counter!(NAME, "number of proxied requests");
    register_counter!(NAME)
});
pub static CONCURRENT_CONNECTIONS_COUNT: Lazy<Gauge> = Lazy::new(|| {
    const NAME: &str = "libsql_server_concurrent_connections";
    describe_gauge!(NAME, "number of concurrent connections");
    register_gauge!(NAME)
});
pub static NAMESPACE_LOAD_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_namespace_load_latency";
    describe_histogram!(NAME, "latency is us when loading a namespace");
    register_histogram!(NAME)
});
pub static CONNECTION_CREATE_TIME: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_connection_create_time";
    describe_histogram!(NAME, "time to create a connection");
    register_histogram!(NAME)
});
pub static CONNECTION_ALIVE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_connection_alive_duration";
    describe_histogram!(NAME, "duration for which a connection was kept alive");
    register_histogram!(NAME)
});
pub static WRITE_TXN_DURATION: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_write_txn_duration";
    describe_histogram!(NAME, "duration for which a write transaction was kept open");
    register_histogram!(NAME)
});

pub static STATEMENT_EXECUTION_TIME: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_statement_execution_time";
    describe_histogram!(NAME, "time to execute a statement");
    register_histogram!(NAME)
});
pub static VACUUM_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_vacuum_count";
    describe_counter!(NAME, "number of vacuum operations");
    register_counter!(NAME)
});
pub static WAL_CHECKPOINT_TIME: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_wal_checkpoint_time";
    describe_histogram!(NAME, "time to checkpoint the WAL");
    register_histogram!(NAME)
});
pub static WAL_CHECKPOINT_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_wal_checkpoint_count";
    describe_counter!(NAME, "number of WAL checkpoints");
    register_counter!(NAME)
});
pub static STATEMENT_MEM_USED_BYTES: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_statement_mem_used_bytes";
    describe_histogram!(NAME, "memory used by a prepared statement");
    register_histogram!(NAME)
});
pub static RETURNED_BYTES: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_returned_bytes";
    describe_histogram!(NAME, "number of bytes of values returned to the client");
    register_histogram!(NAME)
});
pub static PROGRAM_EXEC_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_libsql_execute_program";
    describe_counter!(NAME, "number of hrana program executions");
    register_counter!(NAME)
});
pub static REPLICA_LOCAL_EXEC_MISPREDICT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_replica_exec_mispredict";
    describe_counter!(
        NAME,
        "number of mispredicted hrana program executions on a replica"
    );
    register_counter!(NAME)
});
pub static REPLICA_LOCAL_PROGRAM_EXEC: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_replica_exec";
    describe_counter!(
        NAME,
        "number of local hrana programs executions on a replica"
    );
    register_counter!(NAME)
});
pub static DESCRIBE_COUNT: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_describe_count";
    describe_counter!(NAME, "number of calls to describe");
    register_counter!(NAME)
});
pub static LEGACY_HTTP_CALL: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_legacy_http_call";
    describe_counter!(NAME, "number of calls to the legacy HTTP API");
    register_counter!(NAME)
});
pub static DIRTY_STARTUP: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_dirty_startup";
    describe_counter!(
        NAME,
        "how many times an instance started with a dirty state"
    );
    register_counter!(NAME)
});
pub static REPLICATION_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    const NAME: &str = "libsql_server_replication_latency";
    describe_counter!(
        NAME,
        "Latency between the time a transaction was commited on the primary and the commit frame was received by the replica"
    );
    register_histogram!(NAME)
});
pub static REPLICATION_LATENCY_OUT_OF_SYNC: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_replication_latency_out_of_sync";
    describe_counter!(
        NAME,
        "Number of replication latency timestamps that were out-of-sync (clocks likely not synchronized)"
    );
    register_counter!(NAME)
});
pub static REPLICATION_LATENCY_CACHE_MISS: Lazy<Counter> = Lazy::new(|| {
    const NAME: &str = "libsql_server_replication_latencies_cache_misses";
    describe_counter!(NAME, "Number of replication latency cache misses");
    register_counter!(NAME)
});
pub static SERVER_COUNT: Lazy<Gauge> = Lazy::new(|| {
    const NAME: &str = "libsql_server_count";
    describe_gauge!(NAME, "a gauge counting the number of active servers");
    register_gauge!(NAME)
});
