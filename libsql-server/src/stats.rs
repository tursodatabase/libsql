use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};

use metrics::{counter, gauge, increment_counter};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::time::Duration;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopQuery {
    #[serde(skip)]
    pub weight: u64,
    pub rows_written: u64,
    pub rows_read: u64,
    pub query: String,
}

impl TopQuery {
    pub fn new(query: String, rows_read: u64, rows_written: u64) -> Self {
        Self {
            weight: rows_read + rows_written,
            rows_read,
            rows_written,
            query,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlowestQuery {
    pub elapsed_ms: u64,
    pub query: String,
    pub rows_written: u64,
    pub rows_read: u64,
}

impl SlowestQuery {
    pub fn new(query: String, elapsed_ms: u64, rows_read: u64, rows_written: u64) -> Self {
        Self {
            elapsed_ms,
            query,
            rows_read,
            rows_written,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    #[serde(skip)]
    namespace: NamespaceName,

    #[serde(default)]
    rows_written: AtomicU64,
    #[serde(default)]
    rows_read: AtomicU64,
    #[serde(default)]
    storage_bytes_used: AtomicU64,
    // number of write requests delegated from a replica to primary
    #[serde(default)]
    write_requests_delegated: AtomicU64,
    #[serde(default)]
    current_frame_no: AtomicU64,
    // Lowest value in currently stored top queries
    #[serde(default)]
    top_query_threshold: AtomicU64,
    #[serde(default)]
    top_queries: Arc<RwLock<BTreeSet<TopQuery>>>,
    // Lowest value in currently stored slowest queries
    #[serde(default)]
    slowest_query_threshold: AtomicU64,
    #[serde(default)]
    slowest_queries: Arc<RwLock<BTreeSet<SlowestQuery>>>,
}

impl Stats {
    pub async fn new(
        namespace: NamespaceName,
        db_path: &Path,
        join_set: &mut JoinSet<anyhow::Result<()>>,
    ) -> anyhow::Result<Arc<Self>> {
        let stats_path = db_path.join("stats.json");
        let mut this = if stats_path.try_exists()? {
            let data = tokio::fs::read_to_string(&stats_path).await?;
            serde_json::from_str(&data)?
        } else {
            Stats::default()
        };

        this.namespace = namespace;
        let this = Arc::new(this);

        join_set.spawn(spawn_stats_persist_thread(
            Arc::downgrade(&this),
            stats_path.to_path_buf(),
        ));

        Ok(this)
    }

    /// increments the number of written rows by n
    pub fn inc_rows_written(&self, n: u64) {
        counter!("libsql_server_rows_written", n, "namespace" => self.namespace.to_string());
        self.rows_written.fetch_add(n, Ordering::Relaxed);
    }

    /// increments the number of read rows by n
    pub fn inc_rows_read(&self, n: u64) {
        counter!("libsql_server_rows_read", n, "namespace" => self.namespace.to_string());
        self.rows_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_storage_bytes_used(&self, n: u64) {
        gauge!("libsql_server_storage", n as f64, "namespace" => self.namespace.to_string());
        self.storage_bytes_used.store(n, Ordering::Relaxed);
    }

    /// returns the total number of rows read since this database was created
    pub fn rows_read(&self) -> u64 {
        self.rows_read.load(Ordering::Relaxed)
    }

    /// returns the total number of rows written since this database was created
    pub fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }

    /// returns the total number of bytes used by the database (excluding uncheckpointed WAL entries)
    pub fn storage_bytes_used(&self) -> u64 {
        self.storage_bytes_used.load(Ordering::Relaxed)
    }

    /// increments the number of the write requests which were delegated from a replica to primary
    pub fn inc_write_requests_delegated(&self) {
        increment_counter!("libsql_server_write_requests_delegated", "namespace" => self.namespace.to_string());
        self.write_requests_delegated
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_requests_delegated(&self) -> u64 {
        self.write_requests_delegated.load(Ordering::Relaxed)
    }

    pub fn set_current_frame_no(&self, fno: FrameNo) {
        gauge!("libsql_server_current_frame_no", fno as f64, "namespace" => self.namespace.to_string());
        self.current_frame_no.store(fno, Ordering::Relaxed);
    }

    pub(crate) fn get_current_frame_no(&self) -> FrameNo {
        self.current_frame_no.load(Ordering::Relaxed)
    }

    pub(crate) fn add_top_query(&self, query: TopQuery) {
        let mut top_queries = self.top_queries.write().unwrap();
        tracing::debug!(
            "top query: {},{}:{}",
            query.rows_read,
            query.rows_written,
            query.query
        );
        top_queries.insert(query);
        if top_queries.len() > 10 {
            top_queries.pop_first();
            self.top_query_threshold
                .store(top_queries.first().unwrap().weight, Ordering::Relaxed);
        }
    }

    pub(crate) fn qualifies_as_top_query(&self, weight: u64) -> bool {
        weight >= self.top_query_threshold.load(Ordering::Relaxed)
    }

    pub(crate) fn top_queries(&self) -> &Arc<RwLock<BTreeSet<TopQuery>>> {
        &self.top_queries
    }

    pub(crate) fn reset_top_queries(&self) {
        self.top_queries.write().unwrap().clear();
        self.top_query_threshold.store(0, Ordering::Relaxed);
    }

    pub(crate) fn add_slowest_query(&self, query: SlowestQuery) {
        let mut slowest_queries = self.slowest_queries.write().unwrap();
        tracing::debug!("slowest query: {}: {}", query.elapsed_ms, query.query);
        slowest_queries.insert(query);
        if slowest_queries.len() > 10 {
            slowest_queries.pop_first();
            self.slowest_query_threshold.store(
                slowest_queries.first().unwrap().elapsed_ms,
                Ordering::Relaxed,
            );
        }
    }

    pub(crate) fn qualifies_as_slowest_query(&self, elapsed_ms: u64) -> bool {
        elapsed_ms >= self.slowest_query_threshold.load(Ordering::Relaxed)
    }

    pub(crate) fn slowest_queries(&self) -> &Arc<RwLock<BTreeSet<SlowestQuery>>> {
        &self.slowest_queries
    }

    pub(crate) fn reset_slowest_queries(&self) {
        self.slowest_queries.write().unwrap().clear();
        self.slowest_query_threshold.store(0, Ordering::Relaxed);
    }

    pub(crate) fn update_query_metrics(
        &self,
        sql: String,
        rows_read: u64,
        rows_written: u64,
        mem_used: u64,
        elapsed: u64,
    ) {
        increment_counter!("libsql_server_query_count", "namespace" => self.namespace.to_string(), "query" => sql.clone());
        counter!("libsql_server_query_latency", elapsed, "namespace" => self.namespace.to_string(), "query" => sql.clone());
        counter!("libsql_server_query_rows_read", rows_read, "namespace" => self.namespace.to_string(), "query" => sql.clone());
        counter!("libsql_server_query_rows_written", rows_written, "namespace" => self.namespace.to_string(), "query" => sql.clone());
        counter!("libsql_server_query_mem_used", mem_used, "namespace" => self.namespace.to_string(), "query" => sql.clone());
    }
}

async fn spawn_stats_persist_thread(stats: Weak<Stats>, path: PathBuf) -> anyhow::Result<()> {
    loop {
        if let Err(e) = try_persist_stats(stats.clone(), &path).await {
            tracing::error!("error persisting stats file: {e}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn try_persist_stats(stats: Weak<Stats>, path: &Path) -> anyhow::Result<()> {
    let temp_path = path.with_extension("tmp");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&temp_path)
        .await?;
    file.set_len(0).await?;
    file.write_all(&serde_json::to_vec(&stats)?).await?;
    file.flush().await?;
    tokio::fs::rename(temp_path, path).await?;
    Ok(())
}
