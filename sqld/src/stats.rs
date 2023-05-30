use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct Stats {
    inner: Arc<StatsInner>,
}

#[derive(Serialize, Deserialize, Default)]
struct StatsInner {
    rows_written: AtomicU64,
    rows_read: AtomicU64,
    storage_bytes_used: AtomicU64,
}

impl Stats {
    pub fn new(db_path: &Path) -> anyhow::Result<Self> {
        let stats_path = db_path.join("stats.json");
        let stats_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(stats_path)?;

        let stats_inner =
            serde_json::from_reader(&stats_file).unwrap_or_else(|_| StatsInner::default());
        let inner = Arc::new(stats_inner);

        spawn_stats_persist_thread(inner.clone(), stats_file);

        Ok(Self { inner })
    }

    /// increments the number of written rows by n
    pub fn inc_rows_written(&self, n: u64) {
        self.inner.rows_written.fetch_add(n, Ordering::Relaxed);
    }

    /// increments the number of read rows by n
    pub fn inc_rows_read(&self, n: u64) {
        self.inner.rows_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_storage_bytes_used(&self, n: u64) {
        self.inner.storage_bytes_used.store(n, Ordering::Relaxed);
    }

    /// returns the total number of rows read since this database was created
    pub fn rows_read(&self) -> u64 {
        self.inner.rows_read.load(Ordering::Relaxed)
    }

    /// returns the total number of rows written since this database was created
    pub fn rows_written(&self) -> u64 {
        self.inner.rows_written.load(Ordering::Relaxed)
    }

    /// returns the total number of bytes used by the database (excluding uncheckpointed WAL entries)
    pub fn storage_bytes_used(&self) -> u64 {
        self.inner.storage_bytes_used.load(Ordering::Relaxed)
    }
}

fn spawn_stats_persist_thread(stats: Arc<StatsInner>, mut file: File) {
    std::thread::spawn(move || loop {
        if file.rewind().is_ok() {
            let _ = serde_json::to_writer(&mut file, &stats);
        }
        std::thread::sleep(Duration::from_secs(5));
    });
}
