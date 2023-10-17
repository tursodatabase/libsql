use anyhow::Result;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct TransactionPageCache {
    /// Threshold (in pages) after which, the cache will start flushing pages on disk.
    swap_after_pages: u32,
    page_size: u32,
    /// Recovery file used to flushing pages on disk. Reusable between transactions.
    cache: Cache,
    recovery_fpath: Arc<str>,
}

impl TransactionPageCache {
    pub fn new(swap_after_pages: u32, page_size: u32, recovery_fpath: Arc<str>) -> Self {
        TransactionPageCache {
            swap_after_pages,
            page_size,
            recovery_fpath,
            cache: Cache::Memory(BTreeMap::new()),
        }
    }

    pub async fn insert(&mut self, pgno: u32, page: &[u8]) -> Result<()> {
        match &mut self.cache {
            Cache::Memory(map) => {
                let len = map.len();
                match map.entry(pgno) {
                    Entry::Vacant(_) if len > self.swap_after_pages as usize => {
                        let page_size = self.page_size;
                        match self.swap().await {
                            Cache::Disk { index, file } => {
                                Self::persist(index, file, pgno, page_size, page).await?;
                            }
                            Cache::Memory(map) => {
                                map.insert(pgno, page.into());
                            }
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(page.into());
                    }
                    Entry::Occupied(mut e) => {
                        let buf = e.get_mut();
                        buf.copy_from_slice(page);
                    }
                }
            }
            Cache::Disk { index, file } => {
                Self::persist(index, file, pgno, self.page_size, page).await?;
            }
        }
        Ok(())
    }

    async fn persist(
        index: &mut BTreeMap<u32, u64>,
        file: &mut File,
        pgno: u32,
        page_size: u32,
        page: &[u8],
    ) -> Result<()> {
        let end = (index.len() as u64) * (page_size as u64);
        match index.entry(pgno) {
            Entry::Vacant(e) => {
                file.seek(SeekFrom::End(0)).await?;
                file.write_all(page).await?;
                e.insert(end);
            }
            Entry::Occupied(e) => {
                let offset = *e.get();
                file.seek(SeekFrom::Start(offset)).await?;
                file.write_all(page).await?;
            }
        }
        Ok(())
    }

    /// Swaps current memory cache onto disk.
    async fn swap(&mut self) -> &mut Cache {
        if let Cache::Disk { .. } = self.cache {
            tracing::trace!("Swap called on cache already using disk space.");
            return &mut self.cache; // already swapped
        }
        tracing::trace!("Swapping transaction pages to file {}", self.recovery_fpath);
        let mut index = BTreeMap::new();
        let result = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&*self.recovery_fpath)
            .await;
        match result {
            Ok(mut file) => {
                if let Cache::Memory(old) = &self.cache {
                    let mut end = 0u64;
                    for (&pgno, page) in old {
                        if let Err(e) = file.write_all(page).await {
                            tracing::warn!(
                                "Failed to swap transaction page cache to disk due to: {}",
                                e
                            );
                            // fallback to use memory cache
                            return &mut self.cache;
                        }
                        index.insert(pgno, end);
                        end += page.len() as u64;
                    }
                }
                self.cache = Cache::Disk { index, file };
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create transaction page cache file '{}': {}",
                    self.recovery_fpath,
                    e
                );
            }
        }
        &mut self.cache
    }

    pub async fn flush(mut self, db_file: &mut File) -> Result<()> {
        use tokio::io::AsyncReadExt;
        match &mut self.cache {
            Cache::Memory(map) => {
                for (&pgno, page) in map.iter() {
                    let offset = (pgno - 1) as u64 * (self.page_size as u64);
                    db_file.seek(SeekFrom::Start(offset)).await?;
                    db_file.write_all(page).await?;
                }
            }
            Cache::Disk { index, file } => {
                for (&pgno, &off) in index.iter() {
                    let offset = (pgno - 1) as u64 * (self.page_size as u64);
                    db_file.seek(SeekFrom::Start(offset)).await?;
                    let mut f = file.try_clone().await?;
                    f.seek(SeekFrom::Start(off)).await?;
                    let mut page = f.take(self.page_size as u64);
                    tokio::io::copy(&mut page, db_file).await?;
                }
                file.shutdown().await?;
                db_file.flush().await?;
                tokio::fs::remove_file(&*self.recovery_fpath).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Cache {
    /// Map storing page number and pages themselves in memory.
    Memory(BTreeMap<u32, Vec<u8>>),
    /// Map storing page number and offsets in transaction recovery file.
    Disk {
        index: BTreeMap<u32, u64>,
        file: File,
    },
}
