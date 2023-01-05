use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

pub use tokio::sync::mpsc::unbounded_channel;
pub use tokio::sync::mpsc::UnboundedReceiver;
pub use tokio::sync::mpsc::UnboundedSender;

pub(crate) struct Replicator {
    // FoundationDB handle
    fdb: foundationdb::Database,
    // Current maximum frame number
    top_frameno: AtomicU32,
    // Map of pages that take part in current transaction,
    // but aren't committed yet
    pending_pages: HashMap<u32, (u32, Vec<u8>)>,
    // Map of frames that take part in current transaction,
    // but aren't committed yet
    pending_frames: HashMap<u32, u32>,
}

impl Replicator {
    pub(crate) fn new(fdb_config_path: Option<String>) -> anyhow::Result<Self> {
        let fdb = foundationdb::Database::new(fdb_config_path.as_deref())
            .map_err(|fdb_error| anyhow::anyhow!("{}", fdb_error))?;

        Ok(Replicator {
            fdb,
            top_frameno: AtomicU32::new(1),
            pending_pages: HashMap::new(),
            pending_frames: HashMap::new(),
        })
    }

    pub(crate) fn load_top_frameno(&self) {
        tracing::debug!("Loading top frame number");
        let trx = self.fdb.create_trx().unwrap();
        let bound = "iku-val-\x7f".to_string();
        let range_opt = foundationdb::RangeOption {
            limit: Some(1),
            ..foundationdb::RangeOption::from((
                foundationdb::KeySelector::last_less_than(bound.as_bytes()),
                foundationdb::KeySelector::first_greater_than(bound.as_bytes()),
            ))
        };
        let result = trx.get_range(&range_opt, 1, true); // true -> it's a snapshot read: https://apple.github.io/foundationdb/api-c.html#snapshots
        let top = match tokio::runtime::Handle::current().block_on(result) {
            Ok(fdb_values) => match fdb_values.first() {
                Some(elem) => {
                    // Proprietary ultimate genius key format: iku-val-XframeXX
                    let frame = std::str::from_utf8(&elem.key()[8..=15])
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    tracing::debug!("Top frame detected: {}", frame);
                    frame
                }
                None => {
                    tracing::debug!("Top frame not found");
                    1
                }
            },
            Err(e) => {
                tracing::debug!("Error: {}", e);
                1
            }
        };
        self.top_frameno.store(top, Ordering::Relaxed);
    }

    pub(crate) fn next_frameno(&self) -> u32 {
        self.top_frameno.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn add_pending(&mut self, pages_iter: super::PageHdrIter) {
        for (pgno, data) in pages_iter {
            let frameno = self.next_frameno();
            tracing::debug!("Page {} put into pending as frame {}", pgno, frameno);
            self.pending_pages.insert(pgno, (frameno, data));
            self.pending_frames.insert(frameno, pgno);
        }
    }

    // Each page generates two records:
    // 1. a mapping from page number to its frame numbers
    // 2. a mapping from frame number to data
    pub(crate) fn flush_pending_pages(&mut self) {
        tracing::debug!("Flushing {} pages", self.pending_pages.len());
        for (pgno, (frameno, data)) in self.pending_pages.iter() {
            let trx = self.fdb.create_trx().unwrap();
            let index_key = format!("iku-key-{:08}-{:08}", pgno, frameno);
            let frame_key = format!("iku-val-{:08}", frameno);
            trx.set(index_key.as_bytes(), &[]);
            trx.set(frame_key.as_bytes(), data);
            let result = trx.commit();
            tokio::runtime::Handle::current().block_on(result).unwrap(); //FIXME: unwrap
            tracing::debug!("{} and {} sent to FoundationDB", index_key, frame_key);
        }
        self.clear_pending()
    }

    pub(crate) fn find_frame(&self, pgno: u32) -> u32 {
        tracing::debug!("Looking for frame for page {}", pgno,);

        if let Some((frame, _)) = self.pending_pages.get(&pgno) {
            tracing::debug!("Page {} found in pending pages as frame {}", pgno, frame);
            return *frame;
        }

        let trx = self.fdb.create_trx().unwrap();
        let bound = format!("iku-key-{:08}", pgno + 1); // assumes memcmp ordering on frame numbers
                                                        // Looks for a frame for this page number with highest frame index, which means
                                                        // that it's the newest frame.
        let range_opt = foundationdb::RangeOption {
            limit: Some(1),
            ..foundationdb::RangeOption::from((
                foundationdb::KeySelector::last_less_than(bound.as_bytes()),
                foundationdb::KeySelector::first_greater_than(bound.as_bytes()),
            ))
        };
        let result = trx.get_range(&range_opt, 1, true); // true -> it's a snapshot read: https://apple.github.io/foundationdb/api-c.html#snapshots
        match tokio::runtime::Handle::current().block_on(result) {
            Ok(fdb_values) => match fdb_values.first() {
                Some(elem) => {
                    // Proprietary ultimate genius key format: iku-key-XXpageXX-XframeXX
                    let page = std::str::from_utf8(&elem.key()[8..=15])
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    let frame = std::str::from_utf8(&elem.key()[17..=24])
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    if page == pgno {
                        frame
                    } else {
                        tracing::debug!("Frame for {} not found", pgno);
                        0
                    }
                }
                None => {
                    tracing::debug!("No frames found");
                    0
                }
            },
            Err(e) => {
                tracing::debug!("Error: {}", e);
                0
            }
        }
    }

    pub(crate) fn get_frame(&self, frameno: u32) -> Vec<u8> {
        tracing::debug!("Looking for data for frame {}", frameno);

        if let Some(pgno) = self.pending_frames.get(&frameno) {
            let (_, data) = self.pending_pages.get(pgno).unwrap(); // unwrapping, because we always update both maps
            tracing::debug!(
                "Retrieved frame {} for page {} from pending pages",
                frameno,
                pgno
            );
            return data.clone();
        }
        for (page, (pending_frameno, data)) in self.pending_pages.iter() {
            if frameno == *pending_frameno {
                tracing::debug!(
                    "Retrieved frame {} for page {} from pending pages",
                    frameno,
                    page
                );
                return data.clone();
            }
        }

        let trx = self.fdb.create_trx().unwrap();
        let key = format!("iku-val-{:08}", frameno);
        let result = trx.get(key.as_bytes(), true); // true -> it's a snapshot read: https://apple.github.io/foundationdb/api-c.html#snapshots
        match tokio::runtime::Handle::current().block_on(result) {
            Ok(opt_fdb_slice) => match opt_fdb_slice {
                Some(slice) => slice.to_vec(),
                None => {
                    tracing::debug!("No frame found: {}", frameno);
                    vec![]
                }
            },
            Err(e) => {
                tracing::debug!("Error: {}", e);
                vec![]
            }
        }
    }

    pub(crate) fn set_number_of_pages(&self, n: u32) {
        tracing::debug!("Setting the number of pages to {}", n);
        let trx = self.fdb.create_trx().unwrap();
        trx.set("iku-npages".as_bytes(), format!("{}", n).as_bytes());
        let result = trx.commit();
        tokio::runtime::Handle::current().block_on(result).unwrap(); //fixme
    }

    pub(crate) fn get_number_of_pages(&self) -> u32 {
        tracing::debug!("Looking for the number of pages");
        let trx = self.fdb.create_trx().unwrap();
        let result = trx.get("iku-npages".as_bytes(), true); // true -> it's a snapshot read: https://apple.github.io/foundationdb/api-c.html#snapshots
        match tokio::runtime::Handle::current().block_on(result) {
            Ok(opt_fdb_slice) => match opt_fdb_slice {
                Some(slice) => std::str::from_utf8(&slice).unwrap().parse::<u32>().unwrap(),
                None => 0,
            },
            Err(_) => 0,
        }
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_pages.clear();
        self.pending_frames.clear();
    }
}
