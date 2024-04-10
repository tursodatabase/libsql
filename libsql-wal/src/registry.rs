#![allow(dead_code, unused_variables, unreachable_code)]
use std::fs::OpenOptions;
use std::sync::Arc;
use std::path::{PathBuf, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use hashbrown::HashMap;
use libsql_sys::ffi::Sqlite3DbHeader;
use parking_lot::RwLock;
use zerocopy::{FromZeroes, AsBytes};

use crate::file::FileExt;
use crate::log::{SealedLog, Log};
use crate::name::NamespaceName;
use crate::segment_list::SegmentList;
use crate::shared_wal::SharedWal;
use crate::transaction::{WriteTransaction, Transaction};

/// Wal Registry maintains a set of shared Wal, and their respective set of files.
pub struct WalRegistry {
    path: PathBuf,
    shutdown: AtomicBool,
    openned: RwLock<HashMap<NamespaceName, Arc<SharedWal>>>,
}

impl WalRegistry {
    pub fn new(path: PathBuf) -> Self {
        std::fs::create_dir_all(&path).unwrap();
        Self {
            path,
            openned: Default::default(),
            shutdown: Default::default(),
        }
    }

    #[tracing::instrument(skip(self, db_path))]
    pub fn open(self: Arc<Self>, namespace: NamespaceName, db_path: &Path) -> Arc<SharedWal> {
        if self.shutdown.load(Ordering::SeqCst) {
            todo!("open after shutdown");
        }

        let mut openned = self.openned.upgradable_read();
        if let Some(entry) = openned.get(&namespace) {
            return entry.clone();
        }

        let path = self.path.join(namespace.as_str());
        std::fs::create_dir_all(&path).unwrap();
        let dir = walkdir::WalkDir::new(&path)
            .sort_by_file_name()
            .into_iter();

        let segments = SegmentList::default();
        for entry in dir {
            let entry = entry.unwrap();
            if entry.path().extension().map(|e| e.to_str().unwrap() != "log").unwrap_or(true) {
                continue
            }
            let file = OpenOptions::new().read(true).open(entry.path()).unwrap();
            let sealed = SealedLog::open(file, entry.path().to_path_buf(), Default::default());
            segments.push_log(sealed);
        }

        let (db_size, start_frame_no) = segments.with_head(|log| {
            let header = log.header();
            (header.db_size.get(), header.last_commited_frame_no.get() + 1)
        }).unwrap_or((1, 0));

        let current_path = path.join(format!("{namespace}:{start_frame_no:020}.log"));
        let current = arc_swap::ArcSwap::new(Arc::new(Log::create(&current_path, start_frame_no, db_size)));

        let db_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .unwrap();

        // If this is a fresh database, we want to patch the header value for reserved space at the
        // end of the file to store the replication index
        let mut header: Sqlite3DbHeader = Sqlite3DbHeader::new_zeroed();
        db_file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.reserved_in_page, 8, "bad db");
        
        let shared = Arc::new(SharedWal {
            current,
            segments,
            wal_lock: Default::default(),
            db_file,
            registry: self.clone(),
            namespace: namespace.clone(),
        });

        openned.with_upgraded(|openned| {
            openned.insert(namespace.clone(), shared.clone());
        });

        shared
    }

    #[tracing::instrument(skip_all)]
    pub fn swap_current(&self, shared: &SharedWal, tx: &WriteTransaction) {
        let before = Instant::now();
        assert!(tx.is_commited());
        // at this point we must hold a lock to a commited transation. 
        // First, we'll acquire the lock to the current transaction to make sure no one steals it from us:
        let lock = shared.wal_lock.tx_id.lock();
//        println!("lock_acquired: {}", before.elapsed().as_micros());1
        // Make sure that we still own the transaction:
        if lock.is_none() || lock.unwrap() != tx.id {
            return
        }

        // we have the lock, now create a new log
        let current = shared.current.load();
        let start_frame_no = current.last_commited() + 1;
        let path = self.path.join(shared.namespace.as_str()).join(format!("{}:{start_frame_no:020}.log", shared.namespace));
        let log = Log::create(&path, start_frame_no, current.db_size());
//        println!("log_created: {}", before.elapsed().as_micros());1
        // seal the old log and add it to the list
        let sealed = current.seal();
//        println!("log_sealed: {}", before.elapsed().as_micros());1
        {
            shared.segments.push_log(sealed);
//            println!("segment_written: {}", before.elapsed().as_micros());1
        }

        // place the new log
        shared.current.swap(Arc::new(log));
//        println!("log_swapped: {}", before.elapsed().as_micros());1
        tracing::debug!("current log swapped");
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let mut openned = self.openned.write();
        for (_, shared) in openned.drain() {
            let mut tx = Transaction::Read(shared.begin_read(u64::MAX));
            shared.upgrade(&mut tx).unwrap();
            tx.commit();
            self.swap_current(&shared, &mut tx.as_write_mut().unwrap());
            shared.current.load().seal();
            drop(tx);
            shared.segments.checkpoint(&shared.db_file);
        }
    }
}
