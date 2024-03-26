use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use libsql_sys::ffi::Sqlite3DbHeader;
use parking_lot::{Condvar, Mutex, RwLock, RwLockUpgradableReadGuard};
use zerocopy::{AsBytes, FromZeroes};

use crate::error::Result;
use crate::fs::file::FileExt;
use crate::fs::{FileSystem, StdFs};
use crate::name::NamespaceName;
use crate::segment::list::SegmentList;
use crate::segment::{current::CurrentSegment, sealed::SealedSegment};
use crate::shared_wal::SharedWal;
use crate::transaction::{Transaction, WriteTransaction};

/// Translates a path to a namespace name
pub trait NamespaceResolver {
    fn resolve(&self, path: &Path) -> NamespaceName;
}

impl<F: Fn(&Path) -> NamespaceName + Send + Sync + 'static> NamespaceResolver for F {
    fn resolve(&self, path: &Path) -> NamespaceName {
        (self)(path)
    }
}

enum Slot<FS: FileSystem> {
    Wal(Arc<SharedWal<FS>>),
    /// Only a single thread is allowed to instantiate the wal. The first thread to acquire an
    /// entry in the registry map puts a building slot. Other connections will wait for the mutex
    /// to turn to true, after the slot has been updated to contain the wal
    Building(Arc<(Condvar, Mutex<bool>)>),
}

/// Wal Registry maintains a set of shared Wal, and their respective set of files.
pub struct WalRegistry<FS: FileSystem> {
    fs: FS,
    path: PathBuf,
    shutdown: AtomicBool,
    opened: RwLock<HashMap<NamespaceName, Slot<FS>>>,
    resolver: Box<dyn NamespaceResolver + Send + Sync + 'static>,
}

impl WalRegistry<StdFs> {
    pub fn new(
        path: PathBuf,
        resolver: impl NamespaceResolver + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::new_with_fs(StdFs(()), path, resolver)
    }
}

impl<FS: FileSystem> WalRegistry<FS> {
    pub fn new_with_fs(
        fs: FS,
        path: PathBuf,
        resolver: impl NamespaceResolver + Send + Sync + 'static,
    ) -> Result<Self> {
        fs.create_dir_all(&path)?;
        Ok(Self {
            fs,
            path,
            opened: Default::default(),
            shutdown: Default::default(),
            resolver: Box::new(resolver),
        })
    }

    #[tracing::instrument(skip(self, db_path))]
    pub fn open(self: Arc<Self>, db_path: &Path) -> Result<Arc<SharedWal<FS>>> {
        if self.shutdown.load(Ordering::SeqCst) {
            todo!("open after shutdown");
        }

        let namespace = self.resolver.resolve(db_path);
        loop {
            let mut opened = self.opened.upgradable_read();
            if let Some(entry) = opened.get(&namespace) {
                match entry {
                    Slot::Wal(wal) => return Ok(wal.clone()),
                    Slot::Building(cond) => {
                        let cond = cond.clone();
                        drop(opened);
                        cond.0
                            .wait_while(&mut cond.1.lock(), |ready: &mut bool| !*ready);
                        // the slot was updated: try again
                        continue;
                    }
                }
            }

            // another thread may have got the slot first, just retry if that's the case
            let Ok(notifier) = opened.with_upgraded(|map| match map.entry(namespace.clone()) {
                Entry::Occupied(_) => Err(()),
                Entry::Vacant(entry) => {
                    let notifier = Arc::new((Condvar::new(), Mutex::new(false)));
                    entry.insert(Slot::Building(notifier.clone()));
                    Ok(notifier)
                }
            }) else {
                continue;
            };

            // if try_open succedded, then the slot was updated and contains the shared wal, if it
            // failed we need to remove the slot. Either way, notify all waiters
            let ret = self.clone().try_open(&namespace, db_path, &mut opened);
            if ret.is_err() {
                opened.with_upgraded(|map| {
                    map.remove(&namespace);
                })
            }

            *notifier.1.lock() = true;
            notifier.0.notify_all();

            return ret;
        }
    }

    fn try_open(
        self: Arc<Self>,
        namespace: &NamespaceName,
        db_path: &Path,
        opened: &mut RwLockUpgradableReadGuard<HashMap<NamespaceName, Slot<FS>>>,
    ) -> Result<Arc<SharedWal<FS>>> {
        let path = self.path.join(namespace.as_str());
        self.fs.create_dir_all(&path)?;
        let dir = walkdir::WalkDir::new(&path).sort_by_file_name().into_iter();

        let tail = SegmentList::default();
        for entry in dir {
            let entry = entry.map_err(|e| e.into_io_error().unwrap())?;
            if entry
                .path()
                .extension()
                .map(|e| e.to_str().unwrap() != "seg")
                .unwrap_or(true)
            {
                continue;
            }

            let file = self.fs.open(false, true, true, entry.path())?;

            if let Some(sealed) =
                SealedSegment::open(file.into(), entry.path().to_path_buf(), Default::default())?
            {
                tail.push_log(sealed);
            }
        }

        let db_file = self.fs.open(false, true, true, db_path)?;

        let mut header: Sqlite3DbHeader = Sqlite3DbHeader::new_zeroed();
        db_file.read_exact_at(header.as_bytes_mut(), 0)?;

        let (db_size, next_frame_no) = tail
            .with_head(|segment| {
                let header = segment.header();
                (header.db_size(), header.next_frame_no())
            })
            .unwrap_or((
                header.db_size.get(),
                NonZeroU64::new(header.replication_index.get() + 1)
                    .unwrap_or(NonZeroU64::new(1).unwrap()),
            ));

        let current_path = path.join(format!("{namespace}:{next_frame_no:020}.seg"));

        let segment_file = self.fs.open(true, true, true, &current_path)?;

        let current = arc_swap::ArcSwap::new(Arc::new(CurrentSegment::create(
            segment_file,
            current_path,
            next_frame_no,
            db_size,
            tail.into(),
        )?));

        let shared = Arc::new(SharedWal {
            current,
            wal_lock: Default::default(),
            db_file,
            registry: self.clone(),
            namespace: namespace.clone(),
        });

        opened.with_upgraded(|opened| {
            opened.insert(namespace.clone(), Slot::Wal(shared.clone()));
        });

        return Ok(shared);
    }

    #[tracing::instrument(skip_all)]
    pub fn swap_current(
        &self,
        shared: &SharedWal<FS>,
        tx: &WriteTransaction<FS::File>,
    ) -> Result<()> {
        assert!(tx.is_commited());
        // at this point we must hold a lock to a commited transaction.
        // First, we'll acquire the lock to the current transaction to make sure no one steals it from us:
        let lock = shared.wal_lock.tx_id.lock();
        // Make sure that we still own the transaction:
        if lock.is_none() || lock.unwrap() != tx.id {
            return Ok(());
        }

        let current = shared.current.load();
        if current.is_empty() {
            return Ok(());
        }
        let start_frame_no = current.next_frame_no();
        let path = self
            .path
            .join(shared.namespace().as_str())
            .join(format!("{}:{start_frame_no:020}.seg", shared.namespace()));

        let segment_file = self.fs.open(true, true, true, &path)?;

        let new = CurrentSegment::create(
            segment_file,
            path,
            start_frame_no,
            current.db_size(),
            current.tail().clone(),
        )?;
        // sealing must the last fallible operation, because we don't want to end up in a situation
        // where the current log is sealed and it wasn't swapped.
        if let Some(sealed) = current.seal()? {
            new.tail().push_log(sealed);
        }

        shared.current.swap(Arc::new(new));
        tracing::debug!("current segment swapped");

        Ok(())
    }

    // On shutdown, we checkpoint all the WALs. This require sealing the current segment, and when
    // checkpointing all the segments
    pub fn shutdown(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let mut opened = self.opened.write();
        for (_, shared) in opened.drain() {
            let Slot::Wal(shared) = shared else {
                // TODO: figure out what to do when the wal is being opened
                continue;
            };
            let mut tx = Transaction::Read(shared.begin_read(u64::MAX));
            shared.upgrade(&mut tx)?;
            tx.commit();
            self.swap_current(&shared, &mut tx.as_write_mut().unwrap())?;
            // The current segment will not be used anymore. It's empty, but we still seal it so that
            // the next startup doesn't find an unsealed segment.
            shared.current.load().seal()?;
            drop(tx);
            shared.current.load().tail().checkpoint(&shared.db_file)?;
        }

        Ok(())
    }
}
