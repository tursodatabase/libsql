use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use roaring::RoaringBitmap;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use uuid::Uuid;
use zerocopy::{AsBytes, FromZeroes};

use crate::checkpointer::CheckpointMessage;
use crate::error::Result;
use crate::io::file::FileExt;
use crate::io::{Io, StdIO};
use crate::replication::injector::Injector;
use crate::replication::storage::{ReplicateFromStorage as _, StorageReplicator};
use crate::segment::sealed::SealedSegment;
use crate::shared_wal::SharedWal;
use crate::storage::Storage;
use crate::{LibsqlFooter, LIBSQL_PAGE_SIZE};
use libsql_sys::name::NamespaceName;

enum Slot<IO: Io, S> {
    Wal(Arc<SharedWal<IO, S>>),
    /// Only a single thread is allowed to instantiate the wal. The first thread to acquire an
    /// entry in the registry map puts a building slot. Other connections will wait for the mutex
    /// to turn to true, after the slot has been updated to contain the wal
    Building(Arc<(Condvar, Mutex<bool>)>, Arc<Notify>),
    /// The namespace was removed
    Tombstone,
}

/// Wal Registry maintains a set of shared Wal, and their respective set of files.
pub struct WalRegistry<IO: Io, S> {
    io: Arc<IO>,
    shutdown: AtomicBool,
    opened: DashMap<NamespaceName, Slot<IO, S>>,
    storage: Arc<S>,
    checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
}

impl<S> WalRegistry<StdIO, S> {
    pub fn new(
        storage: Arc<S>,
        checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
    ) -> Result<Self> {
        Self::new_with_io(StdIO(()), storage, checkpoint_notifier)
    }
}

impl<IO: Io, S> WalRegistry<IO, S> {
    pub fn new_with_io(
        io: IO,
        storage: Arc<S>,
        checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
    ) -> Result<Self> {
        let registry = Self {
            io: io.into(),
            opened: Default::default(),
            shutdown: Default::default(),
            storage,
            checkpoint_notifier,
        };

        Ok(registry)
    }

    pub async fn get_async(&self, namespace: &NamespaceName) -> Option<Arc<SharedWal<IO, S>>> {
        loop {
            let notify = {
                match self.opened.get(namespace).as_deref() {
                    Some(Slot::Wal(wal)) => return Some(wal.clone()),
                    Some(Slot::Building(_, notify)) => notify.clone(),
                    Some(Slot::Tombstone) => return None,
                    None => return None,
                }
            };

            notify.notified().await
        }
    }
}

impl<IO, S> WalRegistry<IO, S>
where
    IO: Io,
    S: Storage<Segment = SealedSegment<IO::File>>,
{
    #[tracing::instrument(skip(self))]
    pub fn open(
        self: Arc<Self>,
        db_path: &Path,
        namespace: &NamespaceName,
    ) -> Result<Arc<SharedWal<IO, S>>> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(crate::error::Error::ShuttingDown);
        }

        loop {
            if let Some(entry) = self.opened.get(namespace) {
                match &*entry {
                    Slot::Wal(wal) => return Ok(wal.clone()),
                    Slot::Building(cond, _) => {
                        let cond = cond.clone();
                        cond.0
                            .wait_while(&mut cond.1.lock(), |ready: &mut bool| !*ready);
                        // the slot was updated: try again
                        continue;
                    }
                    Slot::Tombstone => return Err(crate::error::Error::DeletingWal),
                }
            }

            let action = match self.opened.entry(namespace.clone()) {
                dashmap::Entry::Occupied(e) => match e.get() {
                    Slot::Wal(shared) => return Ok(shared.clone()),
                    Slot::Building(wait, _) => Err(wait.clone()),
                    Slot::Tombstone => return Err(crate::error::Error::DeletingWal),
                },
                dashmap::Entry::Vacant(e) => {
                    let notifier = Arc::new((Condvar::new(), Mutex::new(false)));
                    let async_notifier = Arc::new(Notify::new());
                    e.insert(Slot::Building(notifier.clone(), async_notifier.clone()));
                    Ok((notifier, async_notifier))
                }
            };

            match action {
                Ok((notifier, async_notifier)) => {
                    // if try_open succedded, then the slot was updated and contains the shared wal, if it
                    // failed we need to remove the slot. Either way, notify all waiters
                    let ret = match SharedWal::try_open(
                        self.io.clone(),
                        self.storage.clone(),
                        &self.checkpoint_notifier,
                        namespace,
                        db_path,
                    ) {
                        Ok(shared) => {
                            let shared = Arc::new(shared);
                            self.opened
                                .insert(namespace.clone(), Slot::Wal(shared.clone()));
                            Ok(shared)
                        }
                        Err(e) => {
                            tracing::error!("error opening wal: {e}");
                            self.opened.remove(namespace);
                            Err(e)
                        }
                    };

                    *notifier.1.lock() = true;
                    notifier.0.notify_all();
                    async_notifier.notify_waiters();

                    return ret;
                }
                Err(cond) => {
                    cond.0
                        .wait_while(&mut cond.1.lock(), |ready: &mut bool| !*ready);
                    // the slot was updated: try again
                    continue;
                }
            }
        }
    }

    pub async fn tombstone(&self, namespace: &NamespaceName) -> Option<Arc<SharedWal<IO, S>>> {
        // if a wal is currently being openned, let it
        {
            let v = self.opened.get(namespace)?;
            if let Slot::Building(_, ref notify) = *v {
                notify.clone().notified().await;
            }
        }

        match self.opened.insert(namespace.clone(), Slot::Tombstone) {
            Some(Slot::Tombstone) => None,
            Some(Slot::Building(_, _)) => {
                // FIXME: that could happen is someone removed it and immediately reopenned the
                // wal. fix by retrying in a loop
                unreachable!("already waited for ns to open")
            }
            Some(Slot::Wal(wal)) => Some(wal),
            None => None,
        }
    }

    pub async fn remove(&self, namespace: &NamespaceName) {
        // if a wal is currently being openned, let it
        {
            let v = self.opened.get(namespace);
            if let Some(Slot::Building(_, ref notify)) = v.as_deref() {
                notify.clone().notified().await;
            }
        }

        self.opened.remove(namespace);
    }

    /// Attempts to sync all loaded dbs with durable storage
    pub async fn sync_all(&self, conccurency: usize) -> Result<()>
    where
        S: Storage,
    {
        let mut join_set = JoinSet::new();
        tracing::info!("syncing {} namespaces", self.opened.len());
        // FIXME: arbitrary value, maybe use something like numcpu * 2?
        let before_sync = Instant::now();
        let sem = Arc::new(Semaphore::new(conccurency));
        for entry in self.opened.iter() {
            let Slot::Wal(shared) = entry.value() else {
                panic!("all wals should already be opened")
            };
            let storage = self.storage.clone();
            let shared = shared.clone();
            let sem = sem.clone();
            let permit = sem.acquire_owned().await.unwrap();

            join_set.spawn(async move {
                let _permit = permit;
                sync_one(shared, storage).await
            });

            if let Some(ret) = join_set.try_join_next() {
                ret.unwrap()?;
            }
        }

        while let Some(ret) = join_set.join_next().await {
            ret.unwrap()?;
        }

        tracing::info!("synced in {:?}", before_sync.elapsed());

        Ok(())
    }

    // On shutdown, we checkpoint all the WALs. This require sealing the current segment, and when
    // checkpointing all the segments
    pub async fn shutdown(self: Arc<Self>) -> Result<()> {
        tracing::info!("shutting down registry");
        self.shutdown.store(true, Ordering::SeqCst);

        let mut join_set = JoinSet::<Result<()>>::new();
        let semaphore = Arc::new(Semaphore::new(8));
        for item in self.opened.iter() {
            let (name, slot) = item.pair();
            loop {
                match slot {
                    Slot::Wal(shared) => {
                        // acquire a permit or drain the join set
                        let permit = loop {
                            tokio::select! {
                                permit = semaphore.clone().acquire_owned() => break permit,
                                _ = join_set.join_next() => (),
                            }
                        };
                        let shared = shared.clone();
                        let name = name.clone();

                        join_set.spawn_blocking(move || {
                            let _permit = permit;
                            if let Err(e) = shared.shutdown() {
                                tracing::error!("error shutting down `{name}`: {e}");
                            }

                            Ok(())
                        });
                        break;
                    }
                    Slot::Building(_, notify) => {
                        // wait for shared to finish building
                        notify.notified().await;
                    }
                    Slot::Tombstone => continue,
                }
            }
        }

        while join_set.join_next().await.is_some() {}

        // we process any pending storage job, then checkpoint everything
        self.storage.shutdown().await;

        // wait for checkpointer to exit
        let _ = self
            .checkpoint_notifier
            .send(CheckpointMessage::Shutdown)
            .await;
        self.checkpoint_notifier.closed().await;

        tracing::info!("registry shutdown gracefully");

        Ok(())
    }

    pub fn storage(&self) -> Arc<S> {
        self.storage.clone()
    }
}

#[tracing::instrument(skip_all, fields(namespace = shared.namespace.as_str()))]
async fn sync_one<IO, S>(shared: Arc<SharedWal<IO, S>>, storage: Arc<S>) -> Result<()>
where
    IO: Io,
    S: Storage<Segment = SealedSegment<IO::File>>,
{
    let remote_durable_frame_no = storage
        .durable_frame_no(shared.namespace(), None)
        .await
        .map_err(Box::new)?;
    let local_current_frame_no = shared.current.load().next_frame_no().get() - 1;

    if remote_durable_frame_no > local_current_frame_no {
        tracing::info!(
            remote_durable_frame_no,
            local_current_frame_no,
            "remote storage has newer segments"
        );
        let mut seen = RoaringBitmap::new();
        let replicator = StorageReplicator::new(storage, shared.namespace().clone());
        let stream = replicator
            .stream(&mut seen, remote_durable_frame_no, 1)
            .peekable();
        let mut injector = Injector::new(shared.clone(), 10)?;
        // we set the durable frame_no before we start injecting, because the wal may want to
        // checkpoint on commit.
        injector.set_durable(remote_durable_frame_no);
        // use pin to the heap so that we can drop the stream in the loop, and count `seen`.
        let mut stream = Box::pin(stream);
        loop {
            match stream.next().await {
                Some(Ok(mut frame)) => {
                    if stream.peek().await.is_none() {
                        drop(stream);
                        frame.header_mut().set_size_after(seen.len() as _);
                        injector.insert_frame(frame).await?;
                        break;
                    } else {
                        injector.insert_frame(frame).await?;
                    }
                }
                Some(Err(e)) => todo!("handle error: {e}, {}", shared.namespace()),
                None => break,
            }
        }
    }

    tracing::info!("local database is up to date");

    Ok(())
}

fn read_log_id_from_footer<F: FileExt>(db_file: &F, db_size: u64) -> io::Result<Uuid> {
    let mut footer: LibsqlFooter = LibsqlFooter::new_zeroed();
    let footer_offset = LIBSQL_PAGE_SIZE as u64 * db_size;
    // FIXME: failing to read the footer here is a sign of corrupted database: either we
    // have a tail to the segment list, or we have fully checkpointed the database. Can we
    // recover from that?
    db_file.read_exact_at(footer.as_bytes_mut(), footer_offset)?;
    Ok(footer.log_id())
}
