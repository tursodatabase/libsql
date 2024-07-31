use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use libsql_sys::name::NamespaceName;
use parking_lot::{ArcMutexGuard, RawMutex};
use tokio::sync::mpsc;

use crate::checkpointer::CheckpointMessage;
use crate::segment::current::{CurrentSegment, SegmentIndex};
use crate::shared_wal::WalLock;

pub enum Transaction<F> {
    Write(WriteTransaction<F>),
    Read(ReadTransaction<F>),
}

impl<T> From<ReadTransaction<T>> for Transaction<T> {
    fn from(value: ReadTransaction<T>) -> Self {
        Self::Read(value)
    }
}

impl<F> Transaction<F> {
    pub fn as_write_mut(&mut self) -> Option<&mut WriteTransaction<F>> {
        if let Self::Write(ref mut v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn max_frame_no(&self) -> u64 {
        match self {
            Transaction::Write(w) => w.next_frame_no - 1,
            Transaction::Read(read) => read.max_frame_no,
        }
    }

    pub(crate) fn end(self) {
        match self {
            Transaction::Write(tx) => {
                tx.downgrade();
            }
            Transaction::Read(_) => (),
        }
    }
}

impl<F> Deref for Transaction<F> {
    type Target = ReadTransaction<F>;

    fn deref(&self) -> &Self::Target {
        match self {
            Transaction::Write(tx) => &tx,
            Transaction::Read(tx) => &tx,
        }
    }
}

impl<F> DerefMut for Transaction<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Transaction::Write(ref mut tx) => tx,
            Transaction::Read(ref mut tx) => tx,
        }
    }
}

pub struct ReadTransaction<F> {
    pub id: u64,
    /// Max frame number that this transaction can read
    pub max_frame_no: u64,
    pub db_size: u32,
    /// The segment to which we have a read lock
    pub current: Arc<CurrentSegment<F>>,
    pub created_at: Instant,
    pub conn_id: u64,
    /// number of pages read by this transaction. This is used to determine whether a write lock
    /// will be re-acquired.
    pub pages_read: usize,
    pub namespace: NamespaceName,
    pub checkpoint_notifier: mpsc::Sender<CheckpointMessage>,
}

// fixme: clone should probably not be implemented for this type, figure a way to do it
impl<F> Clone for ReadTransaction<F> {
    fn clone(&self) -> Self {
        self.current.inc_reader_count();
        Self {
            id: self.id,
            max_frame_no: self.max_frame_no,
            current: self.current.clone(),
            db_size: self.db_size,
            created_at: self.created_at,
            conn_id: self.conn_id,
            pages_read: self.pages_read,
            namespace: self.namespace.clone(),
            checkpoint_notifier: self.checkpoint_notifier.clone(),
        }
    }
}

impl<F> Drop for ReadTransaction<F> {
    fn drop(&mut self) {
        // FIXME: it would be more approriate to wait till the segment is stored before notfying,
        // because we are not waiting for read to be released before that
        if self.current.dec_reader_count() && self.current.is_sealed() {
            let _: Result<_, _> = self
                .checkpoint_notifier
                .try_send(self.namespace.clone().into());
        }
    }
}

pub struct Savepoint {
    pub next_offset: u32,
    pub next_frame_no: u64,
    pub current_checksum: u32,
    pub index: BTreeMap<u32, u32>,
}

/// The savepoints must be passed from most recent to oldest
pub(crate) fn merge_savepoints<'a>(
    savepoints: impl Iterator<Item = &'a BTreeMap<u32, u32>>,
    out: &SegmentIndex,
) {
    for savepoint in savepoints {
        for (k, v) in savepoint.iter() {
            out.insert(*k, *v);
        }
    }
}

pub struct WriteTransaction<F> {
    /// id of the transaction currently holding the lock
    pub wal_lock: Arc<WalLock>,
    pub savepoints: Vec<Savepoint>,
    pub next_frame_no: u64,
    pub next_offset: u32,
    pub current_checksum: u32,
    pub is_commited: bool,
    pub read_tx: ReadTransaction<F>,
    /// if transaction overwrote frames, then the running checksum needs to be recomputed.
    /// We store here the lowest segment offset at which a frame was overwritten
    pub recompute_checksum: Option<u32>,
}

pub struct TxGuard<'a, F> {
    _lock: ArcMutexGuard<RawMutex, Option<u64>>,
    inner: &'a mut WriteTransaction<F>,
}

impl<'a, F> Deref for TxGuard<'a, F> {
    type Target = WriteTransaction<F>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, F> DerefMut for TxGuard<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<F> WriteTransaction<F> {
    pub(crate) fn merge_savepoints(&self, out: &SegmentIndex) {
        let savepoints = self.savepoints.iter().rev().map(|s| &s.index);
        merge_savepoints(savepoints, out);
    }

    pub fn savepoint(&mut self) -> usize {
        let savepoint_id = self.savepoints.len();
        self.savepoints.push(Savepoint {
            next_offset: self.next_offset,
            next_frame_no: self.next_frame_no,
            index: BTreeMap::new(),
            current_checksum: self.current_checksum,
        });
        savepoint_id
    }

    pub fn lock(&mut self) -> TxGuard<F> {
        if self.is_commited {
            tracing::error!("transaction already commited");
            todo!("txn has already been commited");
        }

        let g = self.wal_lock.tx_id.lock_arc();
        match *g {
            // we still hold the lock, we can proceed
            Some(id) if self.id == id => TxGuard {
                _lock: g,
                inner: self,
            },
            // Somebody took the lock from us
            Some(_) => todo!("lock stolen"),
            None => todo!("not a transaction"),
        }
    }

    pub fn reset(&mut self, savepoint_id: usize) {
        if savepoint_id >= self.savepoints.len() {
            unreachable!("savepoint doesn't exist");
        }

        self.savepoints.drain(savepoint_id + 1..).count();
        self.savepoints[savepoint_id].index.clear();
        let last_savepoint = self.savepoints.last().unwrap();
        self.next_frame_no = last_savepoint.next_frame_no;
        self.next_offset = last_savepoint.next_offset;
    }

    pub fn index_page_iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.savepoints
            .iter()
            .map(|s| s.index.keys().copied())
            .flatten()
    }

    pub fn not_empty(&self) -> bool {
        self.savepoints.iter().any(|s| !s.index.is_empty())
    }

    #[tracing::instrument(skip(self))]
    pub fn downgrade(self) -> ReadTransaction<F> {
        tracing::trace!("downgrading write transaction");
        let Self {
            wal_lock, read_tx, ..
        } = self;
        let mut lock = wal_lock.tx_id.lock();
        match *lock {
            Some(lock_id) if lock_id == read_tx.id => {
                lock.take();
            }
            _ => (),
        }

        if let Some(id) = *wal_lock.reserved.lock() {
            tracing::trace!("tx already reserved by {id}");
            return read_tx;
        }

        loop {
            match wal_lock.waiters.steal() {
                crossbeam::deque::Steal::Empty => {
                    tracing::trace!("no connection waiting");
                    break;
                }
                crossbeam::deque::Steal::Success((unparker, id)) => {
                    tracing::trace!("waking up {id}");
                    wal_lock.reserved.lock().replace(id);
                    unparker.unpark();
                    break;
                }
                crossbeam::deque::Steal::Retry => (),
            }
        }

        tracing::debug!(id = read_tx.id, "lock released");

        read_tx
    }

    pub fn is_commited(&self) -> bool {
        self.is_commited
    }

    pub(crate) fn find_frame_offset(&self, page_no: u32) -> Option<u32> {
        let iter = self.savepoints.iter().rev().map(|s| &s.index);
        for index in iter {
            if let Some(val) = index.get(&page_no) {
                return Some(*val);
            }
        }

        None
    }

    pub(crate) fn commit(&mut self) {
        self.is_commited = true;
    }

    pub(crate) fn current_checksum(&self) -> u32 {
        self.savepoints.last().unwrap().current_checksum
    }
}

impl<F> Deref for WriteTransaction<F> {
    type Target = ReadTransaction<F>;

    fn deref(&self) -> &Self::Target {
        &self.read_tx
    }
}

impl<F> DerefMut for WriteTransaction<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.read_tx
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::segment::current::SegmentIndex;

    use super::merge_savepoints;

    #[test]
    fn test_merge_savepoints() {
        let first = [(1, 1), (3, 2)].into_iter().collect::<BTreeMap<_, _>>();
        let second = [(1, 3), (4, 6)].into_iter().collect::<BTreeMap<_, _>>();

        let out = SegmentIndex::new(0);
        merge_savepoints([first, second].iter().rev(), &out);

        let mut iter = out.iter(0, 100);
        assert_eq!(iter.next(), Some((1, 3, 3)));
        assert_eq!(iter.next(), Some((3, 2, 2)));
        assert_eq!(iter.next(), Some((4, 6, 6)));
    }
}
