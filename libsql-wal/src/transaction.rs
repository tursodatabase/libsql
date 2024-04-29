use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use crate::segment::current::CurrentSegment;
use crate::shared_wal::WalLock;

pub enum Transaction<F> {
    Write(WriteTransaction<F>),
    Read(ReadTransaction<F>),
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

    pub(crate) fn commit(&mut self) {
        match self {
            Transaction::Write(tx) => {
                tx.is_commited = true;
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
}

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
        }
    }
}

impl<F> Drop for ReadTransaction<F> {
    fn drop(&mut self) {
        // FIXME: if the count drops to 0, register for compaction.
        self.current.dec_reader_count();
    }
}

pub struct Savepoint {
    pub next_offset: u32,
    pub next_frame_no: u64,
    pub index: BTreeMap<u32, u32>,
}

/// The savepoints must be passed from most recent to oldest
pub fn merge_savepoints<'a>(
    savepoints: impl Iterator<Item = &'a BTreeMap<u32, u32>>,
    out: &mut BTreeMap<u32, Vec<u32>>,
) {
    for savepoint in savepoints {
        for (k, v) in savepoint.iter() {
            let entry = out.entry(*k).or_default();
            match entry.last() {
                Some(i) if i >= v => continue,
                _ => {
                    entry.push(*v);
                }
            }
        }
    }
}

pub struct WriteTransaction<F> {
    /// id of the transaction currently holding the lock
    pub wal_lock: Arc<WalLock>,
    pub savepoints: Vec<Savepoint>,
    pub next_frame_no: u64,
    pub next_offset: u32,
    pub is_commited: bool,
    pub read_tx: ReadTransaction<F>,
}

impl<F> WriteTransaction<F> {
    /// enter the lock critical section
    pub fn enter<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        if self.is_commited {
            tracing::error!("transaction already commited");
            todo!("txn has already been commited");
        }

        let wal_lock = self.wal_lock.clone();
        let g = wal_lock.tx_id.lock();
        match *g {
            // we still hold the lock, we can proceed
            Some(id) if self.id == id => f(self),
            // Somebody took the lock from us
            Some(_) => todo!("lock stolen"),
            None => todo!("not a transaction"),
        }
    }

    pub fn not_empty(&self) -> bool {
        self.savepoints.iter().any(|s| !s.index.is_empty())
    }

    pub fn merge_savepoints(&self, out: &mut BTreeMap<u32, Vec<u32>>) {
        let savepoints = self.savepoints.iter().rev().map(|s| &s.index);
        merge_savepoints(savepoints, out);
    }

    pub fn savepoint(&mut self) -> usize {
        let savepoint_id = self.savepoints.len();
        self.savepoints.push(Savepoint {
            next_offset: self.next_offset,
            next_frame_no: self.next_frame_no,
            index: BTreeMap::new(),
        });
        savepoint_id
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
        // let iter = self.savepoints.iter().filter_map(|s| s.index.as_ref());
        // let mut union = iter.collect::<OpBuilder>().union();
        // std::iter::from_fn(move || match union.next() {
        //     Some((key, vals)) => {
        //         let key = u32::from_be_bytes(key.try_into().unwrap());
        //         let val = vals.iter().max_by_key(|i| i.index).unwrap().value;
        //         Some((key, val))
        //     }
        //     None => None,
        // })
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

    use super::merge_savepoints;

    #[test]
    fn test_merge_savepoints() {
        let first = [(1, 1), (3, 2)].into_iter().collect::<BTreeMap<_, _>>();
        let second = [(1, 3), (4, 6)].into_iter().collect::<BTreeMap<_, _>>();

        let mut out = BTreeMap::new();
        merge_savepoints([first, second].iter().rev(), &mut out);

        let mut iter = out.into_iter();
        assert_eq!(iter.next(), Some((1, vec![3])));
        assert_eq!(iter.next(), Some((3, vec![2])));
        assert_eq!(iter.next(), Some((4, vec![6])));
    }
}
