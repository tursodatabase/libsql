use std::ops::Deref;
use std::sync::{Arc, atomic::Ordering};

use crossbeam::deque::Injector;
use crossbeam::sync::Unparker;
use fst::map::Map;
use parking_lot::Mutex;

use crate::log::Log;

pub enum Transaction {
    Write(WriteTransaction),
    Read(ReadTransaction),
}

impl Deref for Transaction {
    type Target = ReadTransaction;

    fn deref(&self) -> &Self::Target {
        match self {
            Transaction::Write(tx) => &tx,
            Transaction::Read(tx) => &tx,
        }
    }
}

pub struct ReadTransaction {
    /// Max frame number that this transaction can read
    pub max_frame_no: u64,
    pub db_size: u32,
    /// The log to which we have a read lock
    pub log: Arc<Log>,
}

impl Clone for ReadTransaction {
    fn clone(&self) -> Self {
        self.log.read_locks.fetch_add(1, Ordering::SeqCst);
        Self { max_frame_no: self.max_frame_no, log: self.log.clone(),  db_size: self.db_size }
    }
}

impl Drop for ReadTransaction {
    fn drop(&mut self) {
        // FIXME: if the count drops to 0, register for compaction.
        self.log.read_locks.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct WriteTransaction {
    pub id: u64,
    /// id of the transaction currently holding the lock
    pub lock: Arc<Mutex<Option<u64>>>,
    pub waiters: Arc<Injector<Unparker>>,
    pub index: Option<Map<Vec<u8>>>,
    pub next_frame_no: u64,
    pub next_offset: u32,
    pub is_commited: bool,
    pub read_tx: ReadTransaction,
}

impl WriteTransaction {
    /// enter the lock critical section
    pub fn enter<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        if self.is_commited {
            tracing::error!("transaction already commited");
            todo!("txn has already been commited");
        }

        let lock = self.lock.clone();
        let g = lock.lock();
        match *g {
            // we still hold the lock, we can proceed
            Some(id) if self.id == id => {
                f(self)
            },
            // Somebody took the lock from us
            Some(_) => todo!("lock stolen"),
            None => todo!("not a transaction"),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn downgrade(self) -> ReadTransaction {
        let Self { id, lock, read_tx, .. } = self;
        let mut lock = lock.lock();
        match *lock {
            Some(lock_id) if lock_id == id => {
                lock.take();
            }
            _ => (),
        }

        loop {
            match self.waiters.steal() {
                crossbeam::deque::Steal::Empty => break,
                crossbeam::deque::Steal::Success(unparker) => {
                    unparker.unpark();
                    break
                },
                crossbeam::deque::Steal::Retry => (),
            }
        }

        tracing::debug!(id=self.id, "lock released");

        read_tx
    }

    pub fn is_commited(&self) -> bool {
        self.is_commited
    }
}

impl Deref for WriteTransaction {
    type Target = ReadTransaction;

    fn deref(&self) -> &Self::Target {
        &self.read_tx
    }
}
