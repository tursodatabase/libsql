use std::{
    collections::{hash_map::Entry, HashMap},
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::broadcast::{self};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Copy, Clone, Serialize, Default)]
pub struct BroadcastMsg {
    #[serde(skip_serializing_if = "is_zero")]
    pub unknown: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub delete: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub insert: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub update: u64,
}

fn is_zero(num: &u64) -> bool {
    *num == 0
}

#[derive(Debug, Default)]
pub struct BroadcasterInner {
    state: Mutex<HashMap<String, BroadcastMsg>>,
    senders: Mutex<HashMap<String, broadcast::Sender<BroadcastMsg>>>,
    active: AtomicBool,
}

#[derive(Debug, Default, Clone)]
pub struct Broadcaster {
    inner: Arc<BroadcasterInner>,
}

impl Broadcaster {
    const BROADCAST_CAP: usize = 1024;

    pub fn active(&self) -> bool {
        self.inner.active.load(Ordering::Relaxed)
    }

    pub fn notify(&self, table: &str, action: rusqlite::hooks::Action) {
        if !self.active() {
            return;
        }
        let mut state = self.inner.state.lock();
        if let Some(entry) = state.get_mut(table) {
            Self::increment(entry, action);
        } else {
            let mut entry = BroadcastMsg::default();
            Self::increment(&mut entry, action);
            state.insert(table.into(), entry);
        }
    }

    fn increment(value: &mut BroadcastMsg, action: rusqlite::hooks::Action) {
        match action {
            rusqlite::hooks::Action::SQLITE_DELETE => value.delete += 1,
            rusqlite::hooks::Action::SQLITE_INSERT => value.insert += 1,
            rusqlite::hooks::Action::SQLITE_UPDATE => value.update += 1,
            _ => value.unknown += 1,
        }
    }

    pub fn commit(&self) {
        if !self.active() {
            return;
        }
        let senders = self.inner.senders.lock();
        for (table, entry) in self.flush_changes() {
            if let Some(sender) = senders.get(&table) {
                _ = sender.send(entry);
            }
        }
    }

    pub fn flush_changes(&self) -> HashMap<String, BroadcastMsg> {
        let mut changes = HashMap::new();
        mem::swap(&mut changes, &mut *self.inner.state.lock());
        changes
    }

    pub fn rollback(&self) {
        if !self.active() {
            return;
        }
        self.flush_changes();
    }

    pub fn subscribe(&self, table: String) -> BroadcastStream<BroadcastMsg> {
        let receiver = match self.inner.senders.lock().entry(table) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (sender, receiver) = broadcast::channel(Self::BROADCAST_CAP);
                entry.insert(sender);
                self.inner.active.store(true, Ordering::Relaxed);
                receiver
            }
        };

        BroadcastStream::new(receiver)
    }

    pub fn unsubscribe(&self, table: &String) {
        let mut tables = self.inner.senders.lock();
        if let Some(sender) = tables.get(table) {
            if sender.receiver_count() == 0 {
                tables.remove(table);
                if tables.is_empty() {
                    self.inner.active.store(false, Ordering::Relaxed);
                }
            }
        }
    }
}
