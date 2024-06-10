use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::{self};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BroadcastMsg {
    Commit,
    Rollback,
    #[serde(untagged)]
    Change {
        action: Action,
        rowid: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    UNKNOWN,
    DELETE,
    INSERT,
    UPDATE,
}

impl From<rusqlite::hooks::Action> for Action {
    fn from(value: rusqlite::hooks::Action) -> Self {
        match value {
            rusqlite::hooks::Action::SQLITE_DELETE => Action::DELETE,
            rusqlite::hooks::Action::SQLITE_INSERT => Action::INSERT,
            rusqlite::hooks::Action::SQLITE_UPDATE => Action::UPDATE,
            _ => Action::UNKNOWN,
        }
    }
}

impl From<&str> for Action {
    fn from(value: &str) -> Self {
        match value {
            "delete" => Action::DELETE,
            "insert" => Action::INSERT,
            "update" => Action::UPDATE,
            _ => Action::UNKNOWN,
        }
    }
}

pub struct UpdateSubscription {
    pub inner: BroadcastStream<BroadcastMsg>,
}

const BROADCAST_CAP: usize = 1024;

#[derive(Debug, Default)]
pub struct BroadcasterInner {
    senders: Mutex<HashMap<String, broadcast::Sender<BroadcastMsg>>>,
    active: AtomicBool,
}

#[derive(Debug, Default, Clone)]
pub struct Broadcaster {
    inner: Arc<BroadcasterInner>,
}

impl Broadcaster {
    pub fn active(&self) -> bool {
        self.inner.active.load(Ordering::Relaxed)
    }

    pub fn notify(&self, table: &str, msg: BroadcastMsg) {
        if !self.active() {
            return;
        }
        self.inner
            .senders
            .lock()
            .get(table)
            .map(|sender| sender.send(msg));
    }

    pub fn notify_all(&self, msg: BroadcastMsg) {
        if !self.active() {
            return;
        }
        self.inner.senders.lock().values().for_each(|sender| {
            _ = sender.send(msg.clone());
        });
    }

    pub fn subscribe(&self, table: String) -> UpdateSubscription {
        let receiver = match self.inner.senders.lock().entry(table) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (sender, receiver) = broadcast::channel(BROADCAST_CAP);
                entry.insert(sender);
                self.inner.active.store(true, Ordering::Relaxed);
                receiver
            }
        };

        UpdateSubscription {
            inner: BroadcastStream::new(receiver),
        }
    }

    pub fn unsubscribe(&self, table: String) {
        let mut tables = self.inner.senders.lock();
        if let Some(sender) = tables.get(&table) {
            if sender.receiver_count() == 0 {
                tables.remove(&table);
                if tables.is_empty() {
                    self.inner.active.store(false, Ordering::Relaxed);
                }
            }
        }
    }
}
