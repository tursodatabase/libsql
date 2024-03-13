use std::sync::Arc;

use hashbrown::{hash_map::Entry, HashMap};
use parking_lot::Mutex;

use super::NamespaceName;

#[derive(Default)]
pub struct SchemaLocksRegistry {
    locks: Arc<Mutex<HashMap<NamespaceName, Arc<tokio::sync::RwLock<()>>>>>,
}

enum SchemaLockKind {
    Shared(tokio::sync::OwnedRwLockReadGuard<()>),
    Exclusive(tokio::sync::OwnedRwLockWriteGuard<()>),
}

pub struct SchemaLock {
    schema: NamespaceName,
    _guard: Option<SchemaLockKind>,
    locks: Arc<Mutex<HashMap<NamespaceName, Arc<tokio::sync::RwLock<()>>>>>,
}

impl Drop for SchemaLock {
    fn drop(&mut self) {
        let mut locks = self.locks.lock();
        match locks.entry(self.schema.clone()) {
            Entry::Occupied(entry) => {
                // there's only two ref left: the maps, and ours
                if Arc::strong_count(entry.get()) == 2 {
                    entry.remove();
                }
            }
            Entry::Vacant(_) => unreachable!("lock entry removed while we still hold a lock to it"),
        }
    }
}

impl SchemaLocksRegistry {
    pub async fn acquire_shared(&self, schema: NamespaceName) -> SchemaLock {
        let lock = {
            let mut lock = self.locks.lock();
            let lock = lock.entry(schema.clone()).or_default();
            lock.clone()
        };
        let guard = lock.read_owned().await;
        SchemaLock {
            schema,
            _guard: Some(SchemaLockKind::Shared(guard)),
            locks: self.locks.clone(),
        }
    }

    pub async fn acquire_exlusive(&self, schema: NamespaceName) -> SchemaLock {
        let lock = {
            let mut lock = self.locks.lock();
            let lock = lock.entry(schema.clone()).or_default();
            lock.clone()
        };
        let guard = lock.write_owned().await;
        SchemaLock {
            schema,
            _guard: Some(SchemaLockKind::Exclusive(guard)),
            locks: self.locks.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn schema_lock_works() {
        let locks = SchemaLocksRegistry::default();

        let lock1 = locks.acquire_shared("schema".into()).await;
        let lock2 = locks.acquire_shared("schema".into()).await;
        assert_eq!(locks.locks.lock().len(), 1);

        drop(lock1);
        assert_eq!(locks.locks.lock().len(), 1);

        drop(lock2);
        assert!(locks.locks.lock().is_empty());

        let lock1 = locks.acquire_exlusive("schema1".into()).await;
        let lock2 = locks.acquire_exlusive("schema2".into()).await;
        assert_eq!(locks.locks.lock().len(), 2);
        drop(lock1);
        drop(lock2);

        assert!(locks.locks.lock().is_empty());
    }
}
