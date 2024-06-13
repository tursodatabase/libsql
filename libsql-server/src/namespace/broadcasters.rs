use std::sync::Arc;

use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio_stream::wrappers::BroadcastStream;

use crate::broadcaster::{BroadcastMsg, Broadcaster};

use super::NamespaceName;

type BroadcasterRegistryInner = Mutex<HashMap<NamespaceName, Broadcaster>>;

#[derive(Default)]
pub struct BroadcasterRegistry {
    inner: Arc<BroadcasterRegistryInner>,
}

impl BroadcasterRegistry {
    pub(crate) fn handle(&self, namespace: NamespaceName) -> BroadcasterHandle {
        BroadcasterHandle {
            namespace: namespace,
            registry: self.inner.clone(),
        }
    }

    pub(crate) fn subscribe(
        &self,
        namespace: NamespaceName,
        table: String,
    ) -> BroadcastStream<BroadcastMsg> {
        self.inner
            .lock()
            .entry(namespace.clone())
            .or_insert_with(|| Default::default())
            .subscribe(table)
    }

    pub(crate) fn unsubscribe(&self, namespace: NamespaceName, table: &String) {
        let mut broadcasters = self.inner.lock();
        let remove = broadcasters
            .get(&namespace)
            .map_or(false, |broadcaster| !broadcaster.unsubscribe(table));
        if remove {
            broadcasters.remove(&namespace);
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct BroadcasterHandle {
    namespace: NamespaceName,
    registry: Arc<BroadcasterRegistryInner>,
}

impl BroadcasterHandle {
    pub fn get(&self) -> Option<Broadcaster> {
        self.registry.lock().get(&self.namespace).map(|b| b.clone())
    }

    pub fn active(&self) -> bool {
        self.registry.lock().contains_key(&self.namespace)
    }

    pub fn handle(&self, namespace: NamespaceName) -> BroadcasterHandle {
        BroadcasterHandle {
            namespace,
            registry: self.registry.clone(),
        }
    }
}
