use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use hashbrown::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Dns {
    // maps service to hostnames
    hosts: Arc<Mutex<HashMap<String, VecDeque<Uuid>>>>
}

impl Dns {
    pub fn get_host(&self, svc: &str) -> Uuid {
        let mut hosts = self.hosts.lock();
        // get host in a round-robin way
        let q = hosts.get_mut(svc).unwrap();
        let svc = q.pop_back().unwrap();
        q.push_front(svc);
        svc
    }

    pub fn register(&self, svc: String, host: Uuid) {
        self.hosts.lock().entry(svc).or_default().push_front(host);
    }

    pub fn new() -> Self {
        Self { hosts: Default::default() }
    }
}
