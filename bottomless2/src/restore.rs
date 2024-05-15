use chrono::{DateTime, Utc};
use tokio::io::AsyncWrite;

use crate::storage::Storage;
use crate::NamespaceName;
use crate::Result;

/// Restore a Namespace from bottomless
pub struct BottomlessRestore<C> {
    config: C,
    namespace: NamespaceName,
    before: Option<DateTime<Utc>>,
}

impl<C> BottomlessRestore<C> {
    pub fn new(config: C, namespace: NamespaceName, before: Option<DateTime<Utc>>) -> Self {
        Self {
            config,
            namespace,
            before,
        }
    }

    fn restore<S>(self, _storage: S, _dest: impl AsyncWrite) -> Result<()>
    where
        S: Storage<Config = C>,
    {
        todo!()
    }
}
