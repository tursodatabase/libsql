use tokio::sync::mpsc;

use crate::connection::program::Program;
use crate::namespace::{NamespaceStore, NamespaceName};

use super::SchedulerMessage;
use super::error::Error;

#[derive(Clone)]
pub struct Scheduler {
    namespace_store: NamespaceStore,
}

impl Scheduler {
    pub async fn run(self, mut receiver: mpsc::Receiver<SchedulerMessage>) {
        while let Some(msg) = receiver.recv().await {
            match msg {
                SchedulerMessage::ScheduleMigration { schema, migration, ret } => {
                    let res = self.register_migration_task(schema, migration).await;
                    let _ = ret.send(res);
                },
            }
        }

        tracing::info!("all scheduler handles dropped: exiting.");
    }

    pub async fn register_migration_task(&self, schema: NamespaceName, migration: Program) -> Result<i64, Error> {
        let job_id = self.namespace_store
            .meta_store()
            .register_schema_migration(schema, migration)
            .await
            .map_err(|e| Error::Registration(Box::new(e)))?;
        Ok(job_id)
    }

    pub(crate) fn new(namespace_store: NamespaceStore) -> Self {
        Self { namespace_store }
    }
}
