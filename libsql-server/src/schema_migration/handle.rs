use tokio::sync::{mpsc, oneshot};

use crate::connection::program::Program;

use super::{SchedulerMessage, error::Error};

#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::Sender<SchedulerMessage>,
}

impl From<mpsc::Sender<SchedulerMessage>> for SchedulerHandle {
    fn from(sender: mpsc::Sender<SchedulerMessage>) -> Self {
        Self { sender }
    }
}

impl SchedulerHandle {
    pub(crate) async fn register_migration_task(&self, schema: crate::namespace::NamespaceName, migration: Program) -> Result<i64, Error> {
        let (ret, rcv) = oneshot::channel();
        let msg = SchedulerMessage::ScheduleMigration {
            schema,
            migration,
            ret,
        };
        self.sender.send(msg).await.map_err(|_| Error::SchedulerExited)?;
        rcv.await.unwrap()
    } 
}
