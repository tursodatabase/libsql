use std::sync::Arc;

use tokio::sync::{broadcast::Receiver, mpsc, oneshot};

use crate::connection::program::Program;

use super::{error::Error, MigrationJobStatus, SchedulerMessage};

#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::Sender<SchedulerMessage>,
}

impl From<mpsc::Sender<SchedulerMessage>> for SchedulerHandle {
    fn from(sender: mpsc::Sender<SchedulerMessage>) -> Self {
        Self { sender }
    }
}

pub struct JobHandle {
    job_id: i64,
    notifier: Receiver<(i64, MigrationJobStatus)>,
}

impl JobHandle {
    pub(crate) fn new(job_id: i64, notifier: Receiver<(i64, MigrationJobStatus)>) -> JobHandle {
        Self { job_id, notifier }
    }

    pub async fn wait_for(&mut self, f: impl Fn(MigrationJobStatus) -> bool) {
        while let Ok(next) = self.notifier.recv().await {
            if next.0 == self.job_id && f(next.1) {
                return;
            }
        }
    }

    pub(crate) fn job_id(&self) -> i64 {
        self.job_id
    }
}

impl SchedulerHandle {
    pub(crate) async fn register_migration_task(
        &self,
        schema: crate::namespace::NamespaceName,
        migration: Arc<Program>,
    ) -> Result<JobHandle, Error> {
        let (ret, rcv) = oneshot::channel();
        let msg = SchedulerMessage::ScheduleMigration {
            schema,
            migration,
            ret,
        };
        self.sender
            .send(msg)
            .await
            .map_err(|_| Error::SchedulerExited)?;
        rcv.await.unwrap()
    }

    pub(crate) async fn get_job_status(
        &self,
        job_id: i64,
    ) -> Result<(MigrationJobStatus, Option<String>), Error> {
        let (ret, rcv) = oneshot::channel();
        let msg = SchedulerMessage::GetJobStatus { job_id, ret };
        self.sender
            .send(msg)
            .await
            .map_err(|_| Error::SchedulerExited)?;
        rcv.await.unwrap()
    }
}
