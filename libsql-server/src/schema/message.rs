use std::sync::Arc;

use tokio::sync::oneshot;

use crate::connection::program::Program;
use crate::namespace::NamespaceName;

use super::error::Error;
use super::handle::JobHandle;
use super::MigrationJobStatus;

pub enum SchedulerMessage {
    ScheduleMigration {
        schema: NamespaceName,
        migration: Arc<Program>,
        ret: oneshot::Sender<Result<JobHandle, Error>>,
    },
    GetJobStatus {
        job_id: i64,
        ret: oneshot::Sender<Result<(MigrationJobStatus, Option<String>), Error>>,
    },
}
