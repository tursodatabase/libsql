use tokio::sync::oneshot;

use crate::namespace::NamespaceName;
use crate::connection::program::Program;

use super::error::Error;

pub enum SchedulerMessage {
    ScheduleMigration {
        schema: NamespaceName,
        migration: Program,
        ret: oneshot::Sender<Result<i64, Error>>,
    }
}

