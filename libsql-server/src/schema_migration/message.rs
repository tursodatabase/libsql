use tokio::sync::oneshot;

use crate::connection::program::Program;
use crate::namespace::NamespaceName;

use super::error::Error;

pub enum SchedulerMessage {
    ScheduleMigration {
        schema: NamespaceName,
        migration: Program,
        ret: oneshot::Sender<Result<i64, Error>>,
    },
}
