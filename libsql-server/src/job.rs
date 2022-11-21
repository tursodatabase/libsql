use std::fmt;

use tokio::sync::mpsc::UnboundedSender as TokioSender;

use crate::messages::Responder;
use crate::scheduler::{ClientId, UpdateStateMessage};
use crate::statements::Statements;

pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub client_id: ClientId,
    pub responder: Box<dyn Responder>,
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Job")
            .field("scheduler_sender", &self.scheduler_sender)
            .field("statements", &self.statements)
            .field("client_id", &self.client_id)
            .finish()
    }
}
