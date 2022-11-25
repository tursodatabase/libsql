use tokio::sync::mpsc::UnboundedSender as TokioSender;
use tokio::sync::oneshot;

use crate::coordinator::query::QueryResult;
use crate::coordinator::scheduler::{ClientId, UpdateStateMessage};
use crate::coordinator::statements::Statements;

#[derive(Debug)]
pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub client_id: ClientId,
    pub responder: oneshot::Sender<QueryResult>,
}
