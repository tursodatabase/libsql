use tokio::sync::mpsc::UnboundedSender as TokioSender;
use tokio::sync::oneshot;

use crate::coordinator::statements::Statements;
use crate::query::QueryResult;
use crate::scheduler::{ClientId, UpdateStateMessage};

#[derive(Debug)]
pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub client_id: ClientId,
    pub responder: oneshot::Sender<QueryResult>,
}
