use tokio::sync::mpsc::UnboundedSender as TokioSender;
use tokio::sync::oneshot;

use crate::query::QueryResult;
use crate::scheduler::{ClientId, UpdateStateMessage};
use crate::statements::Statements;

#[derive(Debug)]
pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub client_id: ClientId,
    pub responder: oneshot::Sender<QueryResult>,
}
