use message_io::network::Endpoint;
use tokio::sync::mpsc::UnboundedSender as TokioSender;

use crate::messages::Responder;
use crate::scheduler::UpdateStateMessage;
use crate::statements::Statements;

pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub endpoint: Endpoint,
    pub responder: Box<dyn Responder>,
}
