use message_io::network::Endpoint;
use message_io::node::NodeHandler;
use tokio::sync::mpsc::UnboundedSender as TokioSender;

use crate::scheduler::UpdateStateMessage;
use crate::statements::Statements;

pub struct Job {
    pub scheduler_sender: TokioSender<UpdateStateMessage>,
    pub statements: Statements,
    pub endpoint: Endpoint,
    pub handler: NodeHandler<()>,
}
