use std::net::ToSocketAddrs;

use anyhow::Result;
use message_io::network::{Endpoint, NetEvent, Transport};
use message_io::node::{self, NodeHandler};
use tokio::sync::mpsc::UnboundedSender as TokioSender;

use crate::messages::{Message, Responder};
use crate::scheduler::{Action, ServerMessage};
use crate::statements::Statements;

struct MessageIoResponder(NodeHandler<()>, Endpoint);

impl Responder for MessageIoResponder {
    fn respond(&self, message: &Message) {
        let handler = self.0.clone();
        let endpoint = self.1;
        let data = bincode::serialize(&message).unwrap();
        let _ = handler.network().send(endpoint, &data);
    }
}

pub async fn start(
    listen_addr: impl ToSocketAddrs,
    scheduler_sender: TokioSender<ServerMessage>,
) -> Result<tokio::task::JoinHandle<()>> {
    let (handler, listener) = node::split::<()>();
    handler
        .network()
        .listen(Transport::FramedTcp, &listen_addr)?;

    println!(
        "ChiselEdge server running at {:?}",
        listen_addr.to_socket_addrs()?.next()
    );

    let mut n = listener.for_each_async(move |event| match event.network() {
        NetEvent::Connected(_, _) => unreachable!(),
        NetEvent::Accepted(_, _) => (),
        NetEvent::Message(endpoint, input_data) => {
            let message: Message = bincode::deserialize(input_data).unwrap();
            match message {
                Message::Execute(stmt) => {
                    println!(">> {}", stmt);
                    scheduler_sender
                        .send(ServerMessage {
                            endpoint,
                            // TODO: handle parse error
                            action: Action::Execute(Statements::parse(stmt).unwrap()),
                            responder: Box::new(MessageIoResponder(handler.clone(), endpoint)),
                        })
                        .unwrap();
                }
                _ => {
                    todo!();
                }
            }
        }
        NetEvent::Disconnected(endpoint) => {
            scheduler_sender
                .send(ServerMessage {
                    endpoint,
                    action: Action::Disconnect,
                    responder: Box::new(MessageIoResponder(handler.clone(), endpoint)),
                })
                .unwrap();
        }
    });

    let handle = tokio::task::spawn_blocking(move || n.wait());

    Ok(handle)
}
