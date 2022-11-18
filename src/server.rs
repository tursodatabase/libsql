use std::net::ToSocketAddrs;

use anyhow::Result;
use message_io::network::{NetEvent, Transport};
use message_io::node;
use tokio::sync::mpsc::UnboundedSender as TokioSender;

use crate::messages::Message;
use crate::scheduler::{Action, ServerMessage};
use crate::statements::Statements;

pub async fn start(
    listen_addr: impl ToSocketAddrs,
    scheduler_sender: TokioSender<ServerMessage>,
) -> Result<()> {
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
                            handler: handler.clone(),
                            // TODO: handle parse error
                            action: Action::Execute(Statements::parse(stmt).unwrap()),
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
                    handler: handler.clone(),
                    action: Action::Disconnect,
                })
                .unwrap();
        }
    });

    tokio::task::spawn_blocking(move || n.wait()).await?;

    Ok(())
}
