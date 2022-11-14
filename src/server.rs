use crate::coordinator::Coordinator;
use crate::messages::Message;
use anyhow::Result;
use message_io::network::{NetEvent, Transport};
use message_io::node::{self};

pub(crate) fn start() -> Result<()> {
    let (handler, node_listener) = node::split::<()>();
    let listen_addr = "127.0.0.1:5000";
    handler
        .network()
        .listen(Transport::FramedTcp, listen_addr)?;
    let coordinator = Coordinator::start()?;
    println!("ChiselEdge server running at {}", listen_addr);
    node_listener.for_each(move |event| match event.network() {
        NetEvent::Connected(_, _) => unreachable!(),
        NetEvent::Accepted(_, _) => (),
        NetEvent::Message(endpoint, input_data) => {
            let message: Message = bincode::deserialize(input_data).unwrap();
            match message {
                Message::Execute(stmt) => {
                    println!(">> {}", stmt);
                    let message = coordinator.on_execute(endpoint.to_string(), stmt).unwrap();
                    let output_data = bincode::serialize(&message).unwrap();
                    handler.network().send(endpoint, &output_data);
                }
                _ => {
                    todo!();
                }
            }
        }
        NetEvent::Disconnected(endpoint) => {
            coordinator.on_disconnect(endpoint.to_string()).unwrap();
        }
    });
    Ok(())
}
