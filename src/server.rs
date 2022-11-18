use std::cell::RefCell;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::rc::Rc;

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

/// A self recycling id
#[derive(Debug)]
struct Id {
    val: u32,
    pool: Rc<RefCell<Vec<u32>>>,
}

impl Drop for Id {
    fn drop(&mut self) {
        dbg!(self.val);
        self.pool.borrow_mut().push(self.val);
    }
}

impl Deref for Id {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

#[derive(Default, Debug)]
struct IdPool {
    pool: Rc<RefCell<Vec<u32>>>,
    current: u32,
}

impl IdPool {
    fn get(&mut self) -> Id {
        dbg!(&self.pool);
        let val = match self.pool.borrow_mut().pop() {
            Some(val) => val,
            None => {
                let val = self.current;
                self.current += 1;
                val
            }
        };

        Id {
            val,
            pool: self.pool.clone(),
        }
    }
}

#[derive(Default, Debug)]
pub struct Server {
    /// Map endpoint to an index
    id_pool: IdPool,
    connections: HashMap<Endpoint, Id>,
}

impl Server {
    pub fn start(
        mut self,
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

        listener.for_each(|event| match event.network() {
            NetEvent::Connected(_, _) => unreachable!(),
            NetEvent::Accepted(endpoint, _) => {
                let id = self.id_pool.get();
                self.connections.insert(endpoint, id);
            }
            NetEvent::Message(endpoint, input_data) => {
                let message: Message = bincode::deserialize(input_data).unwrap();
                match message {
                    Message::Execute(stmt) => {
                        if let Some(client_id) = self.connections.get(&endpoint) {
                            println!(">> {}", stmt);

                            scheduler_sender
                                .send(ServerMessage {
                                    client_id: **client_id,
                                    // TODO: handle parse error
                                    action: Action::Execute(Statements::parse(stmt).unwrap()),
                                    responder: Box::new(MessageIoResponder(
                                        handler.clone(),
                                        endpoint,
                                    )),
                                })
                                .unwrap();
                        } else {
                            log::warn!("Unknown client: {endpoint:?}");
                        }
                    }
                    _ => {
                        todo!();
                    }
                }
            }
            NetEvent::Disconnected(endpoint) => {
                if let Some(client_id) = self.connections.remove(&endpoint) {
                    scheduler_sender
                        .send(ServerMessage {
                            client_id: *client_id,
                            action: Action::Disconnect,
                            responder: Box::new(MessageIoResponder(handler.clone(), endpoint)),
                        })
                        .unwrap();
                } else {
                    log::warn!("unkown client disconnected: {endpoint:?}");
                }
            }
        });

        Ok(())
    }
}
