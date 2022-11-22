use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::rc::Rc;
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc;

use crate::messages::{Message, Responder};
use crate::net::NetworkManager;
use crate::scheduler::{Action, ServerMessage};
use crate::statements::Statements;

/// Do Responder that does nothing
struct SinkResponder;

impl Responder for SinkResponder {
    fn respond(&self, _: Message) {}
}

#[derive(Clone)]
struct AsyncServerResponder(mpsc::UnboundedSender<Message>);

impl Responder for AsyncServerResponder {
    fn respond(&self, message: Message) {
        let _ = self.0.send(message);
    }
}

pub async fn start(
    listen_addr: impl ToSocketAddrs,
    scheduler_sender: mpsc::UnboundedSender<ServerMessage>,
) -> Result<()> {
    let mut handles = FuturesUnordered::new();
    let scheduler_sender_clone = scheduler_sender.clone();
    let on_message = Box::new(move |msg, sender, client_id| {
        match msg {
            Message::Execute(stmts) => {
                let message = ServerMessage {
                    client_id,
                    responder: Box::new(AsyncServerResponder(sender)),
                    action: Action::Execute(Statements::parse(stmts).unwrap()),
                };

                scheduler_sender_clone.send(message)?;
            }
            Message::ResultSet(_) => (),
            Message::Error(_, _) => (),
        }

        Ok(())
    });
    let scheduler_sender_clone = scheduler_sender.clone();
    let on_disconnect = Rc::new(move |client_id| {
        let _ = scheduler_sender_clone.send(ServerMessage {
            client_id,
            action: Action::Disconnect,
            // there isn't much to respond to anyways...
            responder: Box::new(SinkResponder),
        });
    });

    let mut listener = NetworkManager::listen(listen_addr, on_message, on_disconnect).await?;

    loop {
        tokio::select! {
            Some(Ok(conn)) = listener.next() => {
                handles.push(conn.run());
            }
            _ = &mut handles.next() => (),
            else => break,
        }
    }

    Ok(())
}
