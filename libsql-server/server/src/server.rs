use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
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
    let mut listener = NetworkManager::listen(listen_addr).await?;
    let mut ids = 0;

    loop {
        tokio::select! {
            Some(Ok(mut conn)) = listener.next() => {
                let client_id = ids;
                ids += 1;
                let message_sender = conn.sender();
                let scheduler_sender_clone = scheduler_sender.clone();
                conn.set_on_message(move |msg| {
                    match msg {
                        Message::Execute(stmts) => {
                            let message = ServerMessage {
                                client_id,
                                responder: Box::new(AsyncServerResponder(message_sender.clone())),
                                action: Action::Execute(Statements::parse(stmts).unwrap()),
                            };

                            scheduler_sender_clone.send(message)?;
                        },
                        Message::ResultSet(_) => (),
                        Message::Error(_, _) => (),
                    }

                    Ok(())
                });

                let scheduler_sender_clone = scheduler_sender.clone();
                conn.set_on_disconnect(move || {
                    let _ = scheduler_sender_clone.send(ServerMessage {
                        client_id,
                        action: Action::Disconnect,
                        // there isn't much to respond to anyways...
                        responder: Box::new(SinkResponder),
                    });
                });

                handles.push(conn.run());
            }
            _ = &mut handles.next() => (),
            else => break,
        }
    }

    Ok(())
}
