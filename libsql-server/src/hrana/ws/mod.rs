use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use enclose::enclose;
use tokio::pin;
use tokio::sync::{mpsc, oneshot};

use crate::auth::Auth;
use crate::namespace::NamespaceStore;
use crate::net::Conn;
use crate::utils::services::idle_shutdown::IdleKicker;

pub mod proto;

mod conn;
mod handshake;
mod protobuf;
mod session;

struct Server {
    namespaces: NamespaceStore,
    user_auth_strategy: Auth,
    idle_kicker: Option<IdleKicker>,
    max_response_size: u64,
    next_conn_id: AtomicU64,
    disable_default_namespace: bool,
    disable_namespaces: bool,
}

pub struct Accept {
    pub socket: Box<dyn Conn>,
    pub peer_addr: SocketAddr,
}

#[derive(Debug)]
pub struct Upgrade {
    pub request: hyper::Request<hyper::Body>,
    pub response_tx: oneshot::Sender<hyper::Response<hyper::Body>>,
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    user_auth_strategy: Auth,
    idle_kicker: Option<IdleKicker>,
    max_response_size: u64,
    mut accept_rx: mpsc::Receiver<Accept>,
    mut upgrade_rx: mpsc::Receiver<Upgrade>,
    namespaces: NamespaceStore,
    disable_default_namespace: bool,
    disable_namespaces: bool,
) -> Result<()> {
    let server = Arc::new(Server {
        user_auth_strategy,
        idle_kicker,
        max_response_size,
        next_conn_id: AtomicU64::new(0),
        namespaces,
        disable_default_namespace,
        disable_namespaces,
    });

    let mut join_set = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            Some(accept) = accept_rx.recv() => {
                let conn_id = server.next_conn_id.fetch_add(1, Ordering::AcqRel);
                tracing::info!("Received TCP connection #{} from {}", conn_id, accept.peer_addr);

                join_set.spawn(enclose!{(server, conn_id) async move {
                    match conn::handle_tcp(server, accept.socket, conn_id).await {
                        Ok(_) => tracing::info!("TCP connection #{} was terminated", conn_id),
                        Err(err) => tracing::error!("TCP connection #{} failed: {:?}", conn_id, err),
                    }
                }});
            },
            Some(upgrade) = upgrade_rx.recv() => {
                let conn_id = server.next_conn_id.fetch_add(1, Ordering::AcqRel);
                tracing::info!("Received HTTP upgrade connection #{}", conn_id);

                join_set.spawn(enclose!{(server, conn_id) async move {
                    match conn::handle_upgrade(server, upgrade, conn_id).await {
                        Ok(_) => tracing::info!("HTTP upgrade connection #{} was terminated", conn_id),
                        Err(err) => tracing::error!("HTTP upgrade connection #{} failed: {:?}", conn_id, err),
                    }
                }});
            },
            Some(task_res) = join_set.join_next(), if !join_set.is_empty() => {
                task_res.expect("Hrana connection task failed")
            },
            else => {
                tracing::error!("hrana server loop exited");
                return Ok(())
            }
        }

        if let Some(kicker) = server.idle_kicker.as_ref() {
            kicker.kick();
        }
    }
}

pub async fn listen<A>(acceptor: A, accept_tx: mpsc::Sender<Accept>)
where
    A: crate::net::Accept,
{
    pin!(acceptor);

    while let Some(maybe_conn) = poll_fn(|cx| acceptor.as_mut().poll_accept(cx)).await {
        match maybe_conn {
            Ok(conn) => {
                let Some(peer_addr) = conn.connect_info().remote_addr() else {
                    tracing::error!("connection missing remote addr");
                    continue;
                };
                let socket: Box<dyn Conn> = Box::new(conn);
                let _: Result<_, _> = accept_tx.send(Accept { socket, peer_addr }).await;
            }
            Err(e) => {
                tracing::error!("error handling incoming hrana ws connection: {e}");
            }
        }
    }
}
