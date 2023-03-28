use crate::auth::Auth;
use crate::database::service::DbFactory;
use crate::utils::services::idle_shutdown::IdleKicker;
use anyhow::{Context as _, Result};
use enclose::enclose;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub mod batch;
mod conn;
mod handshake;
pub mod proto;
mod session;
pub mod stmt;

struct Server {
    db_factory: Arc<dyn DbFactory>,
    auth: Arc<Auth>,
    idle_kicker: Option<IdleKicker>,
    next_conn_id: AtomicU64,
}

#[derive(Debug)]
pub struct Upgrade {
    pub request: hyper::Request<hyper::Body>,
    pub response_tx: oneshot::Sender<hyper::Response<hyper::Body>>,
}

pub async fn serve(
    db_factory: Arc<dyn DbFactory>,
    auth: Arc<Auth>,
    idle_kicker: Option<IdleKicker>,
    bind_addr: SocketAddr,
    mut upgrade_rx: mpsc::Receiver<Upgrade>,
) -> Result<()> {
    let server = Arc::new(Server {
        db_factory,
        auth,
        idle_kicker,
        next_conn_id: AtomicU64::new(0),
    });

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .context("Could not bind TCP listener")?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Listening for Hrana connections on {}", local_addr);

    let mut join_set = tokio::task::JoinSet::new();
    loop {
        if let Some(kicker) = server.idle_kicker.as_ref() {
            kicker.kick();
        }

        tokio::select! {
            accept_res = listener.accept() => {
                let (socket, peer_addr) = accept_res
                    .context("Could not accept a TCP connection")?;
                let conn_id = server.next_conn_id.fetch_add(1, Ordering::AcqRel);
                tracing::info!("Received TCP connection #{} from {}", conn_id, peer_addr);

                join_set.spawn(enclose!{(server, conn_id) async move {
                    match conn::handle_tcp(server, socket, conn_id).await {
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
            Some(task_res) = join_set.join_next() => {
                task_res.expect("Hrana connection task failed")
            },
        }
    }
}
