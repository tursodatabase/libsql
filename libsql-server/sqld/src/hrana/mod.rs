use crate::auth::Auth;
use crate::database::factory::DbFactory;
use crate::utils::services::idle_shutdown::IdleKicker;
use anyhow::{Context as _, Result};
use enclose::enclose;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub use self::batch::{execute_batch, proto_batch_to_program, BatchError};
pub use self::handshake::Protocol;
pub use self::stmt::{
    describe_stmt, execute_stmt, proto_sql_to_sql, proto_stmt_to_query, StmtError,
};

mod batch;
mod conn;
mod handshake;
pub mod proto;
mod session;
mod stmt;

struct Server {
    db_factory: Arc<dyn DbFactory>,
    auth: Arc<Auth>,
    idle_kicker: Option<IdleKicker>,
    next_conn_id: AtomicU64,
}

#[derive(Debug)]
pub struct Accept {
    pub socket: tokio::net::TcpStream,
    pub peer_addr: SocketAddr,
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
    mut accept_rx: mpsc::Receiver<Accept>,
    mut upgrade_rx: mpsc::Receiver<Upgrade>,
) -> Result<()> {
    let server = Arc::new(Server {
        db_factory,
        auth,
        idle_kicker,
        next_conn_id: AtomicU64::new(0),
    });

    let mut join_set = tokio::task::JoinSet::new();
    loop {
        if let Some(kicker) = server.idle_kicker.as_ref() {
            kicker.kick();
        }

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
            Some(task_res) = join_set.join_next() => {
                task_res.expect("Hrana connection task failed")
            },
        }
    }
}

pub async fn listen(bind_addr: SocketAddr, accept_tx: mpsc::Sender<Accept>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .context("Could not bind TCP listener")?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Listening for Hrana connections on {}", local_addr);

    loop {
        let (socket, peer_addr) = listener
            .accept()
            .await
            .context("Could not accept a TCP connection")?;
        let _: Result<_, _> = accept_tx.send(Accept { socket, peer_addr }).await;
    }
}
