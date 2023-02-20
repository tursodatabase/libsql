use crate::database::service::DbFactory;
use anyhow::{Context as _, Result};
use enclose::enclose;
use std::net::SocketAddr;
use std::sync::Arc;

mod conn;
mod proto;
mod session;

struct Server {
    db_factory: Arc<dyn DbFactory>,
}

pub async fn serve(db_factory: Arc<dyn DbFactory>, bind_addr: SocketAddr) -> Result<()> {
    let server = Arc::new(Server { db_factory });

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .context("Could not bind TCP listener")?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Listening for Hrana connections on {}", local_addr);

    let mut join_set = tokio::task::JoinSet::new();
    let mut conn_id = 0;
    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                let (socket, peer_addr) = accept_res
                    .context("Could not accept a TCP connection")?;
                tracing::info!("Accepted connection #{} from {}", conn_id, peer_addr);

                join_set.spawn(enclose!{(server, conn_id) async move {
                    match conn::handle_conn(server, socket, conn_id).await {
                        Ok(_) => tracing::info!("Connection #{} was terminated", conn_id),
                        Err(err) => tracing::error!("Connection #{} failed: {:?}", conn_id, err),
                    }
                }});

                conn_id += 1;
            },
            Some(task_res) = join_set.join_next() => {
                task_res.expect("Hrana connection task failed")
            },
        }
    }
}
