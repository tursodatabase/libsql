use crate::database::service::DbFactory;
use anyhow::{bail, Context as _, Result};
use enclose::enclose;
use std::net::SocketAddr;
use std::sync::Arc;

mod conn;
mod proto;
mod session;

struct Server {
    db_factory: Arc<dyn DbFactory>,
    jwt_key: Option<jsonwebtoken::DecodingKey>,
}

pub async fn serve(
    db_factory: Arc<dyn DbFactory>,
    bind_addr: SocketAddr,
    jwt_key: Option<jsonwebtoken::DecodingKey>,
) -> Result<()> {
    let server = Arc::new(Server {
        db_factory,
        jwt_key,
    });

    if server.jwt_key.is_some() {
        tracing::info!("Hrana authentication is enabled");
    } else {
        tracing::warn!("Hrana authentication is disabled, the server is unprotected");
    }

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

pub fn parse_jwt_key(data: &str) -> Result<jsonwebtoken::DecodingKey> {
    if data.starts_with("-----BEGIN PUBLIC KEY-----") {
        jsonwebtoken::DecodingKey::from_ed_pem(data.as_bytes())
            .context("Could not decode Ed25519 public key from PEM")
    } else if data.starts_with("-----BEGIN PRIVATE KEY-----") {
        bail!("Received a private key, but a public key is expected")
    } else if data.starts_with("-----BEGIN") {
        bail!("Key is in unsupported PEM format")
    } else {
        jsonwebtoken::DecodingKey::from_ed_components(data)
            .context("Could not decode Ed25519 public key from base64")
    }
}
