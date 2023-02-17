use crate::database::service::DbFactory;
use anyhow::{Context as _, Result, bail};
use enclose::enclose;
use std::{fs, str};
use std::net::SocketAddr;
use std::path::Path;
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
    let server = Arc::new(Server { db_factory, jwt_key });

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

pub fn load_jwt_key(path: &Path) -> Result<jsonwebtoken::DecodingKey> {
    let data = fs::read(path)?;
    if data.starts_with(b"-----BEGIN PUBLIC KEY-----") {
        jsonwebtoken::DecodingKey::from_ed_pem(&data)
            .context("Could not decode Ed25519 public key from PEM")
    } else if data.starts_with(b"-----BEGIN PRIVATE KEY-----") {
        bail!("Received a private key, but a public key is expected")
    } else if data.starts_with(b"-----BEGIN") {
        bail!("Key is in unsupported PEM format")
    } else if let Ok(data_str) = str::from_utf8(&data) {
        jsonwebtoken::DecodingKey::from_ed_components(&data_str)
            .context("Could not decode Ed25519 public key from base64")
    } else {
        bail!("Key is in an unsupported binary format")
    }
}
