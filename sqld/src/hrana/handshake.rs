use anyhow::{anyhow, bail, Context as _, Result};
use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite;
use tungstenite::http;

use super::Upgrade;

#[derive(Debug, Copy, Clone)]
pub enum Protocol {
    Hrana1,
}

#[derive(Debug)]
pub enum WebSocket {
    Tcp(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>),
    Upgraded(tokio_tungstenite::WebSocketStream<hyper::upgrade::Upgraded>),
}

pub async fn handshake_tcp(socket: tokio::net::TcpStream) -> Result<WebSocket> {
    let callback = |req: &http::Request<()>, resp: http::Response<()>| {
        let (mut resp_parts, _) = resp.into_parts();
        resp_parts
            .headers
            .insert("server", http::HeaderValue::from_static("sqld-hrana-tcp"));

        match negotiate_protocol(req.headers(), &mut resp_parts.headers) {
            Ok(_protocol) => Ok(http::Response::from_parts(resp_parts, ())),
            Err(resp_body) => Err(http::Response::from_parts(resp_parts, Some(resp_body))),
        }
    };

    let ws_config = Some(get_ws_config());
    let stream =
        tokio_tungstenite::accept_hdr_async_with_config(socket, callback, ws_config).await?;
    Ok(WebSocket::Tcp(stream))
}

pub async fn handshake_upgrade(upgrade: Upgrade) -> Result<WebSocket> {
    let mut req = upgrade.request;

    let ws_config = Some(get_ws_config());
    let (mut resp, stream_fut_res) = match hyper_tungstenite::upgrade(&mut req, ws_config) {
        Ok((mut resp, stream_fut)) => match negotiate_protocol(req.headers(), resp.headers_mut()) {
            Ok(_protocol) => (resp, Ok(stream_fut)),
            Err(msg) => {
                *resp.status_mut() = http::StatusCode::BAD_REQUEST;
                *resp.body_mut() = hyper::Body::from(msg.clone());
                (
                    resp,
                    Err(anyhow!("Could not negotiate subprotocol: {}", msg)),
                )
            }
        },
        Err(err) => {
            let resp = http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(hyper::Body::from(format!("{err}")))
                .unwrap();
            (
                resp,
                Err(anyhow!(err).context("Protocol error in HTTP upgrade")),
            )
        }
    };

    resp.headers_mut().insert(
        "server",
        http::HeaderValue::from_static("sqld-hrana-upgrade"),
    );
    if upgrade.response_tx.send(resp).is_err() {
        bail!("Could not send the HTTP upgrade response")
    }

    let stream_fut = stream_fut_res?;
    let stream = stream_fut
        .await
        .context("Could not upgrade HTTP request to a WebSocket")?;
    Ok(WebSocket::Upgraded(stream))
}

fn negotiate_protocol(
    req_headers: &http::HeaderMap,
    resp_headers: &mut http::HeaderMap,
) -> Result<Protocol, String> {
    if let Some(protocol_hdr) = req_headers.get("sec-websocket-protocol") {
        let has_hrana1 = protocol_hdr
            .to_str()
            .unwrap_or("")
            .split(',')
            .any(|p| p.trim() == "hrana1");
        if has_hrana1 {
            resp_headers.append(
                "sec-websocket-protocol",
                http::HeaderValue::from_static("hrana1"),
            );
            Ok(Protocol::Hrana1)
        } else {
            Err("Only the 'hrana1' subprotocol is supported".into())
        }
    } else {
        // Sec-WebSocket-Protocol header not present, assume that the client wants hrana1
        // According to RFC 6455, we must not set the Sec-WebSocket-Protocol response header
        Ok(Protocol::Hrana1)
    }
}

fn get_ws_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig {
        max_send_queue: Some(1 << 20),
        ..Default::default()
    }
}

impl WebSocket {
    pub async fn recv(&mut self) -> Option<tungstenite::Result<tungstenite::Message>> {
        match self {
            Self::Tcp(stream) => stream.next().await,
            Self::Upgraded(stream) => stream.next().await,
        }
    }

    pub async fn send(&mut self, msg: tungstenite::Message) -> tungstenite::Result<()> {
        match self {
            Self::Tcp(stream) => stream.send(msg).await,
            Self::Upgraded(stream) => stream.send(msg).await,
        }
    }
}
