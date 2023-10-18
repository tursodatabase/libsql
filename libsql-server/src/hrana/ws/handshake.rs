use anyhow::{anyhow, bail, Context as _, Result};
use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite;
use tungstenite::http;

use crate::http::user::db_factory::namespace_from_headers;
use crate::namespace::NamespaceName;
use crate::net::Conn;

use super::super::{Encoding, Version};
use super::Upgrade;

pub enum WebSocket {
    Tcp(tokio_tungstenite::WebSocketStream<Box<dyn Conn>>),
    Upgraded(tokio_tungstenite::WebSocketStream<hyper::upgrade::Upgraded>),
}

#[derive(Debug, Copy, Clone)]
enum Subproto {
    Hrana1,
    Hrana2,
    Hrana3,
    Hrana3Protobuf,
}

pub struct Output {
    pub ws: WebSocket,
    pub version: Version,
    pub encoding: Encoding,
    pub namespace: NamespaceName,
}

pub async fn handshake_tcp(
    socket: Box<dyn Conn>,
    disable_default_ns: bool,
    disable_namespaces: bool,
) -> Result<Output> {
    let mut subproto = None;
    let mut namespace = None;
    let callback = |req: &http::Request<()>, resp: http::Response<()>| {
        let (mut resp_parts, _) = resp.into_parts();
        resp_parts
            .headers
            .insert("server", http::HeaderValue::from_static("sqld-hrana-tcp"));

        namespace =
            match namespace_from_headers(req.headers(), disable_default_ns, disable_namespaces) {
                Ok(ns) => Some(ns),
                Err(e) => return Err(http::Response::from_parts(resp_parts, Some(e.to_string()))),
            };

        match negotiate_subproto(req.headers(), &mut resp_parts.headers) {
            Ok(subproto_) => {
                subproto = Some(subproto_);
                Ok(http::Response::from_parts(resp_parts, ()))
            }
            Err(resp_body) => Err(http::Response::from_parts(resp_parts, Some(resp_body))),
        }
    };

    let ws_config = Some(get_ws_config());
    let stream =
        tokio_tungstenite::accept_hdr_async_with_config(socket, callback, ws_config).await?;

    let (version, encoding) = subproto.unwrap().version_encoding();
    Ok(Output {
        ws: WebSocket::Tcp(stream),
        version,
        encoding,
        namespace: namespace.unwrap(),
    })
}

pub async fn handshake_upgrade(
    upgrade: Upgrade,
    disable_default_ns: bool,
    disable_namespaces: bool,
) -> Result<Output> {
    let mut req = upgrade.request;

    let namespace = namespace_from_headers(req.headers(), disable_default_ns, disable_namespaces)?;
    let ws_config = Some(get_ws_config());
    let (mut resp, stream_fut_subproto_res) = match hyper_tungstenite::upgrade(&mut req, ws_config)
    {
        Ok((mut resp, stream_fut)) => match negotiate_subproto(req.headers(), resp.headers_mut()) {
            Ok(subproto) => (resp, Ok((stream_fut, subproto))),
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

    let (stream_fut, subproto) = stream_fut_subproto_res?;
    let stream = stream_fut
        .await
        .context("Could not upgrade HTTP request to a WebSocket")?;

    let (version, encoding) = subproto.version_encoding();
    Ok(Output {
        ws: WebSocket::Upgraded(stream),
        version,
        encoding,
        namespace,
    })
}

fn negotiate_subproto(
    req_headers: &http::HeaderMap,
    resp_headers: &mut http::HeaderMap,
) -> Result<Subproto, String> {
    if let Some(protocol_hdr) = req_headers.get("sec-websocket-protocol") {
        let client_subprotos = protocol_hdr
            .to_str()
            .unwrap_or("")
            .split(',')
            .map(|p| p.trim())
            .collect::<Vec<_>>();

        let server_subprotos = [
            Subproto::Hrana3Protobuf,
            Subproto::Hrana3,
            Subproto::Hrana2,
            Subproto::Hrana1,
        ];

        let Some(subproto) = select_subproto(&client_subprotos, &server_subprotos) else {
            let supported = server_subprotos
                .iter()
                .copied()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            return Err(format!(
                "Only these WebSocket subprotocols are supported: {}",
                supported
            ));
        };

        tracing::debug!(
            "Client subprotocols {:?}, selected {:?}",
            client_subprotos,
            subproto
        );

        resp_headers.append(
            "sec-websocket-protocol",
            http::HeaderValue::from_str(subproto.as_str()).unwrap(),
        );
        Ok(subproto)
    } else {
        // Sec-WebSocket-Protocol header not present, assume that the client wants hrana1
        // According to RFC 6455, we must not set the Sec-WebSocket-Protocol response header
        Ok(Subproto::Hrana1)
    }
}

fn select_subproto(client_subprotos: &[&str], server_subprotos: &[Subproto]) -> Option<Subproto> {
    for &server_subproto in server_subprotos.iter() {
        for client_subproto in client_subprotos.iter() {
            if client_subproto.eq_ignore_ascii_case(server_subproto.as_str()) {
                return Some(server_subproto);
            }
        }
    }
    None
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

impl Subproto {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hrana1 => "hrana1",
            Self::Hrana2 => "hrana2",
            Self::Hrana3 => "hrana3",
            Self::Hrana3Protobuf => "hrana3-protobuf",
        }
    }

    fn version_encoding(self) -> (Version, Encoding) {
        match self {
            Self::Hrana1 => (Version::Hrana1, Encoding::Json),
            Self::Hrana2 => (Version::Hrana2, Encoding::Json),
            Self::Hrana3 => (Version::Hrana3, Encoding::Json),
            Self::Hrana3Protobuf => (Version::Hrana3, Encoding::Protobuf),
        }
    }
}
