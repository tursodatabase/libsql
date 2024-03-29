use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{bail, Context as _, Result};
use futures::stream::FuturesUnordered;
use futures::{ready, FutureExt as _, StreamExt as _};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite;
use tungstenite::protocol::frame::coding::CloseCode;

use crate::namespace::NamespaceName;

use super::super::{Encoding, ProtocolError, Version};
use super::handshake::WebSocket;
use super::{handshake, proto, session, Server, Upgrade};

/// State of a Hrana connection.
struct Conn {
    conn_id: u64,
    server: Arc<Server>,
    ws: WebSocket,
    ws_closed: bool,
    /// The version of the protocol that has been negotiated in the WebSocket handshake.
    version: Version,
    /// The encoding of messages that has been negotiated in the WebSocket handshake.
    encoding: Encoding,
    /// After a successful authentication, this contains the session-level state of the connection.
    session: Option<session::Session>,
    /// Join set for all tasks that were spawned to handle the connection.
    join_set: tokio::task::JoinSet<()>,
    /// Future responses to requests that we have received but are evaluating asynchronously.
    responses: FuturesUnordered<ResponseFuture>,
    /// Namespace queried by this connections
    namespace: NamespaceName,
}

/// A `Future` that stores a handle to a future response to request which is being evaluated
/// asynchronously.
struct ResponseFuture {
    /// The request id, which must be included in the response.
    request_id: i32,
    /// The future that will be resolved with the response.
    response_rx: futures::future::Fuse<oneshot::Receiver<Result<proto::Response>>>,
}

pub(super) async fn handle_tcp(
    server: Arc<Server>,
    socket: Box<dyn crate::net::Conn>,
    conn_id: u64,
) -> Result<()> {
    let handshake::Output {
        ws,
        version,
        encoding,
        namespace,
    } = handshake::handshake_tcp(
        socket,
        server.disable_default_namespace,
        server.disable_namespaces,
    )
    .await
    .context("Could not perform the WebSocket handshake on TCP connection")?;
    handle_ws(server, ws, version, encoding, conn_id, namespace).await
}

pub(super) async fn handle_upgrade(
    server: Arc<Server>,
    upgrade: Upgrade,
    conn_id: u64,
) -> Result<()> {
    let handshake::Output {
        ws,
        version,
        encoding,
        namespace,
    } = handshake::handshake_upgrade(
        upgrade,
        server.disable_default_namespace,
        server.disable_namespaces,
    )
    .await
    .context("Could not perform the WebSocket handshake on HTTP connection")?;
    handle_ws(server, ws, version, encoding, conn_id, namespace).await
}

async fn handle_ws(
    server: Arc<Server>,
    ws: WebSocket,
    version: Version,
    encoding: Encoding,
    conn_id: u64,
    namespace: NamespaceName,
) -> Result<()> {
    let mut conn = Conn {
        conn_id,
        server,
        ws,
        ws_closed: false,
        version,
        encoding,
        session: None,
        join_set: tokio::task::JoinSet::new(),
        responses: FuturesUnordered::new(),
        namespace,
    };

    loop {
        tokio::select! {
            Some(client_msg_res) = conn.ws.recv() => {
                let client_msg = client_msg_res
                    .context("Could not receive a WebSocket message")?;
                match handle_msg(&mut conn, client_msg).await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(err) => {
                        match err.downcast::<ProtocolError>() {
                            Ok(proto_err) => {
                                tracing::warn!(
                                    "Connection #{} terminated due to protocol error: {}",
                                    conn.conn_id,
                                    proto_err,
                                );
                                let close_code = protocol_error_to_close_code(&proto_err);
                                close(&mut conn, close_code, proto_err.to_string()).await;
                                return Ok(())
                            }
                            Err(err) => {
                                close(&mut conn, CloseCode::Error, "Internal server error".into()).await;
                                return Err(err);
                            }
                        }
                    }
                }
            },
            Some(task_res) = conn.join_set.join_next() => {
                task_res.expect("Connection subtask failed")
            },
            Some(response_res) = conn.responses.next() => {
                let response_msg = response_res?;
                send_msg(&mut conn, &response_msg).await?;
            },
            else => break,
        }

        if let Some(kicker) = conn.server.idle_kicker.as_ref() {
            kicker.kick();
        }
    }

    close(
        &mut conn,
        CloseCode::Normal,
        "Thank you for using sqld".into(),
    )
    .await;
    Ok(())
}

async fn handle_msg(conn: &mut Conn, client_msg: tungstenite::Message) -> Result<bool> {
    match client_msg {
        tungstenite::Message::Text(client_msg) => {
            if conn.encoding != Encoding::Json {
                bail!(ProtocolError::TextWebSocketMessage)
            }

            let client_msg: proto::ClientMsg = serde_json::from_str(&client_msg)
                .map_err(|err| ProtocolError::JsonDeserialize { source: err })?;
            handle_client_msg(conn, client_msg).await
        }
        tungstenite::Message::Binary(client_msg) => {
            if conn.encoding != Encoding::Protobuf {
                bail!(ProtocolError::BinaryWebSocketMessage)
            }

            let client_msg = <proto::ClientMsg as prost::Message>::decode(client_msg.as_slice())
                .map_err(|err| ProtocolError::ProtobufDecode { source: err })?;
            handle_client_msg(conn, client_msg).await
        }
        tungstenite::Message::Ping(ping_data) => {
            let pong_msg = tungstenite::Message::Pong(ping_data);
            conn.ws
                .send(pong_msg)
                .await
                .context("Could not send pong to the WebSocket")?;
            Ok(true)
        }
        tungstenite::Message::Pong(_) => Ok(true),
        tungstenite::Message::Close(_) => Ok(false),
        tungstenite::Message::Frame(_) => panic!("Received a tungstenite::Message::Frame"),
    }
}

async fn handle_client_msg(conn: &mut Conn, client_msg: proto::ClientMsg) -> Result<bool> {
    tracing::trace!("Received client msg: {:?}", client_msg);
    match client_msg {
        proto::ClientMsg::None => bail!(ProtocolError::NoneClientMsg),
        proto::ClientMsg::Hello(msg) => handle_hello_msg(conn, msg.jwt).await,
        proto::ClientMsg::Request(msg) => match msg.request {
            Some(request) => handle_request_msg(conn, msg.request_id, request).await,
            None => bail!(ProtocolError::NoneRequest),
        },
    }
}

async fn handle_hello_msg(conn: &mut Conn, jwt: Option<String>) -> Result<bool> {
    let auth = session::handle_hello(&conn.server, jwt, conn.namespace.clone()).await;

    let hello_res = auth
        .map(|a| {
            if let Some(sess) = conn.session.as_mut() {
                sess.update_auth(a)
            } else {
                conn.session = Some(session::Session::new(a, conn.version));
                Ok(())
            }
        })
        .and_then(|o| o);

    match hello_res {
        Ok(_) => {
            send_msg(conn, &proto::ServerMsg::HelloOk(proto::HelloOkMsg {})).await?;
            Ok(true)
        }
        Err(err) => match downcast_error(err) {
            Ok(error) => {
                send_msg(
                    conn,
                    &proto::ServerMsg::HelloError(proto::HelloErrorMsg { error }),
                )
                .await?;
                Ok(false)
            }
            Err(err) => Err(err),
        },
    }
}

async fn handle_request_msg(
    conn: &mut Conn,
    request_id: i32,
    request: proto::Request,
) -> Result<bool> {
    let Some(session) = conn.session.as_mut() else {
        bail!(ProtocolError::RequestBeforeHello)
    };

    let response_rx = session::handle_request(
        &conn.server,
        session,
        &mut conn.join_set,
        request,
        conn.namespace.clone(),
    )
    .await
    .unwrap_or_else(|err| {
        // we got an error immediately, but let's treat it as a special case of the general
        // flow
        let (tx, rx) = oneshot::channel();
        tx.send(Err(err)).unwrap();
        rx
    });

    conn.responses.push(ResponseFuture {
        request_id,
        response_rx: response_rx.fuse(),
    });
    Ok(true)
}

impl Future for ResponseFuture {
    type Output = Result<proto::ServerMsg>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match ready!(Pin::new(&mut self.response_rx).poll(cx)) {
            Ok(Ok(response)) => {
                Poll::Ready(Ok(proto::ServerMsg::ResponseOk(proto::ResponseOkMsg {
                    request_id: self.request_id,
                    response: Some(response),
                })))
            }
            Ok(Err(err)) => match downcast_error(err) {
                Ok(error) => Poll::Ready(Ok(proto::ServerMsg::ResponseError(
                    proto::ResponseErrorMsg {
                        request_id: self.request_id,
                        error,
                    },
                ))),
                Err(err) => Poll::Ready(Err(err)),
            },
            Err(_recv_err) => {
                // do not propagate this error, because the error that caused the receiver to drop
                // is very likely propagating from another task at this moment, and we don't want
                // to hide it.
                // this is also the reason why we need to use `Fuse` in self.response_rx
                tracing::warn!("Response sender was dropped");
                Poll::Pending
            }
        }
    }
}

fn downcast_error(err: anyhow::Error) -> Result<proto::Error> {
    match err.downcast_ref::<session::ResponseError>() {
        Some(error) => Ok(proto::Error {
            message: error.to_string(),
            code: error.code().into(),
        }),
        None => Err(err),
    }
}

async fn send_msg(conn: &mut Conn, msg: &proto::ServerMsg) -> Result<()> {
    let msg = match conn.encoding {
        Encoding::Json => {
            let msg =
                serde_json::to_string(&msg).context("Could not serialize response message")?;
            tungstenite::Message::Text(msg)
        }
        Encoding::Protobuf => {
            let msg = <proto::ServerMsg as prost::Message>::encode_to_vec(msg);
            tungstenite::Message::Binary(msg)
        }
    };
    conn.ws
        .send(msg)
        .await
        .context("Could not send message to the WebSocket")
}

async fn close(conn: &mut Conn, code: CloseCode, reason: String) {
    if conn.ws_closed {
        return;
    }

    let close_frame = tungstenite::protocol::frame::CloseFrame {
        code,
        reason: Cow::Owned(reason),
    };
    if let Err(err) = conn
        .ws
        .send(tungstenite::Message::Close(Some(close_frame)))
        .await
    {
        if !matches!(
            err,
            tungstenite::Error::AlreadyClosed | tungstenite::Error::ConnectionClosed
        ) {
            tracing::warn!(
                "Could not send close frame to WebSocket of connection #{}: {:?}",
                conn.conn_id,
                err
            );
        }
    }

    conn.ws_closed = true;
}

fn protocol_error_to_close_code(err: &ProtocolError) -> CloseCode {
    match err {
        ProtocolError::JsonDeserialize { .. } => CloseCode::Invalid,
        ProtocolError::ProtobufDecode { .. } => CloseCode::Invalid,
        ProtocolError::BinaryWebSocketMessage => CloseCode::Unsupported,
        ProtocolError::TextWebSocketMessage => CloseCode::Unsupported,
        _ => CloseCode::Policy,
    }
}
