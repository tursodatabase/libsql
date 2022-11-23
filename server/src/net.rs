use std::{
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    task::{ready, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{text_query_response, FieldInfo, Response, TextDataRowEncoder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::mpsc,
};

use crate::messages::Message;

pub struct NetworkManager {
    tcp_listener: TcpListener,
}

impl NetworkManager {
    pub async fn listen(addr: impl ToSocketAddrs) -> Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        log::info!("listening on: {:?}", tcp_listener.local_addr()?);

        Ok(Self { tcp_listener })
    }
}

impl Stream for NetworkManager {
    type Item = Result<Connection>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match ready!(self.tcp_listener.poll_accept(cx)) {
            Ok((stream, addr)) => {
                log::info!("new connection from {addr:?}");
                let con = Connection { addr, stream };
                Poll::Ready(Some(Ok(con)))
            }
            Err(e) => Poll::Ready(Some(Err(e.into()))),
        }
    }
}

struct QueryHandler {
    on_message: Box<dyn Fn(Message, mpsc::UnboundedSender<Message>) -> Result<()> + Send + Sync>,
}

#[async_trait]
impl SimpleQueryHandler for QueryHandler {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let msg = Message::Execute(query.to_string());
        (self.on_message)(msg, sender).or(Err(PgWireError::IoError(Error::new(
            ErrorKind::Other,
            "server error",
        ))))?;
        let res = receiver.recv().await;
        if let Some(msg) = res {
            match msg {
                Message::ResultSet(rows) => {
                    let data_row_stream = stream::iter(rows.into_iter()).map(|r| {
                        let mut encoder = TextDataRowEncoder::new(1);
                        encoder.append_field(Some(&r))?;
                        encoder.finish()
                    });
                    return Ok(vec![Response::Query(text_query_response(
                        vec![FieldInfo::new("row".into(), None, None, Type::VARCHAR)],
                        data_row_stream,
                    ))]);
                }
                Message::Error(_code, msg) => {
                    return Err(PgWireError::IoError(Error::new(ErrorKind::Other, msg)))
                }
                _ => return Ok(vec![]),
            }
        }

        Ok(vec![])
    }
}

#[async_trait]
impl ExtendedQueryHandler for QueryHandler {
    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!()
    }
}

pub struct Connection {
    pub addr: SocketAddr,
    stream: TcpStream,
}

impl Connection {
    pub async fn run(
        self,
        on_message: Box<
            dyn Fn(Message, mpsc::UnboundedSender<Message>) -> Result<()> + Send + Sync,
        >,
        on_disconnect: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<(), std::io::Error> {
        let authenticator = Arc::new(NoopStartupHandler);
        let query_handler = Arc::new(QueryHandler { on_message });
        process_socket(
            self.stream,
            None,
            authenticator.clone(),
            query_handler.clone(),
            query_handler,
        )
        .await?;
        on_disconnect();
        log::info!("client {} disconnected", self.addr);
        Ok(())
    }
}
