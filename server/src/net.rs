use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    rc::Rc,
    sync::{Arc, Mutex},
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
use crate::scheduler::ClientId;

pub struct NetworkManager {
    tcp_listener: TcpListener,
    next_client_id: ClientId,
    connected_clients: Arc<Mutex<HashMap<SocketAddr, ClientId>>>,
    query_handler: Arc<QueryHandler>,
    on_disconnect: Rc<dyn Fn(ClientId) + Send + Sync>,
}

impl NetworkManager {
    pub async fn listen(
        addr: impl ToSocketAddrs,
        on_message: Box<
            dyn Fn(Message, mpsc::UnboundedSender<Message>, ClientId) -> Result<()> + Send + Sync,
        >,
        on_disconnect: Rc<dyn Fn(ClientId) + Send + Sync>,
    ) -> Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        log::info!("listening on: {:?}", tcp_listener.local_addr()?);

        let connected_clients = Arc::new(Mutex::new(HashMap::<SocketAddr, ClientId>::new()));

        let query_handler = Arc::new(QueryHandler {
            connected_clients: connected_clients.clone(),
            on_message,
        });

        Ok(Self {
            tcp_listener,
            next_client_id: 0,
            connected_clients,
            query_handler,
            on_disconnect,
        })
    }
}

impl Stream for NetworkManager {
    type Item = Result<Connection>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match ready!(self.tcp_listener.poll_accept(cx)) {
            Ok((stream, addr)) => {
                log::info!("new connection from {addr:?}");
                {
                    let mut map = self.connected_clients.lock().unwrap();
                    let res = map.insert(addr.clone(), self.next_client_id);
                    assert!(res.is_none());
                }
                let con = Connection {
                    addr,
                    client_id: self.next_client_id,
                    connected_clients: self.connected_clients.clone(),
                    stream,
                    on_disconnect: self.on_disconnect.clone(),
                    query_handler: self.query_handler.clone(),
                };
                self.next_client_id += 1;
                Poll::Ready(Some(Ok(con)))
            }
            Err(e) => Poll::Ready(Some(Err(e.into()))),
        }
    }
}

struct QueryHandler {
    connected_clients: Arc<Mutex<HashMap<SocketAddr, ClientId>>>,
    on_message:
        Box<dyn Fn(Message, mpsc::UnboundedSender<Message>, ClientId) -> Result<()> + Send + Sync>,
}

#[async_trait]
impl SimpleQueryHandler for QueryHandler {
    async fn do_query<C>(&self, client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let client_id = {
            let id = {
                let map = self.connected_clients.lock().unwrap();
                map.get(&client.socket_addr()).copied()
            };
            if id.is_none() {
                return Err(PgWireError::IoError(Error::new(
                    ErrorKind::Other,
                    "no client id for {client.socket_addr()}",
                )));
            }
            id.unwrap()
        };
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let msg = Message::Execute(query.to_string());
        (self.on_message)(msg, sender, client_id).or(Err(PgWireError::IoError(Error::new(
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
    pub client_id: ClientId,
    connected_clients: Arc<Mutex<HashMap<SocketAddr, ClientId>>>,
    query_handler: Arc<QueryHandler>,
    stream: TcpStream,
    on_disconnect: Rc<dyn Fn(ClientId) + Send + Sync>,
}

impl Connection {
    pub async fn run(self) -> Result<(), std::io::Error> {
        let authenticator = Arc::new(NoopStartupHandler);
        process_socket(
            self.stream,
            None,
            authenticator.clone(),
            self.query_handler.clone(),
            self.query_handler,
        )
        .await?;
        (self.on_disconnect)(self.client_id);
        {
            let mut map = self.connected_clients.lock().unwrap();
            map.remove(&self.addr);
        }
        log::info!("client {} disconnected", self.addr);
        Ok(())
    }
}
