use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::PgWireConnectionState;
use pgwire::error::PgWireError;
use pgwire::tokio::PgWireMessageServerCodec;
use pgwire::{api::ClientInfoHolder, messages::PgWireFrontendMessage};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::codec::{Decoder, Framed};
use tower::MakeService;
use tower::Service;

use crate::error::Error;
use crate::postgres::authenticator::PgAuthenticator;
use crate::query::{Queries, QueryResult};
use crate::server::AsyncPeekable;

use super::proto::{peek_for_sslrequest, process_error, QueryHandler};

/// Manages a postgres wire connection.
pub struct PgWireConnection<T, S> {
    socket: Framed<T, PgWireMessageServerCodec>,
    authenticator: Arc<PgAuthenticator>,
    handler: QueryHandler<S>,
}

impl<T, S> PgWireConnection<T, S>
where
    S: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
    T: AsyncRead + AsyncWrite + Unpin + Send + Sync,
    S::Future: Send,
{
    async fn run(&mut self) {
        loop {
            let result = match self.socket.next().await {
                // TODO: handle error correctly
                Some(Ok(msg)) => self.handle_message(msg).await,
                Some(Err(error)) => Err(error),
                None => break,
            };

            match result {
                Ok(true) => (),
                Ok(false) => break,
                Err(e) => {
                    // double error, just exit
                    let Ok(_) = self.handle_error(e).await else { break };
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: PgWireFrontendMessage) -> Result<bool, PgWireError> {
        match self.socket.codec().client_info().state() {
            PgWireConnectionState::AwaitingStartup
            | PgWireConnectionState::AuthenticationInProgress => {
                self.authenticator
                    .authenticate(&mut self.socket, msg)
                    .await?;
            }
            _ => {
                match msg {
                    PgWireFrontendMessage::Query(q) => {
                        self.handler.on_query(&mut self.socket, q).await?;
                    }
                    PgWireFrontendMessage::Parse(p) => {
                        self.handler.on_parse(&mut self.socket, p).await?;
                    }
                    PgWireFrontendMessage::Close(c) => {
                        self.handler.on_close(&mut self.socket, c).await?;
                    }
                    PgWireFrontendMessage::Bind(b) => {
                        self.handler.on_bind(&mut self.socket, b).await?;
                    }
                    PgWireFrontendMessage::Describe(d) => {
                        self.handler.on_describe(&mut self.socket, d).await?;
                    }
                    PgWireFrontendMessage::Execute(e) => {
                        self.handler.on_execute(&mut self.socket, e).await?;
                    }
                    PgWireFrontendMessage::Sync(s) => {
                        self.handler.on_sync(&mut self.socket, s).await?;
                    }
                    PgWireFrontendMessage::Terminate(_) => return Ok(false),
                    // These messages are handled by the connection service on startup.
                    PgWireFrontendMessage::Startup(_)
                    | PgWireFrontendMessage::PasswordMessageFamily(_) => (),
                    // We don't need to respond flush for now
                    PgWireFrontendMessage::Flush(_) => (),
                }
            }
        }
        Ok(true)
    }

    async fn handle_error(&mut self, error: PgWireError) -> Result<(), io::Error> {
        process_error(&mut self.socket, error).await
    }
}

/// A connection factory that takes a stream, and a ServiceFactory, and creates a PgWireConnection
pub struct PgConnectionFactory<S> {
    authenticator: Arc<PgAuthenticator>,
    factory: S,
}

impl<S> PgConnectionFactory<S> {
    pub fn new(inner: S) -> Self {
        Self {
            authenticator: Arc::new(PgAuthenticator),
            factory: inner,
        }
    }
}

impl<T, F> Service<(T, SocketAddr)> for PgConnectionFactory<F>
where
    T: AsyncRead + AsyncWrite + AsyncPeekable + Unpin + Send + Sync + 'static,
    F: MakeService<(), Queries, MakeError = Error> + Sync,
    F::Future: 'static + Send,
    F::Service: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
    <F::Service as Service<Queries>>::Future: Send,
{
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, (mut stream, addr): (T, SocketAddr)) -> Self::Future {
        let client_info = ClientInfoHolder::new(addr, false);
        let svc_fut = self.factory.make_service(());
        let authenticator = self.authenticator.clone();
        Box::pin(async move {
            let service = svc_fut.await.unwrap();
            peek_for_sslrequest(&mut stream, false).await?;
            let decoder = PgWireMessageServerCodec::new(client_info);
            let socket = decoder.framed(stream);

            let mut connection = PgWireConnection {
                socket,
                authenticator,
                handler: QueryHandler::new(Arc::new(Mutex::new(service))),
            };

            connection.run().await;

            // cleanup socket
            let mut socket = connection.socket.into_inner();
            socket.flush().await?;
            socket.shutdown().await?;

            Ok(())
        })
    }
}
