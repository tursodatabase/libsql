use std::convert::Infallible;
use std::future::{poll_fn, Future};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;

use futures::StreamExt;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::PgWireConnectionState;
use pgwire::error::PgWireError;
use pgwire::tokio::PgWireMessageServerCodec;
use pgwire::{api::ClientInfoHolder, messages::PgWireFrontendMessage};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, Framed};
use tower::MakeService;
use tower::Service;

use crate::postgres::authenticator::PgAuthenticator;
use crate::query::{Query, QueryError, QueryResponse};
use crate::server::AsyncPeekable;

use super::proto::{peek_for_sslrequest, process_error, SimpleHandler};

/// Manages a postgres wire connection.
pub struct PgWireConnection<T, S> {
    socket: Framed<T, PgWireMessageServerCodec>,
    authenticator: Rc<PgAuthenticator>,
    service: S,
}

impl<T, S> PgWireConnection<T, S>
where
    S: Service<Query, Response = QueryResponse, Error = QueryError>,
    T: AsyncRead + AsyncWrite + Unpin + Send + Sync,
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

        self.shutdown().await;
    }

    async fn handle_message(&mut self, msg: PgWireFrontendMessage) -> Result<bool, PgWireError> {
        match self.socket.codec().client_info().state() {
            PgWireConnectionState::AwaitingStartup
            | PgWireConnectionState::AuthenticationInProgress => {
                self.authenticator
                    .authenticate(&mut self.socket, &msg)
                    .await?;
            }
            _ => match msg {
                PgWireFrontendMessage::Query(q) => {
                    let query = Query::SimpleQuery(q.query().to_string());

                    poll_fn(|c| self.service.poll_ready(c)).await.unwrap();
                    let resp = self.service.call(query).await;
                    SimpleHandler(resp).on_query(&mut self.socket, &q).await?;
                }
                // TODO: handle extended queries.
                PgWireFrontendMessage::Parse(_) => todo!(),
                PgWireFrontendMessage::Close(_) => todo!(),
                PgWireFrontendMessage::Bind(_) => todo!(),
                PgWireFrontendMessage::Describe(_) => todo!(),
                PgWireFrontendMessage::Execute(_) => todo!(),
                PgWireFrontendMessage::Sync(_) => todo!(),
                PgWireFrontendMessage::Terminate(_) => return Ok(false),
                // These messages are handled by the connection service on startup.
                PgWireFrontendMessage::Startup(_) | PgWireFrontendMessage::Password(_) => (),
            },
        }
        Ok(true)
    }

    async fn shutdown(&mut self) {
        let _ = self.service.call(Query::Disconnect).await;
    }

    async fn handle_error(&mut self, error: PgWireError) -> Result<(), io::Error> {
        process_error(&mut self.socket, error).await
    }
}

/// A connection factory that takes a stream, and a ServiceFactory, and creates a PgWireConnection
pub struct PgConnectionFactory<S> {
    authenticator: Rc<PgAuthenticator>,
    factory: S,
}

impl<S> PgConnectionFactory<S> {
    pub fn new(inner: S) -> Self {
        Self {
            authenticator: Rc::new(PgAuthenticator),
            factory: inner,
        }
    }
}

impl<T, F, S> Service<(T, SocketAddr)> for PgConnectionFactory<F>
where
    // Send not necessary, get rid of it when implementing authentication.
    T: AsyncRead + AsyncWrite + AsyncPeekable + Unpin + Send + Sync + 'static,
    F: MakeService<(), Query, MakeError = Infallible, Service = S>,
    F::Future: 'static,
    S: Service<Query, Response = QueryResponse, Error = QueryError>,
{
    type Response = ();
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

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
                service,
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
