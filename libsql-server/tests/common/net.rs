use std::io::Error as IoError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};

use futures_core::Future;
use hyper::client::connect::Connected;
use hyper::server::accept::Accept as HyperAccept;
use hyper::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use sqld::net::Accept;
use sqld::net::AddrStream;
use sqld::Server;

type TurmoilAddrStream = AddrStream<turmoil::net::TcpStream>;

pub struct TurmoilAcceptor {
    acceptor: Pin<
        Box<dyn HyperAccept<Conn = TurmoilAddrStream, Error = IoError> + Send + Sync + 'static>,
    >,
}

impl TurmoilAcceptor {
    pub async fn bind(addr: impl Into<SocketAddr>) -> std::io::Result<Self> {
        let addr = addr.into();
        let stream = async_stream::stream! {
            let listener = turmoil::net::TcpListener::bind(addr).await?;
            loop {
                yield listener.accept().await.and_then(|(stream, remote_addr)| Ok(AddrStream {
                    remote_addr,
                    local_addr: stream.local_addr()?,
                    stream,
                }));
            }
        };
        let acceptor = hyper::server::accept::from_stream(stream);
        Ok(Self {
            acceptor: Box::pin(acceptor),
        })
    }
}

impl Accept for TurmoilAcceptor {
    type Connection = TurmoilAddrStream;
}

impl HyperAccept for TurmoilAcceptor {
    type Conn = TurmoilAddrStream;
    type Error = IoError;

    fn poll_accept(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        self.acceptor.as_mut().poll_accept(cx)
    }
}

#[derive(Clone)]
pub struct TurmoilConnector;

pin_project_lite::pin_project! {
    pub struct TurmoilStream {
        #[pin]
        inner: turmoil::net::TcpStream,
    }
}

impl AsyncWrite for TurmoilStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

impl AsyncRead for TurmoilStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl hyper::client::connect::Connection for TurmoilStream {
    fn connected(&self) -> hyper::client::connect::Connected {
        Connected::new()
    }
}

impl Service<Uri> for TurmoilConnector {
    type Response = TurmoilStream;
    type Error = IoError;
    type Future = Pin<Box<dyn Future<Output = std::io::Result<Self::Response>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let host = uri.host().unwrap();
            let host = host.split('.').collect::<Vec<_>>();
            // get the domain from `namespace.domain` and `domain` hosts
            let domain = if host.len() == 1 { host[0] } else { host[1] };
            let addr = turmoil::lookup(domain);
            let port = uri.port().unwrap().as_u16();
            let inner = turmoil::net::TcpStream::connect((addr, port)).await?;
            Ok(TurmoilStream { inner })
        })
    }
}

pub type TestServer = Server<TurmoilConnector, TurmoilAcceptor, TurmoilConnector>;

#[async_trait::async_trait]
pub trait SimServer {
    async fn start_sim(self, user_api_port: usize) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl SimServer for TestServer {
    async fn start_sim(mut self, user_api_port: usize) -> anyhow::Result<()> {
        // We need to ensure that libsql's init code runs before we do anything
        // with rusqlite in sqld. This is because libsql has saftey checks and
        // needs to configure the sqlite api. Thus if we init sqld first
        // it will fail. To work around this we open a temp db in memory db
        // to ensure we run libsql's init code first. This DB is not actually
        // used in the test only for its run once init code.
        //
        // This does change the serialization mode for sqld but because the mode
        // that we use in libsql is safer than the sqld one it is still safe.
        let db = libsql::Database::open_in_memory().unwrap();
        db.connect().unwrap();

        let user_api = TurmoilAcceptor::bind(([0, 0, 0, 0], user_api_port as u16)).await?;
        self.user_api_config.http_acceptor = Some(user_api);

        self.start().await?;

        Ok(())
    }
}

pub fn init_tracing() {
    static INIT_TRACING: Once = Once::new();
    INIT_TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
    });
}
