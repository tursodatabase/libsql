#![allow(deprecated)]

use std::io::Error as IoError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};

use futures_core::Future;
use hyper::Uri;
use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::{Connection, Connected};
use metrics_util::debugging::DebuggingRecorder;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use libsql_server::net::Accept;
use libsql_server::net::AddrStream;
use libsql_server::Server;

type TurmoilAddrStream = AddrStream<turmoil::net::TcpStream>;

pub struct TurmoilAcceptor {
    listener: turmoil::net::TcpListener,
}

impl TurmoilAcceptor {
    pub async fn bind(addr: impl Into<SocketAddr>) -> std::io::Result<Self> {
        let addr = addr.into();
        let listener = turmoil::net::TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }
}

impl Accept for TurmoilAcceptor {
    type Connection = TurmoilAddrStream;
    type Error = IoError;

    fn poll_accept(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>> {
        let listener = &self.listener;
        // We need to use the underlying std listener to poll
        // Since turmoil::net::TcpListener doesn't expose poll_accept directly,
        // we'll use a workaround with tokio's async listener pattern
        match listener.accept().now_or_never() {
            Some(Ok((stream, remote_addr))) => {
                let local_addr = stream.local_addr()?;
                Poll::Ready(Some(Ok(AddrStream {
                    remote_addr,
                    local_addr,
                    stream,
                })))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Pending,
        }
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

impl TurmoilStream {
    pub fn new(stream: turmoil::net::TcpStream) -> Self {
        Self { inner: stream }
    }
}

// Implement tokio's AsyncRead/AsyncWrite by delegating directly to the inner stream
impl AsyncRead for TurmoilStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
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

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

// Implement hyper's Read/Write traits by bridging from tokio traits
impl Read for TurmoilStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        // SAFETY: We're creating a tokio ReadBuf from the hyper ReadBufCursor
        let mut read_buf = unsafe { tokio::io::ReadBuf::uninit(buf.as_mut()) };
        
        match self.project().inner.poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let filled = read_buf.filled().len();
                unsafe { buf.advance(filled) };
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Write for TurmoilStream {
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

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

impl Connection for TurmoilStream {
    fn connected(&self) -> Connected {
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
            let stream = turmoil::net::TcpStream::connect((addr, port)).await?;
            Ok(TurmoilStream::new(stream))
        })
    }
}

pub type TestServer = Server<TurmoilAcceptor>;

#[async_trait::async_trait]
pub trait SimServer {
    async fn start_sim(self, user_api_port: usize) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl SimServer for TestServer {
    async fn start_sim(mut self, user_api_port: usize) -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt::try_init();

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

        // Ignore the result because we may set it many times in a single process.
        let _ = DebuggingRecorder::per_thread().install();

        let user_api = TurmoilAcceptor::bind(([0, 0, 0, 0], user_api_port as u16)).await?;
        self.user_api_config.http_acceptor = Some(user_api);

        // Disable prom metrics since we already created our recorder.
        if let Some(admin_api) = &mut self.admin_api_config {
            admin_api.disable_metrics = true;
        }

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

// Helper trait for polling futures
use std::future::Future as StdFuture;
trait NowOrNever<T> {
    fn now_or_never(self) -> Option<T>;
}

impl<F, T> NowOrNever<T> for F
where
    F: StdFuture<Output = T>,
{
    fn now_or_never(self) -> Option<T> {
        use std::task::Wake;
        use std::sync::Arc;
        
        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
        }
        
        let waker = std::task::Waker::from(Arc::new(NoopWaker));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = Box::pin(self);
        
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(val) => Some(val),
            Poll::Pending => None,
        }
    }
}
