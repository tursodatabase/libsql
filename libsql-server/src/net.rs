use std::error::Error as StdError;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use hyper::client::connect::Connection;
use hyper::server::accept::Accept as HyperAccept;
use hyper::Uri;
use hyper_rustls::acceptor::TlsStream;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tower::Service;

pub trait Connector:
    Service<Uri, Response = Self::Conn, Future = Self::Fut, Error = Self::Err>
    + Send
    + Sync
    + 'static
    + Clone
{
    type Conn: Unpin + Send + 'static + AsyncRead + AsyncWrite + Connection;
    type Fut: Send + 'static + Unpin;
    type Err: Into<Box<dyn StdError + Send + Sync>> + Send + Sync;
}

impl<T> Connector for T
where
    T: Service<Uri> + Send + Sync + 'static + Clone,
    T::Response: Unpin + Send + 'static + AsyncRead + AsyncWrite + Connection,
    T::Future: Send + 'static + Unpin,
    T::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync,
{
    type Conn = Self::Response;
    type Fut = Self::Future;
    type Err = Self::Error;
}

pub trait Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    fn connect_info(&self) -> TcpConnectInfo;
}

pub trait Accept:
    HyperAccept<Conn = Self::Connection, Error = IoError> + Unpin + Send + 'static
{
    type Connection: Conn;
}

pub struct AddrIncoming {
    listener: tokio::net::TcpListener,
}

impl AddrIncoming {
    pub fn new(listener: tokio::net::TcpListener) -> Self {
        Self { listener }
    }
}

impl HyperAccept for AddrIncoming {
    type Conn = AddrStream;
    type Error = IoError;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        match ready!(self.listener.poll_accept(cx)) {
            Ok((stream, remote_addr)) => {
                // disable naggle algorithm
                stream.set_nodelay(true)?;
                let local_addr = stream.local_addr()?;
                Poll::Ready(Some(Ok(AddrStream {
                    stream,
                    local_addr,
                    remote_addr,
                })))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pin_project! {
    pub struct AddrStream<S = tokio::net::TcpStream> {
        #[pin]
        pub stream: S,
        pub remote_addr: SocketAddr,
        pub local_addr: SocketAddr,
    }
}

impl Accept for AddrIncoming {
    type Connection = AddrStream;
}

impl<T> Conn for AddrStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn connect_info(&self) -> TcpConnectInfo {
        TcpConnectInfo {
            local_addr: Some(self.local_addr),
            remote_addr: Some(self.remote_addr),
        }
    }
}

impl<C: Conn> Conn for TlsStream<C> {
    fn connect_info(&self) -> TcpConnectInfo {
        self.io().unwrap().connect_info()
    }
}

impl<S> AsyncRead for AddrStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for AddrStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().stream.poll_shutdown(cx)
    }
}

impl<S> Connected for AddrStream<S> {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: Some(self.local_addr),
            remote_addr: Some(self.remote_addr),
        }
    }
}
