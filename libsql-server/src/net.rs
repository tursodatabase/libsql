use std::error::Error as StdError;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use http::Uri;
use hyper::rt::{Read, Write};
use hyper_util::client::legacy::connect::Connection;
use hyper_util::rt::TokioIo;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tower::Service;

pin_project! {
    /// A wrapper that adds hyper 1.0's Read/Write traits to any tokio AsyncRead/AsyncWrite type.
    /// This uses TokioIo internally to bridge between tokio and hyper traits.
    pub struct HyperStream<S> {
        #[pin]
        inner: TokioIo<S>,
    }
}

impl<S> HyperStream<S> {
    pub fn new(stream: S) -> Self {
        Self {
            inner: TokioIo::new(stream),
        }
    }
    
    pub fn into_inner(self) -> S {
        self.inner.into_inner()
    }
}

// Note: HyperStream only implements hyper's Read/Write traits, not tokio's AsyncRead/AsyncWrite
// TokioIo already bridges between tokio and hyper traits internally
impl<S: AsyncRead + AsyncWrite + Unpin> Read for HyperStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Write for HyperStream<S> {
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

impl<S: AsyncRead + AsyncWrite + Connection + Unpin> Connection for HyperStream<S> {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        self.inner.inner().connected()
    }
}

pub trait Connector:
    Service<Uri, Response = Self::Conn, Future = Self::Fut, Error = Self::Err>
    + Send
    + Sync
    + 'static
    + Clone
{
    type Conn: Unpin + Send + 'static + AsyncRead + AsyncWrite + Read + Write + Connection;
    type Fut: Send + 'static + Unpin;
    type Err: Into<Box<dyn StdError + Send + Sync>> + Send + Sync;
}

impl<T> Connector for T
where
    T: Service<Uri> + Send + Sync + 'static + Clone,
    T::Response: Unpin + Send + 'static + AsyncRead + AsyncWrite + Read + Write + Connection,
    T::Future: Send + 'static + Unpin,
    T::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync,
{
    type Conn = Self::Response;
    type Fut = Self::Future;
    type Err = Self::Error;
}

pub trait Conn: AsyncRead + AsyncWrite + Read + Write + Unpin + Send + 'static {
    fn connect_info(&self) -> TcpConnectInfo;
}

/// Trait for accepting incoming connections.
/// This is the hyper 1.0+ compatible version that replaces `hyper::server::accept::Accept`.
pub trait Accept: Unpin + Send + 'static {
    type Connection: Conn + Connected<ConnectInfo = TcpConnectInfo>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>>;
}

pub struct AddrIncoming {
    listener: tokio::net::TcpListener,
}

impl AddrIncoming {
    pub fn new(listener: tokio::net::TcpListener) -> Self {
        Self { listener }
    }
}

impl Accept for AddrIncoming {
    type Connection = AddrStream;
    type Error = IoError;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>> {
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

// Note: TlsStream doesn't implement Conn directly because it doesn't implement hyper::rt::Read/Write.
// Use HyperStream<TlsStream<C>> when you need a connection that implements Conn.

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

impl<S> Read for AddrStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        // SAFETY: We're creating a tokio ReadBuf from the hyper ReadBufCursor
        let slice = unsafe { 
            std::slice::from_raw_parts_mut(buf.as_mut().as_mut_ptr(), buf.as_mut().len()) 
        };
        let mut read_buf = tokio::io::ReadBuf::new(slice);
        
        match self.project().stream.poll_read(cx, &mut read_buf) {
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

impl<S> Write for AddrStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
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
