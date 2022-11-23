use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use anyhow::Result;
use futures::Future;
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tower::Service;

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    pub async fn serve<S>(self, mut make_svc: S)
    where
        S: Service<(NetStream, SocketAddr)>,
    {
        let mut connections = FuturesOrdered::new();
        loop {
            tokio::select! {
                conn = self.listener.accept() => {
                    match conn {
                        Ok((stream, addr)) => {
                            if poll_fn(|c| make_svc.poll_ready(c)).await.is_err() {
                                eprintln!("there was an error!");
                                break
                            }
                            log::info!("new connection: {addr}");
                            let fut = make_svc.call((NetStream::Tcp { stream } , addr));
                            connections.push_back(fut);
                        }
                        Err(_) => break,
                    }
                }
                _dis = connections.next() => { }
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Represents all the types of stream that the server can handle.
    ///
    /// This type implements AsyncRead and AsyncWrite, and is an abstraction over a Tcp-like network stream, like websocket or tls.
    #[project = NetStreamProj]
    pub enum NetStream {
        Tcp {
            #[pin]
            stream: TcpStream,
        },
    }
}

impl AsyncRead for NetStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NetStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_shutdown(cx),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct Peek<'a, T: ?Sized> {
        buf: &'a mut [u8],
        #[pin]
        peek: &'a T,
    }
}

impl<'a, T> Future for Peek<'a, T>
where
    T: AsyncPeekable + Unpin + ?Sized,
{
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut buf = ReadBuf::new(this.buf);
        let out = ready!(this.peek.poll_peek(cx, &mut buf));
        Poll::Ready(out)
    }
}

pub trait AsyncPeekable {
    fn poll_peek(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<usize>>;

    fn peek<'a>(&'a self, buf: &'a mut [u8]) -> Peek<'a, Self> {
        Peek { buf, peek: self }
    }
}

impl AsyncPeekable for NetStream {
    fn poll_peek(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<usize>> {
        match self {
            NetStream::Tcp { stream } => stream.poll_peek(cx, buf),
        }
    }
}
