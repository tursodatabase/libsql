use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::{fmt, io};

use anyhow::Result;
use futures::stream::select_all;
use futures::{stream::FuturesUnordered, StreamExt};
use futures::{Future, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tower::Service;

use crate::server::ws::WsStreamAdapter;

use self::tcp::TcpAdapter;
use self::ws::WsAdapter;

mod tcp;
mod ws;

type Listener = Box<dyn Stream<Item = io::Result<(NetStream, SocketAddr)>> + Unpin + Send>;

pub struct Server {
    listeners: Vec<Listener>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }

    pub async fn bind_tcp(&mut self, addr: impl ToSocketAddrs) -> Result<&mut Self> {
        let listener = TcpListener::bind(addr).await?;
        self.listeners.push(Box::new(TcpAdapter::new(listener)));

        Ok(self)
    }

    pub async fn bind_ws(&mut self, addr: impl ToSocketAddrs) -> Result<&mut Self> {
        let listener = TcpListener::bind(addr).await?;
        self.listeners.push(Box::new(WsAdapter::new(listener)));

        Ok(self)
    }

    pub async fn serve<S>(self, mut make_svc: S) -> anyhow::Result<()>
    where
        S: Service<(NetStream, SocketAddr)>,
        S::Future: Send,
        S::Error: fmt::Display,
    {
        let mut connections = FuturesUnordered::new();
        let mut listeners = select_all(self.listeners);
        loop {
            tokio::select! {
                conn = listeners.next() => {
                    match conn {
                        Some(Ok((stream, addr))) => {
                            if poll_fn(|c| make_svc.poll_ready(c)).await.is_err() {
                                eprintln!("there was an error!");
                                break
                            }
                            tracing::info!("new connection: {addr}");
                            let fut = make_svc.call((stream , addr));
                            connections.push(fut);
                        }
                        _ => break,
                    }
                }
                disconnect = connections.next(), if !connections.is_empty() => {
                    if let Some(Err(e)) = disconnect {
                        tracing::error!("connection exited with error: {e}")
                    }
                }
            }
        }

        Ok(())
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
        Ws {
            #[pin]
            stream: WsStreamAdapter<TcpStream>,
        }
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
            NetStreamProj::Ws { stream } => stream.poll_read(cx, buf),
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
            NetStreamProj::Ws { stream } => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_flush(cx),
            NetStreamProj::Ws { stream } => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            NetStreamProj::Tcp { stream } => stream.poll_shutdown(cx),
            NetStreamProj::Ws { stream } => stream.poll_shutdown(cx),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct Peek<'a, T: ?Sized> {
        buf: &'a mut [u8],
        #[pin]
        peek: &'a mut T,
    }
}

impl<'a, T> Future for Peek<'a, T>
where
    T: AsyncPeekable + Unpin + ?Sized,
{
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut buf = ReadBuf::new(this.buf);
        let out = ready!(this.peek.poll_peek(cx, &mut buf));
        Poll::Ready(out)
    }
}

pub trait AsyncPeekable {
    fn poll_peek(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>)
        -> Poll<io::Result<usize>>;

    fn peek<'a>(&'a mut self, buf: &'a mut [u8]) -> Peek<'a, Self> {
        Peek { buf, peek: self }
    }
}

impl AsyncPeekable for NetStream {
    fn poll_peek(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<usize>> {
        match self {
            NetStream::Tcp { stream } => stream.poll_peek(cx, buf),
            NetStream::Ws { stream } => stream.poll_peek(cx, buf),
        }
    }
}
