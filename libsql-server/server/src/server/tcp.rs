use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::Stream;
use tokio::net::TcpListener;

use super::NetStream;

pub struct TcpAdapter {
    listener: TcpListener,
}

impl TcpAdapter {
    pub fn new(stream: TcpListener) -> Self {
        Self { listener: stream }
    }
}

impl Stream for TcpAdapter {
    type Item = io::Result<(NetStream, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let conn = ready!(self.as_mut().listener.poll_accept(cx));
        match conn {
            Ok((stream, addr)) => Poll::Ready(Some(Ok((NetStream::Tcp { stream }, addr)))),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}
