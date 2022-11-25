use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::Buf;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use super::{AsyncPeekable, NetStream};

pub struct WsStreamAdapter<S> {
    stream: WebSocketStream<S>,
    buffer: VecDeque<u8>,
    terminated: bool,
}

impl<S> WsStreamAdapter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let next = ready!(self.stream.poll_next_unpin(cx));
        match next {
            Some(Ok(Message::Binary(data))) => self.buffer.extend(data.iter()),
            Some(Err(WsError::Io(io))) => return Poll::Ready(Err(io)),
            Some(Err(WsError::ConnectionClosed)) | None => self.terminated = true,
            _ => unimplemented!("implement unsupported error and message"),
        }

        Poll::Ready(Ok(()))
    }
}

impl<S> AsyncRead for WsStreamAdapter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(e)) = self.poll_fill_buf(cx) {
            return Poll::Ready(Err(e));
        }

        if self.buffer.is_empty() {
            if self.terminated {
                // Read 0
                return Poll::Ready(Ok(()));
            } else {
                return Poll::Pending;
            }
        }

        let to_read = buf.capacity().min(self.buffer.len());
        let mut take = (&mut self.buffer).take(to_read);
        unsafe { buf.assume_init(to_read) };
        take.copy_to_slice(buf.initialized_mut());
        buf.advance(to_read);

        Poll::Ready(Ok(()))
    }
}

impl<S> AsyncWrite for WsStreamAdapter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if let Err(_e) = ready!(self.stream.poll_ready_unpin(cx)) {
            todo!("handle error");
        }

        let message = Message::Binary(buf.to_vec());
        if let Err(_e) = self.stream.start_send_unpin(message) {
            todo!("handle error");
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match ready!(self.stream.poll_flush_unpin(cx)) {
            Err(_e) => todo!("handle error"),
            Ok(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(self.stream.poll_close_unpin(cx)) {
            Err(_e) => todo!("handle error"),
            Ok(_) => Poll::Ready(Ok(())),
        }
    }
}

impl<S> AsyncPeekable for WsStreamAdapter<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    fn poll_peek(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<usize>> {
        if let Poll::Ready(Err(e)) = self.poll_fill_buf(cx) {
            return Poll::Ready(Err(e));
        }

        if self.buffer.has_remaining() {
            let to_peek = self.buffer.len().min(buf.capacity());
            self.buffer.make_contiguous();
            buf.put_slice(&self.buffer.chunk()[..to_peek]);

            Poll::Ready(Ok(to_peek))
        } else {
            Poll::Pending
        }
    }
}

impl<S> WsStreamAdapter<S> {
    fn new(stream: WebSocketStream<S>) -> Self {
        Self {
            stream,
            buffer: Default::default(),
            terminated: false,
        }
    }
}

pub struct WsAdapter {
    listener: TcpListener,
    init: FuturesUnordered<Pin<Box<dyn Future<Output = (WebSocketStream<TcpStream>, SocketAddr)>>>>,
}

impl WsAdapter {
    pub fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            init: FuturesUnordered::new(),
        }
    }
}

impl Stream for WsAdapter {
    type Item = io::Result<(NetStream, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(conn) = self.as_mut().listener.poll_accept(cx) {
            match conn {
                Ok((stream, addr)) => {
                    let init_fut = WebSocketStream::from_raw_socket(stream, Role::Server, None);
                    self.init
                        .push(Box::pin(init_fut.map(move |stream| (stream, addr))));
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }

        let Some((stream, addr)) = ready!(self.init.poll_next_unpin(cx)) else { return Poll::Pending };
        let stream = NetStream::Ws {
            stream: WsStreamAdapter::new(stream),
        };

        Poll::Ready(Some(Ok((stream, addr))))
    }
}
