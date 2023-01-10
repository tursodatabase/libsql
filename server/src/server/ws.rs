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
            Some(Ok(Message::Close(_))) => self.terminated = true,
            Some(Ok(_)) => (),
            Some(Err(WsError::Io(io))) => return Poll::Ready(Err(io)),
            Some(Err(WsError::ConnectionClosed)) | None => self.terminated = true,
            _ => unimplemented!("implement unsupported error"),
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

        let to_read = buf.remaining().min(self.buffer.remaining());
        let mut take = (&mut self.buffer).take(to_read);
        let dest = buf.initialize_unfilled_to(to_read);
        take.copy_to_slice(dest);
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
        if let Err(e) = ready!(self.stream.poll_ready_unpin(cx)) {
            return Poll::Ready(Err(ws_error_to_io_error(e)));
        }

        let message = Message::Binary(buf.to_vec());
        if let Err(e) = self.stream.start_send_unpin(message) {
            return Poll::Ready(Err(ws_error_to_io_error(e)));
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match ready!(self.stream.poll_flush_unpin(cx)) {
            Err(e) => Poll::Ready(Err(ws_error_to_io_error(e))),
            Ok(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match ready!(self.stream.poll_close_unpin(cx)) {
            Err(e) => Poll::Ready(Err(ws_error_to_io_error(e))),
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
            let to_peek = self.buffer.remaining().min(buf.capacity());
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

type WsAdapterInitFut =
    Pin<Box<dyn Future<Output = (WebSocketStream<TcpStream>, SocketAddr)> + Send>>;

pub struct WsAdapter {
    listener: TcpListener,
    init: FuturesUnordered<WsAdapterInitFut>,
}

impl WsAdapter {
    pub fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            init: FuturesUnordered::new(),
        }
    }
}

fn ws_error_to_io_error(error: WsError) -> io::Error {
    match error {
        WsError::ConnectionClosed | WsError::AlreadyClosed => {
            io::Error::new(io::ErrorKind::BrokenPipe, "")
        }
        WsError::Io(io) => io,
        error => io::Error::new(io::ErrorKind::Other, error),
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use rand::{prelude::*, Fill};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_read_from_socket() {
        let (client, server) = tokio::io::duplex(512);
        let server_stream = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut adapter = WsStreamAdapter::new(server_stream);
        let shandle = tokio::task::spawn(async move {
            let mut buffer = Vec::new();
            adapter.read_to_end(&mut buffer).await.unwrap();
            buffer
        });

        let mut client_stream = WebSocketStream::from_raw_socket(client, Role::Client, None).await;

        let local = tokio::task::LocalSet::new();
        let chandle = local.run_until(async move {
            let mut expected = Vec::new();
            let mut rng = thread_rng();
            let n_messages = rng.gen_range(10..50);
            let mut buffer = [0; 64];
            for _ in 0..n_messages {
                buffer.try_fill(&mut rng).unwrap();
                expected.extend_from_slice(&buffer);
                client_stream
                    .send(Message::Binary(buffer.to_vec()))
                    .await
                    .unwrap();
            }

            client_stream.close(None).await.unwrap();
            expected
        });
        let (found, expected) = tokio::join!(shandle, chandle);
        assert_eq!(found.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_write_from_socket() {
        let (client, server) = tokio::io::duplex(512);
        let server_stream = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut adapter = WsStreamAdapter::new(server_stream);

        let local = tokio::task::LocalSet::new();
        let shandle = local.run_until(async move {
            let mut expected = Vec::new();
            let mut rng = thread_rng();
            let n_messages = rng.gen_range(10..50);
            let mut buffer = [0; 64];
            for _ in 0..n_messages {
                buffer.try_fill(&mut rng).unwrap();
                expected.extend_from_slice(&buffer);
                adapter.write_all(&buffer).await.unwrap();
            }

            adapter.flush().await.unwrap();
            adapter.shutdown().await.unwrap();

            expected
        });

        let mut client_stream = WebSocketStream::from_raw_socket(client, Role::Client, None).await;

        let chandle = tokio::spawn(async move {
            let mut found = Vec::new();
            while let Some(msg) = client_stream.next().await {
                match msg.unwrap() {
                    Message::Binary(data) => found.extend_from_slice(&data),
                    Message::Close(_) => break,
                    _ => panic!(),
                }
            }

            found
        });
        let (expected, found) = tokio::join!(shandle, chandle);
        assert_eq!(found.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_peek_from_socket() {
        let (client, server) = tokio::io::duplex(512);
        let server_stream = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut adapter = WsStreamAdapter::new(server_stream);

        let shandle = tokio::spawn(async move {
            let buf = &mut [0; 32];
            let peeked = adapter.peek(buf).await.unwrap();
            assert_eq!(&buf[..peeked], &[1, 2, 3, 4]);
            // sync with client
            adapter.write_u8(1).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            let peeked = adapter.peek(buf).await.unwrap();
            assert_eq!(&buf[..peeked], &[1, 2, 3, 4, 5, 6, 7, 8]);
        });

        let mut client_stream = WebSocketStream::from_raw_socket(client, Role::Client, None).await;

        let chandle = tokio::spawn(async move {
            client_stream
                .send(Message::Binary(vec![1, 2, 3, 4]))
                .await
                .unwrap();
            client_stream.next().await;
            client_stream
                .send(Message::Binary(vec![5, 6, 7, 8]))
                .await
                .unwrap();
        });

        shandle.await.unwrap();
        chandle.await.unwrap();
    }
}
