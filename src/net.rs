use std::{
    net::SocketAddr,
    task::{ready, Poll},
};

use anyhow::Result;
use bytes::{Buf, BytesMut};
use futures::Stream;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::mpsc,
};

use crate::messages::Message;

pub struct NetworkManager {
    tcp_listener: TcpListener,
}

impl NetworkManager {
    pub async fn listen(addr: impl ToSocketAddrs) -> Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        log::info!("listening on: {:?}", tcp_listener.local_addr()?);

        Ok(Self { tcp_listener })
    }
}

impl Stream for NetworkManager {
    type Item = Result<Connection>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match ready!(self.tcp_listener.poll_accept(cx)) {
            Ok((stream, addr)) => {
                log::info!("new connection from {addr:?}");
                let (sender, receiver) = mpsc::unbounded_channel();
                let con = Connection {
                    addr,
                    stream,
                    sender,
                    receiver,
                    on_disconnect: None,
                    on_message: None,
                };
                Poll::Ready(Some(Ok(con)))
            }
            Err(e) => Poll::Ready(Some(Err(e.into()))),
        }
    }
}

pub struct Connection {
    pub addr: SocketAddr,
    stream: TcpStream,
    sender: mpsc::UnboundedSender<Message>,
    receiver: mpsc::UnboundedReceiver<Message>,
    on_message: Option<Box<dyn Fn(Message) -> Result<()>>>,
    on_disconnect: Option<Box<dyn FnOnce()>>,
}

impl Connection {
    /// Creates a connection that connects to the remote addr
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let addr = stream.peer_addr()?;
        let (sender, receiver) = mpsc::unbounded_channel();
        Ok(Self {
            addr,
            stream,
            sender,
            receiver,
            on_message: None,
            on_disconnect: None,
        })
    }

    /// Returns a sender to the connection.
    /// This sender is used to send Messages to the connection.
    pub fn sender(&self) -> mpsc::UnboundedSender<Message> {
        self.sender.clone()
    }

    /// Set a handler for messages for this connection. If an error is returned from the handler,
    /// the connection is closed.
    pub fn set_on_message(&mut self, handler: impl Fn(Message) -> Result<()> + 'static) {
        self.on_message.replace(Box::new(handler));
    }

    /// Set handler for when the client disconnects.
    pub fn set_on_disconnect(&mut self, handler: impl FnOnce() + 'static) {
        self.on_disconnect.replace(Box::new(handler));
    }

    pub async fn run(mut self) {
        let (mut read_half, mut write_half) = self.stream.split();
        let mut buf = BytesMut::new();
        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    match msg {
                        Some(msg) => {
                            send_message(&mut write_half, msg).await;
                        }
                        None => break,
                    }

                }
                res = read_half.read_buf(&mut buf) => {
                    match res {
                        Ok(0) => break,
                        Ok(_) => {
                            match read_message(&mut read_half, &mut buf).await {
                                Ok(msg) => {
                                    match self.on_message.as_mut().map(|h| h(msg)) {
                                        Some(Err(e)) => {
                                            log::warn!("error handling client message, disconnecting: {e}");
                                            break
                                        }
                                        _ => (),
                                    }
                                }
                                Err(e) => {
                                    log::warn!("error reading message: {e}");
                                    continue
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("error reading message: {e}");
                            continue
                        }
                    }
                }
            }
        }

        self.on_disconnect.map(|h| h());
        log::info!("client {} disconnected", self.addr);
    }
}

/// given a buf with some bytes already initialized to the beginning of a message, proceeds to read
/// the rest of the message from reader.
///
/// Not cancellation safe
async fn read_message<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut BytesMut) -> Result<Message> {
    let len = buf.get_u32() as usize;
    let mut total_read = buf.len();
    while total_read < len {
        match reader.read_buf(buf).await {
            Ok(0) => break,
            Ok(n) => total_read += n,
            Err(e) => anyhow::bail!(e),
        }
    }

    let message = bincode::deserialize_from(buf.reader())?;

    Ok(message)
}

async fn send_message<W: AsyncWrite + Unpin>(writer: &mut W, message: Message) {
    let data = bincode::serialize(&message).unwrap();
    // encode the lengh of the message as a u32 big endian bytes array.
    // TODO: use varint maybe?
    let len = (data.len() as u32).to_be_bytes();
    let mut to_send = Buf::chain(len.as_slice(), data.as_slice());
    loop {
        match writer.write_buf(&mut to_send).await {
            Ok(0) => break,
            Ok(_) => (),
            Err(_) => {
                log::warn!("error sending data to client");
                break;
            }
        }
    }
}
