use std::error::Error as StdError;

use hyper::client::connect::Connection;
use hyper::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
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
