use super::box_clone_service::BoxCloneService;

pub trait Socket:
    hyper::rt::Read + hyper::rt::Write + hyper_util::client::legacy::connect::Connection + Send + Unpin + 'static + Sync
{
}

impl<T> Socket for T where
    T: hyper::rt::Read + hyper::rt::Write + hyper_util::client::legacy::connect::Connection + Send + Unpin + 'static + Sync
{
}

impl hyper_util::client::legacy::connect::Connection for Box<dyn Socket> {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}

pub type ConnectorService =
    BoxCloneService<http::Uri, Box<dyn Socket>, Box<dyn std::error::Error + Sync + Send + 'static>>;

#[cfg(feature = "replication")]
pub type HttpRequestCallback = std::sync::Arc<dyn Fn(&mut http::Request<()>) + Send + Sync>;
