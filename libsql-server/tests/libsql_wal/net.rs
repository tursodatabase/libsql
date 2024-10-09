use std::task::{Context, Poll};
use std::pin::Pin;
use std::io::Error as IoError;

use futures::Future;
use hyper::Uri;
use tower::Service;

use crate::common::net::TurmoilStream;

use super::dns::Dns;

#[derive(Clone)]
pub struct TurmoilConnector(Dns);

impl TurmoilConnector {
    pub fn new(dns: Dns) -> Self {
        Self(dns)
    }
}

impl Service<Uri> for TurmoilConnector {
    type Response = TurmoilStream;
    type Error = IoError;
    type Future = Pin<Box<dyn Future<Output = std::io::Result<Self::Response>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let dns = self.0.clone();
        Box::pin(async move {
            let host = uri.host().unwrap();
            let host = host.split('.').collect::<Vec<_>>();
            // get the domain from `namespace.domain` and `domain` hosts
            let domain = if host.len() == 1 { host[0] } else { host[1] };
            let host = dns.get_host(domain).to_string();
            let addr = turmoil::lookup(host);
            let port = uri.port().unwrap().as_u16();
            let inner = turmoil::net::TcpStream::connect((addr, port)).await?;
            Ok(TurmoilStream { inner })
        })
    }
}

