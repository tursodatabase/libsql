use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use hyper::{
    client::{conn::SendRequest, HttpConnector},
    Client,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use tokio::sync::Mutex;
use tonic::body::BoxBody;
use tower::Service;

#[derive(Debug, Clone)]
pub struct H2cChannel {
    client: Client<HttpsConnector<HttpConnector>>,
    h2_client: Arc<Mutex<Option<SendRequest<BoxBody>>>>,
}

impl H2cChannel {
    #[allow(unused)]
    pub fn new() -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();

        let client = Client::builder().build(https);
        let h2_client = Arc::new(Mutex::new(None));

        Self { client, h2_client }
    }
}

impl Service<http::Request<BoxBody>> for H2cChannel {
    type Response = http::Response<hyper::Body>;
    type Error = anyhow::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let client = self.client.clone();
        let h2_client_lock = self.h2_client.clone();

        Box::pin(async move {
            let mut lock = h2_client_lock.lock().await;

            if let Some(client) = &mut *lock {
                let res = client.send_request(req).await;

                // If we get an error from the request then we should throw away
                // the client so that it can recreate. Most normal operational
                // errors go through the response type and the Err's returned
                // from hyper are for more fatal style errors.
                if let Err(e) = &res {
                    tracing::debug!("Client error: {}, throwing it away", e);
                    *lock = None;
                }

                res.map_err(Into::into)
            } else {
                let origin = req.uri();

                let h2c_req = hyper::Request::builder()
                    .uri(origin)
                    .header(http::header::UPGRADE, "h2c")
                    .body(hyper::Body::empty())
                    .unwrap();

                let res = client.request(h2c_req).await?;

                if res.status() != http::StatusCode::SWITCHING_PROTOCOLS {
                    anyhow::bail!("We did not get an http2 upgrade, status: {}", res.status());
                }

                let upgraded_io = hyper::upgrade::on(res).await?;

                tracing::debug!("Upgraded connection to h2");

                let (mut h2_client, conn) = hyper::client::conn::Builder::new()
                    .http2_only(true)
                    .http2_keep_alive_interval(std::time::Duration::from_secs(10))
                    .http2_keep_alive_while_idle(true)
                    .handshake(upgraded_io)
                    .await?;
                tokio::spawn(conn);

                let fut = h2_client.send_request(req);
                *lock = Some(h2_client);

                fut.await.map_err(Into::into)
            }
        })
    }
}
