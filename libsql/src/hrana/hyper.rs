use crate::hrana::pipeline::ServerMsg;
use crate::hrana::Result;
use crate::hrana::{HranaError, HttpSend};
use crate::util::ConnectorService;
use futures::future::BoxFuture;
use http::header::AUTHORIZATION;
use http::StatusCode;
use hyper::Error;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};

#[derive(Clone, Debug)]
pub struct HttpSender {
    inner: hyper::Client<HttpsConnector<ConnectorService>, hyper::Body>,
}

impl HttpSender {
    pub fn new(connector: ConnectorService) -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let inner = hyper::Client::builder().build(https);

        Self { inner }
    }

    async fn send(&self, url: String, auth: String, body: String) -> Result<ServerMsg> {
        let req = hyper::Request::post(url)
            .header(AUTHORIZATION, auth)
            .body(hyper::Body::from(body))
            .unwrap();

        let res = self.inner.request(req).await.map_err(HranaError::from)?;

        if res.status() != StatusCode::OK {
            let body = hyper::body::to_bytes(res.into_body())
                .await
                .map_err(HranaError::from)?;
            let body = String::from_utf8(body.into()).unwrap();
            return Err(HranaError::Api(body));
        }

        let body = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(HranaError::from)?;

        let msg = serde_json::from_slice::<ServerMsg>(&body[..]).map_err(HranaError::from)?;

        Ok(msg)
    }
}

impl<'a> HttpSend<'a> for HttpSender {
    type Result = BoxFuture<'a, Result<ServerMsg>>;

    fn http_send(&'a self, url: String, auth: String, body: String) -> Self::Result {
        let fut = self.send(url, auth, body);
        Box::pin(fut)
    }
}

impl From<hyper::Error> for HranaError {
    fn from(value: Error) -> Self {
        HranaError::Http(value.to_string())
    }
}
