use axum::http::HeaderName;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

use super::net::TurmoilConnector;

/// A hyper client that resolves URI within a turmoil simulation.
pub struct Client {
    inner: hyper_util::client::legacy::Client<TurmoilConnector, Full<Bytes>>,
}

pub struct Response(hyper::Response<hyper::body::Incoming>);

impl Response {
    pub async fn json<T: serde::de::DeserializeOwned>(self) -> anyhow::Result<T> {
        let body = self.0.into_body();
        let collected = body.collect().await?;
        let bytes = collected.to_bytes();
        let v = serde_json::from_slice(&bytes)?;
        Ok(v)
    }

    pub async fn json_value(self) -> anyhow::Result<serde_json::Value> {
        self.json().await
    }

    pub async fn body_string(self) -> anyhow::Result<String> {
        let body = self.0.into_body();
        let collected = body.collect().await?;
        let bytes = collected.to_bytes();
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    pub fn status(&self) -> hyper::http::StatusCode {
        self.0.status()
    }
}

impl Client {
    pub fn new() -> Self {
        let connector = TurmoilConnector;
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(connector);
        Self { inner: client }
    }

    pub async fn get(&self, s: &str) -> anyhow::Result<Response> {
        let body = Full::new(Bytes::new());
        let req = hyper::Request::get(s).body(body)?;
        Ok(Response(self.inner.request(req).await?))
    }

    pub(crate) async fn post<T: serde::Serialize>(
        &self,
        url: &str,
        body: T,
    ) -> anyhow::Result<Response> {
        self.post_with_headers(url, &[], body).await
    }

    pub(crate) async fn post_with_headers<T: serde::Serialize>(
        &self,
        url: &str,
        headers: &[(HeaderName, &str)],
        body: T,
    ) -> anyhow::Result<Response> {
        let bytes: Bytes = serde_json::to_vec(&body)?.into();
        let body = Full::new(bytes);
        let mut request = hyper::Request::post(url)
            .header("Content-Type", "application/json")
            .body(body)?;

        for (key, val) in headers {
            request
                .headers_mut()
                .insert(key.clone(), val.parse().unwrap());
        }

        let resp = self.inner.request(request).await?;

        if resp.status().is_server_error() {
            anyhow::bail!("request was not successful {:?}", resp.status());
        }

        Ok(Response(resp))
    }

    pub(crate) async fn delete<T: serde::Serialize>(
        &self,
        url: &str,
        body: T,
    ) -> anyhow::Result<Response> {
        let bytes: Bytes = serde_json::to_vec(&body)?.into();
        let body = Full::new(bytes);
        let request = hyper::Request::delete(url)
            .header("Content-Type", "application/json")
            .body(body)?;
        let resp = self.inner.request(request).await?;

        Ok(Response(resp))
    }
}
