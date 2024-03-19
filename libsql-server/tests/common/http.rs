use bytes::Bytes;
use hyper::Body;
use serde::{de::DeserializeOwned, Serialize};

use super::net::TurmoilConnector;

/// An hyper client that resolves URI within a turmoil simulation.
pub struct Client(hyper::Client<TurmoilConnector>);

pub struct Response(hyper::Response<Body>);

impl Response {
    pub async fn json<T: DeserializeOwned>(self) -> anyhow::Result<T> {
        let bytes = hyper::body::to_bytes(self.0.into_body()).await?;
        let v = serde_json::from_slice(&bytes)?;
        Ok(v)
    }

    pub async fn json_value(self) -> anyhow::Result<serde_json::Value> {
        self.json().await
    }

    pub async fn body_string(self) -> anyhow::Result<String> {
        let bytes = hyper::body::to_bytes(self.0.into_body()).await?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    pub fn status(&self) -> hyper::http::StatusCode {
        self.0.status()
    }
}

impl Client {
    pub fn new() -> Self {
        let connector = TurmoilConnector;
        Self(hyper::client::Client::builder().build(connector))
    }

    pub async fn get(&self, s: &str) -> anyhow::Result<Response> {
        Ok(Response(self.0.get(s.parse()?).await?))
    }

    pub(crate) async fn post<T: Serialize>(&self, url: &str, body: T) -> anyhow::Result<Response> {
        let bytes: Bytes = serde_json::to_vec(&body)?.into();
        let body = Body::from(bytes);
        let request = hyper::Request::post(url)
            .header("Content-Type", "application/json")
            .body(body)?;
        let resp = self.0.request(request).await?;

        if resp.status().is_server_error() {
            anyhow::bail!("request was not successful {:?}", resp.status());
        }

        Ok(Response(resp))
    }

    pub(crate) async fn delete<T: Serialize>(
        &self,
        url: &str,
        body: T,
    ) -> anyhow::Result<Response> {
        let bytes: Bytes = serde_json::to_vec(&body)?.into();
        let body = Body::from(bytes);
        let request = hyper::Request::delete(url)
            .header("Content-Type", "application/json")
            .body(body)?;
        let resp = self.0.request(request).await?;

        Ok(Response(resp))
    }
}
