use crate::hrana::{HranaClient, HranaError, HttpSend, ServerMsg};
use crate::params::Params;
use crate::{Result, Rows};
use std::pin::Pin;

pub struct Connection {
    client: HranaClient<CloudflareSender>,
}

impl Connection {
    pub fn open(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        let client = HranaClient::new(url.into(), auth_token.into(), CloudflareSender);
        Connection { client }
    }

    pub async fn query(&self, sql: &str, params: Params) -> Result<Rows> {
        let mut stmt = self.client.prepare(sql);
        stmt.query(&params).await
    }

    pub async fn execute(&self, sql: &str, params: Params) -> Result<usize> {
        let mut stmt = self.client.prepare(sql);
        stmt.execute(&params).await
    }
}

#[derive(Debug, Copy, Clone)]
struct CloudflareSender;

impl CloudflareSender {
    async fn send(
        url: String,
        auth: String,
        body: String,
    ) -> std::result::Result<ServerMsg, HranaError> {
        use worker::*;

        let mut response = Fetch::Request(Request::new_with_init(
            &url,
            &RequestInit {
                body: Some(wasm_bindgen::JsValue::from_str(&body)),
                headers: {
                    let mut headers = Headers::new();
                    headers.append("Authorization", &auth).ok();
                    headers
                },
                cf: CfProperties::new(),
                method: Method::Post,
                redirect: RequestRedirect::Follow,
            },
        )?)
        .send()
        .await?;
        let body = response.text().await?;
        if response.status_code() != 200 {
            Err(HranaError::Api(body))
        } else {
            let msg: ServerMsg = serde_json::from_str(&body)?;
            Ok(msg)
        }
    }
}

impl HttpSend for CloudflareSender {
    type Result = DynFuture<std::result::Result<ServerMsg, HranaError>>;

    fn http_send(&self, url: String, auth: String, body: String) -> Self::Result {
        let fut = Self::send(url, auth, body);
        Box::pin(fut)
    }
}

type DynFuture<T> = Pin<Box<dyn std::future::Future<Output = T> + 'static>>;

impl From<worker::Error> for HranaError {
    fn from(value: worker::Error) -> Self {
        HranaError::Http(value.to_string())
    }
}
