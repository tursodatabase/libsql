use crate::hrana::{HranaClient, HranaError, HttpSend, ServerMsg};
use crate::params::IntoParams;
use crate::{Result, Rows};
use futures::future::LocalBoxFuture;

#[derive(Debug, Clone)]
pub struct Connection {
    client: HranaClient<CloudflareSender>,
}

impl Connection {
    pub fn open(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        let client = HranaClient::new(url.into(), auth_token.into(), CloudflareSender);
        Connection { client }
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.client.prepare(sql);
        stmt.query(&params.into_params()?).await
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<usize> {
        let mut stmt = self.client.prepare(sql);
        stmt.execute(&params.into_params()?).await
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

impl<'a> HttpSend<'a> for CloudflareSender {
    type Result = LocalBoxFuture<'a, std::result::Result<ServerMsg, HranaError>>;

    fn http_send(&self, url: String, auth: String, body: String) -> Self::Result {
        let fut = Self::send(url, auth, body);
        Box::pin(fut)
    }
}

impl From<worker::Error> for HranaError {
    fn from(value: worker::Error) -> Self {
        HranaError::Http(value.to_string())
    }
}
