use crate::hrana::connection::HttpConnection;
use crate::hrana::pipeline::ServerMsg;
use crate::hrana::{HranaError, HttpSend, Result};
use crate::params::IntoParams;
use crate::Rows;
use futures::future::LocalBoxFuture;
use worker::wasm_bindgen::JsValue;

#[derive(Debug, Clone)]
pub struct Connection {
    conn: HttpConnection<CloudflareSender>,
}

impl Connection {
    pub fn open(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Connection {
            conn: HttpConnection::new(url.into(), auth_token.into(), CloudflareSender),
        }
    }

    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> crate::Result<u64> {
        tracing::trace!("executing `{}`", sql);
        let mut stmt = crate::hrana::Statement::new(self.conn.clone(), sql.to_string(), true);
        let rows = stmt.execute(&params.into_params()?).await?;
        Ok(rows as u64)
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> crate::Result<Rows> {
        tracing::trace!("querying `{}`", sql);
        let mut stmt = crate::hrana::Statement::new(self.conn.clone(), sql.to_string(), true);
        stmt.query(&params.into_params()?).await
    }
}

#[derive(Debug, Copy, Clone)]
struct CloudflareSender;

impl CloudflareSender {
    async fn send(url: String, auth: String, body: String) -> Result<ServerMsg> {
        use worker::*;

        let mut response = Fetch::Request(Request::new_with_init(
            &url,
            &RequestInit {
                body: Some(JsValue::from(body)),
                headers: {
                    let mut headers = Headers::new();
                    headers.append("Authorization", &auth)?;
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
    type Result = LocalBoxFuture<'a, Result<ServerMsg>>;

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
