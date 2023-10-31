use crate::{Error, HttpSend, Result, ServerMsg};
use futures::future::LocalBoxFuture;

#[derive(Debug, Copy, Clone)]
pub struct CloudflareSender;

impl CloudflareSender {
    async fn send(url: String, auth: String, body: String) -> Result<ServerMsg> {
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
            Err(Error::Api(body))
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

impl From<worker::Error> for Error {
    fn from(value: worker::Error) -> Self {
        Error::Http(value.to_string())
    }
}
