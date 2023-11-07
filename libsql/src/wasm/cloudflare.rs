use crate::hrana::pipeline::ServerMsg;
use crate::hrana::{HranaError, HttpSend, Result};
use std::future::Future;
use std::pin::Pin;
use worker::wasm_bindgen::JsValue;

#[derive(Debug, Copy, Clone)]
pub struct CloudflareSender(());

impl CloudflareSender {
    pub(crate) fn new() -> Self {
        CloudflareSender(())
    }

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
    type Result = Pin<Box<dyn Future<Output = Result<ServerMsg>> + 'a>>;

    fn http_send(&self, url: String, auth: String, body: String) -> Self::Result {
        let fut = Self::send(url, auth, body);
        Box::pin(fut)
    }
}

impl From<worker::Error> for HranaError {
    fn from(value: worker::Error) -> Self {
        // This converts it to a string due to the error type being !Send/!Sync which will break
        // a lot of stuff.
        HranaError::Http(value.to_string())
    }
}
