use crate::hrana::pipeline::ServerMsg;
use crate::hrana::{HranaError, HttpSend, Result};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use worker::wasm_bindgen::JsValue;

#[derive(Debug, Copy, Clone)]
pub struct CloudflareSender(());

impl CloudflareSender {
    pub(crate) fn new() -> Self {
        CloudflareSender(())
    }

    async fn send(url: &str, auth: &str, body: String) -> Result<Bytes> {
        use worker::*;

        let mut response = Fetch::Request(Request::new_with_init(
            url,
            &RequestInit {
                body: Some(JsValue::from(body)),
                headers: {
                    let mut headers = Headers::new();
                    headers.append("Authorization", auth)?;
                    headers
                },
                cf: CfProperties::new(),
                method: Method::Post,
                redirect: RequestRedirect::Follow,
            },
        )?)
        .send()
        .await?;
        if response.status_code() != 200 {
            let body = response.text().await?;
            Err(HranaError::Api(body))
        } else {
            let body = response.bytes().await?;
            Ok(Bytes::from(body))
        }
    }
}

impl<'a> HttpSend<'a> for CloudflareSender {
    type Result = Pin<Box<dyn Future<Output = Result<Bytes>> + 'a>>;

    fn http_send(&'a self, url: &'a str, auth: &'a str, body: String) -> Self::Result {
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
