use crate::hrana::{HranaError, HttpBody, HttpSend, Result};
use bytes::Bytes;
use futures::{ready, Stream};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use worker::wasm_bindgen::JsValue;

#[derive(Debug, Copy, Clone)]
pub struct CloudflareSender(());

impl CloudflareSender {
    pub(crate) fn new() -> Self {
        CloudflareSender(())
    }

    async fn send(url: &str, auth: &str, body: String) -> Result<HttpBody> {
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
            let body = match response.body() {
                ResponseBody::Empty => HttpBody::Body(Bytes::new()),
                ResponseBody::Body(body) => HttpBody::Body(Bytes::from(body.clone())),
                _ => HttpBody::Stream(Box::new(HttpStream(response.stream()?))),
            };
            Ok(body)
        }
    }
}

impl<'a> HttpSend<'a> for CloudflareSender {
    type Result = Pin<Box<dyn Future<Output = Result<HttpBody>> + 'a>>;

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

struct HttpStream(worker::ByteStream);

impl Stream for HttpStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = Pin::new(&mut self.0);
        let res = ready!(pin.poll_next(cx));
        match res {
            None => Poll::Ready(None),
            Some(Ok(data)) => Poll::Ready(Some(Ok(Bytes::from(data)))),
            Some(Err(e)) => Poll::Ready(Some(Err(HranaError::Http(format!(
                "cloudflare HTTP stream error: {}",
                e
            ))))),
        }
    }
}
