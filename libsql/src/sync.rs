use crate::{util::ConnectorService, Result};
use bytes::Bytes;
use hyper::Body;

const DEFAULT_MAX_RETRIES: usize = 5;

pub struct SyncContext {
    sync_url: String,
    auth_token: Option<String>,
    max_retries: usize,
    durable_frame_num: u32,
    client: hyper::Client<ConnectorService, Body>,
}

impl SyncContext {
    pub fn new(connector: ConnectorService, sync_url: String, auth_token: Option<String>) -> Self {
        // TODO(lucio): add custom connector + tls support here
        let client = hyper::client::Client::builder().build::<_, hyper::Body>(connector);

        Self {
            sync_url,
            auth_token,
            durable_frame_num: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            client,
        }
    }

    pub(crate) async fn push_one_frame(
        &self,
        frame: Bytes,
        generation: u32,
        frame_no: u32,
    ) -> Result<u32> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + 1
        );
        let max_frame_no = self.push_with_retry(uri, frame, self.max_retries).await?;

        Ok(max_frame_no)
    }

    async fn push_with_retry(&self, uri: String, frame: Bytes, max_retries: usize) -> Result<u32> {
        let mut nr_retries = 0;
        loop {
            let mut req = http::Request::post(uri.clone());

            match &self.auth_token {
                Some(auth_token) => {
                    let auth_header =
                        http::HeaderValue::try_from(format!("Bearer {}", auth_token.to_owned()))
                            .unwrap();

                    req.headers_mut()
                        .expect("valid http request")
                        .insert("Authorization", auth_header);
                }
                None => {}
            }

            // TODO(lucio): convert this to use bytes to make this clone cheap, it should be
            // to possible use BytesMut when reading frames from the WAL and efficiently use Bytes
            // from that.
            let req = req.body(frame.clone().into()).expect("valid body");

            let res = self.client.request(req).await.unwrap();

            // TODO(lucio): only retry on server side errors
            if res.status().is_success() {
                let res_body = hyper::body::to_bytes(res.into_body()).await.unwrap();
                let resp = serde_json::from_slice::<serde_json::Value>(&res_body[..]).unwrap();

                let max_frame_no = resp.get("max_frame_no").unwrap().as_u64().unwrap();
                return Ok(max_frame_no as u32);
            }

            if nr_retries > max_retries {
                return Err(crate::errors::Error::ConnectionFailed(format!(
                    "Failed to push frame: {}",
                    res.status()
                )));
            }
            let delay = std::time::Duration::from_millis(100 * (1 << nr_retries));
            tokio::time::sleep(delay).await;
            nr_retries += 1;
        }
    }

    pub(crate) fn durable_frame_num(&self) -> u32 {
        self.durable_frame_num
    }
}
