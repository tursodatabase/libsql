const DEFAULT_MAX_RETRIES: usize = 5;

use bytes::Bytes;
use http::{HeaderValue, Request, Uri};
use tokio::sync::Mutex;

use crate::{util::ConnectorService, Result};

pub struct SyncContext {
    sync_url: String,
    auth_token: Option<String>,
    max_retries: usize,
    durable_frame_num: u32,
    db_path: String,
    max_frame_no: u32,

    client: hyper::Client<ConnectorService, hyper::Body>,
}

impl SyncContext {
    pub async fn new(
        sync_url: String,
        auth_token: Option<String>,
        db_path: impl Into<String>,
        connector: ConnectorService,
    ) -> Self {
        let mut ctx = Self {
            sync_url,
            auth_token,
            durable_frame_num: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            db_path: db_path.into(),
            max_frame_no: 0,
            client: hyper::Client::builder().build(connector),
        };

        ctx.read_and_update_metadata().await.unwrap();

        ctx
    }

    pub(crate) async fn send_frame(
        &mut self,
        frame: Bytes,
        generation: u32,
        frame_no: u32,
    ) -> Result<u32> {
        let url = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + 1
        );

        let maybe_auth_header = if let Some(auth_token) = &self.auth_token {
            Some(HeaderValue::from_str(&format!("Bearer {}", auth_token)).unwrap())
        } else {
            None
        };

        let mut attempts = 0;

        loop {
            let mut req = Request::post(url.clone());

            if let Some(auth_header) = &maybe_auth_header {
                req.headers_mut()
                    .unwrap()
                    .insert("Authorization", auth_header.clone());
            }

            let req = req.body(frame.clone().into()).unwrap();

            let res = self.client.request(req).await.unwrap();

            if res.status().is_success() {
                let body = hyper::body::to_bytes(res.into_body()).await.unwrap();

                let resp = serde_json::from_slice::<serde_json::Value>(&body[..]).unwrap();

                let max_frame_no = resp.get("max_frame_no").unwrap().as_u64().unwrap() as u32;

                // Update our best known max_frame_no from the server and write it to disk.
                self.set_max_frame_no(max_frame_no).await.unwrap();

                return Ok(max_frame_no);
            } else if res.status().is_server_error() || attempts < self.max_retries {
                let delay = std::time::Duration::from_millis(100 * (1 << attempts));
                tokio::time::sleep(delay).await;
                attempts += 1;

                continue;
            } else {
                return Err(crate::errors::Error::ConnectionFailed(format!(
                    "Failed to push frame: {}",
                    res.status()
                )));
            }
        }
    }

    pub(crate) fn max_frame_no(&self) -> u32 {
        self.max_frame_no
    }

    pub(crate) fn durable_frame_num(&self) -> u32 {
        self.durable_frame_num
    }

    pub(crate) async fn set_max_frame_no(&mut self, max_frame_no: u32) -> Result<()> {
        // TODO: check if max_frame_no is larger than current known max_frame_no
        self.max_frame_no = max_frame_no;

        self.update_metadata().await?;

        Ok(())
    }

    async fn update_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let contents = serde_json::to_vec(&MetadataJson {
            max_frame_no: self.max_frame_no,
        })
        .unwrap();

        tokio::fs::write(path, contents).await.unwrap();

        Ok(())
    }

    async fn read_and_update_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let contents = tokio::fs::read(&path).await.unwrap();

        let metadata = serde_json::from_slice::<MetadataJson>(&contents[..]).unwrap();

        self.max_frame_no = metadata.max_frame_no;

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetadataJson {
    max_frame_no: u32,
}
