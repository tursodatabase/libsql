use crate::{util::ConnectorService, Result};

use std::path::Path;

use bytes::Bytes;
use hyper::Body;
use tokio::io::AsyncWriteExt as _;
use uuid::Uuid;

const METADATA_VERSION: u32 = 0;

const DEFAULT_MAX_RETRIES: usize = 5;

pub struct SyncContext {
    db_path: String,
    sync_url: String,
    auth_token: Option<String>,
    max_retries: usize,
    /// Represents the max_frame_no from the server.
    durable_frame_num: u32,
    client: hyper::Client<ConnectorService, Body>,
}

impl SyncContext {
    pub async fn new(
        connector: ConnectorService,
        db_path: String,
        sync_url: String,
        auth_token: Option<String>,
    ) -> Result<Self> {
        let client = hyper::client::Client::builder().build::<_, hyper::Body>(connector);

        let mut me = Self {
            db_path,
            sync_url,
            auth_token,
            durable_frame_num: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            client,
        };

        me.read_metadata().await?;

        Ok(me)
    }

    #[tracing::instrument(skip(self, frame))]
    pub(crate) async fn push_one_frame(
        &mut self,
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
        tracing::debug!("pushing frame");

        let durable_frame_num = self.push_with_retry(uri, frame, self.max_retries).await?;

        tracing::debug!(?durable_frame_num, "frame successfully pushed");

        // Update our last known max_frame_no from the server.
        self.durable_frame_num = durable_frame_num;

        self.write_metadata().await?;

        Ok(durable_frame_num)
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

    async fn write_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let contents = serde_json::to_vec(&MetadataJson {
            version: METADATA_VERSION,
            durable_frame_num: self.durable_frame_num,
        })
        .unwrap();

        atomic_write(path, &contents[..]).await.unwrap();

        Ok(())
    }

    async fn read_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        if !std::fs::exists(&path).unwrap() {
            tracing::debug!("no metadata info file found");
            return Ok(());
        }

        let contents = tokio::fs::read(&path).await.unwrap();

        let metadata = serde_json::from_slice::<MetadataJson>(&contents[..]).unwrap();

        assert_eq!(
            metadata.version, METADATA_VERSION,
            "Reading metadata from a different version than expected"
        );

        self.durable_frame_num = metadata.durable_frame_num;

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetadataJson {
    version: u32,
    durable_frame_num: u32,
}

async fn atomic_write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    // Create a temporary file in the same directory as the target file
    let directory = path.as_ref().parent().unwrap();

    let temp_name = format!(".tmp.{}", Uuid::new_v4());
    let temp_path = directory.join(temp_name);

    // Write data to temporary file
    let mut temp_file = tokio::fs::File::create(&temp_path).await.unwrap();

    temp_file.write_all(data).await.unwrap();

    // Ensure all data is flushed to disk
    temp_file.sync_all().await.unwrap();

    // Close the file explicitly
    drop(temp_file);

    // Atomically rename temporary file to target file
    tokio::fs::rename(&temp_path, &path).await.unwrap();

    Ok(())
}
