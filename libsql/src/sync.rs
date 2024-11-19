use crate::{util::ConnectorService, Result};
use bytes::Bytes;
use hyper::Body;

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
        let max_frame_no = self.push_with_retry(uri, frame, self.max_retries).await?;

        // Update our last known max_frame_no from the server.
        self.durable_frame_num = max_frame_no;

        self.write_metadata().await?;

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

    async fn write_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let contents = serde_json::to_vec(&MetadataJson {
            version: METADATA_VERSION,
            durable_frame_num: self.durable_frame_num,
        })
        .unwrap();

        tokio::fs::write(path, contents).await.unwrap();

        Ok(())
    }

    async fn read_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

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
