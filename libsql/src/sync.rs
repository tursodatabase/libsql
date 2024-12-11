use crate::{util::ConnectorService, Result};

use std::path::Path;

use bytes::Bytes;
use chrono::Utc;
use http::{HeaderValue, StatusCode};
use hyper::Body;
use tokio::io::AsyncWriteExt as _;
use uuid::Uuid;

#[cfg(test)]
mod test;

const METADATA_VERSION: u32 = 0;

const DEFAULT_MAX_RETRIES: usize = 5;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum SyncError {
    #[error("io: msg={msg}, err={err}")]
    Io {
        msg: &'static str,
        #[source]
        err: std::io::Error,
    },
    #[error("invalid auth header: {0}")]
    InvalidAuthHeader(http::header::InvalidHeaderValue),
    #[error("http dispatch error: {0}")]
    HttpDispatch(hyper::Error),
    #[error("body error: {0}")]
    HttpBody(hyper::Error),
    #[error("json decode error: {0}")]
    JsonDecode(serde_json::Error),
    #[error("json value error, unexpected value: {0}")]
    JsonValue(serde_json::Value),
    #[error("json encode error: {0}")]
    JsonEncode(serde_json::Error),
    #[error("failed to push frame: status={0}, error={1}")]
    PushFrame(StatusCode, String),
    #[error("failed to verify metadata file version: expected={0}, got={1}")]
    VerifyVersion(u32, u32),
    #[error("failed to verify metadata file hash: expected={0}, got={1}")]
    VerifyHash(u32, u32),
    #[error("server returned a lower frame_no: sent={0}, got={1}")]
    InvalidPushFrameNoLow(u32, u32),
    #[error("server returned a higher frame_no: sent={0}, got={1}")]
    InvalidPushFrameNoHigh(u32, u32),
    #[error("failed to pull frame: status={0}, error={1}")]
    PullFrame(StatusCode, String),
}

impl SyncError {
    fn io(msg: &'static str) -> impl FnOnce(std::io::Error) -> SyncError {
        move |err| SyncError::Io { msg, err }
    }
}

pub struct SyncContext {
    db_path: String,
    client: hyper::Client<ConnectorService, Body>,
    sync_url: String,
    auth_token: Option<HeaderValue>,
    max_retries: usize,
    /// Represents the max_frame_no from the server.
    durable_frame_num: u32,
    /// Represents the current checkpoint generation.
    generation: u32,
}

impl SyncContext {
    pub async fn new(
        connector: ConnectorService,
        db_path: String,
        sync_url: String,
        auth_token: Option<String>,
    ) -> Result<Self> {
        let client = hyper::client::Client::builder().build::<_, hyper::Body>(connector);

        let auth_token = match auth_token {
            Some(t) => Some(
                HeaderValue::try_from(format!("Bearer {}", t))
                    .map_err(SyncError::InvalidAuthHeader)?,
            ),
            None => None,
        };

        let mut me = Self {
            db_path,
            sync_url,
            auth_token,
            max_retries: DEFAULT_MAX_RETRIES,
            client,
            durable_frame_num: 0,
            generation: 1,
        };

        if let Err(e) = me.read_metadata().await {
            tracing::error!(
                "failed to read sync metadata file, resetting back to defaults: {}",
                e
            );
        }

        Ok(me)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn pull_one_frame(&mut self, generation: u32, frame_no: u32) -> Result<Option<Bytes>> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + 1
        );
        tracing::debug!("pulling frame");
        match self.pull_with_retry(uri, self.max_retries).await? {
            Some(frame) => {
                self.durable_frame_num = frame_no;
                Ok(Some(frame))
            }
            None => Ok(None),
        }
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

        if durable_frame_num > frame_no {
            tracing::error!(
                "server returned durable_frame_num larger than what we sent: sent={}, got={}",
                frame_no,
                durable_frame_num
            );

            return Err(SyncError::InvalidPushFrameNoHigh(frame_no, durable_frame_num).into());
        }

        if durable_frame_num < frame_no {
            // Update our knowledge of where the server is at frame wise.
            self.durable_frame_num = durable_frame_num;

            tracing::debug!(
                "server returned durable_frame_num lower than what we sent: sent={}, got={}",
                frame_no,
                durable_frame_num
            );

            // Return an error and expect the caller to re-call push with the updated state.
            return Err(SyncError::InvalidPushFrameNoLow(frame_no, durable_frame_num).into());
        }

        tracing::debug!(?durable_frame_num, "frame successfully pushed");

        // Update our last known max_frame_no from the server.
        self.durable_frame_num = durable_frame_num;

        Ok(durable_frame_num)
    }

    async fn push_with_retry(&self, uri: String, frame: Bytes, max_retries: usize) -> Result<u32> {
        let mut nr_retries = 0;
        loop {
            let mut req = http::Request::post(uri.clone());

            match &self.auth_token {
                Some(auth_token) => {
                    req.headers_mut()
                        .expect("valid http request")
                        .insert("Authorization", auth_token.clone());
                }
                None => {}
            }

            let req = req.body(frame.clone().into()).expect("valid body");

            let res = self
                .client
                .request(req)
                .await
                .map_err(SyncError::HttpDispatch)?;

            if res.status().is_success() {
                let res_body = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(SyncError::HttpBody)?;

                let resp = serde_json::from_slice::<serde_json::Value>(&res_body[..])
                    .map_err(SyncError::JsonDecode)?;

                let max_frame_no = resp
                    .get("max_frame_no")
                    .ok_or_else(|| SyncError::JsonValue(resp.clone()))?;

                let max_frame_no = max_frame_no
                    .as_u64()
                    .ok_or_else(|| SyncError::JsonValue(max_frame_no.clone()))?;

                return Ok(max_frame_no as u32);
            }

            // If we've retried too many times or the error is not a server error,
            // return the error.
            if nr_retries > max_retries || !res.status().is_server_error() {
                let status = res.status();

                let res_body = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(SyncError::HttpBody)?;

                let msg = String::from_utf8_lossy(&res_body[..]);

                return Err(SyncError::PushFrame(status, msg.to_string()).into());
            }

            let delay = std::time::Duration::from_millis(100 * (1 << nr_retries));
            tokio::time::sleep(delay).await;
            nr_retries += 1;
        }
    }

    async fn pull_with_retry(&self, uri: String, max_retries: usize) -> Result<Option<Bytes>> {
        let mut nr_retries = 0;
        loop {
            let mut req = http::Request::builder().method("GET").uri(uri.clone());

            match &self.auth_token {
                Some(auth_token) => {
                    req = req.header("Authorization", auth_token);
                }
                None => {}
            }

            let req = req.body(Body::empty()).expect("valid request");

            let res = self
                .client
                .request(req)
                .await
                .map_err(SyncError::HttpDispatch)?;

            if res.status().is_success() {
                let frame = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(SyncError::HttpBody)?;
                return Ok(Some(frame));
            }
            if res.status() == StatusCode::BAD_REQUEST {
                return Ok(None);
            }
            // If we've retried too many times or the error is not a server error,
            // return the error.
            if nr_retries > max_retries || !res.status().is_server_error() {
                let status = res.status();

                let res_body = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(SyncError::HttpBody)?;

                let msg = String::from_utf8_lossy(&res_body[..]);

                return Err(SyncError::PullFrame(status, msg.to_string()).into());
            }

            let delay = std::time::Duration::from_millis(100 * (1 << nr_retries));
            tokio::time::sleep(delay).await;
            nr_retries += 1;
        }
    }

    pub(crate) fn durable_frame_num(&self) -> u32 {
        self.durable_frame_num
    }

    pub(crate) fn generation(&self) -> u32 {
        self.generation
    }

    pub(crate) async fn write_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let mut metadata = MetadataJson {
            hash: 0,
            version: METADATA_VERSION,
            durable_frame_num: self.durable_frame_num,
            generation: self.generation,
        };

        metadata.set_hash();

        let contents = serde_json::to_vec(&metadata).map_err(SyncError::JsonEncode)?;

        atomic_write(path, &contents[..]).await?;

        Ok(())
    }

    async fn read_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        if !Path::new(&path).try_exists().map_err(SyncError::io("metadata file exists"))? {
            tracing::debug!("no metadata info file found");
            return Ok(());
        }

        let contents = tokio::fs::read(&path)
            .await
            .map_err(SyncError::io("metadata read"))?;

        let metadata =
            serde_json::from_slice::<MetadataJson>(&contents[..]).map_err(SyncError::JsonDecode)?;

        metadata.verify_hash()?;

        if metadata.version != METADATA_VERSION {
            return Err(SyncError::VerifyVersion(metadata.version, METADATA_VERSION).into());
        }

        tracing::debug!(
            "read sync metadata for db_path={:?}, metadata={:?}",
            self.db_path,
            metadata
        );

        self.durable_frame_num = metadata.durable_frame_num;
        self.generation = metadata.generation;

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct MetadataJson {
    hash: u32,
    version: u32,
    durable_frame_num: u32,
    generation: u32,
}

impl MetadataJson {
    fn calculate_hash(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        // Hash each field in a consistent order
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.durable_frame_num.to_le_bytes());
        hasher.update(&self.generation.to_le_bytes());

        hasher.finalize()
    }

    fn set_hash(&mut self) {
        self.hash = self.calculate_hash();
    }

    fn verify_hash(&self) -> Result<()> {
        let calculated_hash = self.calculate_hash();

        if self.hash == calculated_hash {
            Ok(())
        } else {
            Err(SyncError::VerifyHash(self.hash, calculated_hash).into())
        }
    }
}

async fn atomic_write<P: AsRef<Path>>(path: P, data: &[u8]) -> Result<()> {
    // Create a temporary file in the same directory as the target file
    let directory = path.as_ref().parent().ok_or_else(|| {
        SyncError::io("parent path")(std::io::Error::other(
            "unable to get parent of the provided path",
        ))
    })?;

    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let temp_name = format!(".tmp.{}.{}", timestamp, Uuid::new_v4());
    let temp_path = directory.join(temp_name);

    // Write data to temporary file
    let mut temp_file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(SyncError::io("temp file create"))?;

    temp_file
        .write_all(data)
        .await
        .map_err(SyncError::io("temp file write_all"))?;

    // Ensure all data is flushed to disk
    temp_file
        .sync_all()
        .await
        .map_err(SyncError::io("temp file sync_all"))?;

    // Close the file explicitly
    drop(temp_file);

    // Atomically rename temporary file to target file
    tokio::fs::rename(&temp_path, &path)
        .await
        .map_err(SyncError::io("atomic rename"))?;

    Ok(())
}
