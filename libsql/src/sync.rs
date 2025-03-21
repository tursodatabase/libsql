use crate::{local::Connection, util::ConnectorService, Error, Result};

use std::path::Path;

use bytes::Bytes;
use chrono::Utc;
use http::{HeaderValue, StatusCode};
use hyper::Body;
use tokio::io::AsyncWriteExt as _;
use uuid::Uuid;

#[cfg(test)]
mod test;

pub mod connection;
pub mod statement;
pub mod transaction;

const METADATA_VERSION: u32 = 0;

const DEFAULT_MAX_RETRIES: usize = 5;
const DEFAULT_PUSH_BATCH_SIZE: u32 = 128;

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
    #[error("server returned a conflict: sent={0}, got={1}")]
    InvalidPushFrameConflict(u32, u32),
    #[error("failed to pull frame: status={0}, error={1}")]
    PullFrame(StatusCode, String),
    #[error("failed to get location header for redirect: {0}")]
    RedirectHeader(http::header::ToStrError),
    #[error("redirect response with no location header")]
    NoRedirectLocationHeader,
}

impl SyncError {
    fn io(msg: &'static str) -> impl FnOnce(std::io::Error) -> SyncError {
        move |err| SyncError::Io { msg, err }
    }
}

pub struct PushResult {
    status: PushStatus,
    generation: u32,
    max_frame_no: u32,
}

pub enum PushStatus {
    Ok,
    Conflict,
}

pub enum PullResult {
    /// A frame was successfully pulled.
    Frame(Bytes),
    /// We've reached the end of the generation.
    EndOfGeneration { max_generation: u32 },
}

pub struct SyncContext {
    db_path: String,
    client: hyper::Client<ConnectorService, Body>,
    sync_url: String,
    auth_token: Option<HeaderValue>,
    max_retries: usize,
    push_batch_size: u32,
    /// The current durable generation.
    durable_generation: u32,
    /// Represents the max_frame_no from the server.
    durable_frame_num: u32,
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
            push_batch_size: DEFAULT_PUSH_BATCH_SIZE,
            client,
            durable_generation: 1,
            durable_frame_num: 0,
        };

        if let Err(e) = me.read_metadata().await {
            tracing::error!(
                "failed to read sync metadata file, resetting back to defaults: {}",
                e
            );
        }

        Ok(me)
    }

    pub fn set_push_batch_size(&mut self, push_batch_size: u32) {
        self.push_batch_size = push_batch_size;
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn pull_one_frame(
        &mut self,
        generation: u32,
        frame_no: u32,
    ) -> Result<PullResult> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + 1
        );
        tracing::debug!("pulling frame");
        self.pull_with_retry(uri, self.max_retries).await
    }

    #[tracing::instrument(skip(self, frames))]
    pub(crate) async fn push_frames(
        &mut self,
        frames: Bytes,
        generation: u32,
        frame_no: u32,
        frames_count: u32,
    ) -> Result<u32> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + frames_count
        );
        tracing::debug!("pushing frame");

        let result = self.push_with_retry(uri, frames, self.max_retries).await?;

        match result.status {
            PushStatus::Conflict => {
                return Err(SyncError::InvalidPushFrameConflict(frame_no, result.max_frame_no).into());
            }
            _ => {}
        }
        let generation = result.generation;
        let durable_frame_num = result.max_frame_no;

        if durable_frame_num > frame_no + frames_count - 1 {
            tracing::error!(
                "server returned durable_frame_num larger than what we sent: sent={}, got={}",
                frame_no,
                durable_frame_num
            );

            return Err(SyncError::InvalidPushFrameNoHigh(frame_no, durable_frame_num).into());
        }

        if durable_frame_num < frame_no + frames_count - 1 {
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
        tracing::debug!(?generation, ?durable_frame_num, "updating remote generation and durable_frame_num");
        self.durable_generation = generation;
        self.durable_frame_num = durable_frame_num;

        Ok(durable_frame_num)
    }

    async fn push_with_retry(&self, mut uri: String, body: Bytes, max_retries: usize) -> Result<PushResult> {
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

            let req = req.body(body.clone().into()).expect("valid body");

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

                let status = resp
                    .get("status")
                    .ok_or_else(|| SyncError::JsonValue(resp.clone()))?;

                let status = status
                    .as_str()
                    .ok_or_else(|| SyncError::JsonValue(status.clone()))?;

                let generation = resp
                    .get("generation")
                    .ok_or_else(|| SyncError::JsonValue(resp.clone()))?;

                let generation = generation
                    .as_u64()
                    .ok_or_else(|| SyncError::JsonValue(generation.clone()))?;

                let max_frame_no = resp
                    .get("max_frame_no")
                    .ok_or_else(|| SyncError::JsonValue(resp.clone()))?;

                let max_frame_no = max_frame_no
                    .as_u64()
                    .ok_or_else(|| SyncError::JsonValue(max_frame_no.clone()))?;

                let status = match status {
                    "ok" => PushStatus::Ok,
                    "conflict" => PushStatus::Conflict,
                    _ => return Err(SyncError::JsonValue(resp.clone()).into()),
                };
                let generation = generation as u32; 
                let max_frame_no = max_frame_no as u32;
                return Ok(PushResult { status, generation, max_frame_no });
            }

            if res.status().is_redirection() {
                uri = match res.headers().get(hyper::header::LOCATION) {
                    Some(loc) => loc.to_str().map_err(SyncError::RedirectHeader)?.to_string(),
                    None => return Err(SyncError::NoRedirectLocationHeader.into()),
                };
                if nr_retries == 0 {
                    nr_retries += 1;
                    continue;
                }
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

    async fn pull_with_retry(&self, mut uri: String, max_retries: usize) -> Result<PullResult> {
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
                return Ok(PullResult::Frame(frame));
            }
            // BUG ALERT: The server returns a 500 error if the remote database is empty.
            // This is a bug and should be fixed.
            if res.status() == StatusCode::BAD_REQUEST || res.status() == StatusCode::INTERNAL_SERVER_ERROR {
                let res_body = hyper::body::to_bytes(res.into_body())
                    .await
                    .map_err(SyncError::HttpBody)?;

                let resp = serde_json::from_slice::<serde_json::Value>(&res_body[..])
                    .map_err(SyncError::JsonDecode)?;

                let generation = resp
                    .get("generation")
                    .ok_or_else(|| SyncError::JsonValue(resp.clone()))?;

                let generation = generation
                    .as_u64()
                    .ok_or_else(|| SyncError::JsonValue(generation.clone()))?;
                return Ok(PullResult::EndOfGeneration { max_generation: generation as u32 });
            }
            if res.status().is_redirection() {
                uri = match res.headers().get(hyper::header::LOCATION) {
                    Some(loc) => loc.to_str().map_err(SyncError::RedirectHeader)?.to_string(),
                    None => return Err(SyncError::NoRedirectLocationHeader.into()),
                };
                if nr_retries == 0 {
                    nr_retries += 1;
                    continue;
                }
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


    pub(crate) fn next_generation(&mut self) {
        self.durable_generation += 1;
        self.durable_frame_num = 0;
    }

    pub(crate) fn durable_frame_num(&self) -> u32 {
        self.durable_frame_num
    }

    pub(crate) fn durable_generation(&self) -> u32 {
        self.durable_generation
    }

    pub(crate) async fn write_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        let mut metadata = MetadataJson {
            hash: 0,
            version: METADATA_VERSION,
            durable_frame_num: self.durable_frame_num,
            generation: self.durable_generation,
        };

        metadata.set_hash();

        let contents = serde_json::to_vec(&metadata).map_err(SyncError::JsonEncode)?;

        atomic_write(path, &contents[..]).await?;

        Ok(())
    }

    async fn read_metadata(&mut self) -> Result<()> {
        let path = format!("{}-info", self.db_path);

        if !Path::new(&path)
            .try_exists()
            .map_err(SyncError::io("metadata file exists"))?
        {
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

        self.durable_generation = metadata.generation;
        self.durable_frame_num = metadata.durable_frame_num;

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

/// Sync WAL frames to remote.
pub async fn sync_offline(
    sync_ctx: &mut SyncContext,
    conn: &Connection,
) -> Result<crate::database::Replicated> {
    if is_ahead_of_remote(&sync_ctx, &conn) {
        match try_push(sync_ctx, conn).await {
            Ok(rep) => Ok(rep),
            Err(Error::Sync(err)) => {
                // Retry the sync because we are ahead of the server and we need to push some older
                // frames.
                if let Some(SyncError::InvalidPushFrameNoLow(_, _)) = err.downcast_ref() {
                    tracing::debug!("got InvalidPushFrameNo, retrying push");
                    try_push(sync_ctx, conn).await
                } else {
                    Err(Error::Sync(err))
                }
            }
            Err(e) => Err(e),
        }
    } else {
        try_pull(sync_ctx, conn).await
    }
    .or_else(|err| {
        let Error::Sync(err) = err else {
            return Err(err);
        };

        // TODO(levy): upcasting should be done *only* at the API boundary, doing this in
        // internal code just sucks.
        let Some(SyncError::HttpDispatch(_)) = err.downcast_ref() else {
            return Err(Error::Sync(err));
        };

        Ok(crate::database::Replicated {
            frame_no: None,
            frames_synced: 0,
        })
    })
}

fn is_ahead_of_remote(sync_ctx: &SyncContext, conn: &Connection) -> bool {
    let max_local_frame = conn.wal_frame_count();
    max_local_frame > sync_ctx.durable_frame_num()
}

async fn try_push(
    sync_ctx: &mut SyncContext,
    conn: &Connection,
) -> Result<crate::database::Replicated> {
    let page_size = {
        let rows = conn
            .query("PRAGMA page_size", crate::params::Params::None)?
            .unwrap();
        let row = rows.next()?.unwrap();
        let page_size = row.get::<u32>(0)?;
        page_size
    };

    let max_frame_no = conn.wal_frame_count();
    if max_frame_no == 0 {
        return Ok(crate::database::Replicated {
            frame_no: None,
            frames_synced: 0,
        });
    }

    let generation = sync_ctx.durable_generation();
    let start_frame_no = sync_ctx.durable_frame_num() + 1;
    let end_frame_no = max_frame_no;

    let mut frame_no = start_frame_no;
    while frame_no <= end_frame_no {
        let batch_size = sync_ctx.push_batch_size.min(end_frame_no - frame_no + 1);
        let mut frames = conn.wal_get_frame(frame_no, page_size)?;
        if batch_size > 1 {
            frames.reserve((batch_size - 1) as usize * frames.len());
        }
        for idx in 1..batch_size {
            let frame = conn.wal_get_frame(frame_no + idx, page_size)?;
            frames.extend_from_slice(frame.as_ref())
        }

        // The server returns its maximum frame number. To avoid resending
        // frames the server already knows about, we need to update the
        // frame number to the one returned by the server.
        let max_frame_no = sync_ctx
            .push_frames(frames.freeze(), generation, frame_no, batch_size)
            .await?;

        if max_frame_no > frame_no {
            frame_no = max_frame_no + 1;
        } else {
            frame_no += batch_size;
        }
    }

    sync_ctx.write_metadata().await?;

    // TODO(lucio): this can underflow if the server previously returned a higher max_frame_no
    // than what we have stored here.
    let frame_count = end_frame_no - start_frame_no + 1;
    Ok(crate::database::Replicated {
        frame_no: None,
        frames_synced: frame_count as usize,
    })
}

async fn try_pull(
    sync_ctx: &mut SyncContext,
    conn: &Connection,
) -> Result<crate::database::Replicated> {
    let insert_handle = conn.wal_insert_handle()?;

    let mut err = None;
    
    loop {
        let generation = sync_ctx.durable_generation();
        let frame_no = sync_ctx.durable_frame_num() + 1;
        match sync_ctx.pull_one_frame(generation, frame_no).await {
            Ok(PullResult::Frame(frame)) => {
                insert_handle.insert(&frame)?;
                assert!(conn.check_integrity()?);
                sync_ctx.durable_frame_num = frame_no;
            }
            Ok(PullResult::EndOfGeneration { max_generation }) => {
                // If there are no more generations to pull, we're done.
                if generation >= max_generation {
                    break;
                }
                insert_handle.end()?;
                sync_ctx.write_metadata().await?;

                assert!(conn.check_integrity()?);
                // TODO: Make this crash-proof.
                conn.wal_checkpoint(true)?;
                assert!(conn.check_integrity()?);

                sync_ctx.next_generation();
                sync_ctx.write_metadata().await?;

                insert_handle.begin()?;
            }
            Err(e) => {
                tracing::debug!("pull_one_frame error: {:?}", e);
                err.replace(e);
                break;
            }
        }
    }
    // This is crash-proof because we:
    //
    // 1. Write WAL frame first
    // 2. Write new max frame to temporary metadata
    // 3. Atomically rename the temporary metadata to the real metadata
    //
    // If we crash before metadata rename completes, the old metadata still
    // points to last successful frame, allowing safe retry from that point.
    // If we happen to have the frame already in the WAL, it's fine to re-pull
    // because append locally is idempotent.
    insert_handle.end()?;
    sync_ctx.write_metadata().await?;

    if let Some(err) = err {
        Err(err)
    } else {
        Ok(crate::database::Replicated {
            frame_no: None,
            frames_synced: 1,
        })
    }
}
