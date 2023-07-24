use crate::replication::LogReadError;
use crate::replication::{frame::Frame, primary::frame_stream::FrameStream, ReplicationLogger};
use anyhow::{Context, Result};
use axum::extract::State;
use hyper::{Body, Response};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FramesRequest {
    pub next_offset: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Frames {
    pub frames: Vec<Frame>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Hello {
    pub generation_id: uuid::Uuid,
    pub generation_start_index: u64,
    pub database_id: uuid::Uuid,
}

// Thin wrapper to allow returning anyhow errors from axum
struct AppError(anyhow::Error);

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Replication failed: {}", self.0),
        )
            .into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

pub async fn run(addr: SocketAddr, logger: Arc<ReplicationLogger>) -> Result<()> {
    use axum::routing::{get, post};
    let router = axum::Router::new()
        .route("/hello", get(handle_hello))
        .route("/frames", post(handle_frames))
        .with_state(logger);

    let server = hyper::Server::try_bind(&addr)
        .context("Could not bind admin HTTP API server")?
        .serve(router.into_make_service());

    tracing::info!(
        "Listening for replication HTTP API requests on {}",
        server.local_addr()
    );
    server.await?;
    Ok(())
}

impl Frames {
    pub fn new() -> Self {
        Self { frames: Vec::new() }
    }

    pub fn push(&mut self, frame: Frame) {
        self.frames.push(frame);
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

async fn handle_hello(
    State(logger): State<Arc<ReplicationLogger>>,
) -> std::result::Result<Response<Body>, AppError> {
    let hello = Hello {
        generation_id: logger.generation.id,
        generation_start_index: logger.generation.start_index,
        database_id: logger.database_id()?,
    };

    let resp = Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from(serde_json::to_vec(&hello)?))
        .unwrap();
    Ok(resp)
}

fn error(msg: &str, code: hyper::StatusCode) -> Response<Body> {
    let err = serde_json::json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

async fn handle_frames(
    State(logger): State<Arc<ReplicationLogger>>,
    req: String, // it's a JSON, but Axum errors-out if Content-Type isn't set to json, which is too strict
) -> std::result::Result<Response<Body>, AppError> {
    const MAX_FRAMES_IN_SINGLE_RESPONSE: usize = 256;

    let FramesRequest { next_offset } = match serde_json::from_str(&req) {
        Ok(req) => req,
        Err(resp) => return Ok(error(&resp.to_string(), hyper::StatusCode::BAD_REQUEST)),
    };
    tracing::trace!("Requested next offset: {next_offset}");

    let next_offset = std::cmp::max(next_offset, 1); // Frames start from 1
    let current_frameno = next_offset - 1;
    let mut frame_stream = FrameStream::new(logger.clone(), current_frameno);
    tracing::trace!(
        "Max available frame_no: {}",
        frame_stream.max_available_frame_no
    );
    if frame_stream.max_available_frame_no < next_offset {
        tracing::trace!("No frames available starting {next_offset}, returning 204 No Content");
        return Ok(Response::builder()
            .status(hyper::StatusCode::NO_CONTENT)
            .body(Body::empty())?);
    }

    let mut frames = Frames::new();
    for _ in 0..MAX_FRAMES_IN_SINGLE_RESPONSE {
        use futures::StreamExt;

        match frame_stream.next().await {
            Some(Ok(frame)) => {
                tracing::trace!("Read frame {}", frame_stream.current_frame_no);
                frames.push(frame);
            }
            Some(Err(LogReadError::SnapshotRequired)) => {
                drop(frame_stream);
                if frames.is_empty() {
                    tracing::debug!("Snapshot required, switching to snapshot mode");
                    frames = load_snapshot(logger, next_offset)?;
                } else {
                    tracing::debug!("Snapshot required, but some frames were read - returning.");
                }
                break;
            }
            Some(Err(e)) => {
                tracing::error!("Error reading frame: {}", e);
                return Ok(Response::builder()
                    .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap());
            }
            None => break,
        }

        if frame_stream.max_available_frame_no <= frame_stream.current_frame_no {
            break;
        }
    }

    if frames.is_empty() {
        return Ok(Response::builder()
            .status(hyper::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap());
    }

    Ok(Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from(serde_json::to_string(&frames)?))
        .unwrap())
}

// FIXME: In the HTTP stateless spirit, we just unconditionally send the whole snapshot
// here, which is an obvious overcommit. We should instead stream in smaller parts
// if the snapshot is large.
fn load_snapshot(logger: Arc<ReplicationLogger>, from: u64) -> Result<Frames> {
    let snapshot = match logger.get_snapshot_file(from) {
        Ok(Some(snapshot)) => snapshot,
        _ => {
            tracing::trace!("No snapshot available, returning no frames");
            return Ok(Frames { frames: Vec::new() });
        }
    };
    let mut frames = Frames::new();
    for bytes in snapshot.frames_iter_from(from) {
        frames.push(Frame::try_from_bytes(bytes?)?);
    }
    tracing::trace!(
        "Loaded {} frames from the snapshot file",
        frames.frames.len()
    );
    Ok(frames)
}
