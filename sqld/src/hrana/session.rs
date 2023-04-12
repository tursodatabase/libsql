use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};

use super::{proto, Server};
use crate::auth::{AuthError, Authenticated};
use crate::database::Database;
use crate::hrana::batch::{execute_batch, BatchError};
use crate::hrana::stmt::{execute_stmt, StmtError};

/// Session-level state of an authenticated Hrana connection.
pub struct Session {
    authenticated: Authenticated,
    streams: HashMap<i32, StreamHandle>,
}

struct StreamHandle {
    job_tx: mpsc::Sender<StreamJob>,
}

/// An arbitrary job that is executed on a [`Stream`].
///
/// All jobs are executed sequentially on a single task (as evidenced by the `&mut Stream` passed
/// to `f`).
struct StreamJob {
    /// The async function which performs the job.
    #[allow(clippy::type_complexity)]
    f: Box<dyn for<'s> FnOnce(&'s mut Stream) -> BoxFuture<'s, Result<proto::Response>> + Send>,
    /// The result of `f` will be sent here.
    resp_tx: oneshot::Sender<Result<proto::Response>>,
}

/// State of a Hrana stream, which corresponds to a standalone database connection.
struct Stream {
    /// The database handle is `None` when the stream is created, and normally set to `Some` by the
    /// first job executed on the stream by the [`proto::OpenStreamReq`] request. However, if that
    /// request returns an error, the following requests may encounter a `None` here.
    db: Option<Arc<dyn Database>>,
}

/// An error which can be converted to a Hrana [Error][proto::Error].
///
/// In the future, we may want to extend Hrana errors with a machine readable reason code, which
/// will correspond to a variant of this enum.
#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Authentication failed: {source}")]
    Auth { source: AuthError },

    #[error("Stream {stream_id} not found")]
    StreamNotFound { stream_id: i32 },
    #[error("Stream {stream_id} already exists")]
    StreamExists { stream_id: i32 },
    #[error("Stream {stream_id} has failed to open")]
    StreamNotOpen { stream_id: i32 },

    #[error(transparent)]
    Batch(BatchError),
    #[error(transparent)]
    Stmt(StmtError),
}

pub(super) async fn handle_hello(server: &Server, jwt: Option<String>) -> Result<Session> {
    let authenticated = server
        .auth
        .authenticate_jwt(jwt.as_deref())
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))?;

    Ok(Session {
        authenticated,
        streams: HashMap::new(),
    })
}

pub(super) async fn handle_request(
    server: &Server,
    session: &mut Session,
    join_set: &mut tokio::task::JoinSet<()>,
    req: proto::Request,
) -> Result<oneshot::Receiver<Result<proto::Response>>> {
    let (resp_tx, resp_rx) = oneshot::channel();

    macro_rules! stream_respond {
        ($stream_hnd:expr, async move |$stream:ident| { $($body:tt)* }) => {
            stream_respond($stream_hnd, resp_tx, move |$stream| {
                Box::pin(async move { $($body)* })
            })
            .await
        };
    }

    match req {
        proto::Request::OpenStream(req) => {
            let stream_id = req.stream_id;
            if session.streams.contains_key(&stream_id) {
                bail!(ResponseError::StreamExists { stream_id })
            }

            let mut stream_hnd = stream_spawn(join_set, Stream { db: None });

            let db_factory = server.db_factory.clone();
            stream_respond!(&mut stream_hnd, async move |stream| {
                let db = db_factory
                    .create()
                    .await
                    .context("Could not create a database connection")?;
                stream.db = Some(db);
                Ok(proto::Response::OpenStream(proto::OpenStreamResp {}))
            });

            session.streams.insert(stream_id, stream_hnd);
        }
        proto::Request::CloseStream(req) => {
            let stream_id = req.stream_id;
            let Some(mut stream_hnd) = session.streams.remove(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            stream_respond!(&mut stream_hnd, async move |_stream| {
                Ok(proto::Response::CloseStream(proto::CloseStreamResp {}))
            });
        }
        proto::Request::Execute(req) => {
            let stream_id = req.stream_id;
            let Some(stream_hnd) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                match execute_stmt(&**db, auth, &req.stmt).await {
                    Ok(result) => Ok(proto::Response::Execute(proto::ExecuteResp { result })),
                    Err(err) => bail!(ResponseError::Stmt(err.downcast::<StmtError>()?)),
                }
            });
        }
        proto::Request::Batch(req) => {
            let stream_id = req.stream_id;
            let Some(stream_hnd) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                match execute_batch(&**db, auth, &req.batch).await {
                    Ok(result) => Ok(proto::Response::Batch(proto::BatchResp { result })),
                    Err(err) => bail!(ResponseError::Batch(err.downcast::<BatchError>()?)),
                }
            });
        }
    }
    Ok(resp_rx)
}

fn stream_spawn(join_set: &mut tokio::task::JoinSet<()>, stream: Stream) -> StreamHandle {
    let (job_tx, mut job_rx) = mpsc::channel::<StreamJob>(8);
    join_set.spawn(async move {
        let mut stream = stream;
        while let Some(job) = job_rx.recv().await {
            let res = (job.f)(&mut stream).await;
            let _: Result<_, _> = job.resp_tx.send(res);
        }
    });
    StreamHandle { job_tx }
}

async fn stream_respond<F>(
    stream_hnd: &mut StreamHandle,
    resp_tx: oneshot::Sender<Result<proto::Response>>,
    f: F,
) where
    for<'s> F: FnOnce(&'s mut Stream) -> BoxFuture<'s, Result<proto::Response>>,
    F: Send + 'static,
{
    let job = StreamJob {
        f: Box::new(f),
        resp_tx,
    };
    let _: Result<_, _> = stream_hnd.job_tx.send(job).await;
}

impl ResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Auth { source } => source.code(),
            Self::StreamNotFound { .. } => "STREAM_NOT_FOUND",
            Self::StreamExists { .. } => "STREAM_EXISTS",
            Self::StreamNotOpen { .. } => "STREAM_NOT_OPEN",
            Self::Batch(err) => err.code(),
            Self::Stmt(err) => err.code(),
        }
    }
}
