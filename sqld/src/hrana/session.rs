use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};

use super::conn::ProtocolError;
use super::handshake::Protocol;
use super::{proto, Server};
use crate::auth::{AuthError, Authenticated};
use crate::database::Database;
use crate::hrana::batch::{
    execute_batch, execute_sequence, proto_batch_to_program, proto_sequence_to_program, BatchError,
};
use crate::hrana::stmt::{
    describe_stmt, execute_stmt, proto_sql_to_sql, proto_stmt_to_query, StmtError,
};

/// Session-level state of an authenticated Hrana connection.
pub struct Session {
    authenticated: Authenticated,
    protocol: Protocol,
    streams: HashMap<i32, StreamHandle>,
    sqls: HashMap<i32, String>,
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

    #[error("SQL text {sql_id} not found")]
    SqlNotFound { sql_id: i32 },
    #[error("SQL text {sql_id} already exists")]
    SqlExists { sql_id: i32 },

    #[error(transparent)]
    Batch(BatchError),
    #[error(transparent)]
    Stmt(StmtError),
}

pub(super) fn handle_initial_hello(
    server: &Server,
    protocol: Protocol,
    jwt: Option<String>,
) -> Result<Session> {
    let authenticated = server
        .auth
        .authenticate_jwt(jwt.as_deref())
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))?;

    Ok(Session {
        authenticated,
        protocol,
        streams: HashMap::new(),
        sqls: HashMap::new(),
    })
}

pub(super) fn handle_repeated_hello(
    server: &Server,
    session: &mut Session,
    jwt: Option<String>,
) -> Result<()> {
    if session.protocol < Protocol::Hrana2 {
        bail!(ProtocolError::from_message(
            "Hello message can only be sent once in protocol version below 2"
        ))
    }

    session.authenticated = server
        .auth
        .authenticate_jwt(jwt.as_deref())
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))?;
    Ok(())
}

pub(super) async fn handle_request(
    server: &Server,
    session: &mut Session,
    join_set: &mut tokio::task::JoinSet<()>,
    req: proto::Request,
) -> Result<oneshot::Receiver<Result<proto::Response>>> {
    // TODO: this function has rotten: it is too long and contains too much duplicated code. It
    // should be refactored at the next opportunity, together with code in stmt.rs and batch.rs

    let (resp_tx, resp_rx) = oneshot::channel();

    macro_rules! stream_respond {
        ($stream_hnd:expr, async move |$stream:ident| { $($body:tt)* }) => {
            stream_respond($stream_hnd, resp_tx, move |$stream| {
                Box::pin(async move { $($body)* })
            })
            .await
        };
    }

    macro_rules! respond {
        ($value:expr) => {
            resp_tx.send(Ok($value)).unwrap()
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

            let query = proto_stmt_to_query(&req.stmt, &session.sqls, session.protocol)
                .map_err(wrap_stmt_error)?;
            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                let result = execute_stmt(&**db, auth, query)
                    .await
                    .map_err(wrap_stmt_error)?;
                Ok(proto::Response::Execute(proto::ExecuteResp { result }))
            });
        }
        proto::Request::Batch(req) => {
            let stream_id = req.stream_id;
            let Some(stream_hnd) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let pgm = proto_batch_to_program(&req.batch, &session.sqls, session.protocol)
                .map_err(wrap_batch_error)?;
            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                let result = execute_batch(&**db, auth, pgm)
                    .await
                    .map_err(wrap_batch_error)?;
                Ok(proto::Response::Batch(proto::BatchResp { result }))
            });
        }
        proto::Request::Sequence(req) => {
            if session.protocol < Protocol::Hrana2 {
                bail!(ProtocolError::from_message(
                    "The `sequence` request is only supported in protocol version 2 and higher"
                ))
            }

            let stream_id = req.stream_id;
            let Some(stream_hnd) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let sql = proto_sql_to_sql(
                req.sql.as_deref(),
                req.sql_id,
                &session.sqls,
                session.protocol,
            )?;
            let pgm = proto_sequence_to_program(sql).map_err(wrap_batch_error)?;
            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                execute_sequence(&**db, auth, pgm)
                    .await
                    .map_err(wrap_stmt_error)?;
                Ok(proto::Response::Sequence(proto::SequenceResp {}))
            });
        }
        proto::Request::Describe(req) => {
            if session.protocol < Protocol::Hrana2 {
                bail!(ProtocolError::from_message(
                    "The `describe` request is only supported in protocol version 2 and higher"
                ))
            }

            let stream_id = req.stream_id;
            let Some(stream_hnd) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let sql = proto_sql_to_sql(
                req.sql.as_deref(),
                req.sql_id,
                &session.sqls,
                session.protocol,
            )?
            .into();
            let auth = session.authenticated;
            stream_respond!(stream_hnd, async move |stream| {
                let Some(db) = stream.db.as_ref() else {
                    bail!(ResponseError::StreamNotOpen { stream_id })
                };
                let result = describe_stmt(&**db, auth, sql)
                    .await
                    .map_err(wrap_stmt_error)?;
                Ok(proto::Response::Describe(proto::DescribeResp { result }))
            });
        }
        proto::Request::StoreSql(req) => {
            if session.protocol < Protocol::Hrana2 {
                bail!(ProtocolError::from_message(
                    "The `store_sql` request is only supported in protocol version 2 and higher"
                ))
            }

            let sql_id = req.sql_id;
            if session.sqls.contains_key(&sql_id) {
                bail!(ResponseError::SqlExists { sql_id })
            }

            session.sqls.insert(sql_id, req.sql);
            respond!(proto::Response::StoreSql(proto::StoreSqlResp {}));
        }
        proto::Request::CloseSql(req) => {
            if session.protocol < Protocol::Hrana2 {
                bail!(ProtocolError::from_message(
                    "The `close_sql` request is only supported in protocol version 2 and higher"
                ))
            }

            session.sqls.remove(&req.sql_id);
            respond!(proto::Response::CloseSql(proto::CloseSqlResp {}));
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

fn wrap_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<StmtError>() {
        Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
        Err(err) => err,
    }
}

fn wrap_batch_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<BatchError>() {
        Ok(batch_err) => anyhow!(ResponseError::Batch(batch_err)),
        Err(err) => err,
    }
}

impl ResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Auth { source } => source.code(),
            Self::StreamNotFound { .. } => "STREAM_NOT_FOUND",
            Self::StreamExists { .. } => "STREAM_EXISTS",
            Self::StreamNotOpen { .. } => "STREAM_NOT_OPEN",
            Self::SqlNotFound { .. } => "SQL_NOT_FOUND",
            Self::SqlExists { .. } => "SQL_EXISTS",
            Self::Batch(err) => err.code(),
            Self::Stmt(err) => err.code(),
        }
    }
}
