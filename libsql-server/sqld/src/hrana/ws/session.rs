use std::collections::HashMap;

use anyhow::{anyhow, bail, Context as _, Result};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};

use super::super::{batch, stmt, ProtocolError, Version};
use super::{proto, Server};
use crate::auth::{AuthError, Authenticated};
use crate::database::Database;

/// Session-level state of an authenticated Hrana connection.
pub struct Session<D> {
    authenticated: Authenticated,
    version: Version,
    streams: HashMap<i32, StreamHandle<D>>,
    sqls: HashMap<i32, String>,
}

struct StreamHandle<D> {
    job_tx: mpsc::Sender<StreamJob<D>>,
}

/// An arbitrary job that is executed on a [`Stream`].
///
/// All jobs are executed sequentially on a single task (as evidenced by the `&mut Stream` passed
/// to `f`).
struct StreamJob<D> {
    /// The async function which performs the job.
    #[allow(clippy::type_complexity)]
    f: Box<dyn for<'s> FnOnce(&'s mut Stream<D>) -> BoxFuture<'s, Result<proto::Response>> + Send>,
    /// The result of `f` will be sent here.
    resp_tx: oneshot::Sender<Result<proto::Response>>,
}

/// State of a Hrana stream, which corresponds to a standalone database connection.
struct Stream<D> {
    /// The database handle is `None` when the stream is created, and normally set to `Some` by the
    /// first job executed on the stream by the [`proto::OpenStreamReq`] request. However, if that
    /// request returns an error, the following requests may encounter a `None` here.
    db: Option<D>,
}

/// An error which can be converted to a Hrana [Error][proto::Error].
#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Authentication failed: {source}")]
    Auth { source: AuthError },
    #[error("Stream {stream_id} has failed to open")]
    StreamNotOpen { stream_id: i32 },
    #[error("The server already stores {count} SQL texts, it cannot store more")]
    SqlTooMany { count: usize },
    #[error(transparent)]
    Stmt(stmt::StmtError),
    #[error(transparent)]
    Batch(batch::BatchError),
}

pub(super) fn handle_initial_hello<D: Database>(
    server: &Server<D>,
    version: Version,
    jwt: Option<String>,
) -> Result<Session<D>> {
    let authenticated = server
        .auth
        .authenticate_jwt(jwt.as_deref())
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))?;

    Ok(Session {
        authenticated,
        version,
        streams: HashMap::new(),
        sqls: HashMap::new(),
    })
}

pub(super) fn handle_repeated_hello<DB: Database>(
    server: &Server<DB>,
    session: &mut Session<DB>,
    jwt: Option<String>,
) -> Result<()> {
    if session.version < Version::Hrana2 {
        bail!(ProtocolError::NotSupported {
            what: "Repeated hello message",
            min_version: Version::Hrana2,
        })
    }

    session.authenticated = server
        .auth
        .authenticate_jwt(jwt.as_deref())
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))?;
    Ok(())
}

pub(super) async fn handle_request<DB: Database>(
    server: &Server<DB>,
    session: &mut Session<DB>,
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

    macro_rules! ensure_version {
        ($min_version:expr, $what:expr) => {
            if session.version < $min_version {
                bail!(ProtocolError::NotSupported {
                    what: $what,
                    min_version: $min_version,
                })
            }
        };
    }

    macro_rules! get_stream_mut {
        ($stream_id:expr) => {
            match session.streams.get_mut(&$stream_id) {
                Some(stream_hdn) => stream_hdn,
                None => bail!(ProtocolError::StreamNotFound {
                    stream_id: $stream_id
                }),
            }
        };
    }

    macro_rules! get_stream_db {
        ($stream:expr, $stream_id:expr) => {
            match $stream.db.as_ref() {
                Some(db) => db,
                None => bail!(ResponseError::StreamNotOpen {
                    stream_id: $stream_id
                }),
            }
        };
    }

    match req {
        proto::Request::OpenStream(req) => {
            let stream_id = req.stream_id;
            if session.streams.contains_key(&stream_id) {
                bail!(ProtocolError::StreamExists { stream_id })
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
                bail!(ProtocolError::StreamNotFound { stream_id })
            };

            stream_respond!(&mut stream_hnd, async move |_stream| {
                Ok(proto::Response::CloseStream(proto::CloseStreamResp {}))
            });
        }
        proto::Request::Execute(req) => {
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            let query = stmt::proto_stmt_to_query(&req.stmt, &session.sqls, session.version)
                .map_err(catch_stmt_error)?;
            let auth = session.authenticated;

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = stmt::execute_stmt(db, auth, query)
                    .await
                    .map_err(catch_stmt_error)?;
                Ok(proto::Response::Execute(proto::ExecuteResp { result }))
            });
        }
        proto::Request::Batch(req) => {
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            let pgm = batch::proto_batch_to_program(&req.batch, &session.sqls, session.version)
                .map_err(catch_stmt_error)?;
            let auth = session.authenticated;

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = batch::execute_batch(db, auth, pgm)
                    .await
                    .map_err(catch_batch_error)?;
                Ok(proto::Response::Batch(proto::BatchResp { result }))
            });
        }
        proto::Request::Sequence(req) => {
            ensure_version!(Version::Hrana2, "The `sequence` request");
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            let sql = stmt::proto_sql_to_sql(
                req.sql.as_deref(),
                req.sql_id,
                &session.sqls,
                session.version,
            )?;
            let pgm = batch::proto_sequence_to_program(sql).map_err(catch_stmt_error)?;
            let auth = session.authenticated;

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                batch::execute_sequence(db, auth, pgm)
                    .await
                    .map_err(catch_stmt_error)
                    .map_err(catch_batch_error)?;
                Ok(proto::Response::Sequence(proto::SequenceResp {}))
            });
        }
        proto::Request::Describe(req) => {
            ensure_version!(Version::Hrana2, "The `describe` request");
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            let sql = stmt::proto_sql_to_sql(
                req.sql.as_deref(),
                req.sql_id,
                &session.sqls,
                session.version,
            )?
            .into();
            let auth = session.authenticated;

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = stmt::describe_stmt(db, auth, sql)
                    .await
                    .map_err(catch_stmt_error)?;
                Ok(proto::Response::Describe(proto::DescribeResp { result }))
            });
        }
        proto::Request::StoreSql(req) => {
            ensure_version!(Version::Hrana2, "The `store_sql` request");
            let sql_id = req.sql_id;
            if session.sqls.contains_key(&sql_id) {
                bail!(ProtocolError::SqlExists { sql_id })
            } else if session.sqls.len() >= MAX_SQL_COUNT {
                bail!(ResponseError::SqlTooMany {
                    count: session.sqls.len()
                })
            }

            session.sqls.insert(sql_id, req.sql);
            respond!(proto::Response::StoreSql(proto::StoreSqlResp {}));
        }
        proto::Request::CloseSql(req) => {
            ensure_version!(Version::Hrana2, "The `close_sql` request");
            session.sqls.remove(&req.sql_id);
            respond!(proto::Response::CloseSql(proto::CloseSqlResp {}));
        }
    }
    Ok(resp_rx)
}

const MAX_SQL_COUNT: usize = 150;

fn stream_spawn<D: Database>(
    join_set: &mut tokio::task::JoinSet<()>,
    stream: Stream<D>,
) -> StreamHandle<D> {
    let (job_tx, mut job_rx) = mpsc::channel::<StreamJob<D>>(8);
    join_set.spawn(async move {
        let mut stream = stream;
        while let Some(job) = job_rx.recv().await {
            let res = (job.f)(&mut stream).await;
            let _: Result<_, _> = job.resp_tx.send(res);
        }
    });
    StreamHandle { job_tx }
}

async fn stream_respond<F, D>(
    stream_hnd: &mut StreamHandle<D>,
    resp_tx: oneshot::Sender<Result<proto::Response>>,
    f: F,
) where
    for<'s> F: FnOnce(&'s mut Stream<D>) -> BoxFuture<'s, Result<proto::Response>>,
    F: Send + 'static,
{
    let job = StreamJob {
        f: Box::new(f),
        resp_tx,
    };
    let _: Result<_, _> = stream_hnd.job_tx.send(job).await;
}

fn catch_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<stmt::StmtError>() {
        Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
        Err(err) => err,
    }
}

fn catch_batch_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<batch::BatchError>() {
        Ok(batch_err) => anyhow!(ResponseError::Batch(batch_err)),
        Err(err) => err,
    }
}

impl ResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Auth { source } => source.code(),
            Self::SqlTooMany { .. } => "SQL_STORE_TOO_MANY",
            Self::StreamNotOpen { .. } => "STREAM_NOT_OPEN",
            Self::Stmt(err) => err.code(),
            Self::Batch(err) => err.code(),
        }
    }
}
