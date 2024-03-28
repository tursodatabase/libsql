use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Error, Result};
use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};

use super::super::{batch, cursor, stmt, ProtocolError, Version};
use super::{proto, Server};
use crate::auth::user_auth_strategies::UserAuthContext;
use crate::auth::{Auth, AuthError, Authenticated, Jwt};
use crate::connection::{Connection as _, RequestContext};
use crate::database::Connection;
use crate::namespace::NamespaceName;

/// Session-level state of an authenticated Hrana connection.
pub struct Session {
    auth: Authenticated,
    version: Version,
    streams: HashMap<i32, StreamHandle>,
    sqls: HashMap<i32, String>,
    cursors: HashMap<i32, i32>,
}

impl Session {
    pub fn new(auth: Authenticated, version: Version) -> Self {
        Self {
            auth,
            version,
            streams: HashMap::new(),
            sqls: HashMap::new(),
            cursors: HashMap::new(),
        }
    }

    pub fn update_auth(&mut self, auth: Authenticated) -> Result<(), Error> {
        if self.version < Version::Hrana2 {
            bail!(ProtocolError::NotSupported {
                what: "Repeated hello message",
                min_version: Version::Hrana2,
            })
        }
        self.auth = auth;
        Ok(())
    }
}

struct StreamHandle {
    job_tx: mpsc::Sender<StreamJob>,
    cursor_id: Option<i32>,
}

/// An arbitrary job that is executed on a [`Stream`].
///
/// All jobs are executed sequentially on a single task (as evidenced by the `&mut Stream` passed
/// to `f`).
struct StreamJob {
    /// The async function which performs the job.
    f: Box<dyn for<'s> FnOnce(&'s mut Stream) -> BoxFuture<'s, Result<proto::Response>> + Send>,
    /// The result of `f` will be sent here.
    resp_tx: oneshot::Sender<Result<proto::Response>>,
}

/// State of a Hrana stream, which corresponds to a standalone database connection.
struct Stream {
    /// The database handle is `None` when the stream is created, and normally set to `Some` by the
    /// first job executed on the stream by the [`proto::OpenStreamReq`] request. However, if that
    /// request returns an error, the following requests may encounter a `None` here.
    db: Option<Arc<Connection>>,
    /// Handle to an open cursor, if any.
    cursor_hnd: Option<cursor::CursorHandle>,
}

/// An error which can be converted to a Hrana [Error][proto::Error].
#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Authentication failed: {source}")]
    Auth { source: AuthError },
    #[error("Stream {stream_id} has failed to open")]
    StreamNotOpen { stream_id: i32 },
    #[error("Cursor {cursor_id} has failed to open")]
    CursorNotOpen { cursor_id: i32 },
    #[error("The server already stores {count} SQL texts, it cannot store more")]
    SqlTooMany { count: usize },
    #[error(transparent)]
    Stmt(stmt::StmtError),
    #[error(transparent)]
    Batch(batch::BatchError),
}

pub(super) async fn handle_hello(
    server: &Server,
    jwt: Option<String>,
    namespace: NamespaceName,
) -> Result<Authenticated> {
    let namespace_jwt_key = server
        .namespaces
        .with(namespace.clone(), |ns| ns.jwt_key())
        .await??;

    namespace_jwt_key
        .map(Jwt::new)
        .map(Auth::new)
        .unwrap_or_else(|| server.user_auth_strategy.clone())
        .authenticate(Ok(UserAuthContext::bearer_opt(jwt)))
        .map_err(|err| anyhow!(ResponseError::Auth { source: err }))
}

pub(super) async fn handle_request(
    server: &Server,
    session: &mut Session,
    join_set: &mut tokio::task::JoinSet<()>,
    req: proto::Request,
    namespace: NamespaceName,
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

    macro_rules! get_stream_cursor_hnd {
        ($stream:expr, $cursor_id:expr) => {
            match $stream.cursor_hnd.as_mut() {
                Some(cursor_hnd) => cursor_hnd,
                None => bail!(ResponseError::CursorNotOpen {
                    cursor_id: $cursor_id,
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

            let mut stream_hnd = stream_spawn(
                join_set,
                Stream {
                    db: None,
                    cursor_hnd: None,
                },
            );

            let namespaces = server.namespaces.clone();
            let auth = session.auth.clone();
            stream_respond!(&mut stream_hnd, async move |stream| {
                let db = namespaces
                    .with_authenticated(namespace, auth, |ns| ns.db.connection_maker())
                    .await?
                    .create()
                    .await?;
                stream.db = Some(Arc::new(db));
                Ok(proto::Response::OpenStream(proto::OpenStreamResp {}))
            });
            session.streams.insert(stream_id, stream_hnd);
        }
        proto::Request::CloseStream(req) => {
            let stream_id = req.stream_id;
            let Some(mut stream_hnd) = session.streams.remove(&stream_id) else {
                bail!(ProtocolError::StreamNotFound { stream_id })
            };

            if let Some(cursor_id) = stream_hnd.cursor_id {
                session.cursors.remove(&cursor_id);
            }

            stream_respond!(&mut stream_hnd, async move |_stream| {
                Ok(proto::Response::CloseStream(proto::CloseStreamResp {}))
            });
        }
        proto::Request::Execute(req) => {
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            let query = stmt::proto_stmt_to_query(&req.stmt, &session.sqls, session.version)
                .map_err(catch_stmt_error)?;
            let auth = session.auth.clone();
            let ctx = RequestContext::new(auth, namespace, server.namespaces.meta_store().clone());

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = stmt::execute_stmt(&**db, ctx, query, req.replication_index)
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
            let ctx = RequestContext::new(
                session.auth.clone(),
                namespace,
                server.namespaces.meta_store().clone(),
            );

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = batch::execute_batch(&**db, ctx, pgm, req.batch.replication_index)
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
            let ctx = RequestContext::new(
                session.auth.clone(),
                namespace,
                server.namespaces.meta_store().clone(),
            );

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                batch::execute_sequence(&**db, ctx, pgm, req.replication_index)
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
            let ctx = RequestContext::new(
                session.auth.clone(),
                namespace,
                server.namespaces.meta_store().clone(),
            );

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let result = stmt::describe_stmt(&**db, ctx, sql, req.replication_index)
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
        proto::Request::OpenCursor(req) => {
            ensure_version!(Version::Hrana3, "The `open_cursor` request");

            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);
            if stream_hnd.cursor_id.is_some() {
                bail!(ProtocolError::CursorAlreadyOpen { stream_id })
            }

            let cursor_id = req.cursor_id;
            if session.cursors.contains_key(&cursor_id) {
                bail!(ProtocolError::CursorExists { cursor_id })
            }

            let pgm = batch::proto_batch_to_program(&req.batch, &session.sqls, session.version)
                .map_err(catch_stmt_error)?;
            let ctx = RequestContext::new(
                session.auth.clone(),
                namespace,
                server.namespaces.meta_store().clone(),
            );
            let mut cursor_hnd = cursor::CursorHandle::spawn(join_set);

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                cursor_hnd.open(db.clone(), ctx, pgm, req.batch.replication_index);
                stream.cursor_hnd = Some(cursor_hnd);
                Ok(proto::Response::OpenCursor(proto::OpenCursorResp {}))
            });
            session.cursors.insert(cursor_id, stream_id);
            stream_hnd.cursor_id = Some(cursor_id);
        }
        proto::Request::CloseCursor(req) => {
            ensure_version!(Version::Hrana3, "The `close_cursor` request");

            let cursor_id = req.cursor_id;
            let Some(stream_id) = session.cursors.remove(&cursor_id) else {
                bail!(ProtocolError::CursorNotFound { cursor_id })
            };

            let stream_hnd = get_stream_mut!(stream_id);
            assert_eq!(stream_hnd.cursor_id, Some(cursor_id));
            stream_hnd.cursor_id = None;

            stream_respond!(stream_hnd, async move |stream| {
                stream.cursor_hnd = None;
                Ok(proto::Response::CloseCursor(proto::CloseCursorResp {}))
            });
        }
        proto::Request::FetchCursor(req) => {
            ensure_version!(Version::Hrana3, "The `fetch_cursor` request");

            let cursor_id = req.cursor_id;
            let Some(&stream_id) = session.cursors.get(&cursor_id) else {
                bail!(ProtocolError::CursorNotFound { cursor_id })
            };

            let stream_hnd = get_stream_mut!(stream_id);
            assert_eq!(stream_hnd.cursor_id, Some(cursor_id));

            let max_count = req.max_count as usize;
            let max_total_size = server.max_response_size / 8;
            stream_respond!(stream_hnd, async move |stream| {
                let cursor_hnd = get_stream_cursor_hnd!(stream, cursor_id);

                let mut entries = Vec::new();
                let mut total_size = 0;
                let mut done = false;
                while entries.len() < max_count && total_size < max_total_size {
                    let Some(sized_entry) = cursor_hnd.fetch().await? else {
                        done = true;
                        break;
                    };
                    entries.push(sized_entry.entry);
                    total_size += sized_entry.size;
                }

                Ok(proto::Response::FetchCursor(proto::FetchCursorResp {
                    entries,
                    done,
                }))
            });
        }
        proto::Request::GetAutocommit(req) => {
            ensure_version!(Version::Hrana3, "The `get_autocommit` request");
            let stream_id = req.stream_id;
            let stream_hnd = get_stream_mut!(stream_id);

            stream_respond!(stream_hnd, async move |stream| {
                let db = get_stream_db!(stream, stream_id);
                let is_autocommit = db.is_autocommit().await?;
                Ok(proto::Response::GetAutocommit(proto::GetAutocommitResp {
                    is_autocommit,
                }))
            });
        }
    }
    Ok(resp_rx)
}

const MAX_SQL_COUNT: usize = 150;

fn stream_spawn(join_set: &mut tokio::task::JoinSet<()>, stream: Stream) -> StreamHandle {
    let (job_tx, mut job_rx) = mpsc::channel::<StreamJob>(8);
    join_set.spawn(async move {
        let mut stream = stream;
        while let Some(job) = job_rx.recv().await {
            let res = (job.f)(&mut stream).await;
            let _: Result<_, _> = job.resp_tx.send(res);
        }
    });
    StreamHandle {
        job_tx,
        cursor_id: None,
    }
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

fn catch_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<stmt::StmtError>() {
        Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
        Err(err) => match err.downcast::<crate::Error>() {
            Ok(crate::Error::Migration(crate::schema::Error::MigrationError(_step, message))) => {
                anyhow!(ResponseError::Stmt(stmt::StmtError::SqliteError {
                    source: rusqlite::ffi::Error {
                        code: rusqlite::ffi::ErrorCode::Unknown,
                        extended_code: 4242
                    },
                    message
                }))
            }
            Ok(err) => anyhow!(err),
            Err(err) => err,
        },
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
            Self::CursorNotOpen { .. } => "CURSOR_NOT_OPEN",
            Self::Stmt(err) => err.code(),
            Self::Batch(err) => err.code(),
        }
    }
}
