use anyhow::{anyhow, bail, Result};
use bytesize::ByteSize;

use super::super::{batch, stmt, ProtocolError, Version};
use super::stream;
use crate::connection::{Connection, RequestContext};
use libsql_hrana::proto;

const MAX_SQL_COUNT: usize = 50;
const MAX_STORED_SQL_SIZE: ByteSize = ByteSize::kb(5);

/// An error from executing a [`proto::StreamRequest`]
#[derive(thiserror::Error, Debug)]
enum StreamResponseError {
    #[error("The server already stores {count} SQL texts, it cannot store more")]
    SqlTooMany { count: usize },
    #[error("The statement is too large to be stored: {size}/{MAX_STORED_SQL_SIZE}")]
    SqlTooLarge { size: ByteSize },
    #[error(transparent)]
    Stmt(stmt::StmtError),
    #[error(transparent)]
    Batch(batch::BatchError),
}

pub async fn handle(
    stream_guard: &mut stream::Guard<'_>,
    ctx: RequestContext,
    request: proto::StreamRequest,
    version: Version,
) -> Result<proto::StreamResult> {
    let result = match try_handle(stream_guard, ctx, request, version).await {
        Ok(response) => proto::StreamResult::Ok { response },
        Err(err) => {
            let resp_err = err.downcast::<StreamResponseError>()?;
            let error = proto::Error {
                message: resp_err.to_string(),
                code: resp_err.code().into(),
            };
            proto::StreamResult::Error { error }
        }
    };
    Ok(result)
}

async fn try_handle(
    stream_guard: &mut stream::Guard<'_>,
    ctx: RequestContext,
    request: proto::StreamRequest,
    version: Version,
) -> Result<proto::StreamResponse> {
    macro_rules! ensure_version {
        ($min_version:expr, $what:expr) => {
            if version < $min_version {
                bail!(ProtocolError::NotSupported {
                    what: $what,
                    min_version: $min_version,
                })
            }
        };
    }

    Ok(match request {
        proto::StreamRequest::None => bail!(ProtocolError::NoneStreamRequest),
        proto::StreamRequest::Close(_req) => {
            stream_guard.close_db();
            proto::StreamResponse::Close(proto::CloseStreamResp {})
        }
        proto::StreamRequest::Execute(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let query =
                stmt::proto_stmt_to_query(&req.stmt, sqls, version).map_err(catch_stmt_error)?;
            let result = stmt::execute_stmt(db, ctx, query, req.stmt.replication_index)
                .await
                .map_err(catch_stmt_error)?;
            proto::StreamResponse::Execute(proto::ExecuteStreamResp { result })
        }
        proto::StreamRequest::Batch(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let pgm = batch::proto_batch_to_program(&req.batch, sqls, version)
                .map_err(catch_stmt_error)
                .map_err(catch_batch_error)?;
            let result = batch::execute_batch(db, ctx, pgm, req.batch.replication_index)
                .await
                .map_err(catch_batch_error)?;
            proto::StreamResponse::Batch(proto::BatchStreamResp { result })
        }
        proto::StreamRequest::Sequence(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql = stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, version)?;
            let pgm = batch::proto_sequence_to_program(sql).map_err(catch_stmt_error)?;
            batch::execute_sequence(db, ctx, pgm, req.replication_index)
                .await
                .map_err(catch_stmt_error)
                .map_err(catch_batch_error)?;
            proto::StreamResponse::Sequence(proto::SequenceStreamResp {})
        }
        proto::StreamRequest::Describe(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql = stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, version)?;
            let result = stmt::describe_stmt(db, ctx, sql.into(), req.replication_index)
                .await
                .map_err(catch_stmt_error)?;
            proto::StreamResponse::Describe(proto::DescribeStreamResp { result })
        }
        proto::StreamRequest::StoreSql(req) => {
            let sqls = stream_guard.sqls_mut();
            let sql_id = req.sql_id;
            if sqls.contains_key(&sql_id) {
                bail!(ProtocolError::SqlExists { sql_id })
            } else if sqls.len() >= MAX_SQL_COUNT {
                bail!(StreamResponseError::SqlTooMany { count: sqls.len() })
            } else if req.sql.len() > MAX_STORED_SQL_SIZE.as_u64() as usize {
                bail!(StreamResponseError::SqlTooLarge {
                    size: ByteSize::b(req.sql.len() as _)
                })
            }
            sqls.insert(sql_id, req.sql);
            proto::StreamResponse::StoreSql(proto::StoreSqlStreamResp {})
        }
        proto::StreamRequest::CloseSql(req) => {
            let sqls = stream_guard.sqls_mut();
            sqls.remove(&req.sql_id);
            proto::StreamResponse::CloseSql(proto::CloseSqlStreamResp {})
        }
        proto::StreamRequest::GetAutocommit(_req) => {
            ensure_version!(Version::Hrana3, "The `get_autocommit` request");
            let db = stream_guard.get_db()?;
            let is_autocommit = db.is_autocommit().await?;
            proto::StreamResponse::GetAutocommit(proto::GetAutocommitStreamResp { is_autocommit })
        }
    })
}

fn catch_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<stmt::StmtError>() {
        Ok(stmt_err) => anyhow!(StreamResponseError::Stmt(stmt_err)),
        Err(err) => match err.downcast::<crate::Error>() {
            Ok(crate::Error::Migration(crate::schema::Error::MigrationError(_step, message))) => {
                anyhow!(StreamResponseError::Stmt(stmt::StmtError::SqliteError {
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
        Ok(batch_err) => anyhow!(StreamResponseError::Batch(batch_err)),
        Err(err) => err,
    }
}

impl StreamResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SqlTooMany { .. } => "SQL_STORE_TOO_MANY",
            Self::Stmt(err) => err.code(),
            Self::Batch(err) => err.code(),
            StreamResponseError::SqlTooLarge { .. } => "SQL_STORE_TOO_LARGE",
        }
    }
}
