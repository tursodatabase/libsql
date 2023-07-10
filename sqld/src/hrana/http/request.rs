use anyhow::{anyhow, bail, Result};

use super::super::{batch, stmt, ProtocolError, Version};
use super::{proto, stream};
use crate::auth::Authenticated;
use crate::database::Database;

/// An error from executing a [`proto::StreamRequest`]
#[derive(thiserror::Error, Debug)]
pub enum StreamResponseError {
    #[error("The server already stores {count} SQL texts, it cannot store more")]
    SqlTooMany { count: usize },
    #[error(transparent)]
    Stmt(stmt::StmtError),
    #[error(transparent)]
    Batch(batch::BatchError),
}

pub async fn handle<D: Database>(
    stream_guard: &mut stream::Guard<'_, D>,
    auth: Authenticated,
    request: proto::StreamRequest,
) -> Result<proto::StreamResult> {
    let result = match try_handle(stream_guard, auth, request).await {
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

async fn try_handle<D: Database>(
    stream_guard: &mut stream::Guard<'_, D>,
    auth: Authenticated,
    request: proto::StreamRequest,
) -> Result<proto::StreamResponse> {
    Ok(match request {
        proto::StreamRequest::Close(_req) => {
            stream_guard.close_db();
            proto::StreamResponse::Close(proto::CloseStreamResp {})
        }
        proto::StreamRequest::Execute(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let query = stmt::proto_stmt_to_query(&req.stmt, sqls, Version::Hrana2)
                .map_err(catch_stmt_error)?;
            let result = stmt::execute_stmt(db, auth, query)
                .await
                .map_err(catch_stmt_error)?;
            proto::StreamResponse::Execute(proto::ExecuteStreamResp { result })
        }
        proto::StreamRequest::Batch(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let pgm = batch::proto_batch_to_program(&req.batch, sqls, Version::Hrana2)?;
            let result = batch::execute_batch(db, auth, pgm)
                .await
                .map_err(catch_batch_error)?;
            proto::StreamResponse::Batch(proto::BatchStreamResp { result })
        }
        proto::StreamRequest::Sequence(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql =
                stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, Version::Hrana2)?;
            let pgm = batch::proto_sequence_to_program(sql).map_err(catch_stmt_error)?;
            batch::execute_sequence(db, auth, pgm)
                .await
                .map_err(catch_stmt_error)
                .map_err(catch_batch_error)?;
            proto::StreamResponse::Sequence(proto::SequenceStreamResp {})
        }
        proto::StreamRequest::Describe(req) => {
            let db = stream_guard.get_db()?;
            let sqls = stream_guard.sqls();
            let sql =
                stmt::proto_sql_to_sql(req.sql.as_deref(), req.sql_id, sqls, Version::Hrana2)?;
            let result = stmt::describe_stmt(db, auth, sql.into())
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
            }
            sqls.insert(sql_id, req.sql);
            proto::StreamResponse::StoreSql(proto::StoreSqlStreamResp {})
        }
        proto::StreamRequest::CloseSql(req) => {
            let sqls = stream_guard.sqls_mut();
            sqls.remove(&req.sql_id);
            proto::StreamResponse::CloseSql(proto::CloseSqlStreamResp {})
        }
    })
}

const MAX_SQL_COUNT: usize = 50;

fn catch_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<stmt::StmtError>() {
        Ok(stmt_err) => anyhow!(StreamResponseError::Stmt(stmt_err)),
        Err(err) => err,
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
        }
    }
}
