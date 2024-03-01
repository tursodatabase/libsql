use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::connection::{MakeConnection, RequestContext};
use crate::hrana;

use super::db_factory::MakeConnectionExtractor;

#[derive(thiserror::Error, Debug)]
enum ResponseError {
    #[error(transparent)]
    Stmt(hrana::stmt::StmtError),
}

pub async fn handle_index() -> hyper::Response<hyper::Body> {
    let body = "This is sqld HTTP API v1";
    hyper::Response::builder()
        .header("content-type", "text/plain")
        .body(hyper::Body::from(body))
        .unwrap()
}

pub(crate) async fn handle_execute(
    MakeConnectionExtractor(factory): MakeConnectionExtractor,
    ctx: RequestContext,
    req: hyper::Request<hyper::Body>,
) -> crate::Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        stmt: hrana::proto::Stmt,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: hrana::proto::StmtResult,
    }

    let res = handle_request(factory, req, |db, req_body: ReqBody| async move {
        let query = hrana::stmt::proto_stmt_to_query(
            &req_body.stmt,
            &HashMap::new(),
            hrana::Version::Hrana1,
        )
        .map_err(catch_stmt_error)?;
        hrana::stmt::execute_stmt(&db, ctx, query, req_body.stmt.replication_index)
            .await
            .map(|result| RespBody { result })
            .map_err(catch_stmt_error)
            .context("Could not execute statement")
    })
    .await?;

    Ok(res)
}

pub(crate) async fn handle_batch(
    MakeConnectionExtractor(factory): MakeConnectionExtractor,
    ctx: RequestContext,
    req: hyper::Request<hyper::Body>,
) -> crate::Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        batch: hrana::proto::Batch,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: hrana::proto::BatchResult,
    }

    let res = handle_request(factory, req, |db, req_body: ReqBody| async move {
        let pgm = hrana::batch::proto_batch_to_program(
            &req_body.batch,
            &HashMap::new(),
            hrana::Version::Hrana1,
        )
        .map_err(catch_stmt_error)?;
        hrana::batch::execute_batch(&db, ctx, pgm, req_body.batch.replication_index)
            .await
            .map(|result| RespBody { result })
            .context("Could not execute batch")
    })
    .await?;

    Ok(res)
}

async fn handle_request<ReqBody, RespBody, F, Fut, FT>(
    db_factory: Arc<FT>,
    req: hyper::Request<hyper::Body>,
    f: F,
) -> Result<hyper::Response<hyper::Body>>
where
    ReqBody: DeserializeOwned,
    RespBody: Serialize,
    F: FnOnce(FT::Connection, ReqBody) -> Fut,
    Fut: Future<Output = Result<RespBody>>,
    FT: MakeConnection + ?Sized,
{
    let res: Result<_> = async move {
        let req_body = hyper::body::to_bytes(req.into_body()).await?;
        let req_body = serde_json::from_slice(&req_body)
            .map_err(|e| hrana::ProtocolError::JsonDeserialize { source: e })?;

        let db = db_factory.create().await?;
        let resp_body = f(db, req_body).await?;

        Ok(json_response(hyper::StatusCode::OK, &resp_body))
    }
    .await;

    res.or_else(|err| err.downcast::<ResponseError>().map(response_error_response))
        .or_else(|err| {
            err.downcast::<hrana::ProtocolError>()
                .map(protocol_error_response)
        })
        .or_else(|err| match err.downcast::<crate::Error>() {
            Ok(crate::Error::BuilderError(
                e @ crate::query_result_builder::QueryResultBuilderError::ResponseTooLarge(_),
            )) => Ok(protocol_error_response(
                hrana::ProtocolError::ResponseTooLarge(e.to_string()),
            )),
            Ok(e) => Err(anyhow!(e)),
            Err(e) => Err(e),
        })
}

fn response_error_response(err: ResponseError) -> hyper::Response<hyper::Body> {
    use hrana::stmt::StmtError;
    let status = match &err {
        ResponseError::Stmt(err) => match err {
            StmtError::SqlParse { .. }
            | StmtError::SqlNoStmt
            | StmtError::SqlManyStmts
            | StmtError::ArgsInvalid { .. }
            | StmtError::SqlInputError { .. }
            | StmtError::Proxy(_)
            | StmtError::ResponseTooLarge
            | StmtError::Blocked { .. } => hyper::StatusCode::BAD_REQUEST,
            StmtError::ArgsBothPositionalAndNamed => hyper::StatusCode::NOT_IMPLEMENTED,
            StmtError::TransactionTimeout | StmtError::TransactionBusy => {
                hyper::StatusCode::SERVICE_UNAVAILABLE
            }
            StmtError::SqliteError { .. } => hyper::StatusCode::INTERNAL_SERVER_ERROR,
        },
    };

    json_response(
        status,
        &hrana::proto::Error {
            message: err.to_string(),
            code: err.code().into(),
        },
    )
}

fn protocol_error_response(err: hrana::ProtocolError) -> hyper::Response<hyper::Body> {
    hyper::Response::builder()
        .status(hyper::StatusCode::BAD_REQUEST)
        .header(hyper::http::header::CONTENT_TYPE, "text/plain")
        .body(hyper::Body::from(err.to_string()))
        .unwrap()
}

fn json_response<T: Serialize>(
    status: hyper::StatusCode,
    body: &T,
) -> hyper::Response<hyper::Body> {
    let body = serde_json::to_vec(body).unwrap();
    hyper::Response::builder()
        .status(status)
        .header(hyper::http::header::CONTENT_TYPE, "application/json")
        .body(hyper::Body::from(body))
        .unwrap()
}

fn catch_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<hrana::stmt::StmtError>() {
        Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
        Err(err) => err,
    }
}

impl ResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Stmt(err) => err.code(),
        }
    }
}
