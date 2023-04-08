use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;

use crate::database::factory::DbFactory;
use crate::database::Database;
use crate::hrana;

#[derive(thiserror::Error, Debug)]
enum ResponseError {
    #[error("Could not parse request body: {source}")]
    BadRequestBody { source: serde_json::Error },

    #[error(transparent)]
    Stmt(hrana::stmt::StmtError),
    #[error(transparent)]
    Batch(hrana::batch::BatchError),
}

pub async fn handle_index(
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    let body = "This is sqld HTTP API v1 (\"Hrana over HTTP\")";
    let body = hyper::Body::from(body);
    Ok(hyper::Response::builder()
        .header("content-type", "text/plain")
        .body(body)
        .unwrap())
}

pub async fn handle_execute(
    db_factory: Arc<dyn DbFactory>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        stmt: hrana::proto::Stmt,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: hrana::proto::StmtResult,
    }

    handle_request(db_factory, req, |db, req_body: ReqBody| async move {
        hrana::stmt::execute_stmt(&*db, &req_body.stmt)
            .await
            .map(|result| RespBody { result })
            .map_err(|err| match err.downcast::<hrana::stmt::StmtError>() {
                Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
                Err(err) => err,
            })
            .context("Could not execute statement")
    })
    .await
}

pub async fn handle_batch(
    db_factory: Arc<dyn DbFactory>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        batch: hrana::proto::Batch,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: hrana::proto::BatchResult,
    }

    handle_request(db_factory, req, |db, req_body: ReqBody| async move {
        hrana::batch::execute_batch(&*db, &req_body.batch)
            .await
            .map(|result| RespBody { result })
            .map_err(|err| match err.downcast::<hrana::batch::BatchError>() {
                Ok(batch_err) => anyhow!(ResponseError::Batch(batch_err)),
                Err(err) => err,
            })
            .context("Could not execute batch")
    })
    .await
}

async fn handle_request<ReqBody, RespBody, F, Fut>(
    db_factory: Arc<dyn DbFactory>,
    req: hyper::Request<hyper::Body>,
    f: F,
) -> Result<hyper::Response<hyper::Body>>
where
    ReqBody: DeserializeOwned,
    RespBody: Serialize,
    F: FnOnce(Arc<dyn Database>, ReqBody) -> Fut,
    Fut: Future<Output = Result<RespBody>>,
{
    let res: Result<_> = async move {
        let req_body = hyper::body::to_bytes(req.into_body()).await?;
        let req_body = serde_json::from_slice(&req_body)
            .map_err(|e| ResponseError::BadRequestBody { source: e })?;

        let db = db_factory
            .create()
            .await
            .context("Could not create a database connection")?;
        let resp_body = f(db, req_body).await?;

        Ok(json_response(hyper::StatusCode::OK, &resp_body))
    }
    .await;

    Ok(match res {
        Ok(resp) => resp,
        Err(err) => error_response(err.downcast::<ResponseError>()?),
    })
}

fn error_response(err: ResponseError) -> hyper::Response<hyper::Body> {
    use hrana::batch::BatchError;
    use hrana::stmt::StmtError;
    let status = match &err {
        ResponseError::BadRequestBody { .. } => hyper::StatusCode::BAD_REQUEST,
        ResponseError::Stmt(err) => match err {
            StmtError::SqlParse { .. }
            | StmtError::SqlNoStmt
            | StmtError::SqlManyStmts
            | StmtError::ArgsInvalid { .. }
            | StmtError::SqlInputError { .. } => hyper::StatusCode::BAD_REQUEST,
            StmtError::ArgsBothPositionalAndNamed => hyper::StatusCode::NOT_IMPLEMENTED,
            StmtError::TransactionTimeout | StmtError::TransactionBusy => {
                hyper::StatusCode::SERVICE_UNAVAILABLE
            }
            StmtError::SqliteError { .. } => hyper::StatusCode::INTERNAL_SERVER_ERROR,
        },
        ResponseError::Batch(err) => match err {
            BatchError::CondBadStep => hyper::StatusCode::BAD_REQUEST,
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

fn json_response<T: Serialize>(
    status: hyper::StatusCode,
    body: &T,
) -> hyper::Response<hyper::Body> {
    let body = serde_json::to_vec(body).unwrap();
    hyper::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(hyper::Body::from(body))
        .unwrap()
}

impl ResponseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequestBody { .. } => "HTTP_BAD_REQUEST_BODY",
            Self::Stmt(err) => err.code(),
            Self::Batch(err) => err.code(),
        }
    }
}
