use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::auth::Authenticated;
use crate::database::factory::DbFactory;
use crate::database::Database;
use crate::hrana;

#[derive(thiserror::Error, Debug)]
enum ResponseError {
    #[error("Could not parse request body: {source}")]
    BadRequestBody { source: serde_json::Error },

    #[error(transparent)]
    Stmt(hrana::StmtError),
    #[error(transparent)]
    Batch(hrana::BatchError),
}

pub async fn handle_index(
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    let body = "This is sqld HTTP API v1 and v2 (\"Hrana over HTTP\")";
    Ok(hyper::Response::builder()
        .header("content-type", "text/plain")
        .body(hyper::Body::from(body))
        .unwrap())
}

pub async fn handle_execute(
    db_factory: Arc<dyn DbFactory>,
    protocol: hrana::Protocol,
    auth: Authenticated,
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
        let query = hrana::proto_stmt_to_query(&req_body.stmt, &HashMap::new(), protocol)?;
        hrana::execute_stmt(&*db, auth, query)
            .await
            .map(|result| RespBody { result })
            .map_err(wrap_stmt_error)
            .context("Could not execute statement")
    })
    .await
}

pub async fn handle_batch(
    db_factory: Arc<dyn DbFactory>,
    protocol: hrana::Protocol,
    auth: Authenticated,
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
        let pgm = hrana::proto_batch_to_program(&req_body.batch, &HashMap::new(), protocol)?;
        hrana::execute_batch(&*db, auth, pgm)
            .await
            .map(|result| RespBody { result })
            .map_err(wrap_batch_error)
            .context("Could not execute batch")
    })
    .await
}

pub async fn handle_sequence(
    db_factory: Arc<dyn DbFactory>,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        sql: String,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {}

    handle_request(db_factory, req, |db, req_body: ReqBody| async move {
        let pgm = hrana::proto_sequence_to_program(&req_body.sql).map_err(wrap_stmt_error)?;
        hrana::execute_sequence(&*db, auth, pgm)
            .await
            .map(|_| RespBody {})
            .map_err(wrap_stmt_error)
            .context("Could not execute sequence")
    })
    .await
}

pub async fn handle_describe(
    db_factory: Arc<dyn DbFactory>,
    auth: Authenticated,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>> {
    #[derive(Debug, Deserialize)]
    struct ReqBody {
        sql: String,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: hrana::proto::DescribeResult,
    }

    handle_request(db_factory, req, |db, req_body: ReqBody| async move {
        hrana::describe_stmt(&*db, auth, req_body.sql)
            .await
            .map(|result| RespBody { result })
            .map_err(wrap_stmt_error)
            .context("Could not describe statement")
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
    use hrana::BatchError;
    use hrana::StmtError;
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

fn wrap_stmt_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<hrana::StmtError>() {
        Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
        Err(err) => err,
    }
}

fn wrap_batch_error(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<hrana::BatchError>() {
        Ok(batch_err) => anyhow!(ResponseError::Batch(batch_err)),
        Err(err) => err,
    }
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
