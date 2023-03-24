use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;

use crate::database::service::DbFactory;
use crate::{batch, hrana};

#[derive(thiserror::Error, Debug)]
enum ResponseError {
    #[error("Could not parse request body: {source}")]
    BadRequestBody { source: serde_json::Error },

    #[error(transparent)]
    Stmt(batch::StmtError),
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
        stmt: batch::proto::Stmt,
    }

    #[derive(Debug, Serialize)]
    struct RespBody {
        result: batch::proto::StmtResult,
    }

    let res: Result<_> = async move {
        let req_body = json_request_body::<ReqBody>(req.into_body()).await?;
        let db = db_factory
            .create()
            .await
            .context("Could not create a database connection")?;
        let result = batch::execute_stmt(&*db, &req_body.stmt)
            .await
            .map_err(|err| match err.downcast::<batch::StmtError>() {
                Ok(stmt_err) => anyhow!(ResponseError::Stmt(stmt_err)),
                Err(err) => err,
            })
            .context("Could not execute statement")?;
        Ok(json_response(hyper::StatusCode::OK, &RespBody { result }))
    }
    .await;

    Ok(match res {
        Ok(resp) => resp,
        Err(err) => error_response(err.downcast::<ResponseError>()?),
    })
}

async fn json_request_body<T: DeserializeOwned>(body: hyper::Body) -> Result<T> {
    let body = hyper::body::to_bytes(body).await?;
    let body =
        serde_json::from_slice(&body).map_err(|e| ResponseError::BadRequestBody { source: e })?;
    Ok(body)
}

fn error_response(err: ResponseError) -> hyper::Response<hyper::Body> {
    use batch::StmtError;
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
    };

    json_response(
        status,
        &hrana::proto::Error {
            message: err.to_string(),
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
