use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, Context as _, bail};

use crate::database::Database;
use crate::error::Error;
use crate::query::{Query, QueryResult};
use super::{Server, proto};

pub struct Session {
    streams: HashMap<u32, Stream>,
}

struct Stream {
    db: Arc<dyn Database>,
}

#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Stream {stream_id} not found")]
    StreamNotFound { stream_id: u32 },
    #[error("Stream {stream_id} already exists")]
    StreamExists { stream_id: u32 },
}

pub async fn handle_hello(_jwt: Option<String>) -> Result<Session> {
    // TODO: handle the jwt
    Ok(Session { streams: HashMap::new() })
}

pub(super) async fn handle_request(
    server: &Server,
    session: &mut Session,
    req: proto::Request,
) -> Result<proto::Response> {
    match req {
        proto::Request::OpenStream(req) => {
            let stream_id = req.stream_id;

            if session.streams.contains_key(&stream_id) {
                bail!(ResponseError::StreamExists { stream_id })
            }

            let db = server.db_factory.create().await
                .context("Could not create a database connection")?;
            let stream = Stream { db };
            session.streams.insert(stream_id, stream);

            Ok(proto::Response::OpenStream(proto::OpenStreamResp {}))
        },
        proto::Request::CloseStream(req) => {
            session.streams.remove(&req.stream_id);
            Ok(proto::Response::CloseStream(proto::CloseStreamResp {}))
        },
        proto::Request::Execute(req) => {
            let stream_id = req.stream_id;

            let Some(stream) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let result = execute_stmt(stream, req.stmt).await?;
            Ok(proto::Response::Execute(proto::ExecuteResp { result }))
        },
    }
}

async fn execute_stmt(stream: &mut Stream, stmt: proto::Stmt) -> Result<proto::StmtResult> {
    let query = stmt_to_query(stmt)?;
    let query_result = match stream.db.execute_one(query).await {
        Ok((query_result, _)) => query_result,
        Err(error) => match response_error_from_error(error) {
            Ok(resp_error) => bail!(resp_error),
            Err(error) => bail!(error),
        },
    };
    stmt_result_from_query(query_result)
}

fn stmt_to_query(_stmt: proto::Stmt) -> Result<Query> {
    bail!("not implemented")
}

fn stmt_result_from_query(_result: QueryResult) -> Result<proto::StmtResult> {
    bail!("not implemented")
}

fn response_error_from_error(error: Error) -> Result<ResponseError, Error> {
    Err(error)
}
