use libsql_replication::rpc::proxy::{DescribeRequest, DescribeResult, ExecuteResults, Positional, Program, ProgramReq, Query, Step, query::Params};
use libsql_replication::frame::Frame;
use libsql_replication::snapshot::SnapshotFile;

use parser::Statement;

pub use connection::RemoteConnection;

pub(crate) mod client;
mod connection;
mod parser;
pub(crate) mod remote_client;
pub(crate) mod local_client;

pub enum Frames {
    /// A set of frames, in increasing frame_no.
    Vec(Vec<Frame>),
    /// A stream of snapshot frames. The frames must be in reverse frame_no, and the pages
    /// deduplicated. The snapshot is expected to be a single commit unit.
    Snapshot(SnapshotFile),
}

#[derive(Debug, Clone)]
pub struct Writer {
    pub(crate) client: client::Client,
}

impl Writer {
    pub async fn execute_program(
        &self,
        steps: Vec<Statement>,
        params: impl Into<Params>,
    ) -> anyhow::Result<ExecuteResults> {
        let mut params = Some(params.into());

        let steps = steps
            .into_iter()
            .map(|stmt| Step {
                query: Some(Query {
                    stmt: stmt.stmt,
                    // TODO(lucio): Pass params
                    params: Some(
                        params
                            .take()
                            .unwrap_or(Params::Positional(Positional::default())),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();

        self.client
            .execute_program(ProgramReq {
                client_id: self.client.client_id(),
                pgm: Some(Program { steps }),
            })
            .await
    }

    pub async fn describe(&self, stmt: impl Into<String>) -> anyhow::Result<DescribeResult> {
        let stmt = stmt.into();

        self.client
            .describe(DescribeRequest {
                client_id: self.client.client_id(),
                stmt,
            })
            .await
    }
}
