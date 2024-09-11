use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;

use bytes::Bytes;
use dialoguer::BasicHistory;
use rusqlite::types::ValueRef;
use tokio_stream::{Stream, StreamExt as _};
use tonic::metadata::{AsciiMetadataValue, BinaryMetadataValue};

use crate::connection::Connection as _;
use crate::database::Connection;
use crate::namespace::{NamespaceName, NamespaceStore};

use self::rpc::admin_shell_service_server::{AdminShellService, AdminShellServiceServer};
use self::rpc::response::Resp;
use self::rpc::Null;

mod rpc {
    #![allow(clippy::all)]
    include!("generated/admin_shell.rs");
}

pub(crate) fn make_svc(namespace_store: NamespaceStore) -> AdminShellServiceServer<AdminShell> {
    let admin_shell = AdminShell::new(namespace_store);
    rpc::admin_shell_service_server::AdminShellServiceServer::new(admin_shell)
}

pub(super) struct AdminShell {
    namespace_store: NamespaceStore,
}

impl AdminShell {
    fn new(namespace_store: NamespaceStore) -> Self {
        Self { namespace_store }
    }

    async fn with_namespace(
        &self,
        ns: Bytes,
        queries: impl Stream<Item = Result<rpc::Query, tonic::Status>>,
    ) -> anyhow::Result<impl Stream<Item = Result<rpc::Response, tonic::Status>>> {
        let namespace = NamespaceName::from_bytes(ns).unwrap();
        let connection_maker = self
            .namespace_store
            .with(namespace, |ns| ns.db.connection_maker())
            .await?;
        let connection = connection_maker.create().await?;
        Ok(run_shell(connection, queries))
    }
}

fn run_shell(
    conn: Connection,
    queries: impl Stream<Item = Result<rpc::Query, tonic::Status>>,
) -> impl Stream<Item = Result<rpc::Response, tonic::Status>> {
    async_stream::stream! {
        tokio::pin!(queries);
        while let Some(q) = queries.next().await {
            let Ok(q) = q else { break };
            let res = tokio::task::block_in_place(|| {
                conn.with_raw(move |conn| {
                    run_one(conn, q.query)
                })
            });

            yield res
        }
    }
}

fn run_one(conn: &mut rusqlite::Connection, q: String) -> Result<rpc::Response, tonic::Status> {
    match try_run_one(conn, q) {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(rpc::Response {
            resp: Some(Resp::Error(rpc::Error {
                error: e.to_string(),
            })),
        }),
    }
}

fn try_run_one(conn: &mut rusqlite::Connection, q: String) -> anyhow::Result<rpc::Response> {
    let mut stmt = conn.prepare(&q)?;
    let col_count = stmt.column_count();
    let mut rows = stmt.query(())?;
    let mut out_rows = Vec::new();
    while let Some(row) = rows.next()? {
        let mut out_row = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let rpc_value = match row.get_ref(i).unwrap() {
                ValueRef::Null => rpc::value::Value::Null(Null {}),
                ValueRef::Integer(i) => rpc::value::Value::Integer(i),
                ValueRef::Real(x) => rpc::value::Value::Real(x),
                ValueRef::Text(s) => rpc::value::Value::Text(String::from_utf8(s.to_vec())?),
                ValueRef::Blob(b) => rpc::value::Value::Blob(b.to_vec()),
            };
            out_row.push(rpc::Value {
                value: Some(rpc_value),
            });
        }
        out_rows.push(rpc::Row { values: out_row });
    }

    Ok(rpc::Response {
        resp: Some(Resp::Rows(rpc::Rows { rows: out_rows })),
    })
}

#[async_trait::async_trait]
impl AdminShellService for AdminShell {
    type ShellStream = Pin<Box<dyn Stream<Item = Result<rpc::Response, tonic::Status>> + Send>>;

    async fn shell(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::Query>>,
    ) -> std::result::Result<tonic::Response<Self::ShellStream>, tonic::Status> {
        let Some(namespace) = request.metadata().get_bin("x-namespace-bin") else {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "missing namespace",
            ));
        };
        let Ok(ns_bytes) = namespace.to_bytes() else {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "bad namespace encoding",
            ));
        };

        match self.with_namespace(ns_bytes, request.into_inner()).await {
            Ok(s) => Ok(tonic::Response::new(Box::pin(s))),
            Err(e) => Err(tonic::Status::new(
                tonic::Code::FailedPrecondition,
                e.to_string(),
            )),
        }
    }
}

pub struct AdminShellClient {
    remote_url: String,
    auth: Option<String>,
}

impl AdminShellClient {
    pub fn new(remote_url: String, auth: Option<String>) -> Self {
        Self { remote_url, auth }
    }

    pub async fn run_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        let namespace = NamespaceName::from_string(namespace.to_string())?;
        let mut client = rpc::admin_shell_service_client::AdminShellServiceClient::connect(
            self.remote_url.clone(),
        )
        .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let req_stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

        let mut req = tonic::Request::new(req_stream);
        req.metadata_mut().insert_bin(
            "x-namespace-bin",
            BinaryMetadataValue::from_bytes(namespace.as_slice()),
        );

        if let Some(ref auth) = self.auth {
            req.metadata_mut().insert(
                "authorization",
                AsciiMetadataValue::from_str(&format!("basic {auth}")).unwrap(),
            );
        }

        let mut resp_stream = client.shell(req).await?.into_inner();

        let mut history = BasicHistory::new();
        loop {
            // this is blocking, but the shell runs in it's own process with no other tasks, so
            // that's ok
            let prompt = dialoguer::Input::<String>::new()
                .with_prompt("> ")
                .history_with(&mut history)
                .interact_text();

            match prompt {
                Ok(query) => {
                    let q = rpc::Query { query };
                    sender.send(q).await?;
                    match resp_stream.next().await {
                        Some(Ok(rpc::Response {
                            resp: Some(rpc::response::Resp::Rows(rows)),
                        })) => {
                            println!("{}", RowsFormatter(rows));
                        }
                        Some(Ok(rpc::Response {
                            resp: Some(rpc::response::Resp::Error(rpc::Error { error })),
                        })) => {
                            println!("query error: {error}");
                        }
                        Some(Err(e)) => {
                            println!("rpc error: {}", e.message());
                            break;
                        }
                        _ => break,
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }

        Ok(())
    }
}

struct RowsFormatter(rpc::Rows);

impl Display for RowsFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for row in self.0.rows.iter() {
            let mut is_first = true;
            for value in row.values.iter() {
                if !is_first {
                    f.write_str(", ")?;
                }
                is_first = false;

                match value.value.as_ref().unwrap() {
                    rpc::value::Value::Null(_) => f.write_str("null")?,
                    rpc::value::Value::Real(x) => write!(f, "{x}")?,
                    rpc::value::Value::Integer(i) => write!(f, "{i}")?,
                    rpc::value::Value::Text(s) => f.write_str(&s)?,
                    rpc::value::Value::Blob(b) => {
                        for x in b {
                            write!(f, "{x:0x}")?
                        }
                    }
                }
            }

            writeln!(f)?;
        }

        Ok(())
    }
}
