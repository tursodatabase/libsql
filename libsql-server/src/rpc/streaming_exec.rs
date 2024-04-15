use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures_core::future::BoxFuture;
use futures_core::Stream;
use libsql_replication::rpc::proxy::exec_req::Request;
use libsql_replication::rpc::proxy::exec_resp::{self, Response};
use libsql_replication::rpc::proxy::resp_step::Step;
use libsql_replication::rpc::proxy::row_value::Value;
use libsql_replication::rpc::proxy::{
    AddRowValue, BeginRow, BeginRows, BeginStep, ColsDescription, DescribeCol, DescribeParam,
    DescribeResp, ExecReq, ExecResp, Finish, FinishRow, FinishRows, FinishStep, Init, ProgramResp,
    RespStep, RowValue, StepError, StreamDescribeReq,
};
use prost::Message;
use rusqlite::types::ValueRef;
use tokio::pin;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{Code, Status};

use crate::connection::{Connection, RequestContext};
use crate::error::Error;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;

const MAX_RESPONSE_SIZE: usize = bytesize::ByteSize::kb(100).as_u64() as usize;

pub fn make_proxy_stream<S, C>(
    conn: C,
    ctx: RequestContext,
    request_stream: S,
) -> impl Stream<Item = Result<ExecResp, Status>>
where
    S: Stream<Item = Result<ExecReq, Status>>,
    C: Connection,
{
    make_proxy_stream_inner(conn, ctx, request_stream, MAX_RESPONSE_SIZE)
}

fn make_proxy_stream_inner<S, C>(
    conn: C,
    ctx: RequestContext,
    request_stream: S,
    max_program_resp_size: usize,
) -> impl Stream<Item = Result<ExecResp, Status>>
where
    S: Stream<Item = Result<ExecReq, Status>>,
    C: Connection,
{
    async_stream::stream! {
        let never = || Box::pin(poll_fn(|_| Poll::Pending));
        let mut current_request_fut: BoxFuture<'static, (crate::Result<()>, u32)> = never();
        let (snd, mut recv) = mpsc::channel(1);
        let conn = Arc::new(conn);

        pin!(request_stream);

        let mut last_request_id = None;

        loop {
            tokio::select! {
                biased;
                maybe_req = request_stream.next() => {
                    let Some(maybe_req) = maybe_req else { break };
                    match maybe_req {
                        Err(e) => {
                            tracing::error!("stream error: {e}");
                            break
                        }
                        Ok(req) => {
                            let request_id = req.request_id;
                            if let Some(last_req_id) = last_request_id {
                                if request_id <= last_req_id {
                                    tracing::error!("received request with id less than last received request, closing stream");
                                    yield Err(Status::new(Code::InvalidArgument, "received request with id less than last received request, closing stream"));
                                    return;
                                }
                            }

                            last_request_id = Some(request_id);

                            match req.request {
                                Some(Request::Execute(pgm)) => {
                                    let Ok(pgm) =
                                        crate::connection::program::Program::try_from(pgm.pgm.unwrap()) else {
                                            yield Err(Status::new(Code::InvalidArgument, "invalid program"));
                                            break
                                        };
                                    let conn = conn.clone();
                                    let ctx = ctx.clone();
                                    let sender = snd.clone();

                                    let fut = async move {
                                        let builder = StreamResponseBuilder {
                                            request_id,
                                            sender,
                                            current: None,
                                            current_size: 0,
                                            max_program_resp_size,
                                        };

                                        let ret = conn.execute_program(pgm, ctx, builder, None).await.map(|_| ());
                                        (ret, request_id)
                                    };

                                    current_request_fut = Box::pin(fut);
                                }
                                Some(Request::Describe(StreamDescribeReq { stmt })) => {
                                    let ctx = ctx.clone();
                                    let sender = snd.clone();
                                    let conn = conn.clone();
                                    let fut = async move {
                                        let do_describe = || async move {
                                            let ret = conn.describe(stmt, ctx, None).await??;
                                            Ok(DescribeResp {
                                                cols: ret.cols.into_iter().map(|c| DescribeCol { name: c.name, decltype: c.decltype }).collect(),
                                                params: ret.params.into_iter().map(|p| DescribeParam { name: p.name }).collect(),
                                                is_explain: ret.is_explain,
                                                is_readonly: ret.is_readonly
                                            })
                                        };

                                        let ret: crate::Result<()> = match do_describe().await {
                                            Ok(resp) => {
                                                let _ = sender.send(ExecResp { request_id, response: Some(Response::DescribeResp(resp)) }).await;
                                                Ok(())
                                            }
                                            Err(e) => Err(e),
                                        };

                                        (ret, request_id)
                                    };

                                    current_request_fut = Box::pin(fut);

                                },
                                None => {
                                    yield Err(Status::new(Code::InvalidArgument, "invalid request"));
                                    break
                                }
                            }
                        }
                    }
                },
                Some(res) = recv.recv() => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    yield Ok(res);
                },
                (ret, request_id) = &mut current_request_fut => {
                    current_request_fut = never();
                    if let Err(e) = ret {
                        yield Ok(ExecResp { request_id, response: Some(Response::Error(e.into())) })
                    }
                },
                else => break,
            }
        }
    }
}

struct StreamResponseBuilder {
    request_id: u32,
    sender: mpsc::Sender<ExecResp>,
    current: Option<ProgramResp>,
    current_size: usize,
    max_program_resp_size: usize,
}

impl StreamResponseBuilder {
    fn current(&mut self) -> &mut ProgramResp {
        self.current
            .get_or_insert_with(|| ProgramResp { steps: Vec::new() })
    }

    fn push(&mut self, step: Step) -> Result<(), QueryResultBuilderError> {
        let current = self.current();
        let step = RespStep { step: Some(step) };
        let size = step.encoded_len();
        current.steps.push(step);
        self.current_size += size;

        if self.current_size >= self.max_program_resp_size {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryResultBuilderError> {
        if let Some(current) = self.current.take() {
            let resp = ExecResp {
                request_id: self.request_id,
                response: Some(exec_resp::Response::ProgramResp(current)),
            };
            self.current_size = 0;
            self.sender
                .blocking_send(resp)
                .map_err(|_| QueryResultBuilderError::Internal(anyhow::anyhow!("stream closed")))?;
        }

        Ok(())
    }
}

/// Apply the response to the the builder, and return whether the builder need more steps
pub fn apply_program_resp_to_builder<B: QueryResultBuilder>(
    config: &QueryBuilderConfig,
    builder: &mut B,
    resp: ProgramResp,
    mut on_finish: impl FnMut(Option<FrameNo>, bool),
) -> crate::Result<bool> {
    for step in resp.steps {
        let Some(step) = step.step else {
            return Err(Error::PrimaryStreamMisuse);
        };
        match step {
            Step::Init(_) => builder.init(config)?,
            Step::BeginStep(_) => builder.begin_step()?,
            Step::FinishStep(FinishStep {
                affected_row_count,
                last_insert_rowid,
            }) => builder.finish_step(affected_row_count, last_insert_rowid)?,
            Step::StepError(StepError { error: Some(err) }) => {
                builder.step_error(crate::error::Error::RpcQueryError(err))?
            }
            Step::ColsDescription(ColsDescription { columns }) => {
                let cols = columns.iter().map(|c| Column {
                    name: &c.name,
                    decl_ty: c.decltype.as_deref(),
                });
                builder.cols_description(cols)?
            }
            Step::BeginRows(_) => builder.begin_rows()?,
            Step::BeginRow(_) => builder.begin_row()?,
            Step::AddRowValue(AddRowValue {
                val: Some(RowValue { value: Some(val) }),
            }) => {
                let val = match &val {
                    Value::Text(s) => ValueRef::Text(s.as_bytes()),
                    Value::Integer(i) => ValueRef::Integer(*i),
                    Value::Real(x) => ValueRef::Real(*x),
                    Value::Blob(b) => ValueRef::Blob(b.as_slice()),
                    Value::Null(_) => ValueRef::Null,
                };
                builder.add_row_value(val)?;
            }
            Step::FinishRow(_) => builder.finish_row()?,
            Step::FinishRows(_) => builder.finish_rows()?,
            Step::Finish(Finish {
                last_frame_no,
                is_autocommit,
            }) => {
                on_finish(last_frame_no, is_autocommit);
                builder.finish(last_frame_no, is_autocommit)?;
                return Ok(false);
            }
            _ => return Err(Error::PrimaryStreamMisuse),
        }
    }

    Ok(true)
}

impl QueryResultBuilder for StreamResponseBuilder {
    type Ret = ();

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        self.push(Step::Init(Init {}))?;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginStep(BeginStep {}))?;
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishStep(FinishStep {
            affected_row_count,
            last_insert_rowid,
        }))?;
        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.push(Step::StepError(StepError {
            error: Some(error.into()),
        }))?;
        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::ColsDescription(ColsDescription {
            columns: cols
                .into_iter()
                .map(Into::into)
                .map(|c| libsql_replication::rpc::proxy::Column {
                    name: c.name.into(),
                    decltype: c.decl_ty.map(Into::into),
                })
                .collect::<Vec<_>>(),
        }))?;
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginRows(BeginRows {}))?;
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::BeginRow(BeginRow {}))?;
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.push(Step::AddRowValue(AddRowValue {
            val: Some(v.into()),
        }))?;
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishRow(FinishRow {}))?;
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.push(Step::FinishRows(FinishRows {}))?;
        Ok(())
    }

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.push(Step::Finish(Finish {
            last_frame_no,
            is_autocommit,
        }))?;
        self.flush()?;
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {}

    fn add_stats(&mut self, _rows_read: u64, _rows_written: u64, _duration: Duration) {}
}

#[cfg(test)]
pub mod test {
    use insta::{assert_debug_snapshot, assert_json_snapshot, assert_snapshot};
    use tempfile::tempdir;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::auth::Authenticated;
    use crate::connection::libsql::LibSqlConnection;
    use crate::connection::program::Program;
    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
    use crate::namespace::NamespaceName;
    use crate::query_result_builder::test::{
        fsm_builder_driver, random_transition, TestBuilder, ValidateTraceBuilder,
    };
    use crate::rpc::proxy::rpc::StreamProgramReq;

    use super::*;

    fn exec_req_stmt(s: &str, id: u32) -> ExecReq {
        ExecReq {
            request_id: id,
            request: Some(Request::Execute(StreamProgramReq {
                pgm: Some(Program::seq(&[s]).into()),
            })),
        }
    }

    #[tokio::test]
    async fn invalid_request() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::Anonymous,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));
        pin!(stream);

        let req = ExecReq {
            request_id: 0,
            request: None,
        };

        snd.send(Ok(req)).await.unwrap();

        assert_snapshot!(stream.next().await.unwrap().unwrap_err().to_string());
    }

    #[tokio::test]
    async fn request_stream_dropped() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        drop(snd);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn perform_query_simple() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        let req = exec_req_stmt("create table test (foo)", 0);

        snd.send(Ok(req)).await.unwrap();

        assert_debug_snapshot!(stream.next().await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn single_query_split_response() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        // limit the size of the response to force a split
        let stream = make_proxy_stream_inner(conn, ctx, ReceiverStream::new(rcv), 500);

        pin!(stream);

        let req = exec_req_stmt("create table test (foo)", 0);
        snd.send(Ok(req)).await.unwrap();
        let resp = stream.next().await.unwrap().unwrap();
        assert_eq!(resp.request_id, 0);
        for i in 1..50 {
            let req = exec_req_stmt("insert into test values ('something moderately long')", i);
            snd.send(Ok(req)).await.unwrap();
            let resp = stream.next().await.unwrap().unwrap();
            assert_eq!(resp.request_id, i);
        }

        let req = exec_req_stmt("select * from test", 100);
        snd.send(Ok(req)).await.unwrap();

        let mut num_resp = 0;
        let mut builder = TestBuilder::default();
        loop {
            let Response::ProgramResp(resp) =
                stream.next().await.unwrap().unwrap().response.unwrap()
            else {
                panic!()
            };
            if !apply_program_resp_to_builder(
                &QueryBuilderConfig::default(),
                &mut builder,
                resp,
                |_, _| (),
            )
            .unwrap()
            {
                break;
            }
            num_resp += 1;
        }

        assert_eq!(num_resp, 3);
        assert_debug_snapshot!(builder.into_ret());
    }

    #[tokio::test]
    async fn request_interupted() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(2);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        // request 0 should be dropped, and request 1 should be processed instead
        let req1 = exec_req_stmt("create table test (foo)", 0);
        let req2 = exec_req_stmt("create table test (foo)", 1);
        snd.send(Ok(req1)).await.unwrap();
        snd.send(Ok(req2)).await.unwrap();

        let resp = stream.next().await.unwrap().unwrap();
        assert_eq!(resp.request_id, 1);
    }

    #[tokio::test]
    async fn perform_multiple_queries() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        // request 0 should be dropped, and request 1 should be processed instead
        let req1 = exec_req_stmt("create table test (foo)", 0);
        snd.send(Ok(req1)).await.unwrap();
        assert_json_snapshot!(stream.next().await.unwrap().unwrap());

        let req2 = exec_req_stmt("insert into test values (12)", 1);
        snd.send(Ok(req2)).await.unwrap();
        assert_json_snapshot!(stream.next().await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn query_number_less_than_previous_query() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        // request 0 should be dropped, and request 1 should be processed instead
        let req1 = exec_req_stmt("create table test (foo)", 0);
        snd.send(Ok(req1)).await.unwrap();
        assert_json_snapshot!(stream.next().await.unwrap().unwrap());

        let req2 = exec_req_stmt("insert into test values (12)", 0);
        snd.send(Ok(req2)).await.unwrap();
        let resp = stream.next().await.unwrap();
        assert!(resp.is_err());
        assert_debug_snapshot!(resp);
    }

    #[tokio::test]
    async fn describe() {
        let tmp = tempdir().unwrap();
        let conn = LibSqlConnection::new_test(tmp.path()).await;
        let (snd, rcv) = mpsc::channel(1);
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        let stream = make_proxy_stream(conn, ctx, ReceiverStream::new(rcv));

        pin!(stream);

        // request 0 should be dropped, and request 1 should be processed instead
        let req = ExecReq {
            request_id: 0,
            request: Some(Request::Describe(StreamDescribeReq {
                stmt: "select $hello".into(),
            })),
        };

        snd.send(Ok(req)).await.unwrap();

        assert_debug_snapshot!(stream.next().await.unwrap().unwrap());
    }

    /// This fuction returns a random, valid, program resp for use in other tests
    pub fn random_valid_program_resp(
        size: usize,
        max_resp_size: usize,
    ) -> (impl Stream<Item = ExecResp>, ValidateTraceBuilder) {
        let (sender, receiver) = mpsc::channel(1);
        let builder = StreamResponseBuilder {
            request_id: 0,
            sender,
            current: None,
            current_size: 0,
            max_program_resp_size: max_resp_size,
        };

        let trace = random_transition(size);
        tokio::task::spawn_blocking({
            let trace = trace.clone();
            move || fsm_builder_driver(&trace, builder)
        });

        (
            ReceiverStream::new(receiver),
            ValidateTraceBuilder::new(trace),
        )
    }
}
