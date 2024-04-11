use anyhow::{anyhow, Result};
use rusqlite::types::ValueRef;
use std::mem::take;
use std::sync::Arc;
use std::task;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::connection::program::Program;
use crate::connection::{Connection as _, RequestContext};
use crate::database::Connection;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;

use super::result_builder::{estimate_cols_json_size, value_json_size, value_to_proto};
use super::{batch, proto, stmt};

pub struct CursorHandle {
    open_tx: Option<oneshot::Sender<OpenReq>>,
    entry_rx: mpsc::Receiver<Result<SizedEntry>>,
}

#[derive(Debug)]
pub struct SizedEntry {
    pub entry: proto::CursorEntry,
    pub size: u64,
}

struct OpenReq {
    db: Arc<Connection>,
    ctx: RequestContext,
    pgm: Program,
    replication_index: Option<FrameNo>,
}

impl CursorHandle {
    pub fn spawn(join_set: &mut tokio::task::JoinSet<()>) -> Self {
        let (open_tx, open_rx) = oneshot::channel();
        let (entry_tx, entry_rx) = mpsc::channel(1);

        join_set.spawn(run_cursor(open_rx, entry_tx));
        Self {
            open_tx: Some(open_tx),
            entry_rx,
        }
    }

    pub fn open(
        &mut self,
        db: Arc<Connection>,
        ctx: RequestContext,
        pgm: Program,
        replication_index: Option<FrameNo>,
    ) {
        let open_tx = self.open_tx.take().unwrap();
        let _: Result<_, _> = open_tx.send(OpenReq {
            db,
            ctx,
            pgm,
            replication_index,
        });
    }

    pub async fn fetch(&mut self) -> Result<Option<SizedEntry>> {
        self.entry_rx.recv().await.transpose()
    }

    pub fn poll_fetch(&mut self, cx: &mut task::Context) -> task::Poll<Option<Result<SizedEntry>>> {
        self.entry_rx.poll_recv(cx)
    }
}

async fn run_cursor(
    open_rx: oneshot::Receiver<OpenReq>,
    entry_tx: mpsc::Sender<Result<SizedEntry>>,
) {
    let Ok(open_req) = open_rx.await else { return };

    let result_builder = CursorResultBuilder {
        entry_tx: entry_tx.clone(),
        step_i: 0,
        step_state: StepState::default(),
    };

    if let Err(err) = open_req
        .db
        .execute_program(
            open_req.pgm,
            open_req.ctx,
            result_builder,
            open_req.replication_index,
        )
        .await
    {
        let entry = match batch::batch_error_from_sqld_error(err) {
            Ok(batch_error) => Ok(SizedEntry {
                entry: proto::CursorEntry::Error {
                    error: batch::proto_error_from_batch_error(&batch_error),
                },
                size: 0,
            }),
            Err(sqld_error) => Err(anyhow!(sqld_error)),
        };
        let _: Result<_, _> = entry_tx.send(entry).await;
    }
}

struct CursorResultBuilder {
    entry_tx: mpsc::Sender<Result<SizedEntry>>,
    step_i: u32,
    step_state: StepState,
}

#[derive(Debug, Default)]
struct StepState {
    emitted_begin: bool,
    emitted_error: bool,
    row: Vec<proto::Value>,
    row_size: u64,
}

impl CursorResultBuilder {
    fn emit_entry(&self, entry: Result<SizedEntry>) {
        let _: Result<_, _> = self.entry_tx.blocking_send(entry);
    }
}

impl QueryResultBuilder for CursorResultBuilder {
    type Ret = ();

    fn init(&mut self, _config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.step_state.emitted_begin && !self.step_state.emitted_error {
            self.emit_entry(Ok(SizedEntry {
                entry: proto::CursorEntry::StepEnd(proto::StepEndEntry {
                    affected_row_count,
                    last_insert_rowid,
                }),
                size: 100, // rough, order-of-magnitude estimate of the size of the entry
            }));
        }

        self.step_i += 1;
        self.step_state = StepState::default();
        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        match stmt::stmt_error_from_sqld_error(error) {
            Ok(stmt_error) => {
                if self.step_state.emitted_error {
                    return Ok(());
                }

                self.emit_entry(Ok(SizedEntry {
                    entry: proto::CursorEntry::StepError(proto::StepErrorEntry {
                        step: self.step_i,
                        error: stmt::proto_error_from_stmt_error(&stmt_error),
                    }),
                    size: 100,
                }));
                self.step_state.emitted_error = true;
            }
            Err(err) => {
                self.emit_entry(Err(anyhow!(err)));
            }
        }
        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        col_iter: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(!self.step_state.emitted_begin);
        if self.step_state.emitted_error {
            return Ok(());
        }

        let mut cols_size = 0;
        let cols = col_iter
            .into_iter()
            .map(Into::into)
            .map(|col| {
                cols_size += estimate_cols_json_size(&col);
                proto::Col {
                    name: Some(col.name.to_owned()),
                    decltype: col.decl_ty.map(ToString::to_string),
                }
            })
            .collect();

        self.emit_entry(Ok(SizedEntry {
            entry: proto::CursorEntry::StepBegin(proto::StepBeginEntry {
                step: self.step_i,
                cols,
            }),
            size: cols_size,
        }));
        self.step_state.emitted_begin = true;
        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.step_state.row.is_empty());
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        if self.step_state.emitted_begin && !self.step_state.emitted_error {
            self.step_state.row_size += value_json_size(&v);
            self.step_state.row.push(value_to_proto(v)?);
        }
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        if self.step_state.emitted_begin && !self.step_state.emitted_error {
            let values = take(&mut self.step_state.row);
            self.emit_entry(Ok(SizedEntry {
                entry: proto::CursorEntry::Row {
                    row: proto::Row { values },
                },
                size: self.step_state.row_size,
            }));
        } else {
            self.step_state.row.clear();
        }

        self.step_state.row_size = 0;
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.step_state.row.is_empty());
        Ok(())
    }

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.emit_entry(Ok(SizedEntry {
            entry: proto::CursorEntry::ReplicationIndex {
                replication_index: last_frame_no,
            },
            size: std::mem::size_of::<FrameNo>() as u64,
        }));

        Ok(())
    }

    fn into_ret(self) {}

    fn add_stats(&mut self, _rows_read: u64, _rows_written: u64, _duration: Duration) {}
}
