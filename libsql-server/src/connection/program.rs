use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics::{histogram, increment_counter};
use rusqlite::StatementStatus;

use crate::auth::Permission;
use crate::error::Error;
use crate::metrics::{READ_QUERY_COUNT, WRITE_QUERY_COUNT};
use crate::namespace::{NamespaceName, ResolveNamespacePathFn};
use crate::query::Query;
use crate::query_analysis::StmtKind;
use crate::query_result_builder::QueryResultBuilder;

use super::config::DatabaseConfig;
use super::RequestContext;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Program {
    pub steps: Arc<Vec<Step>>,
}

impl Program {
    pub fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: Arc::new(steps),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.steps.iter().all(|s| s.query.stmt.is_read_only())
    }

    pub fn steps(&self) -> &[Step] {
        self.steps.as_slice()
    }

    #[cfg(test)]
    pub fn seq(stmts: &[&str]) -> Self {
        use crate::{query::Params, query_analysis::Statement};

        let mut steps = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            let step = Step {
                cond: None,
                query: Query {
                    stmt: Statement::parse(stmt).next().unwrap().unwrap(),
                    params: Params::empty(),
                    want_rows: true,
                },
            };

            steps.push(step);
        }

        Self::new(steps)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Step {
    pub cond: Option<Cond>,
    pub query: Query,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Cond {
    Ok { step: usize },
    Err { step: usize },
    Not { cond: Box<Self> },
    Or { conds: Vec<Self> },
    And { conds: Vec<Self> },
    IsAutocommit,
}

#[derive(Debug, Clone)]
pub struct DescribeResponse {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<DescribeCol>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

#[derive(Debug, Clone)]
pub struct DescribeParam {
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DescribeCol {
    pub name: String,
    pub decltype: Option<String>,
}

pub struct Vm<'a, B, F, S> {
    results: Vec<bool>,
    builder: B,
    program: &'a Program,
    current_step: usize,
    should_block: F,
    update_stats: S,
    resolve_attach_path: ResolveNamespacePathFn,
}

impl<'a, B, F, S> Vm<'a, B, F, S>
where
    B: QueryResultBuilder,
    F: Fn(&StmtKind) -> (bool, Option<String>),
    S: Fn(String, u64, u64, u64, Duration),
{
    pub fn new(
        builder: B,
        program: &'a Program,
        should_block: F,
        update_stats: S,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> Self {
        Self {
            results: Vec::with_capacity(program.steps().len()),
            builder,
            program,
            current_step: 0,
            should_block,
            update_stats,
            resolve_attach_path,
        }
    }

    #[inline]
    fn current_step(&self) -> &Step {
        &self.program.steps()[self.current_step]
    }

    pub fn step(&mut self, conn: &rusqlite::Connection) -> crate::Result<bool> {
        match self.try_step(conn) {
            Ok(res) => {
                self.results.push(res);
                self.current_step += 1;
                Ok(self.current_step < self.program.steps().len())
            }
            Err(e) => Err(e),
        }
    }

    fn try_step(&mut self, conn: &rusqlite::Connection) -> crate::Result<bool> {
        self.builder.begin_step()?;
        let mut enabled = match self.current_step().cond.as_ref() {
            Some(cond) => match eval_cond(cond, &self.results, conn.is_autocommit()) {
                Ok(enabled) => enabled,
                Err(e) => {
                    self.builder.step_error(e).unwrap();
                    false
                }
            },
            None => true,
        };

        let (affected_row_count, last_insert_rowid) = if enabled {
            match self.execute_query(conn) {
                // builder error interrupt the execution of query. we should exit immediately.
                Err(e @ Error::BuilderError(_)) => return Err(e),
                Err(mut e) => {
                    if let Error::RusqliteError(err) = e {
                        let extended_code =
                            unsafe { rusqlite::ffi::sqlite3_extended_errcode(conn.handle()) };

                        e = Error::RusqliteErrorExtended(err, extended_code as i32);
                    };

                    self.builder.step_error(e)?;
                    enabled = false;
                    (0, None)
                }
                Ok(x) => x,
            }
        } else {
            (0, None)
        };

        self.builder
            .finish_step(affected_row_count, last_insert_rowid)?;

        Ok(enabled)
    }

    fn prepare_attach_query(&self, attached: &str, attached_alias: &str) -> crate::Result<String> {
        let attached = attached.strip_prefix('"').unwrap_or(attached);
        let attached = attached.strip_suffix('"').unwrap_or(attached);
        let attached = NamespaceName::from_string(attached.into())?;
        let path = (self.resolve_attach_path)(&attached)?;
        let query = format!(
            "ATTACH DATABASE 'file:{}?mode=ro' AS \"{attached_alias}\"",
            path.join("data").display()
        );
        tracing::trace!("ATTACH rewritten to: {query}");
        Ok(query)
    }

    fn execute_query(&mut self, conn: &rusqlite::Connection) -> crate::Result<(u64, Option<i64>)> {
        tracing::debug!("executing query: {}", self.current_step().query.stmt.stmt);

        increment_counter!("libsql_server_libsql_query_execute");

        let start = Instant::now();
        let (blocked, reason) = (self.should_block)(&self.current_step().query.stmt.kind);
        if blocked {
            return Err(Error::Blocked(reason));
        }

        let mut stmt = if matches!(self.current_step().query.stmt.kind, StmtKind::Attach(_)) {
            match &self.current_step().query.stmt.attach_info {
                Some((attached, attached_alias)) => {
                    // nope nope nope: only builder error should return
                    let query = self.prepare_attach_query(attached, attached_alias)?;
                    conn.prepare(&query)?
                }
                None => {
                    return Err(Error::Internal(format!(
                        "Failed to ATTACH: {:?}",
                        self.current_step().query.stmt.attach_info
                    )))
                }
            }
        } else {
            conn.prepare(&self.current_step().query.stmt.stmt)?
        };

        if stmt.readonly() {
            READ_QUERY_COUNT.increment(1);
        } else {
            WRITE_QUERY_COUNT.increment(1);
        }

        let cols = stmt.columns();
        let cols_count = cols.len();
        self.builder.cols_description(cols.iter())?;
        drop(cols);

        self.current_step()
            .query
            .params
            .bind(&mut stmt)
            .map_err(Error::LibSqlInvalidQueryParams)?;

        let mut qresult = stmt.raw_query();

        let mut values_total_bytes = 0;
        self.builder.begin_rows()?;
        while let Some(row) = qresult.next()? {
            self.builder.begin_row()?;
            for i in 0..cols_count {
                let val = row.get_ref(i)?;
                values_total_bytes += value_size(&val);
                self.builder.add_row_value(val)?;
            }
            self.builder.finish_row()?;
        }
        histogram!("libsql_server_returned_bytes", values_total_bytes as f64);

        self.builder.finish_rows()?;

        // sqlite3_changes() is only modified for INSERT, UPDATE or DELETE; it is not reset for SELECT,
        // but we want to return 0 in that case.
        let affected_row_count = match self.current_step().query.stmt.is_iud {
            true => conn.changes(),
            false => 0,
        };

        // sqlite3_last_insert_rowid() only makes sense for INSERTs into a rowid table. we can't detect
        // a rowid table, but at least we can detect an INSERT
        let last_insert_rowid = match self.current_step().query.stmt.is_insert {
            true => Some(conn.last_insert_rowid()),
            false => None,
        };

        drop(qresult);

        let query_duration = start.elapsed();

        let rows_read = stmt.get_status(StatementStatus::RowsRead) as u64;
        let rows_written = stmt.get_status(StatementStatus::RowsWritten) as u64;
        let mem_used = stmt.get_status(StatementStatus::MemUsed) as u64;

        (self.update_stats)(
            self.current_step().query.stmt.stmt.clone(),
            rows_read,
            rows_written,
            mem_used,
            query_duration,
        );

        self.builder
            .add_stats(rows_read, rows_written, query_duration);

        Ok((affected_row_count, last_insert_rowid))
    }

    pub fn builder(&mut self) -> &mut B {
        &mut self.builder
    }

    /// advance the program without executing the step
    pub(crate) fn advance(&mut self) {
        self.current_step += 1;
    }

    pub(crate) fn finished(&self) -> bool {
        self.current_step >= self.program.steps().len()
    }

    pub(crate) fn into_builder(self) -> B {
        self.builder
    }
}

fn eval_cond(cond: &Cond, results: &[bool], is_autocommit: bool) -> crate::Result<bool> {
    let get_step_res = |step: usize| -> crate::Result<bool> {
        let res = results.get(step).ok_or(Error::InvalidBatchStep(step))?;
        Ok(*res)
    };

    Ok(match cond {
        Cond::Ok { step } => get_step_res(*step)?,
        Cond::Err { step } => !get_step_res(*step)?,
        Cond::Not { cond } => !eval_cond(cond, results, is_autocommit)?,
        Cond::And { conds } => conds.iter().try_fold(true, |x, cond| {
            eval_cond(cond, results, is_autocommit).map(|y| x & y)
        })?,
        Cond::Or { conds } => conds.iter().try_fold(false, |x, cond| {
            eval_cond(cond, results, is_autocommit).map(|y| x | y)
        })?,
        Cond::IsAutocommit => is_autocommit,
    })
}

fn value_size(val: &rusqlite::types::ValueRef) -> usize {
    use rusqlite::types::ValueRef;
    match val {
        ValueRef::Null => 0,
        ValueRef::Integer(_) => 8,
        ValueRef::Real(_) => 8,
        ValueRef::Text(s) => s.len(),
        ValueRef::Blob(b) => b.len(),
    }
}

pub fn check_program_auth(
    ctx: &RequestContext,
    pgm: &Program,
    config: &DatabaseConfig,
) -> crate::Result<()> {
    for step in pgm.steps() {
        match &step.query.stmt.kind {
            StmtKind::TxnBegin
            | StmtKind::TxnEnd
            | StmtKind::Read
            | StmtKind::Savepoint
            | StmtKind::Release => {
                ctx.auth.has_right(&ctx.namespace, Permission::Read)?;
            }
            StmtKind::DDL if config.shared_schema_name.is_some() => {
                ctx.auth().ddl_permitted(&ctx.namespace)?;
            }
            StmtKind::DDL | StmtKind::Write => {
                ctx.auth().has_right(&ctx.namespace, Permission::Write)?;
            }
            StmtKind::Attach(ref ns) => {
                ctx.auth.has_right(ns, Permission::AttachRead)?;
                if !ctx.meta_store.handle(ns.clone()).get().allow_attach {
                    return Err(Error::NotAuthorized(format!(
                        "Namespace `{ns}` doesn't allow attach"
                    )));
                }
            }
            StmtKind::Detach => (),
        }
    }

    Ok(())
}

pub fn check_describe_auth(ctx: RequestContext) -> crate::Result<()> {
    ctx.auth().has_right(ctx.namespace(), Permission::Read)?;
    Ok(())
}
