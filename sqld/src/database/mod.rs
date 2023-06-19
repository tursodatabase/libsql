use std::sync::Arc;
use std::time::Duration;

use crate::auth::Authenticated;
use crate::query::{Params, Query};
use crate::query_analysis::{State, Statement};
use crate::query_result_builder::{IgnoreResult, QueryResultBuilder};
use crate::Result;

pub mod config;
pub mod dump;
pub mod factory;
pub mod libsql;
pub mod write_proxy;

const TXN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct Step {
    pub cond: Option<Cond>,
    pub query: Query,
}

#[derive(Debug, Clone)]
pub enum Cond {
    Ok { step: usize },
    Err { step: usize },
    Not { cond: Box<Self> },
    Or { conds: Vec<Self> },
    And { conds: Vec<Self> },
}

pub type DescribeResult = Result<DescribeResponse>;

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

#[async_trait::async_trait]
pub trait Database: Send + Sync + 'static {
    /// Executes a query program
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        reponse_builder: B,
    ) -> Result<(B, State)>;

    /// Execute all the queries in the batch sequentially.
    /// If an query in the batch fails, the remaining queries are ignores, and the batch current
    /// transaction (if any) is rolledback.
    async fn execute_batch_or_rollback<B: QueryResultBuilder>(
        &self,
        batch: Vec<Query>,
        auth: Authenticated,
        result_builder: B,
    ) -> Result<(B, State)> {
        let batch_len = batch.len();
        let mut steps = make_batch_program(batch);

        if !steps.is_empty() {
            // We add a conditional rollback step if the last step was not sucessful.
            steps.push(Step {
                query: Query {
                    stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                    params: Params::empty(),
                    want_rows: false,
                },
                cond: Some(Cond::Not {
                    cond: Box::new(Cond::Ok {
                        step: steps.len() - 1,
                    }),
                }),
            })
        }

        let pgm = Program::new(steps);

        // ignore the rollback result
        let builder = result_builder.take(batch_len);
        let (builder, state) = self.execute_program(pgm, auth, builder).await?;

        Ok((builder.into_inner(), state))
    }

    /// Execute all the queries in the batch sequentially.
    /// If an query in the batch fails, the remaining queries are ignored
    async fn execute_batch<B: QueryResultBuilder>(
        &self,
        batch: Vec<Query>,
        auth: Authenticated,
        result_builder: B,
    ) -> Result<(B, State)> {
        let steps = make_batch_program(batch);
        let pgm = Program::new(steps);
        self.execute_program(pgm, auth, result_builder).await
    }

    async fn rollback(&self, auth: Authenticated) -> Result<()> {
        self.execute_batch(
            vec![Query {
                stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                params: Params::empty(),
                want_rows: false,
            }],
            auth,
            IgnoreResult,
        )
        .await?;

        Ok(())
    }

    /// Parse the SQL statement and return information about it.
    async fn describe(&self, sql: String, auth: Authenticated) -> Result<DescribeResult>;
}

fn make_batch_program(batch: Vec<Query>) -> Vec<Step> {
    let mut steps = Vec::with_capacity(batch.len());
    for (i, query) in batch.into_iter().enumerate() {
        let cond = if i > 0 {
            // only execute if the previous step was a success
            Some(Cond::Ok { step: i - 1 })
        } else {
            None
        };

        let step = Step { cond, query };
        steps.push(step);
    }
    steps
}
