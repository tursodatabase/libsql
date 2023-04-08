use std::sync::Arc;

use crate::query::{Params, Query, QueryResult};
use crate::query_analysis::{State, Statement};
use crate::Result;

pub mod dump_loader;
pub mod factory;
pub mod libsql;
pub mod write_proxy;

const TXN_TIMEOUT_SECS: u64 = 5;

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

#[async_trait::async_trait]
pub trait Database: Send + Sync {
    /// Executes a query program
    async fn execute_program(&self, pgm: Program) -> Result<(Vec<Option<QueryResult>>, State)>;

    /// Unconditionnaly execute a query as part of a program
    async fn execute_one(&self, query: Query) -> Result<(QueryResult, State)> {
        let pgm = Program::new(vec![Step { cond: None, query }]);

        let (results, state) = self.execute_program(pgm).await?;
        Ok((results.into_iter().next().unwrap().unwrap(), state))
    }

    /// Execute all the queries in the batch sequentially.
    /// If an query in the batch fails, the remaining queries are ignores, and the batch current
    /// transaction (if any) is rolledback.
    async fn execute_batch_or_rollback(
        &self,
        batch: Vec<Query>,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
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

        if !steps.is_empty() {
            // We add a conditional rollback step if the last step was not sucessful.
            steps.push(Step {
                query: Query {
                    stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                    params: Params::empty(),
                },
                cond: Some(Cond::Not {
                    cond: Box::new(Cond::Ok {
                        step: steps.len() - 1,
                    }),
                }),
            })
        }

        let pgm = Program::new(steps);

        let (mut results, state) = self.execute_program(pgm).await?;
        // remove the rollback result
        results.pop();

        Ok((results, state))
    }

    async fn rollback(&self) -> Result<()> {
        let (results, _) = self
            .execute_one(Query {
                stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                params: Params::empty(),
            })
            .await?;

        results?;

        Ok(())
    }
}
