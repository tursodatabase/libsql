use std::sync::Arc;

use crate::query::Query;

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
