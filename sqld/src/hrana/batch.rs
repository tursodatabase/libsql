use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::Authenticated;
use crate::database::{Cond, Database, Program, Step};
use crate::query::{Params, Query};
use crate::query_analysis::Statement;

use super::handshake::Protocol;
use super::proto;
use super::stmt::{
    proto_error_from_stmt_error, proto_stmt_result_from_query_response, proto_stmt_to_query,
    stmt_error_from_sqld_error,
};

#[derive(thiserror::Error, Debug)]
pub enum BatchError {
    #[error("Invalid reference to step in a condition")]
    CondBadStep,
}

impl BatchError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::CondBadStep => "BATCH_COND_BAD_STEP",
        }
    }
}

fn proto_cond_to_cond(cond: &proto::BatchCond) -> Result<Cond> {
    let try_convert_step = |step: i32| -> std::result::Result<usize, BatchError> {
        usize::try_from(step).map_err(|_| BatchError::CondBadStep)
    };

    let cond = match cond {
        proto::BatchCond::Ok { step } => Cond::Ok {
            step: try_convert_step(*step)?,
        },
        proto::BatchCond::Error { step } => Cond::Err {
            step: try_convert_step(*step)?,
        },
        proto::BatchCond::Not { cond } => Cond::Not {
            cond: proto_cond_to_cond(cond)?.into(),
        },
        proto::BatchCond::And { conds } => Cond::And {
            conds: conds
                .iter()
                .map(proto_cond_to_cond)
                .collect::<Result<_>>()?,
        },
        proto::BatchCond::Or { conds } => Cond::Or {
            conds: conds
                .iter()
                .map(proto_cond_to_cond)
                .collect::<Result<_>>()?,
        },
    };

    Ok(cond)
}

pub fn proto_batch_to_program(
    batch: &proto::Batch,
    sqls: &HashMap<i32, String>,
    protocol: Protocol,
) -> Result<Program> {
    let mut steps = Vec::with_capacity(batch.steps.len());
    for step in &batch.steps {
        let query = proto_stmt_to_query(&step.stmt, sqls, protocol)?;
        let cond = step
            .condition
            .as_ref()
            .map(proto_cond_to_cond)
            .transpose()?;
        let step = Step { query, cond };

        steps.push(step);
    }

    Ok(Program::new(steps))
}

pub async fn execute_batch(
    db: &dyn Database,
    auth: Authenticated,
    pgm: Program,
) -> Result<proto::BatchResult> {
    let mut step_results = Vec::with_capacity(pgm.steps.len());
    let mut step_errors = Vec::with_capacity(pgm.steps.len());
    let (results, _state) = db.execute_program(pgm, auth).await?;
    for result in results {
        let (step_result, step_error) = match result {
            Some(Ok(r)) => (Some(proto_stmt_result_from_query_response(r)), None),
            Some(Err(e)) => (
                None,
                Some(proto_error_from_stmt_error(&stmt_error_from_sqld_error(e)?)),
            ),
            None => (None, None),
        };

        step_errors.push(step_error);
        step_results.push(step_result);
    }

    Ok(proto::BatchResult {
        step_results,
        step_errors,
    })
}

pub fn proto_sequence_to_program(sql: &str) -> Result<Program> {
    let stmts = Statement::parse(sql).collect::<Result<Vec<_>>>()?;
    let steps = stmts
        .into_iter()
        .enumerate()
        .map(|(step_i, stmt)| {
            let cond = match step_i {
                0 => None,
                _ => Some(Cond::Ok { step: step_i - 1 }),
            };
            let query = Query {
                stmt,
                params: Params::empty(),
                want_rows: false,
            };
            Step { cond, query }
        })
        .collect();
    Ok(Program {
        steps: Arc::new(steps),
    })
}

pub async fn execute_sequence(db: &dyn Database, auth: Authenticated, pgm: Program) -> Result<()> {
    let (results, _state) = db.execute_program(pgm, auth).await?;
    results.into_iter().try_for_each(|result| match result {
        Some(Ok(_)) => Ok(()),
        Some(Err(e)) => match stmt_error_from_sqld_error(e) {
            Ok(stmt_err) => Err(anyhow!(stmt_err)),
            Err(sqld_err) => Err(anyhow!(sqld_err)),
        },
        None => Err(anyhow!("Statement in sequence was not executed")),
    })
}
