use anyhow::Result;

use crate::database::{Cond, Database, Program, Step};

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

fn batch_to_program(batch: &proto::Batch) -> Result<Program> {
    let mut steps = Vec::with_capacity(batch.steps.len());
    for step in &batch.steps {
        let query = proto_stmt_to_query(&step.stmt)?;
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

pub async fn execute_batch(db: &dyn Database, batch: &proto::Batch) -> Result<proto::BatchResult> {
    let pgm = batch_to_program(batch)?;
    let mut step_results = Vec::with_capacity(pgm.steps.len());
    let mut step_errors = Vec::with_capacity(pgm.steps.len());
    let (results, _state) = db.execute_program(pgm).await?;
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
