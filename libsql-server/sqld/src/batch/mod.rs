use self::stmt::proto_error_from_stmt_error;
pub use self::stmt::{execute_stmt, StmtError};
use crate::database::Database;
use anyhow::{bail, Result};

pub mod proto;
mod stmt;

#[derive(thiserror::Error, Debug)]
pub enum BatchError {
    #[error("Invalid reference to step in a condition")]
    CondBadStep,
}

#[derive(Debug, Default)]
struct Ctx {
    results: Vec<Option<Result<proto::StmtResult, StmtError>>>,
}

pub async fn execute_batch(db: &dyn Database, prog: &proto::Batch) -> Result<proto::BatchResult> {
    let mut ctx = Ctx::default();
    for step in prog.steps.iter() {
        execute_step(&mut ctx, db, step).await?;
    }

    let mut step_results = Vec::with_capacity(ctx.results.len());
    let mut step_errors = Vec::with_capacity(ctx.results.len());
    for result in ctx.results.into_iter() {
        let (step_result, step_error) = match result {
            Some(Ok(stmt_res)) => (Some(stmt_res), None),
            Some(Err(stmt_err)) => (None, Some(proto_error_from_stmt_error(&stmt_err))),
            None => (None, None),
        };
        step_results.push(step_result);
        step_errors.push(step_error);
    }

    Ok(proto::BatchResult {
        step_results,
        step_errors,
    })
}

async fn execute_step(ctx: &mut Ctx, db: &dyn Database, step: &proto::BatchStep) -> Result<()> {
    let enabled = match step.condition.as_ref() {
        Some(cond) => eval_cond(ctx, cond)?,
        None => true,
    };

    let result = if enabled {
        Some(match execute_stmt(db, &step.stmt).await {
            Ok(stmt_result) => Ok(stmt_result),
            Err(err) => Err(err.downcast::<StmtError>()?),
        })
    } else {
        None
    };

    ctx.results.push(result);
    Ok(())
}

fn eval_cond(ctx: &Ctx, cond: &proto::BatchCond) -> Result<bool> {
    let get_step_res = |step: i32| -> Result<&Option<Result<proto::StmtResult, StmtError>>> {
        let Ok(step) = usize::try_from(step) else {
            bail!(BatchError::CondBadStep)
        };
        let Some(res) = ctx.results.get(step) else {
            bail!(BatchError::CondBadStep)
        };
        Ok(res)
    };

    Ok(match cond {
        proto::BatchCond::Ok { step } => match get_step_res(*step)? {
            Some(Ok(_)) => true,
            Some(Err(_)) => false,
            None => false,
        },
        proto::BatchCond::Error { step } => match get_step_res(*step)? {
            Some(Ok(_)) => false,
            Some(Err(_)) => true,
            None => false,
        },
        proto::BatchCond::Not { cond } => !eval_cond(ctx, cond)?,
        proto::BatchCond::And { conds } => conds
            .iter()
            .try_fold(true, |x, cond| eval_cond(ctx, cond).map(|y| x & y))?,
        proto::BatchCond::Or { conds } => conds
            .iter()
            .try_fold(false, |x, cond| eval_cond(ctx, cond).map(|y| x | y))?,
    })
}

impl BatchError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::CondBadStep => "BATCH_COND_BAD_STEP",
        }
    }
}
