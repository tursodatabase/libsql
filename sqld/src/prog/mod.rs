use self::stmt::proto_error_from_stmt_error;
pub use self::stmt::{execute_stmt, StmtError};
use crate::database::Database;
use anyhow::{bail, Result};
use std::collections::HashMap;

pub mod proto;
mod stmt;

#[derive(thiserror::Error, Debug)]
pub enum ProgError {
    #[error("Variable {var} is not set")]
    ExprUnsetVar { var: i32 },
}

#[derive(Debug, Default)]
struct Ctx {
    vars: HashMap<i32, proto::Value>,
    results: Vec<Option<Result<proto::StmtResult, StmtError>>>,
    outputs: Vec<proto::Value>,
}

pub async fn execute_prog(db: &dyn Database, prog: &proto::Prog) -> Result<proto::ProgResult> {
    let mut ctx = Ctx::default();
    for step in prog.steps.iter() {
        execute_step(&mut ctx, db, step).await?;
    }

    let mut execute_results = Vec::with_capacity(ctx.results.len());
    let mut execute_errors = Vec::with_capacity(ctx.results.len());
    for result in ctx.results.into_iter() {
        let (execute_result, execute_error) = match result {
            Some(Ok(stmt_res)) => (Some(stmt_res), None),
            Some(Err(stmt_err)) => (None, Some(proto_error_from_stmt_error(&stmt_err))),
            None => (None, None),
        };
        execute_results.push(execute_result);
        execute_errors.push(execute_error);
    }

    Ok(proto::ProgResult {
        execute_results,
        execute_errors,
        outputs: ctx.outputs,
    })
}

async fn execute_step(ctx: &mut Ctx, db: &dyn Database, step: &proto::ProgStep) -> Result<()> {
    match step {
        proto::ProgStep::Execute(step) => {
            let enabled = match step.condition.as_ref() {
                Some(expr) => is_truthy(&eval_expr(ctx, expr)?),
                None => true,
            };

            let result = if enabled {
                let result = match execute_stmt(db, &step.stmt).await {
                    Ok(stmt_result) => Ok(stmt_result),
                    Err(err) => Err(err.downcast::<StmtError>()?),
                };

                let ops = match result.is_ok() {
                    true => &step.on_ok,
                    false => &step.on_error,
                };
                execute_ops(ctx, ops)?;

                Some(result)
            } else {
                None
            };

            ctx.results.push(result);
        }
        proto::ProgStep::Output { expr } => ctx.outputs.push(eval_expr(ctx, expr)?),
        proto::ProgStep::Op { ops } => execute_ops(ctx, ops)?,
    }
    Ok(())
}

fn execute_ops(ctx: &mut Ctx, ops: &[proto::ProgOp]) -> Result<()> {
    ops.iter().try_for_each(|op| execute_op(ctx, op))
}

fn execute_op(ctx: &mut Ctx, op: &proto::ProgOp) -> Result<()> {
    match op {
        proto::ProgOp::Set { var, expr } => {
            let value = eval_expr(ctx, expr)?;
            ctx.vars.insert(*var, value);
        }
    }
    Ok(())
}

fn eval_expr(ctx: &Ctx, expr: &proto::ProgExpr) -> Result<proto::Value> {
    Ok(match expr {
        proto::ProgExpr::Expr(expr) => match expr {
            proto::ProgExpr_::Var { var } => match ctx.vars.get(var) {
                Some(value) => value.clone(),
                None => bail!(ProgError::ExprUnsetVar { var: *var }),
            },
            proto::ProgExpr_::Not { expr } => match is_truthy(&eval_expr(ctx, expr)?) {
                true => proto::Value::Integer { value: 0 },
                false => proto::Value::Integer { value: 1 },
            },
        },
        proto::ProgExpr::Value(value) => value.clone(),
    })
}

fn is_truthy(value: &proto::Value) -> bool {
    match value {
        proto::Value::Null => false,
        proto::Value::Integer { value } => *value != 0,
        proto::Value::Float { value } => *value != 0.,
        proto::Value::Text { value } => !value.is_empty(),
        proto::Value::Blob { value } => !value.is_empty(),
    }
}
