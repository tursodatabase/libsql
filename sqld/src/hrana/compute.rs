use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;

use super::proto;
use super::session::ResponseError;

pub type Result<T> = std::result::Result<T, ResponseError>;

#[derive(Debug, Default)]
pub struct Ctx {
    vars: HashMap<i32, Arc<proto::Value>>,
}

pub fn eval_ops(ctx: &mut Ctx, ops: &[proto::ComputeOp]) -> Result<Vec<Arc<proto::Value>>> {
    ops.iter().map(|op| eval_op(ctx, op)).collect()
}

pub fn eval_op(ctx: &mut Ctx, op: &proto::ComputeOp) -> Result<Arc<proto::Value>> {
    match op {
        proto::ComputeOp::Set { var, expr } => {
            let value = eval_expr(ctx, expr)?;
            ctx.vars.insert(*var, value);
            Ok(NULL.clone())
        }
        proto::ComputeOp::Unset { var } => {
            ctx.vars.remove(var);
            Ok(NULL.clone())
        }
        proto::ComputeOp::Eval { expr } => eval_expr(ctx, expr),
    }
}

static NULL: Lazy<Arc<proto::Value>> = Lazy::new(|| Arc::new(proto::Value::Null));

pub fn eval_expr(ctx: &Ctx, expr: &proto::ComputeExpr) -> Result<Arc<proto::Value>> {
    match expr {
        proto::ComputeExpr::Expr(expr) => match expr {
            proto::ComputeExpr_::Var { var } => match ctx.vars.get(var) {
                Some(value) => Ok(value.clone()),
                None => Err(ResponseError::ExprUnsetVar { var: *var }),
            },
        },
        proto::ComputeExpr::Value(value) => Ok(value.clone()),
    }
}

pub fn is_true(value: &proto::Value) -> bool {
    match value {
        proto::Value::Null => false,
        proto::Value::Integer { value } => *value != 0,
        proto::Value::Float { value } => *value != 0.,
        proto::Value::Text { value } => !value.is_empty(),
        proto::Value::Blob { value } => !value.is_empty(),
    }
}
