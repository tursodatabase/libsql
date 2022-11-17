use std::fmt;

use anyhow::Result;
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};

/// A group of statements to be executed together.
pub struct Statements {
    pub stmts: String,
    /// At least one of the statements starts a transaction.
    pub has_txn_begin: bool,
    /// at least one of the statements ends a transaction.
    pub has_txn_end: bool,
}

impl fmt::Debug for Statements {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.stmts)
    }
}

/// Classify statement in categories of interest.
enum StmtKind {
    /// The begining of a transaction
    TxnBegin,
    /// The end of a transaction
    TxnEnd,
    Other,
}

impl StmtKind {
    fn kind(stmt: &Statement) -> Self {
        match stmt {
            Statement::StartTransaction { .. } => Self::TxnBegin,
            Statement::SetTransaction { .. } => todo!("handle set txn"),
            Statement::Rollback { .. } | Statement::Commit { .. } => Self::TxnEnd,
            Statement::Savepoint { .. } => todo!("handle savepoint"),
            // FIXME: this contains lots of dialect specific nodes, when porting to Postges, check what's
            // in there.
            _ => Self::Other,
        }
    }
}

impl Statements {
    pub fn parse(s: String) -> Result<Self> {
        let statements = Parser::parse_sql(&SQLiteDialect {}, &s)?;
        let mut has_txn_begin = false;
        let mut has_txn_end = false;
        for stmt in &statements {
            match StmtKind::kind(stmt) {
                StmtKind::TxnBegin => has_txn_begin = true,
                StmtKind::TxnEnd => has_txn_end = true,
                StmtKind::Other => (),
            }
        }

        Ok(Self {
            stmts: s,
            has_txn_begin,
            has_txn_end,
        })
    }
}
