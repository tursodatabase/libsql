use std::fmt;

use anyhow::Result;
use sqlparser::{ast::Statement, dialect::SQLiteDialect, parser::Parser};

/// A group of statements to be executed together.
pub struct Statements {
    pub stmts: String,
    kinds: Vec<StmtKind>,
}

impl fmt::Debug for Statements {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.stmts)
    }
}

/// Classify statement in categories of interest.
#[derive(Debug, PartialEq, Clone, Copy)]
enum StmtKind {
    /// The begining of a transaction
    TxnBegin,
    /// The end of a transaction
    TxnEnd,
    Read,
    Write,
    Other,
}

impl StmtKind {
    fn kind(stmt: &Statement) -> Self {
        match stmt {
            Statement::StartTransaction { .. } => Self::TxnBegin,
            Statement::SetTransaction { .. } => todo!("handle set txn"),
            Statement::Rollback { .. } | Statement::Commit { .. } => Self::TxnEnd,
            Statement::Savepoint { .. } => todo!("handle savepoint"),

            Statement::Query(_) => Self::Read,

            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                Self::Write
            }
            Statement::Prepare { .. } => todo!(),
            // FIXME: this contains lots of dialect specific nodes, when porting to Postges, check what's
            // in there.
            _ => Self::Other,
        }
    }
}

/// The state of a transaction for a series of statement
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum State {
    /// The txn in an opened state
    TxnOpened,
    /// The txn in a closed state
    TxnClosed,
    /// This is the initial state of the state machine
    Start,
    /// This is an invalid state for the state machine
    Invalid,
}

impl Statements {
    pub fn parse(s: String) -> Result<Self> {
        // We don't really care about `StmtKind::Other`, we keep it for conceptual simplicity.
        let kinds = Parser::parse_sql(&SQLiteDialect {}, &s)
            .map(|statements| statements.iter().map(StmtKind::kind).collect())
            .unwrap_or_else(|_| vec![StmtKind::Other]);

        Ok(Self { stmts: s, kinds })
    }

    /// Given an initial state, returns the final state a transaction should be in after running these
    /// statements.
    pub fn state(&self, state: State) -> State {
        self.kinds
            .iter()
            .fold(state, |old_state, current| match (old_state, current) {
                (State::TxnOpened, StmtKind::TxnBegin) | (State::TxnClosed, StmtKind::TxnEnd) => {
                    State::Invalid
                }
                (State::TxnOpened, StmtKind::TxnEnd) => State::TxnClosed,
                (State::TxnClosed, StmtKind::TxnBegin) => State::TxnOpened,
                (state, StmtKind::Other | StmtKind::Write | StmtKind::Read) => state,
                (State::Invalid, _) => State::Invalid,
                (State::Start, StmtKind::TxnBegin) => State::TxnOpened,
                (State::Start, StmtKind::TxnEnd) => State::TxnClosed,
            })
    }

    pub fn is_read_only(&self) -> bool {
        let state = self.state(State::Start);
        let is_only_reads = self
            .kinds
            .iter()
            .all(|k| matches!(k, StmtKind::Read | StmtKind::TxnEnd | StmtKind::TxnBegin));
        (state == State::Start || state == State::TxnClosed) && is_only_reads
    }
}
