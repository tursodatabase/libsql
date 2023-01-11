use anyhow::Result;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::{
    ast::{Cmd, Stmt},
    lexer::sql::Parser,
};

/// A group of statements to be executed together.
#[derive(Debug)]
pub struct Statement {
    pub stmt: String,
    pub kind: StmtKind,
}

/// Classify statement in categories of interest.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StmtKind {
    /// The begining of a transaction
    TxnBegin,
    /// The end of a transaction
    TxnEnd,
    Read,
    Write,
    Other,
}

impl StmtKind {
    fn kind(cmd: &Cmd) -> Option<Self> {
        match cmd {
            Cmd::Explain(_) => Some(Self::Other),
            Cmd::ExplainQueryPlan(_) => Some(Self::Other),
            Cmd::Stmt(Stmt::Begin { .. }) => Some(Self::TxnBegin),
            Cmd::Stmt(Stmt::Commit { .. } | Stmt::Rollback { .. }) => Some(Self::TxnEnd),
            Cmd::Stmt(
                Stmt::Insert { .. }
                | Stmt::CreateTable { .. }
                | Stmt::Update { .. }
                | Stmt::Delete { .. }
                | Stmt::DropTable { .. },
            ) => Some(Self::Write),
            Cmd::Stmt(Stmt::Select { .. }) => Some(Self::Read),
            _ => None,
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

impl State {
    pub fn step(&mut self, kind: StmtKind) {
        *self = match (*self, kind) {
            (State::TxnOpened, StmtKind::TxnBegin) | (State::TxnClosed, StmtKind::TxnEnd) => {
                State::Invalid
            }
            (State::TxnOpened, StmtKind::TxnEnd) => State::TxnClosed,
            (State::TxnClosed, StmtKind::TxnBegin) => State::TxnOpened,
            (state, StmtKind::Other | StmtKind::Write | StmtKind::Read) => state,
            (State::Invalid, _) => State::Invalid,
            (State::Start, StmtKind::TxnBegin) => State::TxnOpened,
            (State::Start, StmtKind::TxnEnd) => State::TxnClosed,
        };
    }

    pub fn reset(&mut self) {
        *self = State::Start
    }
}

impl Statement {
    pub fn parse(s: String) -> Result<Option<Self>> {
        let mut parser = Parser::new(s.as_bytes());
        match parser.next()? {
            Some(cmd) => {
                let kind =
                    StmtKind::kind(&cmd).ok_or_else(|| anyhow::anyhow!("unsupported statement"))?;

                Ok(Some(Self {
                    stmt: cmd.to_string(),
                    kind,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(
            self.kind,
            StmtKind::Read | StmtKind::TxnEnd | StmtKind::TxnBegin
        )
    }
}
