use anyhow::Result;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::{
    ast::{Cmd, Stmt},
    lexer::sql::{Parser, ParserError},
};

/// A group of statements to be executed together.
#[derive(Debug)]
pub struct Statement {
    pub stmt: String,
    pub kind: StmtKind,
}

impl Default for Statement {
    fn default() -> Self {
        Self::empty()
    }
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
    Txn,
    /// The txn in a closed state
    Init,
    /// This is an invalid state for the state machine
    Invalid,
}

impl State {
    pub fn step(&mut self, kind: StmtKind) {
        *self = match (*self, kind) {
            (State::Txn, StmtKind::TxnBegin) | (State::Init, StmtKind::TxnEnd) => State::Invalid,
            (State::Txn, StmtKind::TxnEnd) => State::Init,
            (state, StmtKind::Other | StmtKind::Write | StmtKind::Read) => state,
            (State::Invalid, _) => State::Invalid,
            (State::Init, StmtKind::TxnBegin) => State::Txn,
        };
    }

    pub fn reset(&mut self) {
        *self = State::Init
    }
}

impl Statement {
    pub fn empty() -> Self {
        Self {
            stmt: String::new(),
            // empty statement is arbitrarely made of the read kind so it is not send to a writer
            kind: StmtKind::Read,
        }
    }

    pub fn parse(s: &str) -> impl Iterator<Item = Result<Self>> + '_ {
        fn parse_inner(c: Cmd) -> Result<Statement> {
            let kind =
                StmtKind::kind(&c).ok_or_else(|| anyhow::anyhow!("unsupported statement"))?;

            Ok(Statement {
                stmt: c.to_string(),
                kind,
            })
        }
        let mut parser = Box::new(Parser::new(s.as_bytes()));
        std::iter::from_fn(move || match parser.next() {
            Ok(Some(cmd)) => Some(parse_inner(cmd)),
            Ok(None) => None,
            Err(sqlite3_parser::lexer::sql::Error::ParserError(
                ParserError::SyntaxError {
                    token_type: _,
                    found: Some(found),
                },
                Some((line, col)),
            )) => Some(Err(anyhow::anyhow!(
                "syntax error around L{line}:{col}: `{found}`"
            ))),
            Err(e) => Some(Err(e.into())),
        })
    }

    pub fn is_read_only(&self) -> bool {
        matches!(
            self.kind,
            StmtKind::Read | StmtKind::TxnEnd | StmtKind::TxnBegin
        )
    }
}

/// Given a an initial state and an array of queries, return the final state obtained if all the
/// queries succeeded
pub fn final_state<'a>(mut state: State, stmts: impl Iterator<Item = &'a Statement>) -> State {
    for stmt in stmts {
        state.step(stmt.kind);
    }
    state
}
