use anyhow::Result;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::ast::{Cmd, PragmaBody, QualifiedName, Stmt};
use sqlite3_parser::lexer::sql::{Parser, ParserError};

/// A group of statements to be executed together.
#[derive(Debug, Clone)]
pub struct Statement {
    pub stmt: String,
    pub kind: StmtKind,
    /// Is the statement an INSERT, UPDATE or DELETE?
    pub is_iud: bool,
    pub is_insert: bool,
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

fn is_temp(name: &QualifiedName) -> bool {
    name.db_name.as_ref().map(|n| n.0.as_str()) == Some("TEMP")
}

impl StmtKind {
    fn kind(cmd: &Cmd) -> Option<Self> {
        match cmd {
            Cmd::Explain(_) => Some(Self::Other),
            Cmd::ExplainQueryPlan(_) => Some(Self::Other),
            Cmd::Stmt(Stmt::Begin { .. }) => Some(Self::TxnBegin),
            Cmd::Stmt(Stmt::Commit { .. } | Stmt::Rollback { .. }) => Some(Self::TxnEnd),
            Cmd::Stmt(
                Stmt::CreateVirtualTable { tbl_name, .. }
                | Stmt::CreateTable {
                    tbl_name,
                    temporary: false,
                    ..
                },
            ) if !is_temp(tbl_name) => Some(Self::Write),
            Cmd::Stmt(
                Stmt::Insert { .. }
                | Stmt::Update { .. }
                | Stmt::Delete { .. }
                | Stmt::DropTable { .. }
                | Stmt::AlterTable { .. }
                | Stmt::CreateTrigger {
                    temporary: false, ..
                }
                | Stmt::CreateIndex { .. },
            ) => Some(Self::Write),
            Cmd::Stmt(Stmt::Select { .. }) => Some(Self::Read),
            Cmd::Stmt(Stmt::Pragma(name, body)) => {
                if is_pragma_allowed(name, body.as_ref()) {
                    Some(Self::Write)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn is_pragma_allowed(name: &QualifiedName, body: Option<&PragmaBody>) -> bool {
    let name = name.name.0.as_str();
    match name {
        // always ok
        "foreign_key" | "foreign_key_list" | "foreign_key_check" | "collation_list"
        | "compile_options" | "data_version" | "database_list" | "freelist_count"
        | "function_list" | "index_list" | "index_xinfo" | "integrity_check"
        | "legacy_file_format" | "page_count" | "pragma_list" | "quick_check" | "stats" | "table_info" | "table_list" | "table_xinfo" => true,
        // ok without args
        "analysis_limit"
        | "application_id"
        | "auto_vacuum"
        | "automatic_index"
        | "busy_timeout"
        | "cache_size"
        | "cache_spill"
        | "cell_size_check"
        | "checkpoint_fullfsync"
        | "defer_foreign_keys"
        | "encoding"
        | "fullfsync"
        | "hard_heap_limit"
        | "journal_mode"
        | "journal_size_limit"
        | "legacy_alter_table"
        | "locking_mode"
        | "max_page_count"
        | "mmap_size"
        | "page_size"
        | "query_only"
        | "read_uncommitted"
        | "recursive_triggers"
        | "reverse_unordered_selects"
        | "schema_version"
        | "secure_delete"
        | "soft_heap_limit"
        | "synchronous"
        | "temp_store"
        | "threads"
        | "trusted_schema"
        | "user_version"
        | "wal_autocheckpoint"

            if body.is_none() =>
        {
            true
        }
        // changes the state of the connection, and can't be allowed rn:
        "case_sensitive_like" | "ignore_check_constraints" | "incremental_vacuum" 
            // TODO: check if optimize can be safely performed
            | "optimize"
            | "parser_trace"
            | "shrink_memory"
            | "wal_checkpoint"

            => {
            false
        }
        _ => false,
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
            is_iud: false,
            is_insert: false,
        }
    }

    pub fn parse(s: &str) -> impl Iterator<Item = Result<Self>> + '_ {
        fn parse_inner(original: &str, c: Cmd) -> Result<Statement> {
            let kind =
                StmtKind::kind(&c).ok_or_else(|| anyhow::anyhow!("unsupported statement"))?;

            // XXX: Temporary workaround for https://github.com/gwenn/lemon-rs/issues/30
            if let Cmd::Stmt(Stmt::CreateVirtualTable { .. }) = &c {
                return Ok(Statement {
                    stmt: original.to_string(),
                    kind,
                    is_iud: false,
                    is_insert: false,
                });
            }

            let is_iud = matches!(
                c,
                Cmd::Stmt(Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. })
            );
            let is_insert = matches!(c, Cmd::Stmt(Stmt::Insert { .. }));

            Ok(Statement {
                stmt: c.to_string(),
                kind,
                is_iud,
                is_insert,
            })
        }
        // The parser needs to be boxed because it's large, and you don't want it on the stack.
        // There's upstream work to make it smaller, but in the meantime the parser should remain
        // on the heap:
        // - https://github.com/gwenn/lemon-rs/issues/8
        // - https://github.com/gwenn/lemon-rs/pull/19
        let mut parser = Box::new(Parser::new(s.as_bytes()));
        std::iter::from_fn(move || match parser.next() {
            Ok(Some(cmd)) => Some(parse_inner(s, cmd)),
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

/// Given a an initial state and an array of queries, attempts to predict what the final state will
/// be
pub fn predict_final_state<'a>(
    mut state: State,
    stmts: impl Iterator<Item = &'a Statement>,
) -> State {
    for stmt in stmts {
        state.step(stmt.kind);
    }
    state
}
