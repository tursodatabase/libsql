use std::borrow::Cow;

use anyhow::Result;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::ast::{Cmd, Expr, Id, PragmaBody, QualifiedName, Stmt};
use sqlite3_parser::lexer::sql::{Parser, ParserError};

use crate::namespace::NamespaceName;

/// A group of statements to be executed together.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Statement {
    pub stmt: String,
    pub kind: StmtKind,
    /// Is the statement an INSERT, UPDATE or DELETE?
    pub is_iud: bool,
    pub is_insert: bool,
    // Optional id and alias associated with the statement (used for attach/detach)
    pub attach_info: Option<(String, String)>,
}

impl Default for Statement {
    fn default() -> Self {
        Self::empty()
    }
}

/// Classify statement in categories of interest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StmtKind {
    /// The beginning of a transaction
    TxnBegin,
    /// The end of a transaction
    TxnEnd,
    Read,
    Write,
    Savepoint,
    Release,
    Attach(NamespaceName),
    Detach,
    DDL,
}

fn is_temp(name: &QualifiedName) -> bool {
    name.db_name.as_ref().map(|n| n.0.as_str()) == Some("TEMP")
}

fn is_reserved_tbl(name: &QualifiedName) -> bool {
    let n = name.name.0.to_lowercase();
    n == "_litestream_seq" || n == "_litestream_lock" || n == "libsql_wasm_func_table"
}

fn write_if_not_reserved(name: &QualifiedName) -> Option<StmtKind> {
    (!is_reserved_tbl(name)).then_some(StmtKind::Write)
}

fn ddl_if_not_reserved(name: &QualifiedName) -> Option<StmtKind> {
    (!is_reserved_tbl(name)).then_some(StmtKind::DDL)
}

impl StmtKind {
    fn kind(cmd: &Cmd) -> Option<Self> {
        match cmd {
            Cmd::Explain(Stmt::Pragma(name, body)) => Self::pragma_kind(name, body.as_ref()),
            Cmd::Explain(_) => Some(Self::Read),
            Cmd::ExplainQueryPlan(_) => Some(Self::Read),
            Cmd::Stmt(Stmt::Begin { .. }) => Some(Self::TxnBegin),
            Cmd::Stmt(
                Stmt::Commit { .. }
                | Stmt::Rollback {
                    savepoint_name: None,
                    ..
                },
            ) => Some(Self::TxnEnd),
            Cmd::Stmt(
                Stmt::CreateVirtualTable { tbl_name, .. }
                | Stmt::CreateTable {
                    tbl_name,
                    temporary: false,
                    ..
                },
            ) if !is_temp(tbl_name) => Some(Self::DDL),
            Cmd::Stmt(
                Stmt::Insert {
                    with: _,
                    or_conflict: _,
                    tbl_name,
                    ..
                }
                | Stmt::Update {
                    with: _,
                    or_conflict: _,
                    tbl_name,
                    ..
                },
            ) => write_if_not_reserved(tbl_name),

            Cmd::Stmt(Stmt::Delete {
                with: _, tbl_name, ..
            }) => write_if_not_reserved(tbl_name),
            Cmd::Stmt(Stmt::DropTable {
                if_exists: _,
                tbl_name,
            }) => ddl_if_not_reserved(tbl_name),
            Cmd::Stmt(Stmt::AlterTable(tbl_name, _)) => ddl_if_not_reserved(tbl_name),
            Cmd::Stmt(
                Stmt::DropIndex { .. }
                | Stmt::DropTrigger { .. }
                | Stmt::CreateTrigger {
                    temporary: false, ..
                }
                | Stmt::CreateIndex { .. },
            ) => Some(Self::DDL),
            Cmd::Stmt(Stmt::Select { .. }) => Some(Self::Read),
            Cmd::Stmt(Stmt::Pragma(name, body)) => Self::pragma_kind(name, body.as_ref()),
            // Creating regular views is OK, temporary views are bound to a connection
            // and thus disallowed in sqld.
            Cmd::Stmt(Stmt::CreateView {
                temporary: false, ..
            }) => Some(Self::DDL),
            Cmd::Stmt(Stmt::DropView { .. }) => Some(Self::DDL),
            Cmd::Stmt(Stmt::Savepoint(_)) => Some(Self::Savepoint),
            Cmd::Stmt(Stmt::Release(_))
            | Cmd::Stmt(Stmt::Rollback {
                savepoint_name: Some(_),
                ..
            }) => Some(Self::Release),
            Cmd::Stmt(Stmt::Attach {
                expr: Expr::Id(Id(db_name)),
                ..
            }) => {
                let db_name = db_name
                    .strip_prefix('"')
                    .unwrap_or(db_name)
                    .strip_suffix('"')
                    .unwrap_or(db_name);
                Some(Self::Attach(
                    NamespaceName::from_string(db_name.to_string()).ok()?,
                ))
            }
            Cmd::Stmt(Stmt::Detach(_)) => Some(Self::Detach),
            _ => None,
        }
    }

    fn pragma_kind(name: &QualifiedName, body: Option<&PragmaBody>) -> Option<Self> {
        let name = name.name.0.as_str();
        match to_ascii_lower(name).as_ref() {
            // always ok to be served by primary or replicas - pure readonly pragmas
            "table_list" | "index_list" | "table_info" | "table_xinfo" | "index_info" | "index_xinfo"
            | "pragma_list" | "compile_options" | "database_list" | "function_list"
            | "module_list" => Some(Self::Read),
            // special case for `encoding` - it's effectively readonly for connections
            // that already created a database, which is always the case for sqld
            "encoding" => Some(Self::Read),
            "schema_version" if body.is_none() => Some(Self::Read),
            // always ok to be served by primary
            "defer_foreign_keys" | "foreign_keys" | "foreign_key_list" | "foreign_key_check" | "collation_list"
            | "data_version" | "freelist_count" | "integrity_check" | "legacy_file_format"
            | "page_count" | "quick_check" | "stats" | "user_version" => Some(Self::Write),
            // ok to be served by primary without args
            "analysis_limit"
            | "application_id"
            | "auto_vacuum"
            | "automatic_index"
            | "busy_timeout"
            | "cache_size"
            | "cache_spill"
            | "cell_size_check"
            | "checkpoint_fullfsync"
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
            | "secure_delete"
            | "soft_heap_limit"
            | "synchronous"
            | "temp_store"
            | "threads"
            | "trusted_schema"
            | "wal_autocheckpoint" => {
                match body {
                    Some(_) => None,
                    None => Some(Self::Write),
                }
            }
            // changes the state of the connection, and can't be allowed rn:
            "case_sensitive_like" | "ignore_check_constraints" | "incremental_vacuum"
                // TODO: check if optimize can be safely performed
                | "optimize"
                | "parser_trace"
                | "shrink_memory"
                | "wal_checkpoint" => None,
            _ => {
                tracing::debug!("Unknown pragma: {name}");
                None
            },
        }
    }

    /// Returns `true` if the stmt kind is [`Savepoint`].
    ///
    /// [`Savepoint`]: StmtKind::Savepoint
    #[must_use]
    pub fn is_savepoint(&self) -> bool {
        matches!(self, Self::Savepoint)
    }

    /// Returns `true` if the stmt kind is [`Release`].
    ///
    /// [`Release`]: StmtKind::Release
    #[must_use]
    pub fn is_release(&self) -> bool {
        matches!(self, Self::Release)
    }

    /// Returns true if this statement is a transaction related statement
    pub(crate) fn is_txn(&self) -> bool {
        matches!(
            self,
            Self::TxnEnd | Self::TxnBegin | Self::Release | Self::Savepoint
        )
    }
}

fn to_ascii_lower(s: &str) -> Cow<str> {
    if s.chars().all(|c| char::is_ascii_lowercase(&c)) {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(s.to_ascii_lowercase())
    }
}

/// The state of a transaction for a series of statement
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TxnStatus {
    /// The txn in an opened state
    Txn,
    /// The txn in a closed state
    Init,
    /// This is an invalid state for the state machine
    Invalid,
}

impl TxnStatus {
    pub fn step(&mut self, kind: &StmtKind) {
        *self = match (*self, kind) {
            (TxnStatus::Txn, StmtKind::TxnBegin) | (TxnStatus::Init, StmtKind::TxnEnd) => {
                TxnStatus::Invalid
            }
            (TxnStatus::Txn, StmtKind::TxnEnd) => TxnStatus::Init,
            (state, StmtKind::Write | StmtKind::Read | StmtKind::DDL) => state,
            (TxnStatus::Invalid, _) => TxnStatus::Invalid,
            (TxnStatus::Init, StmtKind::TxnBegin) => TxnStatus::Txn,
            _ => TxnStatus::Invalid,
        };
    }

    pub fn reset(&mut self) {
        *self = TxnStatus::Init
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
            attach_info: None,
        }
    }

    pub fn parse(s: &str) -> impl Iterator<Item = Result<Self>> + '_ {
        fn parse_inner(
            original: &str,
            stmt_count: u64,
            has_more_stmts: bool,
            c: Cmd,
        ) -> Result<Statement> {
            let kind = StmtKind::kind(&c)
                .ok_or_else(|| anyhow::anyhow!("unsupported statement: {original}"))?;

            if stmt_count == 1 && !has_more_stmts {
                // XXX: Temporary workaround for integration with Atlas
                if let Cmd::Stmt(Stmt::CreateTable { .. }) = &c {
                    return Ok(Statement {
                        stmt: original.to_string(),
                        kind,
                        is_iud: false,
                        is_insert: false,
                        attach_info: None,
                    });
                }
            }

            let is_iud = matches!(
                c,
                Cmd::Stmt(Stmt::Insert { .. } | Stmt::Update { .. } | Stmt::Delete { .. })
            );
            let is_insert = matches!(c, Cmd::Stmt(Stmt::Insert { .. }));

            let attach_info = match &c {
                Cmd::Stmt(Stmt::Attach {
                    expr: Expr::Id(Id(expr)),
                    db_name: Expr::Id(Id(name)),
                    ..
                }) => Some((expr.clone(), name.clone())),
                _ => None,
            };
            Ok(Statement {
                stmt: c.to_string(),
                kind,
                is_iud,
                is_insert,
                attach_info,
            })
        }
        // The parser needs to be boxed because it's large, and you don't want it on the stack.
        // There's upstream work to make it smaller, but in the meantime the parser should remain
        // on the heap:
        // - https://github.com/gwenn/lemon-rs/issues/8
        // - https://github.com/gwenn/lemon-rs/pull/19
        let mut parser = Some(Box::new(Parser::new(s.as_bytes()).peekable()));
        let mut stmt_count = 0;
        std::iter::from_fn(move || {
            // temporary macro to catch panic from the parser, until we fix it.
            macro_rules! parse {
                ($parser:expr, |$arg:ident| $b:block) => {{
                    let Some(mut p) = $parser.take() else {
                        return None;
                    };
                    match std::panic::catch_unwind(|| {
                        let ret = {
                            let $arg = &mut p.as_mut();
                            $b
                        };
                        (ret, p)
                    }) {
                        Ok((ret, parser)) => {
                            $parser = Some(parser);
                            ret
                        }
                        Err(_) => {
                            return Some(Err(anyhow::anyhow!("unexpected parser error")));
                        }
                    }
                }};
            }

            stmt_count += 1;
            let next = parse!(parser, |p| { p.next() });

            match next {
                Ok(Some(cmd)) => Some(parse_inner(
                    s,
                    stmt_count,
                    parse!(parser, |p| { p.peek().map_or(true, |o| o.is_some()) }),
                    cmd,
                )),
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
            }
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
    mut state: TxnStatus,
    stmts: impl Iterator<Item = &'a Statement>,
) -> TxnStatus {
    for stmt in stmts {
        state.step(&stmt.kind);
    }
    state
}
