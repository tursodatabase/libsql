extern crate pest;
#[macro_use]
extern crate pest_derive;

#[derive(Parser)]
#[grammar = "libsql.pest"]
pub struct LibsqlParser;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StmtKind {
    TxnBegin,
    TxnEnd,
    Read,
    Write,
    Savepoint,
    Release,
    Other,
}

fn is_reserved_tbl(name: &str) -> bool {
    let n = name.to_lowercase();
    n == "_litestream_seq" || n == "_litestream_lock" || n == "libsql_wasm_func_table"
}

fn write_if_not_reserved(name: &str) -> Option<StmtKind> {
    (!is_reserved_tbl(name)).then_some(StmtKind::Write)
}

impl StmtKind {
    pub fn kind(stmt: &str) -> Option<Self> {
        use pest::Parser;

        let cmd = LibsqlParser::parse(Rule::stmt, stmt).ok()?.next()?;

        let inner = cmd.into_inner().next()?;
        let cmd = match inner.as_rule() {
            Rule::explain => {
                let cmd = inner.into_inner().next()?;
                match cmd.as_rule() {
                    Rule::pragma | Rule::vacuum => cmd,
                    _ => return Some(StmtKind::Other),
                }
            }
            Rule::explain_query_plan => return Some(StmtKind::Other),
            Rule::cmd => inner.into_inner().next()?,
            _ => return None,
        };

        match cmd.as_rule() {
            Rule::begin => Some(StmtKind::TxnBegin),
            Rule::commit => Some(StmtKind::TxnEnd),
            Rule::rollback => match cmd.into_inner().next().map(|r| r.as_rule()) {
                Some(Rule::to_savepoint) => Some(StmtKind::Release),
                Some(_) => None,
                None => Some(StmtKind::TxnEnd),
            },
            Rule::savepoint => Some(StmtKind::Savepoint),
            Rule::select => Some(StmtKind::Read),
            Rule::insert | Rule::update | Rule::delete => {
                let name = cmd.into_inner().next()?.as_str();
                write_if_not_reserved(name)
            }
            Rule::create_table
            | Rule::create_view
            | Rule::create_function
            | Rule::create_index
            | Rule::create_trigger => {
                let mut inner = cmd.into_inner();
                let is_temp = matches!(inner.next().map(|r| r.as_rule()), Some(Rule::temp));
                if is_temp {
                    None
                } else {
                    Some(StmtKind::Write)
                }
            }
            Rule::alter_table | Rule::drop_table | Rule::drop_view => {
                let name = cmd.into_inner().next()?.as_str();
                write_if_not_reserved(name)
            }
            Rule::drop_index | Rule::drop_trigger => Some(StmtKind::Write),
            Rule::pragma => {
                let mut inner = cmd.into_inner();
                let name = inner.next()?.into_inner().next()?.as_str();
                let has_body = inner.next().is_some();
                Self::pragma_kind(name, has_body)
            }
            _ => None,
        }
    }

    fn pragma_kind(name: &str, has_body: bool) -> Option<Self> {
        match name {
            // always ok to be served by primary or replicas - pure readonly pragmas
            "table_list" | "index_list" | "table_info" | "table_xinfo" | "index_xinfo"
            | "pragma_list" | "compile_options" | "database_list" | "function_list"
            | "module_list" => Some(Self::Read),
            // special case for `encoding` - it's effectively readonly for connections
            // that already created a database, which is always the case for sqld
            "encoding" => Some(Self::Read),
            // always ok to be served by primary
            "foreign_keys" | "foreign_key_list" | "foreign_key_check" | "collation_list"
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
            | "defer_foreign_keys"
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
            | "wal_autocheckpoint" => {
                if has_body {
                    None
                } else {
                    Some(Self::Write)
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
}

#[cfg(test)]
mod test {
    use super::*;

    const STMTS: &[&str] = &[
        "SELECT * FROM foo",
        "SELECT * FROM foo WHERE bar = 1",
        "INSERT INTO foo VALUES (5, 2)",
        "INSERT INTO foo VALUES (5, 2), (3, 4)",
        "VACUUM yourself",
        "EXPLAIN VACUUM yourself",
        "CREATE FUNCTION xyz LANGUAGE wasm AS '0xdeadbabe'",
        "PRAGMA foreign_keys=on",
        "PRAGMA foreign_keys",
        "PRAGMA journal_mode",
        "PRAGMA journal_mode=delete",
        "PRAGMA pragma_list",
        "CREATE TABLE t(id, v int primary key)",
        "SAVEPOINT abc",
        "ROLLBACK TO SAVEPOINT abc",
        "ROLLBACK",
        "CREATE TEMPORARY TABLE abc(def)",
        "CREATE TEMP TABLE abc(def)",
        "BEGIN IMMEDIATE",
        "BEGIN READONLY",
        "BEGIN DEFERRED",
        "BEGIN",
        "COMMIT",
        "END",
    ];

    #[test]
    fn test_parse0() {
        use pest::Parser;

        for stmt in STMTS {
            let now = std::time::Instant::now();
            let parsed = LibsqlParser::parse(Rule::stmt, stmt)
                .expect("Failed to parse statement")
                .next()
                .expect("No parsed statement found");
            let elapsed = now.elapsed().as_micros();
            println!(
                "{:?}: {:?}",
                parsed.as_rule(),
                parsed
                    .into_inner()
                    .next()
                    .expect("No inner statement found")
                    .into_inner()
                    .next()
                    .expect("No inner statement found")
            );
            println!("\tparsed in {elapsed}μs");
        }
    }

    #[test]
    fn test_categorize0() {
        for stmt in STMTS {
            println!("{:?}:\n\t{:?}", stmt, StmtKind::kind(stmt));
        }
    }
}
