//! Abstract Syntax Tree

use std::fmt::{self, Display, Formatter, Write};
use std::num::ParseIntError;
use std::str::FromStr;

use indexmap::IndexSet;

use crate::dialect::TokenType::{self, *};
use crate::dialect::{from_token, is_identifier, Token};
use crate::parser::{parse::YYCODETYPE, ParserError};

struct FmtTokenStream<'a, 'b> {
    f: &'a mut Formatter<'b>,
    spaced: bool,
}
impl<'a, 'b> TokenStream for FmtTokenStream<'a, 'b> {
    type Error = fmt::Error;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> fmt::Result {
        if !self.spaced {
            match ty {
                TK_COMMA | TK_SEMI | TK_RP | TK_DOT => {}
                _ => {
                    self.f.write_char(' ')?;
                    self.spaced = true;
                }
            };
        }
        if ty == TK_BLOB {
            self.f.write_char('X')?;
            self.f.write_char('\'')?;
            if let Some(str) = value {
                self.f.write_str(str)?;
            }
            return self.f.write_char('\'');
        } else if let Some(str) = ty.as_str() {
            self.f.write_str(str)?;
            self.spaced = ty == TK_LP || ty == TK_DOT; // str should not be whitespace
        }
        if let Some(str) = value {
            // trick for pretty-print
            self.spaced = str.bytes().all(|b| b.is_ascii_whitespace());
            /*if !self.spaced {
                self.f.write_char(' ')?;
            }*/
            self.f.write_str(str)
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
pub struct ParameterInfo {
    pub count: u32,
    pub names: IndexSet<String>,
}

// https://sqlite.org/lang_expr.html#parameters
impl TokenStream for ParameterInfo {
    type Error = ParseIntError;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        if ty == TK_VARIABLE {
            if let Some(variable) = value {
                if variable == "?" {
                    self.count = self.count.saturating_add(1);
                } else if variable.as_bytes()[0] == b'?' {
                    let n = u32::from_str(&variable[1..])?;
                    if n > self.count {
                        self.count = n;
                    }
                } else if self.names.insert(variable.to_owned()) {
                    self.count = self.count.saturating_add(1);
                }
            }
        }
        Ok(())
    }
}

pub trait TokenStream {
    type Error;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error>;
}

pub trait ToTokens {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error>;

    fn to_fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut s = FmtTokenStream { f, spaced: true };
        self.to_tokens(&mut s)
    }
}

impl<T: ?Sized + ToTokens> ToTokens for &T {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        ToTokens::to_tokens(&**self, s)
    }
}

impl ToTokens for String {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_ANY, Some(self.as_ref()))
    }
}

/* FIXME: does not work, find why
impl Display for dyn ToTokens {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut s = FmtTokenStream { f, spaced: true };
        match self.to_tokens(&mut s) {
            Err(_) => Err(fmt::Error),
            Ok(()) => Ok(()),
        }
    }
}
*/

// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    Explain(Stmt),
    ExplainQueryPlan(Stmt),
    Stmt(Stmt),
}

impl ToTokens for Cmd {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Cmd::Explain(stmt) => {
                s.append(TK_EXPLAIN, None)?;
                stmt.to_tokens(s)?;
            }
            Cmd::ExplainQueryPlan(stmt) => {
                s.append(TK_EXPLAIN, None)?;
                s.append(TK_QUERY, None)?;
                s.append(TK_PLAN, None)?;
                stmt.to_tokens(s)?;
            }
            Cmd::Stmt(stmt) => {
                stmt.to_tokens(s)?;
            }
        }
        s.append(TK_SEMI, None)
    }
}

impl Display for Cmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.to_fmt(f)
    }
}

pub(crate) enum ExplainKind {
    Explain,
    QueryPlan,
}

// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stmt {
    // table name, body
    AlterTable(QualifiedName, AlterTableBody),
    // object name
    Analyze(Option<QualifiedName>),
    Attach {
        // TODO distinction between ATTACH and ATTACH DATABASE
        expr: Expr,
        db_name: Expr,
        key: Option<Expr>,
    },
    // tx type, tx name
    Begin(Option<TransactionType>, Option<Name>),
    // tx name
    Commit(Option<Name>), // TODO distinction between COMMIT and END
    CreateIndex {
        unique: bool,
        if_not_exists: bool,
        idx_name: QualifiedName,
        tbl_name: Name,
        columns: Vec<SortedColumn>,
        where_clause: Option<Expr>,
    },
    CreateTable {
        temporary: bool, // TODO distinction between TEMP and TEMPORARY
        if_not_exists: bool,
        tbl_name: QualifiedName,
        body: CreateTableBody,
    },
    CreateTrigger {
        temporary: bool,
        if_not_exists: bool,
        trigger_name: QualifiedName,
        time: Option<TriggerTime>,
        event: TriggerEvent,
        tbl_name: QualifiedName,
        for_each_row: bool,
        when_clause: Option<Expr>,
        commands: Vec<TriggerCmd>,
    },
    CreateView {
        temporary: bool,
        if_not_exists: bool,
        view_name: QualifiedName,
        columns: Option<Vec<IndexedColumn>>,
        select: Select,
    },
    CreateVirtualTable {
        if_not_exists: bool,
        tbl_name: QualifiedName,
        module_name: Name,
        args: Option<Vec<String>>, // TODO smol str
    },
    Delete {
        with: Option<With>,
        tbl_name: QualifiedName,
        indexed: Option<Indexed>,
        where_clause: Option<Expr>,
        returning: Option<Vec<ResultColumn>>,
        order_by: Option<Vec<SortedColumn>>,
        limit: Option<Limit>,
    },
    // db name
    Detach(Expr), // TODO distinction between DETACH and DETACH DATABASE
    DropIndex {
        if_exists: bool,
        idx_name: QualifiedName,
    },
    DropTable {
        if_exists: bool,
        tbl_name: QualifiedName,
    },
    DropTrigger {
        if_exists: bool,
        trigger_name: QualifiedName,
    },
    DropView {
        if_exists: bool,
        view_name: QualifiedName,
    },
    Insert {
        with: Option<With>,
        or_conflict: Option<ResolveType>, // TODO distinction between REPLACE and INSERT OR REPLACE
        tbl_name: QualifiedName,
        columns: Option<Vec<Name>>,
        body: InsertBody,
        returning: Option<Vec<ResultColumn>>,
    },
    // pragma name, body
    Pragma(QualifiedName, Option<PragmaBody>),
    Reindex {
        obj_name: Option<QualifiedName>,
    },
    // savepoint name
    Release(Name), // TODO distinction between RELEASE and RELEASE SAVEPOINT
    Rollback {
        tx_name: Option<Name>,
        savepoint_name: Option<Name>, // TODO distinction between TO and TO SAVEPOINT
    },
    // savepoint name
    Savepoint(Name),
    Select(Select),
    Update {
        with: Option<With>,
        or_conflict: Option<ResolveType>,
        tbl_name: QualifiedName,
        indexed: Option<Indexed>,
        sets: Vec<Set>,
        from: Option<FromClause>,
        where_clause: Option<Expr>,
        returning: Option<Vec<ResultColumn>>,
        order_by: Option<Vec<SortedColumn>>,
        limit: Option<Limit>,
    },
    // database name, into expr
    Vacuum(Option<Name>, Option<Expr>),
}

impl ToTokens for Stmt {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Stmt::AlterTable(tbl_name, body) => {
                s.append(TK_ALTER, None)?;
                s.append(TK_TABLE, None)?;
                tbl_name.to_tokens(s)?;
                body.to_tokens(s)
            }
            Stmt::Analyze(obj_name) => {
                s.append(TK_ANALYZE, None)?;
                if let Some(obj_name) = obj_name {
                    obj_name.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Attach { expr, db_name, key } => {
                s.append(TK_ATTACH, None)?;
                expr.to_tokens(s)?;
                s.append(TK_AS, None)?;
                db_name.to_tokens(s)?;
                if let Some(key) = key {
                    s.append(TK_KEY, None)?;
                    key.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Begin(tx_type, tx_name) => {
                s.append(TK_BEGIN, None)?;
                if let Some(tx_type) = tx_type {
                    tx_type.to_tokens(s)?;
                }
                if let Some(tx_name) = tx_name {
                    s.append(TK_TRANSACTION, None)?;
                    tx_name.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Commit(tx_name) => {
                s.append(TK_COMMIT, None)?;
                if let Some(tx_name) = tx_name {
                    s.append(TK_TRANSACTION, None)?;
                    tx_name.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            } => {
                s.append(TK_CREATE, None)?;
                if *unique {
                    s.append(TK_UNIQUE, None)?;
                }
                s.append(TK_INDEX, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s)?;
                s.append(TK_LP, None)?;
                comma(columns, s)?;
                s.append(TK_RP, None)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s)?;
                body.to_tokens(s)
            }
            Stmt::CreateTrigger {
                temporary,
                if_not_exists,
                trigger_name,
                time,
                event,
                tbl_name,
                for_each_row,
                when_clause,
                commands,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_TRIGGER, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s)?;
                if let Some(time) = time {
                    time.to_tokens(s)?;
                }
                event.to_tokens(s)?;
                s.append(TK_ON, None)?;
                tbl_name.to_tokens(s)?;
                if *for_each_row {
                    s.append(TK_FOR, None)?;
                    s.append(TK_EACH, None)?;
                    s.append(TK_ROW, None)?;
                }
                if let Some(when_clause) = when_clause {
                    s.append(TK_WHEN, None)?;
                    when_clause.to_tokens(s)?;
                }
                s.append(TK_BEGIN, Some("\n"))?;
                for command in commands {
                    command.to_tokens(s)?;
                    s.append(TK_SEMI, Some("\n"))?;
                }
                s.append(TK_END, None)
            }
            Stmt::CreateView {
                temporary,
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                s.append(TK_CREATE, None)?;
                if *temporary {
                    s.append(TK_TEMP, None)?;
                }
                s.append(TK_VIEW, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s)?;
                if let Some(columns) = columns {
                    s.append(TK_LP, None)?;
                    comma(columns, s)?;
                    s.append(TK_RP, None)?;
                }
                s.append(TK_AS, None)?;
                select.to_tokens(s)
            }
            Stmt::CreateVirtualTable {
                if_not_exists,
                tbl_name,
                module_name,
                args,
            } => {
                s.append(TK_CREATE, None)?;
                s.append(TK_VIRTUAL, None)?;
                s.append(TK_TABLE, None)?;
                if *if_not_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_NOT, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s)?;
                s.append(TK_USING, None)?;
                module_name.to_tokens(s)?;
                s.append(TK_LP, None)?;
                if let Some(args) = args {
                    comma(args, s)?;
                }
                s.append(TK_RP, None)
            }
            Stmt::Delete {
                with,
                tbl_name,
                indexed,
                where_clause,
                returning,
                order_by,
                limit,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s)?;
                }
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                if let Some(returning) = returning {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s)?;
                }
                if let Some(order_by) = order_by {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s)?;
                }
                if let Some(limit) = limit {
                    limit.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Detach(expr) => {
                s.append(TK_DETACH, None)?;
                expr.to_tokens(s)
            }
            Stmt::DropIndex {
                if_exists,
                idx_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_INDEX, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                idx_name.to_tokens(s)
            }
            Stmt::DropTable {
                if_exists,
                tbl_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TABLE, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                tbl_name.to_tokens(s)
            }
            Stmt::DropTrigger {
                if_exists,
                trigger_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_TRIGGER, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                trigger_name.to_tokens(s)
            }
            Stmt::DropView {
                if_exists,
                view_name,
            } => {
                s.append(TK_DROP, None)?;
                s.append(TK_VIEW, None)?;
                if *if_exists {
                    s.append(TK_IF, None)?;
                    s.append(TK_EXISTS, None)?;
                }
                view_name.to_tokens(s)
            }
            Stmt::Insert {
                with,
                or_conflict,
                tbl_name,
                columns,
                body,
                returning,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s)?;
                }
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s)?;
                if let Some(columns) = columns {
                    s.append(TK_LP, None)?;
                    comma(columns, s)?;
                    s.append(TK_RP, None)?;
                }
                body.to_tokens(s)?;
                if let Some(returning) = returning {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s)?;
                }
                Ok(())
            }
            Stmt::Pragma(name, value) => {
                s.append(TK_PRAGMA, None)?;
                name.to_tokens(s)?;
                if let Some(value) = value {
                    value.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Reindex { obj_name } => {
                s.append(TK_REINDEX, None)?;
                if let Some(obj_name) = obj_name {
                    obj_name.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Release(name) => {
                s.append(TK_RELEASE, None)?;
                name.to_tokens(s)
            }
            Stmt::Rollback {
                tx_name,
                savepoint_name,
            } => {
                s.append(TK_ROLLBACK, None)?;
                if let Some(tx_name) = tx_name {
                    s.append(TK_TRANSACTION, None)?;
                    tx_name.to_tokens(s)?;
                }
                if let Some(savepoint_name) = savepoint_name {
                    s.append(TK_TO, None)?;
                    savepoint_name.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Savepoint(name) => {
                s.append(TK_SAVEPOINT, None)?;
                name.to_tokens(s)
            }
            Stmt::Select(select) => select.to_tokens(s),
            Stmt::Update {
                with,
                or_conflict,
                tbl_name,
                indexed,
                sets,
                from,
                where_clause,
                returning,
                order_by,
                limit,
            } => {
                if let Some(with) = with {
                    with.to_tokens(s)?;
                }
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s)?;
                }
                tbl_name.to_tokens(s)?;
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s)?;
                }
                s.append(TK_SET, None)?;
                comma(sets, s)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                if let Some(returning) = returning {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s)?;
                }
                if let Some(order_by) = order_by {
                    s.append(TK_ORDER, None)?;
                    s.append(TK_BY, None)?;
                    comma(order_by, s)?;
                }
                if let Some(limit) = limit {
                    limit.to_tokens(s)?;
                }
                Ok(())
            }
            Stmt::Vacuum(name, expr) => {
                s.append(TK_VACUUM, None)?;
                if let Some(ref name) = name {
                    name.to_tokens(s)?;
                }
                if let Some(ref expr) = expr {
                    s.append(TK_INTO, None)?;
                    expr.to_tokens(s)?;
                }
                Ok(())
            }
        }
    }
}

// https://sqlite.org/syntax/expr.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Between {
        lhs: Box<Expr>,
        not: bool,
        start: Box<Expr>,
        end: Box<Expr>,
    },
    Binary(Box<Expr>, Operator, Box<Expr>),
    // CASE expression
    Case {
        base: Option<Box<Expr>>,
        when_then_pairs: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    // CAST expression
    Cast {
        expr: Box<Expr>,
        type_name: Type,
    },
    // COLLATE expression
    Collate(Box<Expr>, String),
    // schema-name.table-name.column-name
    DoublyQualified(Name, Name, Name),
    // EXISTS subquery
    Exists(Box<Select>),
    // call to a built-in function
    FunctionCall {
        name: Id,
        distinctness: Option<Distinctness>,
        args: Option<Vec<Expr>>,
        filter_over: Option<FunctionTail>,
    },
    // Function call expression with '*' as arg
    FunctionCallStar {
        name: Id,
        filter_over: Option<FunctionTail>,
    },
    // Identifier
    Id(Id),
    InList {
        lhs: Box<Expr>,
        not: bool,
        rhs: Option<Vec<Expr>>,
    },
    InSelect {
        lhs: Box<Expr>,
        not: bool,
        rhs: Box<Select>,
    },
    InTable {
        lhs: Box<Expr>,
        not: bool,
        rhs: QualifiedName,
        args: Option<Vec<Expr>>,
    },
    IsNull(Box<Expr>),
    Like {
        lhs: Box<Expr>,
        not: bool,
        op: LikeOperator,
        rhs: Box<Expr>,
        escape: Option<Box<Expr>>,
    },
    // Literal expression
    Literal(Literal),
    Name(Name),
    // "NOT NULL" or "NOTNULL"
    NotNull(Box<Expr>),
    // Parenthesized subexpression
    Parenthesized(Vec<Expr>),
    Qualified(Name, Name),
    // RAISE function call
    Raise(ResolveType, Option<Name>),
    // Subquery expression
    Subquery(Box<Select>),
    // Unary expression
    Unary(UnaryOperator, Box<Expr>),
    // Parameters
    Variable(String),
}

impl Expr {
    pub fn parenthesized(x: Expr) -> Expr {
        Expr::Parenthesized(vec![x])
    }
    pub fn id(xt: YYCODETYPE, x: Token) -> Expr {
        Expr::Id(Id::from_token(xt, x))
    }
    pub fn collate(x: Expr, ct: YYCODETYPE, c: Token) -> Expr {
        Expr::Collate(Box::new(x), from_token(ct, c))
    }
    pub fn cast(x: Expr, type_name: Type) -> Expr {
        Expr::Cast {
            expr: Box::new(x),
            type_name,
        }
    }
    pub fn binary(left: Expr, op: YYCODETYPE, right: Expr) -> Expr {
        Expr::Binary(Box::new(left), Operator::from(op), Box::new(right))
    }
    pub fn ptr(left: Expr, op: Token, right: Expr) -> Expr {
        let mut ptr = Operator::ArrowRight;
        if let Some(ref op) = op.1 {
            if op == "->>" {
                ptr = Operator::ArrowRightShift;
            }
        }
        Expr::Binary(Box::new(left), ptr, Box::new(right))
    }
    pub fn like(lhs: Expr, not: bool, op: LikeOperator, rhs: Expr, escape: Option<Expr>) -> Expr {
        Expr::Like {
            lhs: Box::new(lhs),
            not,
            op,
            rhs: Box::new(rhs),
            escape: escape.map(Box::new),
        }
    }
    pub fn not_null(x: Expr, op: YYCODETYPE) -> Expr {
        if op == TK_ISNULL as YYCODETYPE {
            Expr::IsNull(Box::new(x))
        } else if op == TK_NOTNULL as YYCODETYPE {
            Expr::NotNull(Box::new(x))
        } else {
            unreachable!()
        }
    }
    pub fn unary(op: UnaryOperator, x: Expr) -> Expr {
        Expr::Unary(op, Box::new(x))
    }
    pub fn between(lhs: Expr, not: bool, start: Expr, end: Expr) -> Expr {
        Expr::Between {
            lhs: Box::new(lhs),
            not,
            start: Box::new(start),
            end: Box::new(end),
        }
    }
    pub fn in_list(lhs: Expr, not: bool, rhs: Option<Vec<Expr>>) -> Expr {
        Expr::InList {
            lhs: Box::new(lhs),
            not,
            rhs,
        }
    }
    pub fn in_select(lhs: Expr, not: bool, rhs: Select) -> Expr {
        Expr::InSelect {
            lhs: Box::new(lhs),
            not,
            rhs: Box::new(rhs),
        }
    }
    pub fn in_table(lhs: Expr, not: bool, rhs: QualifiedName, args: Option<Vec<Expr>>) -> Expr {
        Expr::InTable {
            lhs: Box::new(lhs),
            not,
            rhs,
            args,
        }
    }
    pub fn sub_query(query: Select) -> Expr {
        Expr::Subquery(Box::new(query))
    }
}
impl ToTokens for Expr {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Expr::Between {
                lhs,
                not,
                start,
                end,
            } => {
                lhs.to_tokens(s)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_BETWEEN, None)?;
                start.to_tokens(s)?;
                s.append(TK_AND, None)?;
                end.to_tokens(s)
            }
            Expr::Binary(lhs, op, rhs) => {
                lhs.to_tokens(s)?;
                op.to_tokens(s)?;
                rhs.to_tokens(s)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                s.append(TK_CASE, None)?;
                if let Some(ref base) = base {
                    base.to_tokens(s)?;
                }
                for (when, then) in when_then_pairs {
                    s.append(TK_WHEN, None)?;
                    when.to_tokens(s)?;
                    s.append(TK_THEN, None)?;
                    then.to_tokens(s)?;
                }
                if let Some(ref else_expr) = else_expr {
                    s.append(TK_ELSE, None)?;
                    else_expr.to_tokens(s)?;
                }
                s.append(TK_END, None)
            }
            Expr::Cast { expr, type_name } => {
                s.append(TK_CAST, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s)?;
                s.append(TK_AS, None)?;
                type_name.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            Expr::Collate(expr, collation) => {
                expr.to_tokens(s)?;
                s.append(TK_COLLATE, None)?;
                double_quote(collation, s)
            }
            Expr::DoublyQualified(db_name, tbl_name, col_name) => {
                db_name.to_tokens(s)?;
                s.append(TK_DOT, None)?;
                tbl_name.to_tokens(s)?;
                s.append(TK_DOT, None)?;
                col_name.to_tokens(s)
            }
            Expr::Exists(subquery) => {
                s.append(TK_EXISTS, None)?;
                s.append(TK_LP, None)?;
                subquery.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            Expr::FunctionCall {
                name,
                distinctness,
                args,
                filter_over,
            } => {
                name.to_tokens(s)?;
                s.append(TK_LP, None)?;
                if let Some(distinctness) = distinctness {
                    distinctness.to_tokens(s)?;
                }
                if let Some(args) = args {
                    comma(args, s)?;
                }
                s.append(TK_RP, None)?;
                if let Some(filter_over) = filter_over {
                    filter_over.to_tokens(s)?;
                }
                Ok(())
            }
            Expr::FunctionCallStar { name, filter_over } => {
                name.to_tokens(s)?;
                s.append(TK_LP, None)?;
                s.append(TK_STAR, None)?;
                s.append(TK_RP, None)?;
                if let Some(filter_over) = filter_over {
                    filter_over.to_tokens(s)?;
                }
                Ok(())
            }
            Expr::Id(id) => id.to_tokens(s),
            Expr::InList { lhs, not, rhs } => {
                lhs.to_tokens(s)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                if let Some(rhs) = rhs {
                    comma(rhs, s)?;
                }
                s.append(TK_RP, None)
            }
            Expr::InSelect { lhs, not, rhs } => {
                lhs.to_tokens(s)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                s.append(TK_LP, None)?;
                rhs.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            Expr::InTable {
                lhs,
                not,
                rhs,
                args,
            } => {
                lhs.to_tokens(s)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_IN, None)?;
                rhs.to_tokens(s)?;
                if let Some(args) = args {
                    s.append(TK_LP, None)?;
                    comma(args, s)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
            Expr::IsNull(sub_expr) => {
                sub_expr.to_tokens(s)?;
                s.append(TK_ISNULL, None)
            }
            Expr::Like {
                lhs,
                not,
                op,
                rhs,
                escape,
            } => {
                lhs.to_tokens(s)?;
                if *not {
                    s.append(TK_NOT, None)?;
                }
                op.to_tokens(s)?;
                rhs.to_tokens(s)?;
                if let Some(escape) = escape {
                    s.append(TK_ESCAPE, None)?;
                    escape.to_tokens(s)?;
                }
                Ok(())
            }
            Expr::Literal(lit) => lit.to_tokens(s),
            Expr::Name(name) => name.to_tokens(s),
            Expr::NotNull(sub_expr) => {
                sub_expr.to_tokens(s)?;
                s.append(TK_NOTNULL, None)
            }
            Expr::Parenthesized(exprs) => {
                s.append(TK_LP, None)?;
                comma(exprs, s)?;
                s.append(TK_RP, None)
            }
            Expr::Qualified(qualifier, qualified) => {
                qualifier.to_tokens(s)?;
                s.append(TK_DOT, None)?;
                qualified.to_tokens(s)
            }
            Expr::Raise(rt, err) => {
                s.append(TK_RAISE, None)?;
                s.append(TK_LP, None)?;
                rt.to_tokens(s)?;
                if let Some(err) = err {
                    s.append(TK_COMMA, None)?;
                    err.to_tokens(s)?;
                }
                s.append(TK_RP, None)
            }
            Expr::Subquery(query) => {
                s.append(TK_LP, None)?;
                query.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            Expr::Unary(op, sub_expr) => {
                op.to_tokens(s)?;
                sub_expr.to_tokens(s)
            }
            Expr::Variable(var) => match var.chars().next() {
                Some(c) if c == '$' || c == '@' || c == '#' || c == ':' => {
                    s.append(TK_VARIABLE, Some(var))
                }
                Some(_) => s.append(TK_VARIABLE, Some(&("?".to_owned() + var))),
                None => s.append(TK_VARIABLE, Some("?")),
            },
        }
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.to_fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    Numeric(String),
    // TODO Check that string is already quoted and correctly escaped
    String(String),
    // TODO Check that string is valid (only hexa)
    Blob(String),
    Keyword(String),
    Null,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
}

impl Literal {
    pub fn from_ctime_kw(token: Token) -> Literal {
        if let Some(ref token) = token.1 {
            if "CURRENT_DATE".eq_ignore_ascii_case(token) {
                Literal::CurrentDate
            } else if "CURRENT_TIME".eq_ignore_ascii_case(token) {
                Literal::CurrentTime
            } else if "CURRENT_TIMESTAMP".eq_ignore_ascii_case(token) {
                Literal::CurrentTimestamp
            } else {
                unreachable!()
            }
        } else {
            unreachable!()
        }
    }
}
impl ToTokens for Literal {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Literal::Numeric(ref num) => s.append(TK_FLOAT, Some(num)), // TODO Validate TK_FLOAT
            Literal::String(ref str) => s.append(TK_STRING, Some(str)),
            Literal::Blob(ref blob) => s.append(TK_BLOB, Some(blob)),
            Literal::Keyword(ref str) => s.append(TK_ID, Some(str)), // TODO Validate TK_ID
            Literal::Null => s.append(TK_NULL, None),
            Literal::CurrentDate => s.append(TK_CTIME_KW, Some("CURRENT_DATE")),
            Literal::CurrentTime => s.append(TK_CTIME_KW, Some("CURRENT_TIME")),
            Literal::CurrentTimestamp => s.append(TK_CTIME_KW, Some("CURRENT_TIMESTAMP")),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LikeOperator {
    Glob,
    Like,
    Match,
    Regexp,
}

impl LikeOperator {
    pub fn from_token(token_type: YYCODETYPE, token: Token) -> LikeOperator {
        if token_type == TK_MATCH as YYCODETYPE {
            return LikeOperator::Match;
        } else if token_type == TK_LIKE_KW as YYCODETYPE {
            if let Some(ref token) = token.1 {
                if "LIKE".eq_ignore_ascii_case(token) {
                    return LikeOperator::Like;
                } else if "GLOB".eq_ignore_ascii_case(token) {
                    return LikeOperator::Glob;
                } else if "REGEXP".eq_ignore_ascii_case(token) {
                    return LikeOperator::Regexp;
                }
            }
        }
        unreachable!()
    }
}
impl ToTokens for LikeOperator {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            TK_LIKE_KW,
            Some(match self {
                LikeOperator::Glob => "GLOB",
                LikeOperator::Like => "LIKE",
                LikeOperator::Match => "MATCH",
                LikeOperator::Regexp => "REGEXP",
            }),
        )
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Operator {
    Add,
    And,
    ArrowRight,      // ->
    ArrowRightShift, // ->>
    BitwiseAnd,
    BitwiseOr,
    Concat, // String concatenation (||)
    Equals, // = or ==
    Divide,
    Greater,
    GreaterEquals,
    Is,
    IsNot,
    LeftShift,
    Less,
    LessEquals,
    Modulus,
    Multiply,
    NotEquals, // != or <>
    Or,
    RightShift,
    Substract,
}

impl From<YYCODETYPE> for Operator {
    fn from(token_type: YYCODETYPE) -> Operator {
        match token_type {
            x if x == TK_AND as YYCODETYPE => Operator::And,
            x if x == TK_OR as YYCODETYPE => Operator::Or,
            x if x == TK_LT as YYCODETYPE => Operator::Less,
            x if x == TK_GT as YYCODETYPE => Operator::Greater,
            x if x == TK_GE as YYCODETYPE => Operator::GreaterEquals,
            x if x == TK_LE as YYCODETYPE => Operator::LessEquals,
            x if x == TK_EQ as YYCODETYPE => Operator::Equals,
            x if x == TK_NE as YYCODETYPE => Operator::NotEquals,
            x if x == TK_BITAND as YYCODETYPE => Operator::BitwiseAnd,
            x if x == TK_BITOR as YYCODETYPE => Operator::BitwiseOr,
            x if x == TK_LSHIFT as YYCODETYPE => Operator::LeftShift,
            x if x == TK_RSHIFT as YYCODETYPE => Operator::RightShift,
            x if x == TK_PLUS as YYCODETYPE => Operator::Add,
            x if x == TK_MINUS as YYCODETYPE => Operator::Substract,
            x if x == TK_STAR as YYCODETYPE => Operator::Multiply,
            x if x == TK_SLASH as YYCODETYPE => Operator::Divide,
            x if x == TK_REM as YYCODETYPE => Operator::Modulus,
            x if x == TK_CONCAT as YYCODETYPE => Operator::Concat,
            x if x == TK_IS as YYCODETYPE => Operator::Is,
            x if x == TK_NOT as YYCODETYPE => Operator::IsNot,
            _ => unreachable!(),
        }
    }
}
impl ToTokens for Operator {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Operator::Add => s.append(TK_PLUS, None),
            Operator::And => s.append(TK_AND, None),
            Operator::ArrowRight => s.append(TK_PTR, Some("->")),
            Operator::ArrowRightShift => s.append(TK_PTR, Some("->>")),
            Operator::BitwiseAnd => s.append(TK_BITAND, None),
            Operator::BitwiseOr => s.append(TK_BITOR, None),
            Operator::Concat => s.append(TK_CONCAT, None),
            Operator::Equals => s.append(TK_EQ, None),
            Operator::Divide => s.append(TK_SLASH, None),
            Operator::Greater => s.append(TK_GT, None),
            Operator::GreaterEquals => s.append(TK_GE, None),
            Operator::Is => s.append(TK_IS, None),
            Operator::IsNot => {
                s.append(TK_IS, None)?;
                s.append(TK_NOT, None)
            }
            Operator::LeftShift => s.append(TK_LSHIFT, None),
            Operator::Less => s.append(TK_LT, None),
            Operator::LessEquals => s.append(TK_LE, None),
            Operator::Modulus => s.append(TK_REM, None),
            Operator::Multiply => s.append(TK_STAR, None),
            Operator::NotEquals => s.append(TK_NE, None),
            Operator::Or => s.append(TK_OR, None),
            Operator::RightShift => s.append(TK_RSHIFT, None),
            Operator::Substract => s.append(TK_MINUS, None),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum UnaryOperator {
    // bitwise negation (~)
    BitwiseNot,
    // negative-sign
    Negative,
    // "NOT"
    Not,
    // positive-sign
    Positive,
}

impl From<YYCODETYPE> for UnaryOperator {
    fn from(token_type: YYCODETYPE) -> UnaryOperator {
        match token_type {
            x if x == TK_BITNOT as YYCODETYPE => UnaryOperator::BitwiseNot,
            x if x == TK_MINUS as YYCODETYPE => UnaryOperator::Negative,
            x if x == TK_NOT as YYCODETYPE => UnaryOperator::Not,
            x if x == TK_PLUS as YYCODETYPE => UnaryOperator::Positive,
            _ => unreachable!(),
        }
    }
}
impl ToTokens for UnaryOperator {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                UnaryOperator::BitwiseNot => TK_BITNOT,
                UnaryOperator::Negative => TK_MINUS,
                UnaryOperator::Not => TK_NOT,
                UnaryOperator::Positive => TK_PLUS,
            },
            None,
        )
    }
}

// https://sqlite.org/lang_select.html
// https://sqlite.org/syntax/factored-select-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub with: Option<With>,
    pub body: SelectBody,
    pub order_by: Option<Vec<SortedColumn>>,
    pub limit: Option<Limit>,
}
impl ToTokens for Select {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if let Some(ref with) = self.with {
            with.to_tokens(s)?;
        }
        self.body.to_tokens(s)?;
        if let Some(ref order_by) = self.order_by {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(order_by, s)?;
        }
        if let Some(ref limit) = self.limit {
            limit.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectBody {
    pub select: OneSelect,
    pub compounds: Option<Vec<CompoundSelect>>,
}

impl SelectBody {
    pub(crate) fn push(&mut self, cs: CompoundSelect) {
        if let Some(ref mut v) = self.compounds {
            v.push(cs);
        } else {
            self.compounds = Some(vec![cs]);
        }
    }
}
impl ToTokens for SelectBody {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.select.to_tokens(s)?;
        if let Some(ref compounds) = self.compounds {
            for compound in compounds {
                compound.to_tokens(s)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompoundSelect {
    pub operator: CompoundOperator,
    pub select: OneSelect,
}
impl ToTokens for CompoundSelect {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.operator.to_tokens(s)?;
        self.select.to_tokens(s)
    }
}

// https://sqlite.org/syntax/compound-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CompoundOperator {
    Union,
    UnionAll,
    Except,
    Intersect,
}
impl ToTokens for CompoundOperator {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            CompoundOperator::Union => s.append(TK_UNION, None),
            CompoundOperator::UnionAll => {
                s.append(TK_UNION, None)?;
                s.append(TK_ALL, None)
            }
            CompoundOperator::Except => s.append(TK_EXCEPT, None),
            CompoundOperator::Intersect => s.append(TK_INTERSECT, None),
        }
    }
}

// https://sqlite.org/syntax/select-core.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OneSelect {
    Select {
        distinctness: Option<Distinctness>,
        columns: Vec<ResultColumn>,
        from: Option<FromClause>,
        where_clause: Option<Expr>,
        group_by: Option<GroupBy>,
        window_clause: Option<Vec<WindowDef>>,
    },
    Values(Vec<Vec<Expr>>),
}
impl ToTokens for OneSelect {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            OneSelect::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
            } => {
                s.append(TK_SELECT, None)?;
                if let Some(ref distinctness) = distinctness {
                    distinctness.to_tokens(s)?;
                }
                comma(columns, s)?;
                if let Some(ref from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s)?;
                }
                if let Some(ref where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                if let Some(ref group_by) = group_by {
                    group_by.to_tokens(s)?;
                }
                if let Some(ref window_clause) = window_clause {
                    s.append(TK_WINDOW, None)?;
                    comma(window_clause, s)?;
                }
                Ok(())
            }
            OneSelect::Values(values) => {
                for (i, vals) in values.iter().enumerate() {
                    if i == 0 {
                        s.append(TK_VALUES, None)?;
                    } else {
                        s.append(TK_COMMA, None)?;
                    }
                    s.append(TK_LP, None)?;
                    comma(vals, s)?;
                    s.append(TK_RP, None)?;
                }
                Ok(())
            }
        }
    }
}

// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FromClause {
    pub select: Option<Box<SelectTable>>, // FIXME mandatory
    pub joins: Option<Vec<JoinedSelectTable>>,
    op: Option<JoinOperator>, // FIXME transient
}
impl FromClause {
    pub(crate) fn empty() -> FromClause {
        FromClause {
            select: None,
            joins: None,
            op: None,
        }
    }

    pub(crate) fn push(
        &mut self,
        table: SelectTable,
        jc: Option<JoinConstraint>,
    ) -> Result<(), ParserError> {
        let op = self.op.take();
        if let Some(op) = op {
            let jst = JoinedSelectTable {
                operator: op,
                table,
                constraint: jc,
            };
            if let Some(ref mut joins) = self.joins {
                joins.push(jst);
            } else {
                self.joins = Some(vec![jst]);
            }
        } else {
            if jc.is_some() {
                return Err(ParserError::Custom(
                    "a JOIN clause is required before ON".to_string(),
                ));
            }
            debug_assert!(self.select.is_none());
            debug_assert!(self.joins.is_none());
            self.select = Some(Box::new(table));
        }

        Ok(())
    }

    pub(crate) fn push_op(&mut self, op: JoinOperator) {
        self.op = Some(op);
    }
}
impl ToTokens for FromClause {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.select.as_ref().unwrap().to_tokens(s)?;
        if let Some(ref joins) = self.joins {
            for join in joins {
                join.to_tokens(s)?;
            }
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Distinctness {
    Distinct,
    All,
}
impl ToTokens for Distinctness {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                Distinctness::Distinct => TK_DISTINCT,
                Distinctness::All => TK_ALL,
            },
            None,
        )
    }
}

// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultColumn {
    Expr(Expr, Option<As>),
    Star,
    // table name
    TableStar(Name),
}
impl ToTokens for ResultColumn {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            ResultColumn::Expr(expr, alias) => {
                expr.to_tokens(s)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s)?;
                }
                Ok(())
            }
            ResultColumn::Star => s.append(TK_STAR, None),
            ResultColumn::TableStar(tbl_name) => {
                tbl_name.to_tokens(s)?;
                s.append(TK_DOT, None)?;
                s.append(TK_STAR, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum As {
    As(Name),
    Elided(Name), // FIXME Ids
}
impl ToTokens for As {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            As::As(ref name) => {
                s.append(TK_AS, None)?;
                name.to_tokens(s)
            }
            As::Elided(ref name) => name.to_tokens(s),
        }
    }
}

// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinedSelectTable {
    pub operator: JoinOperator,
    pub table: SelectTable,
    pub constraint: Option<JoinConstraint>,
}
impl ToTokens for JoinedSelectTable {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.operator.to_tokens(s)?;
        self.table.to_tokens(s)?;
        if let Some(ref constraint) = self.constraint {
            constraint.to_tokens(s)?;
        }
        Ok(())
    }
}

// https://sqlite.org/syntax/table-or-subquery.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectTable {
    Table(QualifiedName, Option<As>, Option<Indexed>),
    TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
    Select(Select, Option<As>),
    Sub(FromClause, Option<As>),
}
impl ToTokens for SelectTable {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            SelectTable::Table(name, alias, indexed) => {
                name.to_tokens(s)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s)?;
                }
                if let Some(indexed) = indexed {
                    indexed.to_tokens(s)?;
                }
                Ok(())
            }
            SelectTable::TableCall(name, exprs, alias) => {
                name.to_tokens(s)?;
                s.append(TK_LP, None)?;
                if let Some(exprs) = exprs {
                    comma(exprs, s)?;
                }
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s)?;
                }
                Ok(())
            }
            SelectTable::Select(select, alias) => {
                s.append(TK_LP, None)?;
                select.to_tokens(s)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s)?;
                }
                Ok(())
            }
            SelectTable::Sub(from, alias) => {
                s.append(TK_LP, None)?;
                from.to_tokens(s)?;
                s.append(TK_RP, None)?;
                if let Some(alias) = alias {
                    alias.to_tokens(s)?;
                }
                Ok(())
            }
        }
    }
}

// https://sqlite.org/syntax/join-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JoinOperator {
    Comma,
    TypedJoin {
        natural: bool,
        join_type: Option<JoinType>,
    },
}

impl JoinOperator {
    pub(crate) fn from_single(token: Token) -> Result<JoinOperator, ParserError> {
        Ok(if let Some(ref jt) = token.1 {
            if "CROSS".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Cross),
                }
            } else if "INNER".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Inner),
                }
            } else if "LEFT".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Left),
                }
            } else if "RIGHT".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Right),
                }
            } else if "FULL".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Full),
                }
            } else if "NATURAL".eq_ignore_ascii_case(jt) {
                JoinOperator::TypedJoin {
                    natural: true,
                    join_type: None,
                }
            } else {
                return Err(ParserError::Custom(format!(
                    "unsupported JOIN type: {}",
                    jt
                )));
            }
        } else {
            unreachable!()
        })
    }
    pub(crate) fn from_couple(token: Token, name: Name) -> Result<JoinOperator, ParserError> {
        Ok(if let Some(ref jt) = token.1 {
            if "NATURAL".eq_ignore_ascii_case(jt) {
                let join_type = if "INNER".eq_ignore_ascii_case(&name.0) {
                    JoinType::Inner
                } else if "LEFT".eq_ignore_ascii_case(&name.0) {
                    JoinType::Left
                } else if "RIGHT".eq_ignore_ascii_case(&name.0) {
                    JoinType::Right
                } else if "FULL".eq_ignore_ascii_case(&name.0) {
                    JoinType::Full
                } else if "CROSS".eq_ignore_ascii_case(&name.0) {
                    JoinType::Cross
                } else {
                    return Err(ParserError::Custom(format!(
                        "unsupported JOIN type: {} {}",
                        jt, &name.0
                    )));
                };
                JoinOperator::TypedJoin {
                    natural: true,
                    join_type: Some(join_type),
                }
            } else if "OUTER".eq_ignore_ascii_case(&name.0) {
                // If "OUTER" is present then there must also be one of "LEFT", "RIGHT", or "FULL"
                let join_type = if "LEFT".eq_ignore_ascii_case(jt) {
                    JoinType::LeftOuter
                } else if "RIGHT".eq_ignore_ascii_case(jt) {
                    JoinType::RightOuter
                } else if "FULL".eq_ignore_ascii_case(jt) {
                    JoinType::FullOuter
                } else {
                    return Err(ParserError::Custom(format!(
                        "unsupported JOIN type: {} {}",
                        jt, &name.0
                    )));
                };
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(join_type),
                }
            } else if "LEFT".eq_ignore_ascii_case(jt) && "RIGHT".eq_ignore_ascii_case(&name.0) {
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::Full),
                }
            } else if "OUTER".eq_ignore_ascii_case(jt) && "LEFT".eq_ignore_ascii_case(&name.0) {
                // OUTER LEFT JOIN         ->   same as LEFT JOIN
                JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(JoinType::LeftOuter),
                }
            } else {
                return Err(ParserError::Custom(format!(
                    "unsupported JOIN type: {} {}",
                    jt, &name.0
                )));
            }
        } else {
            unreachable!()
        })
    }
    pub(crate) fn from_triple(
        token: Token,
        n1: Name,
        n2: Name,
    ) -> Result<JoinOperator, ParserError> {
        Ok(if let Some(ref jt) = token.1 {
            if "NATURAL".eq_ignore_ascii_case(jt) && "OUTER".eq_ignore_ascii_case(&n2.0) {
                // If "OUTER" is present then there must also be one of "LEFT", "RIGHT", or "FULL"
                let join_type = if "LEFT".eq_ignore_ascii_case(&n1.0) {
                    JoinType::LeftOuter
                } else if "RIGHT".eq_ignore_ascii_case(&n1.0) {
                    JoinType::RightOuter
                } else if "FULL".eq_ignore_ascii_case(&n1.0) {
                    JoinType::FullOuter
                } else {
                    return Err(ParserError::Custom(format!(
                        "unsupported JOIN type: {} {} {}",
                        jt, &n1.0, &n2.0
                    )));
                };
                JoinOperator::TypedJoin {
                    natural: true,
                    join_type: Some(join_type),
                }
            } else if "OUTER".eq_ignore_ascii_case(jt)
                && "LEFT".eq_ignore_ascii_case(&n1.0)
                && "NATURAL".eq_ignore_ascii_case(&n2.0)
            {
                JoinOperator::TypedJoin {
                    natural: true,
                    join_type: Some(JoinType::LeftOuter),
                }
            } else {
                return Err(ParserError::Custom(format!(
                    "unsupported JOIN type: {} {} {}",
                    jt, &n1.0, &n2.0
                )));
            }
        } else {
            unreachable!()
        })
    }
}
impl ToTokens for JoinOperator {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            JoinOperator::Comma => s.append(TK_COMMA, None),
            JoinOperator::TypedJoin { natural, join_type } => {
                if *natural {
                    s.append(TK_JOIN_KW, Some("NATURAL"))?;
                }
                if let Some(ref join_type) = join_type {
                    join_type.to_tokens(s)?;
                }
                s.append(TK_JOIN, None)
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum JoinType {
    Left, // same as LeftOuter
    LeftOuter,
    Inner,
    Cross,
    Right, // same as RightOuter
    RightOuter,
    Full, // same as FullOuter
    FullOuter,
}
impl ToTokens for JoinType {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            TK_JOIN_KW,
            match self {
                JoinType::Left => Some("LEFT"),
                JoinType::LeftOuter => Some("LEFT OUTER"),
                JoinType::Inner => Some("INNER"),
                JoinType::Cross => Some("CROSS"),
                JoinType::Right => Some("RIGHT"),
                JoinType::RightOuter => Some("RIGHT OUTER"),
                JoinType::Full => Some("FULL"),
                JoinType::FullOuter => Some("FULL OUTER"),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinConstraint {
    On(Expr),
    // col names
    Using(Vec<Name>),
}

impl ToTokens for JoinConstraint {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            JoinConstraint::On(expr) => {
                s.append(TK_ON, None)?;
                expr.to_tokens(s)
            }
            JoinConstraint::Using(col_names) => {
                s.append(TK_USING, None)?;
                s.append(TK_LP, None)?;
                comma(col_names, s)?;
                s.append(TK_RP, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBy {
    pub exprs: Vec<Expr>,
    pub having: Option<Expr>,
}
impl ToTokens for GroupBy {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_GROUP, None)?;
        s.append(TK_BY, None)?;
        comma(&self.exprs, s)?;
        if let Some(ref having) = self.having {
            s.append(TK_HAVING, None)?;
            having.to_tokens(s)?;
        }
        Ok(())
    }
}

/// identifier or one of several keywords or `INDEXED`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Id(pub String);

impl Id {
    pub fn from_token(ty: YYCODETYPE, token: Token) -> Id {
        Id(from_token(ty, token))
    }
}
impl ToTokens for Id {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        double_quote(&self.0, s)
    }
}

// TODO ids (identifier or string)

/// identifier or string or `CROSS` or `FULL` or `INNER` or `LEFT` or `NATURAL` or `OUTER` or `RIGHT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Name(pub String); // TODO distinction between Name and "Name"/[Name]/`Name`

impl Name {
    pub fn from_token(ty: YYCODETYPE, token: Token) -> Name {
        Name(from_token(ty, token))
    }
}
impl ToTokens for Name {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        double_quote(&self.0, s)
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.to_fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualifiedName {
    pub db_name: Option<Name>,
    pub name: Name,
    pub alias: Option<Name>, // FIXME restrict alias usage (fullname vs xfullname)
}

impl QualifiedName {
    pub fn single(name: Name) -> Self {
        QualifiedName {
            db_name: None,
            name,
            alias: None,
        }
    }
    pub fn fullname(db_name: Name, name: Name) -> Self {
        QualifiedName {
            db_name: Some(db_name),
            name,
            alias: None,
        }
    }
    pub fn xfullname(db_name: Name, name: Name, alias: Name) -> Self {
        QualifiedName {
            db_name: Some(db_name),
            name,
            alias: Some(alias),
        }
    }
    pub fn alias(name: Name, alias: Name) -> Self {
        QualifiedName {
            db_name: None,
            name,
            alias: Some(alias),
        }
    }
}
impl ToTokens for QualifiedName {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if let Some(ref db_name) = self.db_name {
            db_name.to_tokens(s)?;
            s.append(TK_DOT, None)?;
        }
        self.name.to_tokens(s)?;
        if let Some(ref alias) = self.alias {
            s.append(TK_AS, None)?;
            alias.to_tokens(s)?;
        }
        Ok(())
    }
}

// https://sqlite.org/lang_altertable.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableBody {
    // new table name
    RenameTo(Name),
    AddColumn(ColumnDefinition), // TODO distinction between ADD and ADD COLUMN
    RenameColumn { old: Name, new: Name },
    DropColumn(Name), // TODO distinction between DROP and DROP COLUMN
    AlterColumn { old: Name, cd: ColumnDefinition },
}
impl ToTokens for AlterTableBody {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            AlterTableBody::RenameTo(name) => {
                s.append(TK_RENAME, None)?;
                s.append(TK_TO, None)?;
                name.to_tokens(s)
            }
            AlterTableBody::AddColumn(def) => {
                s.append(TK_ADD, None)?;
                s.append(TK_COLUMNKW, None)?;
                def.to_tokens(s)
            }
            AlterTableBody::RenameColumn { old, new } => {
                s.append(TK_RENAME, None)?;
                old.to_tokens(s)?;
                s.append(TK_TO, None)?;
                new.to_tokens(s)
            }
            AlterTableBody::DropColumn(name) => {
                s.append(TK_DROP, None)?;
                s.append(TK_COLUMNKW, None)?;
                name.to_tokens(s)
            }
            AlterTableBody::AlterColumn { old, cd } => {
                s.append(TK_ALTER, None)?;
                s.append(TK_COLUMNKW, None)?;
                old.to_tokens(s)?;
                s.append(TK_TO, None)?;
                cd.to_tokens(s)
            }
        }
    }
}

// https://sqlite.org/lang_createtable.html
// https://sqlite.org/syntax/create-table-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateTableBody {
    ColumnsAndConstraints {
        columns: Vec<ColumnDefinition>,
        constraints: Option<Vec<NamedTableConstraint>>,
        options: TableOptions,
    },
    AsSelect(Select),
}

impl CreateTableBody {
    pub fn columns_and_constraints(
        columns: Vec<ColumnDefinition>,
        constraints: Option<Vec<NamedTableConstraint>>,
        options: TableOptions,
    ) -> Result<CreateTableBody, ParserError> {
        Ok(CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        })
    }
}

impl ToTokens for CreateTableBody {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints,
                options,
            } => {
                s.append(TK_LP, None)?;
                comma(columns, s)?;
                if let Some(constraints) = constraints {
                    s.append(TK_COMMA, None)?;
                    comma(constraints, s)?;
                }
                s.append(TK_RP, None)?;
                if options.contains(TableOptions::WITHOUT_ROWID) {
                    s.append(TK_WITHOUT, None)?;
                    s.append(TK_ID, Some("ROWID"))?;
                } else if options.contains(TableOptions::RANDOM_ROWID) {
                    s.append(TK_ID, Some("RANDOM"))?;
                    s.append(TK_ID, Some("ROWID"))?;
                }
                if options.contains(TableOptions::STRICT) {
                    s.append(TK_ID, Some("STRICT"))?;
                }
                Ok(())
            }
            CreateTableBody::AsSelect(select) => {
                s.append(TK_AS, None)?;
                select.to_tokens(s)
            }
        }
    }
}

// https://sqlite.org/syntax/column-def.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub col_name: Name,
    pub col_type: Option<Type>,
    pub constraints: Vec<NamedColumnConstraint>,
}
impl ToTokens for ColumnDefinition {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.col_name.to_tokens(s)?;
        if let Some(ref col_type) = self.col_type {
            col_type.to_tokens(s)?;
        }
        for constraint in self.constraints.iter() {
            constraint.to_tokens(s)?;
        }
        Ok(())
    }
}
impl ColumnDefinition {
    pub fn add_column(
        columns: &mut Vec<ColumnDefinition>,
        cd: ColumnDefinition,
    ) -> Result<(), ParserError> {
        if columns
            .iter()
            .any(|c| c.col_name.0.eq_ignore_ascii_case(&cd.col_name.0))
        {
            return Err(ParserError::Custom(format!(
                "duplicate column name: {}",
                cd.col_name
            )));
        }
        columns.push(cd);
        Ok(())
    }
}

// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedColumnConstraint {
    pub name: Option<Name>,
    pub constraint: ColumnConstraint,
}
impl ToTokens for NamedColumnConstraint {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s)?;
        }
        self.constraint.to_tokens(s)
    }
}

// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnConstraint {
    PrimaryKey {
        order: Option<SortOrder>,
        conflict_clause: Option<ResolveType>,
        auto_increment: bool,
    },
    NotNull {
        nullable: bool,
        conflict_clause: Option<ResolveType>,
    },
    Unique(Option<ResolveType>),
    Check(Expr),
    Default(Expr),
    Defer(DeferSubclause), // FIXME
    Collate {
        collation_name: Name, // FIXME Ids
    },
    ForeignKey {
        clause: ForeignKeyClause,
        deref_clause: Option<DeferSubclause>,
    },
    Generated {
        expr: Expr,
        typ: Option<Id>,
    },
}
impl ToTokens for ColumnConstraint {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            ColumnConstraint::PrimaryKey {
                order,
                conflict_clause,
                auto_increment,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                if let Some(order) = order {
                    order.to_tokens(s)?;
                }
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s)?;
                }
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                Ok(())
            }
            ColumnConstraint::NotNull {
                nullable,
                conflict_clause,
            } => {
                if !nullable {
                    s.append(TK_NOT, None)?;
                }
                s.append(TK_NULL, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s)?;
                }
                Ok(())
            }
            ColumnConstraint::Unique(conflict_clause) => {
                s.append(TK_UNIQUE, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s)?;
                }
                Ok(())
            }
            ColumnConstraint::Check(expr) => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            ColumnConstraint::Default(expr) => {
                s.append(TK_DEFAULT, None)?;
                expr.to_tokens(s)
            }
            ColumnConstraint::Defer(deref_clause) => deref_clause.to_tokens(s),
            ColumnConstraint::Collate { collation_name } => {
                s.append(TK_COLLATE, None)?;
                collation_name.to_tokens(s)
            }
            ColumnConstraint::ForeignKey {
                clause,
                deref_clause,
            } => {
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s)?;
                if let Some(deref_clause) = deref_clause {
                    deref_clause.to_tokens(s)?;
                }
                Ok(())
            }
            ColumnConstraint::Generated { expr, typ } => {
                s.append(TK_AS, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s)?;
                s.append(TK_RP, None)?;
                if let Some(typ) = typ {
                    typ.to_tokens(s)?;
                }
                Ok(())
            }
        }
    }
}

// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedTableConstraint {
    pub name: Option<Name>,
    pub constraint: TableConstraint,
}
impl ToTokens for NamedTableConstraint {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if let Some(ref name) = self.name {
            s.append(TK_CONSTRAINT, None)?;
            name.to_tokens(s)?;
        }
        self.constraint.to_tokens(s)
    }
}

// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<SortedColumn>,
        auto_increment: bool,
        conflict_clause: Option<ResolveType>,
    },
    Unique {
        columns: Vec<SortedColumn>,
        conflict_clause: Option<ResolveType>,
    },
    Check(Expr),
    ForeignKey {
        columns: Vec<IndexedColumn>,
        clause: ForeignKeyClause,
        deref_clause: Option<DeferSubclause>,
    },
}
impl ToTokens for TableConstraint {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            TableConstraint::PrimaryKey {
                columns,
                auto_increment,
                conflict_clause,
            } => {
                s.append(TK_PRIMARY, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s)?;
                if *auto_increment {
                    s.append(TK_AUTOINCR, None)?;
                }
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s)?;
                }
                Ok(())
            }
            TableConstraint::Unique {
                columns,
                conflict_clause,
            } => {
                s.append(TK_UNIQUE, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s)?;
                s.append(TK_RP, None)?;
                if let Some(conflict_clause) = conflict_clause {
                    s.append(TK_ON, None)?;
                    s.append(TK_CONFLICT, None)?;
                    conflict_clause.to_tokens(s)?;
                }
                Ok(())
            }
            TableConstraint::Check(expr) => {
                s.append(TK_CHECK, None)?;
                s.append(TK_LP, None)?;
                expr.to_tokens(s)?;
                s.append(TK_RP, None)
            }
            TableConstraint::ForeignKey {
                columns,
                clause,
                deref_clause,
            } => {
                s.append(TK_FOREIGN, None)?;
                s.append(TK_KEY, None)?;
                s.append(TK_LP, None)?;
                comma(columns, s)?;
                s.append(TK_RP, None)?;
                s.append(TK_REFERENCES, None)?;
                clause.to_tokens(s)?;
                if let Some(deref_clause) = deref_clause {
                    deref_clause.to_tokens(s)?;
                }
                Ok(())
            }
        }
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub struct TableOptions: u8 {
        const NONE = 0;
        const WITHOUT_ROWID = 1;
        const STRICT = 2;
        const RANDOM_ROWID = 3;
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}
impl ToTokens for SortOrder {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                SortOrder::Asc => TK_ASC,
                SortOrder::Desc => TK_DESC,
            },
            None,
        )
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}
impl ToTokens for NullsOrder {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_NULLS, None)?;
        s.append(
            match self {
                NullsOrder::First => TK_FIRST,
                NullsOrder::Last => TK_LAST,
            },
            None,
        )
    }
}

// https://sqlite.org/syntax/foreign-key-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyClause {
    pub tbl_name: Name,
    pub columns: Option<Vec<IndexedColumn>>,
    pub args: Vec<RefArg>,
}
impl ToTokens for ForeignKeyClause {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s)?;
        if let Some(ref columns) = self.columns {
            s.append(TK_LP, None)?;
            comma(columns, s)?;
            s.append(TK_RP, None)?;
        }
        for arg in self.args.iter() {
            arg.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefArg {
    OnDelete(RefAct),
    OnInsert(RefAct),
    OnUpdate(RefAct),
    Match(Name),
}
impl ToTokens for RefArg {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            RefArg::OnDelete(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_DELETE, None)?;
                action.to_tokens(s)
            }
            RefArg::OnInsert(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_INSERT, None)?;
                action.to_tokens(s)
            }
            RefArg::OnUpdate(ref action) => {
                s.append(TK_ON, None)?;
                s.append(TK_UPDATE, None)?;
                action.to_tokens(s)
            }
            RefArg::Match(ref name) => {
                s.append(TK_MATCH, None)?;
                name.to_tokens(s)
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RefAct {
    SetNull,
    SetDefault,
    Cascade,
    Restrict,
    NoAction,
}
impl ToTokens for RefAct {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            RefAct::SetNull => {
                s.append(TK_SET, None)?;
                s.append(TK_NULL, None)
            }
            RefAct::SetDefault => {
                s.append(TK_SET, None)?;
                s.append(TK_DEFAULT, None)
            }
            RefAct::Cascade => s.append(TK_CASCADE, None),
            RefAct::Restrict => s.append(TK_RESTRICT, None),
            RefAct::NoAction => {
                s.append(TK_NO, None)?;
                s.append(TK_ACTION, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeferSubclause {
    pub deferrable: bool,
    pub init_deferred: Option<InitDeferredPred>,
}
impl ToTokens for DeferSubclause {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if !self.deferrable {
            s.append(TK_NOT, None)?;
        }
        s.append(TK_DEFERRABLE, None)?;
        if let Some(init_deferred) = self.init_deferred {
            init_deferred.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InitDeferredPred {
    InitiallyDeferred,
    InitiallyImmediate, // default
}
impl ToTokens for InitDeferredPred {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_INITIALLY, None)?;
        s.append(
            match self {
                InitDeferredPred::InitiallyDeferred => TK_DEFERRED,
                InitDeferredPred::InitiallyImmediate => TK_IMMEDIATE,
            },
            None,
        )
    }
}

// https://sqlite.org/syntax/indexed-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedColumn {
    pub col_name: Name,
    pub collation_name: Option<Name>, // FIXME Ids
    pub order: Option<SortOrder>,
}
impl ToTokens for IndexedColumn {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.col_name.to_tokens(s)?;
        if let Some(ref collation_name) = self.collation_name {
            s.append(TK_COLLATE, None)?;
            collation_name.to_tokens(s)?;
        }
        if let Some(order) = self.order {
            order.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Indexed {
    // idx name
    IndexedBy(Name),
    NotIndexed,
}
impl ToTokens for Indexed {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Indexed::IndexedBy(ref name) => {
                s.append(TK_INDEXED, None)?;
                s.append(TK_BY, None)?;
                name.to_tokens(s)
            }
            Indexed::NotIndexed => {
                s.append(TK_NOT, None)?;
                s.append(TK_INDEXED, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortedColumn {
    pub expr: Expr,
    pub order: Option<SortOrder>,
    pub nulls: Option<NullsOrder>,
}
impl ToTokens for SortedColumn {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.expr.to_tokens(s)?;
        if let Some(ref order) = self.order {
            order.to_tokens(s)?;
        }
        if let Some(ref nulls) = self.nulls {
            nulls.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limit {
    pub expr: Expr,
    pub offset: Option<Expr>, // TODO distinction between LIMIT offset, count and LIMIT count OFFSET offset
}
impl ToTokens for Limit {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_LIMIT, None)?;
        self.expr.to_tokens(s)?;
        if let Some(ref offset) = self.offset {
            s.append(TK_OFFSET, None)?;
            offset.to_tokens(s)?;
        }
        Ok(())
    }
}

// https://sqlite.org/lang_insert.html
// https://sqlite.org/syntax/insert-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertBody {
    Select(Select, Option<Upsert>),
    DefaultValues,
}
impl ToTokens for InsertBody {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            InsertBody::Select(select, upsert) => {
                select.to_tokens(s)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s)?;
                }
                Ok(())
            }
            InsertBody::DefaultValues => {
                s.append(TK_DEFAULT, None)?;
                s.append(TK_VALUES, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Set {
    pub col_names: Vec<Name>,
    pub expr: Expr,
}
impl ToTokens for Set {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if self.col_names.len() == 1 {
            comma(&self.col_names, s)?;
        } else {
            s.append(TK_LP, None)?;
            comma(&self.col_names, s)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_EQ, None)?;
        self.expr.to_tokens(s)
    }
}

// https://sqlite.org/syntax/pragma-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PragmaBody {
    Equals(PragmaValue),
    Call(PragmaValue),
}
impl ToTokens for PragmaBody {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            PragmaBody::Equals(value) => {
                s.append(TK_EQ, None)?;
                value.to_tokens(s)
            }
            PragmaBody::Call(value) => {
                s.append(TK_LP, None)?;
                value.to_tokens(s)?;
                s.append(TK_RP, None)
            }
        }
    }
}

// https://sqlite.org/syntax/pragma-value.html
pub type PragmaValue = Expr; // TODO

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TriggerTime {
    Before, // default
    After,
    InsteadOf,
}
impl ToTokens for TriggerTime {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            TriggerTime::Before => s.append(TK_BEFORE, None),
            TriggerTime::After => s.append(TK_AFTER, None),
            TriggerTime::InsteadOf => {
                s.append(TK_INSTEAD, None)?;
                s.append(TK_OF, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TriggerEvent {
    Delete,
    Insert,
    Update,
    // col names
    UpdateOf(Vec<Name>),
}
impl ToTokens for TriggerEvent {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            TriggerEvent::Delete => s.append(TK_DELETE, None),
            TriggerEvent::Insert => s.append(TK_INSERT, None),
            TriggerEvent::Update => s.append(TK_UPDATE, None),
            TriggerEvent::UpdateOf(ref col_names) => {
                s.append(TK_UPDATE, None)?;
                s.append(TK_OF, None)?;
                comma(col_names, s)
            }
        }
    }
}

// https://sqlite.org/lang_createtrigger.html
// https://sqlite.org/syntax/create-trigger-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TriggerCmd {
    Update {
        or_conflict: Option<ResolveType>,
        tbl_name: Name,
        sets: Vec<Set>,
        from: Option<FromClause>,
        where_clause: Option<Expr>,
    },
    Insert {
        or_conflict: Option<ResolveType>,
        tbl_name: Name,
        col_names: Option<Vec<Name>>,
        select: Select,
        upsert: Option<Upsert>,
        returning: Option<Vec<ResultColumn>>,
    },
    Delete {
        tbl_name: Name,
        where_clause: Option<Expr>,
    },
    Select(Select),
}
impl ToTokens for TriggerCmd {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            TriggerCmd::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause,
            } => {
                s.append(TK_UPDATE, None)?;
                if let Some(or_conflict) = or_conflict {
                    s.append(TK_OR, None)?;
                    or_conflict.to_tokens(s)?;
                }
                tbl_name.to_tokens(s)?;
                s.append(TK_SET, None)?;
                comma(sets, s)?;
                if let Some(from) = from {
                    s.append(TK_FROM, None)?;
                    from.to_tokens(s)?;
                }
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                Ok(())
            }
            TriggerCmd::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select,
                upsert,
                returning,
            } => {
                if let Some(ResolveType::Replace) = or_conflict {
                    s.append(TK_REPLACE, None)?;
                } else {
                    s.append(TK_INSERT, None)?;
                    if let Some(or_conflict) = or_conflict {
                        s.append(TK_OR, None)?;
                        or_conflict.to_tokens(s)?;
                    }
                }
                s.append(TK_INTO, None)?;
                tbl_name.to_tokens(s)?;
                if let Some(col_names) = col_names {
                    s.append(TK_LP, None)?;
                    comma(col_names, s)?;
                    s.append(TK_RP, None)?;
                }
                select.to_tokens(s)?;
                if let Some(upsert) = upsert {
                    upsert.to_tokens(s)?;
                }
                if let Some(returning) = returning {
                    s.append(TK_RETURNING, None)?;
                    comma(returning, s)?;
                }
                Ok(())
            }
            TriggerCmd::Delete {
                tbl_name,
                where_clause,
            } => {
                s.append(TK_DELETE, None)?;
                s.append(TK_FROM, None)?;
                tbl_name.to_tokens(s)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                Ok(())
            }
            TriggerCmd::Select(select) => select.to_tokens(s),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ResolveType {
    Rollback,
    Abort, // default
    Fail,
    Ignore,
    Replace,
}
impl ToTokens for ResolveType {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                ResolveType::Rollback => TK_ROLLBACK,
                ResolveType::Abort => TK_ABORT,
                ResolveType::Fail => TK_FAIL,
                ResolveType::Ignore => TK_IGNORE,
                ResolveType::Replace => TK_REPLACE,
            },
            None,
        )
    }
}

// https://sqlite.org/lang_with.html
// https://sqlite.org/syntax/with-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct With {
    pub recursive: bool,
    pub ctes: Vec<CommonTableExpr>,
}
impl ToTokens for With {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_WITH, None)?;
        if self.recursive {
            s.append(TK_RECURSIVE, None)?;
        }
        comma(&self.ctes, s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Materialized {
    Any,
    Yes,
    No,
}

// https://sqlite.org/syntax/common-table-expression.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonTableExpr {
    pub tbl_name: Name,
    pub columns: Option<Vec<IndexedColumn>>,
    pub materialized: Materialized,
    pub select: Select,
}

impl ToTokens for CommonTableExpr {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.tbl_name.to_tokens(s)?;
        if let Some(ref columns) = self.columns {
            s.append(TK_LP, None)?;
            comma(columns, s)?;
            s.append(TK_RP, None)?;
        }
        s.append(TK_AS, None)?;
        match self.materialized {
            Materialized::Any => {}
            Materialized::Yes => {
                s.append(TK_MATERIALIZED, None)?;
            }
            Materialized::No => {
                s.append(TK_NOT, None)?;
                s.append(TK_MATERIALIZED, None)?;
            }
        };
        s.append(TK_LP, None)?;
        self.select.to_tokens(s)?;
        s.append(TK_RP, None)
    }
}

impl CommonTableExpr {
    pub fn add_cte(
        ctes: &mut Vec<CommonTableExpr>,
        cte: CommonTableExpr,
    ) -> Result<(), ParserError> {
        if ctes
            .iter()
            .any(|c| c.tbl_name.0.eq_ignore_ascii_case(&cte.tbl_name.0))
        {
            return Err(ParserError::Custom(format!(
                "duplicate WITH table name: {}",
                cte.tbl_name
            )));
        }
        ctes.push(cte);
        Ok(())
    }
}

// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Type {
    pub name: String, // TODO Validate: Ids+
    pub size: Option<TypeSize>,
}
impl ToTokens for Type {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self.size {
            None => s.append(TK_ID, Some(&self.name)),
            Some(ref size) => {
                s.append(TK_ID, Some(&self.name))?; // TODO check there is no forbidden chars
                s.append(TK_LP, None)?;
                size.to_tokens(s)?;
                s.append(TK_RP, None)
            }
        }
    }
}

// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeSize {
    MaxSize(Box<Expr>),
    TypeSize(Box<Expr>, Box<Expr>),
}

impl ToTokens for TypeSize {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            TypeSize::MaxSize(size) => size.to_tokens(s),
            TypeSize::TypeSize(size1, size2) => {
                size1.to_tokens(s)?;
                s.append(TK_COMMA, None)?;
                size2.to_tokens(s)
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransactionType {
    Deferred, // default
    Immediate,
    Exclusive,
    ReadOnly,
}
impl ToTokens for TransactionType {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                TransactionType::Deferred => TK_DEFERRED,
                TransactionType::Immediate => TK_IMMEDIATE,
                TransactionType::Exclusive => TK_EXCLUSIVE,
                TransactionType::ReadOnly => TK_READONLY,
            },
            None,
        )
    }
}

// https://sqlite.org/lang_upsert.html
// https://sqlite.org/syntax/upsert-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upsert {
    pub index: Option<UpsertIndex>,
    pub do_clause: UpsertDo,
    pub next: Option<Box<Upsert>>,
}

impl ToTokens for Upsert {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_ON, None)?;
        s.append(TK_CONFLICT, None)?;
        if let Some(ref index) = self.index {
            index.to_tokens(s)?;
        }
        self.do_clause.to_tokens(s)?;
        if let Some(ref next) = self.next {
            next.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertIndex {
    pub targets: Vec<SortedColumn>,
    pub where_clause: Option<Expr>,
}

impl ToTokens for UpsertIndex {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        comma(&self.targets, s)?;
        s.append(TK_RP, None)?;
        if let Some(ref where_clause) = self.where_clause {
            s.append(TK_WHERE, None)?;
            where_clause.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpsertDo {
    Set {
        sets: Vec<Set>,
        where_clause: Option<Expr>,
    },
    Nothing,
}

impl ToTokens for UpsertDo {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            UpsertDo::Set { sets, where_clause } => {
                s.append(TK_DO, None)?;
                s.append(TK_UPDATE, None)?;
                s.append(TK_SET, None)?;
                comma(sets, s)?;
                if let Some(where_clause) = where_clause {
                    s.append(TK_WHERE, None)?;
                    where_clause.to_tokens(s)?;
                }
                Ok(())
            }
            UpsertDo::Nothing => {
                s.append(TK_DO, None)?;
                s.append(TK_NOTHING, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionTail {
    pub filter_clause: Option<Box<Expr>>,
    pub over_clause: Option<Box<Over>>,
}
impl ToTokens for FunctionTail {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        if let Some(ref filter_clause) = self.filter_clause {
            s.append(TK_FILTER, None)?;
            s.append(TK_LP, None)?;
            s.append(TK_WHERE, None)?;
            filter_clause.to_tokens(s)?;
            s.append(TK_RP, None)?;
        }
        if let Some(ref over_clause) = self.over_clause {
            s.append(TK_OVER, None)?;
            over_clause.to_tokens(s)?;
        }
        Ok(())
    }
}

// https://sqlite.org/syntax/over-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Over {
    Window(Window),
    Name(Name),
}

impl ToTokens for Over {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            Over::Window(ref window) => window.to_tokens(s),
            Over::Name(ref name) => name.to_tokens(s),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowDef {
    pub name: Name,
    pub window: Window,
}
impl ToTokens for WindowDef {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.name.to_tokens(s)?;
        s.append(TK_AS, None)?;
        self.window.to_tokens(s)
    }
}

// https://sqlite.org/syntax/window-defn.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    pub base: Option<Name>,
    pub partition_by: Option<Vec<Expr>>,
    pub order_by: Option<Vec<SortedColumn>>,
    pub frame_clause: Option<FrameClause>,
}

impl ToTokens for Window {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(TK_LP, None)?;
        if let Some(ref base) = self.base {
            base.to_tokens(s)?;
        }
        if let Some(ref partition_by) = self.partition_by {
            s.append(TK_PARTITION, None)?;
            s.append(TK_BY, None)?;
            comma(partition_by, s)?;
        }
        if let Some(ref order_by) = self.order_by {
            s.append(TK_ORDER, None)?;
            s.append(TK_BY, None)?;
            comma(order_by, s)?;
        }
        if let Some(ref frame_clause) = self.frame_clause {
            frame_clause.to_tokens(s)?;
        }
        s.append(TK_RP, None)
    }
}

// https://sqlite.org/syntax/frame-spec.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameClause {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: Option<FrameBound>,
    pub exclude: Option<FrameExclude>,
}

impl ToTokens for FrameClause {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        self.mode.to_tokens(s)?;
        if let Some(ref end) = self.end {
            s.append(TK_BETWEEN, None)?;
            self.start.to_tokens(s)?;
            s.append(TK_AND, None)?;
            end.to_tokens(s)?;
        } else {
            self.start.to_tokens(s)?;
        }
        if let Some(ref exclude) = self.exclude {
            s.append(TK_EXCLUDE, None)?;
            exclude.to_tokens(s)?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrameMode {
    Groups,
    Range,
    Rows,
}

impl ToTokens for FrameMode {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        s.append(
            match self {
                FrameMode::Groups => TK_GROUPS,
                FrameMode::Range => TK_RANGE,
                FrameMode::Rows => TK_ROWS,
            },
            None,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameBound {
    CurrentRow,
    Following(Expr),
    Preceding(Expr),
    UnboundedFollowing,
    UnboundedPreceding,
}

impl ToTokens for FrameBound {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            FrameBound::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            FrameBound::Following(value) => {
                value.to_tokens(s)?;
                s.append(TK_FOLLOWING, None)
            }
            FrameBound::Preceding(value) => {
                value.to_tokens(s)?;
                s.append(TK_PRECEDING, None)
            }
            FrameBound::UnboundedFollowing => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_FOLLOWING, None)
            }
            FrameBound::UnboundedPreceding => {
                s.append(TK_UNBOUNDED, None)?;
                s.append(TK_PRECEDING, None)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameExclude {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl ToTokens for FrameExclude {
    fn to_tokens<S: TokenStream>(&self, s: &mut S) -> Result<(), S::Error> {
        match self {
            FrameExclude::NoOthers => {
                s.append(TK_NO, None)?;
                s.append(TK_OTHERS, None)
            }
            FrameExclude::CurrentRow => {
                s.append(TK_CURRENT, None)?;
                s.append(TK_ROW, None)
            }
            FrameExclude::Group => s.append(TK_GROUP, None),
            FrameExclude::Ties => s.append(TK_TIES, None),
        }
    }
}

fn comma<I, S: TokenStream>(items: I, s: &mut S) -> Result<(), S::Error>
where
    I: IntoIterator,
    I::Item: ToTokens,
{
    let iter = items.into_iter();
    for (i, item) in iter.enumerate() {
        if i != 0 {
            s.append(TK_COMMA, None)?;
        }
        item.to_tokens(s)?;
    }
    Ok(())
}

// TK_ID: [...] / `...` / "..." / some keywords / non keywords
fn double_quote<S: TokenStream>(name: &str, s: &mut S) -> Result<(), S::Error> {
    if name.is_empty() {
        return s.append(TK_ID, Some("\"\""));
    }
    if is_identifier(name) {
        // identifier must be quoted when they match a keyword...
        /*if is_keyword(name) {
            f.write_char('`')?;
            f.write_str(name)?;
            return f.write_char('`');
        }*/
        return s.append(TK_ID, Some(name));
    }
    /*f.write_char('"')?;
    for c in name.chars() {
        if c == '"' {
            f.write_char(c)?;
        }
        f.write_char(c)?;
    }
    f.write_char('"')*/
    s.append(TK_ID, Some(name))
}
