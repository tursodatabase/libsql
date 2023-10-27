//! SQLite dialect

use std::fmt::Formatter;
use std::str;
use uncased::UncasedStr;

mod token;
pub use token::TokenType;

/// Token value (lexeme)
pub struct Token(pub usize, pub Option<String>, pub usize);

pub(crate) fn sentinel(start: usize) -> Token {
    Token(start, None, start)
}

impl Token {
    pub fn unwrap(self) -> String {
        self.1.unwrap()
    }
    pub fn take(&mut self) -> Self {
        Token(self.0, self.1.take(), self.2)
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Token").field(&self.1).finish()
    }
}

impl TokenType {
    // TODO try Cow<&'static, str> (Borrowed<&'static str> for keyword and Owned<String> for below),
    // => Syntax error on keyword will be better
    // => `from_token` will become unnecessary
    pub(crate) fn to_token(self, start: usize, value: &[u8], end: usize) -> Token {
        Token(
            start,
            match self {
                TokenType::TK_CTIME_KW => Some(from_bytes(value)),
                TokenType::TK_JOIN_KW => Some(from_bytes(value)),
                TokenType::TK_LIKE_KW => Some(from_bytes(value)),
                TokenType::TK_PTR => Some(from_bytes(value)),
                // Identifiers
                TokenType::TK_STRING => Some(from_bytes(value)),
                TokenType::TK_ID => Some(from_bytes(value)),
                TokenType::TK_VARIABLE => Some(from_bytes(value)),
                // Values
                TokenType::TK_ANY => Some(from_bytes(value)),
                TokenType::TK_BLOB => Some(from_bytes(value)),
                TokenType::TK_INTEGER => Some(from_bytes(value)),
                TokenType::TK_FLOAT => Some(from_bytes(value)),
                _ => None,
            },
            end,
        )
    }
}

fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
pub(crate) const MAX_KEYWORD_LEN: usize = 17;

pub fn keyword_token(word: &[u8]) -> Option<TokenType> {
    KEYWORDS
        .get(UncasedStr::new(unsafe { str::from_utf8_unchecked(word) }))
        .cloned()
}

pub(crate) fn is_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = name.as_bytes();
    is_identifier_start(bytes[0])
        && (bytes.len() == 1 || bytes[1..].iter().all(|b| is_identifier_continue(*b)))
}

pub(crate) fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() || b > b'\x7F'
}

pub(crate) fn is_identifier_continue(b: u8) -> bool {
    b == b'$'
        || b.is_ascii_digit()
        || b.is_ascii_uppercase()
        || b == b'_'
        || b.is_ascii_lowercase()
        || b > b'\x7F'
}

// keyword may become an identifier
// see %fallback in parse.y
pub(crate) fn from_token(ty: u16, value: Token) -> String {
    use TokenType::*;
    if let Some(str) = value.1 {
        return str;
    }
    match ty {
        x if x == TK_ABORT as u16 => "ABORT".to_owned(),
        x if x == TK_ACTION as u16 => "ACTION".to_owned(),
        //x if x == TK_ADD as u16 => "ADD".to_owned(),
        x if x == TK_AFTER as u16 => "AFTER".to_owned(),
        //x if x == TK_ALL as u16 => "ALL".to_owned(),
        //x if x == TK_ALTER as u16 => "ALTER".to_owned(),
        x if x == TK_ALWAYS as u16 => "ALWAYS".to_owned(),
        x if x == TK_ANALYZE as u16 => "ANALYZE".to_owned(),
        //x if x == TK_AND as u16 => "AND".to_owned(),
        //x if x == TK_AS as u16 => "AS".to_owned(),
        x if x == TK_ASC as u16 => "ASC".to_owned(),
        x if x == TK_ATTACH as u16 => "ATTACH".to_owned(),
        //x if x == TK_AUTOINCR as u16 => "AUTOINCREMENT".to_owned(),
        x if x == TK_BEFORE as u16 => "BEFORE".to_owned(),
        x if x == TK_BEGIN as u16 => "BEGIN".to_owned(),
        //x if x == TK_BETWEEN as u16 => "BETWEEN".to_owned(),
        x if x == TK_BY as u16 => "BY".to_owned(),
        x if x == TK_CASCADE as u16 => "CASCADE".to_owned(),
        //x if x == TK_CASE as u16 => "CASE".to_owned(),
        x if x == TK_CAST as u16 => "CAST".to_owned(),
        //x if x == TK_CHECK as u16 => "CHECK".to_owned(),
        //x if x == TK_COLLATE as u16 => "COLLATE".to_owned(),
        x if x == TK_COLUMNKW as u16 => "COLUMN".to_owned(),
        //x if x == TK_COMMIT as u16 => "COMMIT".to_owned(),
        x if x == TK_CONFLICT as u16 => "CONFLICT".to_owned(),
        //x if x == TK_CONSTRAINT as u16 => "CONSTRAINT".to_owned(),
        //x if x == TK_CREATE as u16 => "CREATE".to_owned(),
        x if x == TK_CURRENT as u16 => "CURRENT".to_owned(),
        x if x == TK_DATABASE as u16 => "DATABASE".to_owned(),
        x if x == TK_DEFAULT as u16 => "DEFAULT".to_owned(),
        //x if x == TK_DEFERRABLE as u16 => "DEFERRABLE".to_owned(),
        x if x == TK_DEFERRED as u16 => "DEFERRED".to_owned(),
        x if x == TK_DELETE as u16 => "DELETE".to_owned(),
        x if x == TK_DESC as u16 => "DESC".to_owned(),
        x if x == TK_DETACH as u16 => "DETACH".to_owned(),
        //x if x == TK_DISTINCT as u16 => "DISTINCT".to_owned(),
        x if x == TK_DO as u16 => "DO".to_owned(),
        //x if x == TK_DROP as u16 => "DROP".to_owned(),
        x if x == TK_EACH as u16 => "EACH".to_owned(),
        //x if x == TK_ELSE as u16 => "ELSE".to_owned(),
        x if x == TK_END as u16 => "END".to_owned(),
        //x if x == TK_ESCAPE as u16 => "ESCAPE".to_owned(),
        //x if x == TK_EXCEPT as u16 => "EXCEPT".to_owned(),
        x if x == TK_EXCLUDE as u16 => "EXCLUDE".to_owned(),
        x if x == TK_EXCLUSIVE as u16 => "EXCLUSIVE".to_owned(),
        //x if x == TK_EXISTS as u16 => "EXISTS".to_owned(),
        x if x == TK_EXPLAIN as u16 => "EXPLAIN".to_owned(),
        x if x == TK_FAIL as u16 => "FAIL".to_owned(),
        //x if x == TK_FILTER as u16 => "FILTER".to_owned(),
        x if x == TK_FIRST as u16 => "FIRST".to_owned(),
        x if x == TK_FOLLOWING as u16 => "FOLLOWING".to_owned(),
        x if x == TK_FOR as u16 => "FOR".to_owned(),
        //x if x == TK_FOREIGN as u16 => "FOREIGN".to_owned(),
        //x if x == TK_FROM as u16 => "FROM".to_owned(),
        x if x == TK_GENERATED as u16 => "GENERATED".to_owned(),
        //x if x == TK_GROUP as u16 => "GROUP".to_owned(),
        x if x == TK_GROUPS as u16 => "GROUPS".to_owned(),
        //x if x == TK_HAVING as u16 => "HAVING".to_owned(),
        x if x == TK_IF as u16 => "IF".to_owned(),
        x if x == TK_IGNORE as u16 => "IGNORE".to_owned(),
        x if x == TK_IMMEDIATE as u16 => "IMMEDIATE".to_owned(),
        //x if x == TK_IN as u16 => "IN".to_owned(),
        //x if x == TK_INDEX as u16 => "INDEX".to_owned(),
        x if x == TK_INDEXED as u16 => "INDEXED".to_owned(),
        x if x == TK_INITIALLY as u16 => "INITIALLY".to_owned(),
        //x if x == TK_INSERT as u16 => "INSERT".to_owned(),
        x if x == TK_INSTEAD as u16 => "INSTEAD".to_owned(),
        //x if x == TK_INTERSECT as u16 => "INTERSECT".to_owned(),
        //x if x == TK_INTO as u16 => "INTO".to_owned(),
        //x if x == TK_IS as u16 => "IS".to_owned(),
        //x if x == TK_ISNULL as u16 => "ISNULL".to_owned(),
        //x if x == TK_JOIN as u16 => "JOIN".to_owned(),
        x if x == TK_KEY as u16 => "KEY".to_owned(),
        x if x == TK_LAST as u16 => "LAST".to_owned(),
        //x if x == TK_LIMIT as u16 => "LIMIT".to_owned(),
        x if x == TK_MATCH as u16 => "MATCH".to_owned(),
        x if x == TK_MATERIALIZED as u16 => "MATERIALIZED".to_owned(),
        x if x == TK_NO as u16 => "NO".to_owned(),
        //x if x == TK_NOT as u16 => "NOT".to_owned(),
        //x if x == TK_NOTHING as u16 => "NOTHING".to_owned(),
        //x if x == TK_NOTNULL as u16 => "NOTNULL".to_owned(),
        //x if x == TK_NULL as u16 => "NULL".to_owned(),
        x if x == TK_NULLS as u16 => "NULLS".to_owned(),
        x if x == TK_OF as u16 => "OF".to_owned(),
        x if x == TK_OFFSET as u16 => "OFFSET".to_owned(),
        x if x == TK_ON as u16 => "ON".to_owned(),
        //x if x == TK_OR as u16 => "OR".to_owned(),
        //x if x == TK_ORDER as u16 => "ORDER".to_owned(),
        x if x == TK_OTHERS as u16 => "OTHERS".to_owned(),
        //x if x == TK_OVER as u16 => "OVER".to_owned(),
        x if x == TK_PARTITION as u16 => "PARTITION".to_owned(),
        x if x == TK_PLAN as u16 => "PLAN".to_owned(),
        x if x == TK_PRAGMA as u16 => "PRAGMA".to_owned(),
        x if x == TK_PRECEDING as u16 => "PRECEDING".to_owned(),
        //x if x == TK_PRIMARY as u16 => "PRIMARY".to_owned(),
        x if x == TK_QUERY as u16 => "QUERY".to_owned(),
        x if x == TK_RAISE as u16 => "RAISE".to_owned(),
        x if x == TK_RANGE as u16 => "RANGE".to_owned(),
        x if x == TK_READONLY as u16 => "READONLY".to_owned(),
        x if x == TK_RECURSIVE as u16 => "RECURSIVE".to_owned(),
        //x if x == TK_REFERENCES as u16 => "REFERENCES".to_owned(),
        x if x == TK_REINDEX as u16 => "REINDEX".to_owned(),
        x if x == TK_RELEASE as u16 => "RELEASE".to_owned(),
        x if x == TK_RENAME as u16 => "RENAME".to_owned(),
        x if x == TK_REPLACE as u16 => "REPLACE".to_owned(),
        //x if x == TK_RETURNING as u16 => "RETURNING".to_owned(),
        x if x == TK_RESTRICT as u16 => "RESTRICT".to_owned(),
        x if x == TK_ROLLBACK as u16 => "ROLLBACK".to_owned(),
        x if x == TK_ROW as u16 => "ROW".to_owned(),
        x if x == TK_ROWS as u16 => "ROWS".to_owned(),
        x if x == TK_SAVEPOINT as u16 => "SAVEPOINT".to_owned(),
        //x if x == TK_SELECT as u16 => "SELECT".to_owned(),
        //x if x == TK_SET as u16 => "SET".to_owned(),
        //x if x == TK_TABLE as u16 => "TABLE".to_owned(),
        x if x == TK_TEMP as u16 => "TEMP".to_owned(),
        //x if x == TK_TEMP as u16 => "TEMPORARY".to_owned(),
        //x if x == TK_THEN as u16 => "THEN".to_owned(),
        x if x == TK_TIES as u16 => "TIES".to_owned(),
        //x if x == TK_TO as u16 => "TO".to_owned(),
        //x if x == TK_TRANSACTION as u16 => "TRANSACTION".to_owned(),
        x if x == TK_TRIGGER as u16 => "TRIGGER".to_owned(),
        x if x == TK_UNBOUNDED as u16 => "UNBOUNDED".to_owned(),
        //x if x == TK_UNION as u16 => "UNION".to_owned(),
        //x if x == TK_UNIQUE as u16 => "UNIQUE".to_owned(),
        //x if x == TK_UPDATE as u16 => "UPDATE".to_owned(),
        //x if x == TK_USING as u16 => "USING".to_owned(),
        x if x == TK_VACUUM as u16 => "VACUUM".to_owned(),
        x if x == TK_VALUES as u16 => "VALUES".to_owned(),
        x if x == TK_VIEW as u16 => "VIEW".to_owned(),
        x if x == TK_VIRTUAL as u16 => "VIRTUAL".to_owned(),
        //x if x == TK_WHEN as u16 => "WHEN".to_owned(),
        //x if x == TK_WHERE as u16 => "WHERE".to_owned(),
        //x if x == TK_WINDOW as u16 => "WINDOW".to_owned(),
        x if x == TK_WITH as u16 => "WITH".to_owned(),
        x if x == TK_WITHOUT as u16 => "WITHOUT".to_owned(),
        _ => unreachable!(),
    }
}

impl TokenType {
    pub const fn as_str(&self) -> Option<&'static str> {
        use TokenType::*;
        match self {
            TK_ABORT => Some("ABORT"),
            TK_ACTION => Some("ACTION"),
            TK_ADD => Some("ADD"),
            TK_AFTER => Some("AFTER"),
            TK_ALL => Some("ALL"),
            TK_ALTER => Some("ALTER"),
            TK_ANALYZE => Some("ANALYZE"),
            TK_ALWAYS => Some("ALWAYS"),
            TK_AND => Some("AND"),
            TK_AS => Some("AS"),
            TK_ASC => Some("ASC"),
            TK_ATTACH => Some("ATTACH"),
            TK_AUTOINCR => Some("AUTOINCREMENT"),
            TK_BEFORE => Some("BEFORE"),
            TK_BEGIN => Some("BEGIN"),
            TK_BETWEEN => Some("BETWEEN"),
            TK_BY => Some("BY"),
            TK_CASCADE => Some("CASCADE"),
            TK_CASE => Some("CASE"),
            TK_CAST => Some("CAST"),
            TK_CHECK => Some("CHECK"),
            TK_COLLATE => Some("COLLATE"),
            TK_COLUMNKW => Some("COLUMN"),
            TK_COMMIT => Some("COMMIT"),
            TK_CONFLICT => Some("CONFLICT"),
            TK_CONSTRAINT => Some("CONSTRAINT"),
            TK_CREATE => Some("CREATE"),
            TK_CURRENT => Some("CURRENT"),
            TK_DATABASE => Some("DATABASE"),
            TK_DEFAULT => Some("DEFAULT"),
            TK_DEFERRABLE => Some("DEFERRABLE"),
            TK_DEFERRED => Some("DEFERRED"),
            TK_DELETE => Some("DELETE"),
            TK_DESC => Some("DESC"),
            TK_DETACH => Some("DETACH"),
            TK_DISTINCT => Some("DISTINCT"),
            TK_DO => Some("DO"),
            TK_DROP => Some("DROP"),
            TK_EACH => Some("EACH"),
            TK_ELSE => Some("ELSE"),
            TK_END => Some("END"),
            TK_ESCAPE => Some("ESCAPE"),
            TK_EXCEPT => Some("EXCEPT"),
            TK_EXCLUDE => Some("EXCLUDE"),
            TK_EXCLUSIVE => Some("EXCLUSIVE"),
            TK_EXISTS => Some("EXISTS"),
            TK_EXPLAIN => Some("EXPLAIN"),
            TK_FAIL => Some("FAIL"),
            TK_FILTER => Some("FILTER"),
            TK_FIRST => Some("FIRST"),
            TK_FOLLOWING => Some("FOLLOWING"),
            TK_FOR => Some("FOR"),
            TK_FOREIGN => Some("FOREIGN"),
            TK_FROM => Some("FROM"),
            TK_GENERATED => Some("GENERATED"),
            TK_GROUP => Some("GROUP"),
            TK_GROUPS => Some("GROUPS"),
            TK_HAVING => Some("HAVING"),
            TK_IF => Some("IF"),
            TK_IGNORE => Some("IGNORE"),
            TK_IMMEDIATE => Some("IMMEDIATE"),
            TK_IN => Some("IN"),
            TK_INDEX => Some("INDEX"),
            TK_INDEXED => Some("INDEXED"),
            TK_INITIALLY => Some("INITIALLY"),
            TK_INSERT => Some("INSERT"),
            TK_INSTEAD => Some("INSTEAD"),
            TK_INTERSECT => Some("INTERSECT"),
            TK_INTO => Some("INTO"),
            TK_IS => Some("IS"),
            TK_ISNULL => Some("ISNULL"),
            TK_JOIN => Some("JOIN"),
            TK_KEY => Some("KEY"),
            TK_LAST => Some("LAST"),
            TK_LIMIT => Some("LIMIT"),
            TK_MATCH => Some("MATCH"),
            TK_MATERIALIZED => Some("MATERIALIZED"),
            TK_NO => Some("NO"),
            TK_NOT => Some("NOT"),
            TK_NOTHING => Some("NOTHING"),
            TK_NOTNULL => Some("NOTNULL"),
            TK_NULL => Some("NULL"),
            TK_NULLS => Some("NULLS"),
            TK_OF => Some("OF"),
            TK_OFFSET => Some("OFFSET"),
            TK_ON => Some("ON"),
            TK_OR => Some("OR"),
            TK_ORDER => Some("ORDER"),
            TK_OTHERS => Some("OTHERS"),
            TK_OVER => Some("OVER"),
            TK_PARTITION => Some("PARTITION"),
            TK_PLAN => Some("PLAN"),
            TK_PRAGMA => Some("PRAGMA"),
            TK_PRECEDING => Some("PRECEDING"),
            TK_PRIMARY => Some("PRIMARY"),
            TK_QUERY => Some("QUERY"),
            TK_RAISE => Some("RAISE"),
            TK_RANGE => Some("RANGE"),
            TK_RECURSIVE => Some("RECURSIVE"),
            TK_REFERENCES => Some("REFERENCES"),
            TK_REINDEX => Some("REINDEX"),
            TK_RELEASE => Some("RELEASE"),
            TK_RENAME => Some("RENAME"),
            TK_REPLACE => Some("REPLACE"),
            TK_RETURNING => Some("RETURNING"),
            TK_RESTRICT => Some("RESTRICT"),
            TK_ROLLBACK => Some("ROLLBACK"),
            TK_ROW => Some("ROW"),
            TK_ROWS => Some("ROWS"),
            TK_SAVEPOINT => Some("SAVEPOINT"),
            TK_SELECT => Some("SELECT"),
            TK_SET => Some("SET"),
            TK_TABLE => Some("TABLE"),
            TK_TEMP => Some("TEMP"), // or TEMPORARY
            TK_TIES => Some("TIES"),
            TK_THEN => Some("THEN"),
            TK_TO => Some("TO"),
            TK_TRANSACTION => Some("TRANSACTION"),
            TK_TRIGGER => Some("TRIGGER"),
            TK_UNBOUNDED => Some("UNBOUNDED"),
            TK_UNION => Some("UNION"),
            TK_UNIQUE => Some("UNIQUE"),
            TK_UPDATE => Some("UPDATE"),
            TK_USING => Some("USING"),
            TK_VACUUM => Some("VACUUM"),
            TK_VALUES => Some("VALUES"),
            TK_VIEW => Some("VIEW"),
            TK_VIRTUAL => Some("VIRTUAL"),
            TK_WHEN => Some("WHEN"),
            TK_WHERE => Some("WHERE"),
            TK_WINDOW => Some("WINDOW"),
            TK_WITH => Some("WITH"),
            TK_WITHOUT => Some("WITHOUT"),
            TK_BITAND => Some("&"),
            TK_BITNOT => Some("~"),
            TK_BITOR => Some("|"),
            TK_COMMA => Some(","),
            TK_CONCAT => Some("||"),
            TK_DOT => Some("."),
            TK_EQ => Some("="), // or ==
            TK_GT => Some(">"),
            TK_GE => Some(">="),
            TK_LP => Some("("),
            TK_LSHIFT => Some("<<"),
            TK_LE => Some("<="),
            TK_LT => Some("<"),
            TK_MINUS => Some("-"),
            TK_NE => Some("<>"), // or !=
            TK_PLUS => Some("+"),
            TK_REM => Some("%"),
            TK_RP => Some(")"),
            TK_RSHIFT => Some(">>"),
            TK_SEMI => Some(";"),
            TK_SLASH => Some("/"),
            TK_STAR => Some("*"),
            TK_READONLY => Some("READONLY"),
            _ => None,
        }
    }
}
