//! port of dump from `shell.c`
use std::ffi::CString;
use std::fmt::{Display, Write as _};
use std::io::Write;

use anyhow::bail;
use rusqlite::types::ValueRef;
use rusqlite::OptionalExtension;

struct DumpState<W: Write> {
    /// true if db is in writable_schema mode
    writable_schema: bool,
    writer: W,
}

use rusqlite::ffi::{sqlite3_keyword_check, sqlite3_table_column_metadata, SQLITE_OK};

impl<W: Write> DumpState<W> {
    fn run_schema_dump_query(
        &mut self,
        txn: &rusqlite::Connection,
        stmt: &str,
        preserve_rowids: bool,
    ) -> anyhow::Result<()> {
        let mut stmt = txn.prepare(stmt)?;
        let mut rows = stmt.query(())?;
        while let Some(row) = rows.next()? {
            let ValueRef::Text(table) = row.get_ref(0)? else {
                bail!("invalid schema table")
            };
            let ValueRef::Text(ty) = row.get_ref(1)? else {
                bail!("invalid schema table")
            };
            let ValueRef::Text(sql) = row.get_ref(2)? else {
                bail!("invalid schema table")
            };

            if table == b"sqlite_sequence" {
                writeln!(self.writer, "DELETE FROM sqlite_sequence;")?;
            } else if table.starts_with(b"sqlite_stat") {
                writeln!(self.writer, "ANALYZE sqlite_schema;")?;
            } else if table.starts_with(b"sqlite_") {
                continue;
            } else if sql.starts_with(b"CREATE VIRTUAL TABLE") {
                if !self.writable_schema {
                    writeln!(self.writer, "PRAGMA writable_schema=ON;")?;
                    self.writable_schema = true;
                }

                let table_str = std::str::from_utf8(table)?;
                writeln!(
                    self.writer,
                    "INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql)VALUES('table','{}','{}',0,{});",
                    table_str,
                    table_str,
                    Escaped(std::str::from_utf8(sql)?)
                )?;
                continue;
            } else {
                if sql.starts_with(b"CREATE TABLE") {
                    self.writer.write_all(b"CREATE TABLE IF NOT EXISTS ")?;
                    self.writer.write_all(&sql[13..])?;
                } else {
                    self.writer.write_all(sql)?;
                }
                writeln!(self.writer, ";")?;
            }

            if ty == b"table" {
                let table_str = std::str::from_utf8(table)?;
                let (row_id_col, colss) =
                    self.list_table_columns(txn, table_str, preserve_rowids)?;
                let mut insert = String::new();
                write!(&mut insert, "INSERT INTO {}", Quoted(table_str))?;

                if let Some(ref row_id_col) = row_id_col {
                    insert.push('(');
                    insert.push_str(row_id_col);
                    for col in &colss {
                        write!(&mut insert, ",{}", Quoted(col))?;
                    }

                    insert.push(')');
                }

                insert.push_str(" VALUES(");

                let mut select = String::from("SELECT ");
                if let Some(ref row_id_col) = row_id_col {
                    write!(&mut select, "{row_id_col},")?;
                }

                let mut iter = colss.iter().peekable();
                while let Some(col) = iter.next() {
                    write!(&mut select, "{}", Quoted(col))?;
                    if iter.peek().is_some() {
                        select.push(',');
                    }
                }

                write!(&mut select, " FROM {}", Quoted(table_str))?;

                let mut stmt = txn.prepare(&select)?;
                let mut rows = stmt.query(())?;
                while let Some(row) = rows.next()? {
                    write!(self.writer, "{insert}")?;
                    if row_id_col.is_some() {
                        write_value_ref(&mut self.writer, row.get_ref(0)?)?;
                    }

                    let offset = row_id_col.is_some() as usize;
                    for i in 0..colss.len() {
                        if i != 0 || row_id_col.is_some() {
                            write!(self.writer, ",")?;
                        }
                        write_value_ref(&mut self.writer, row.get_ref(i + offset)?)?;
                    }
                    writeln!(self.writer, ");")?;
                }
            }
        }

        Ok(())
    }

    fn run_table_dump_query(&mut self, txn: &rusqlite::Connection, q: &str) -> anyhow::Result<()> {
        let mut stmt = txn.prepare(q)?;
        let col_count = stmt.column_count();
        let mut rows = stmt.query(())?;
        while let Some(row) = rows.next()? {
            let ValueRef::Text(sql) = row.get_ref(0)? else {
                bail!("the first row in a table dump query should be of type text")
            };
            self.writer.write_all(sql)?;
            for i in 1..col_count {
                let ValueRef::Text(s) = row.get_ref(i)? else {
                    bail!("row {i} in table dump query should be of type text")
                };
                let s = std::str::from_utf8(s)?;
                write!(self.writer, ",{s}")?;
            }
            writeln!(self.writer, ";")?;
        }
        Ok(())
    }

    fn list_table_columns(
        &self,
        txn: &rusqlite::Connection,
        table: &str,
        preserve_rowids: bool,
    ) -> anyhow::Result<(Option<String>, Vec<String>)> {
        let mut cols = Vec::new();
        let mut num_primary_keys = 0;
        let mut is_integer_primary_key = false;
        let mut preserve_rowids = preserve_rowids;
        let mut row_id_col = None;

        txn.pragma(None, "table_info", table, |row| {
            let name: String = row.get_unwrap(1);
            cols.push(name);
            // this is a primary key col
            if row.get_unwrap::<_, usize>(5) != 0 {
                num_primary_keys += 1;
                is_integer_primary_key = num_primary_keys == 1
                    && matches!(row.get_ref_unwrap(2), ValueRef::Text(b"INTEGER"));
            }

            Ok(())
        })?;

        // from sqlite:
        // > The decision of whether or not a rowid really needs to be preserved
        // > is tricky.  We never need to preserve a rowid for a WITHOUT ROWID table
        // > or a table with an INTEGER PRIMARY KEY.  We are unable to preserve
        // > rowids on tables where the rowid is inaccessible because there are other
        // > columns in the table named "rowid", "_rowid_", and "oid".
        if is_integer_primary_key {
            // from sqlite:
            // > If a single PRIMARY KEY column with type INTEGER was seen, then it
            // > might be an alise for the ROWID.  But it might also be a WITHOUT ROWID
            // > table or a INTEGER PRIMARY KEY DESC column, neither of which are
            // > ROWID aliases.  To distinguish these cases, check to see if
            // > there is a "pk" entry in "PRAGMA index_list".  There will be
            // > no "pk" index if the PRIMARY KEY really is an alias for the ROWID.

            txn.query_row(
                "SELECT 1 FROM pragma_index_list(?)  WHERE origin='pk'",
                [table],
                |_| {
                    // re-set preserve_row_id if there is a row
                    preserve_rowids = true;
                    Ok(())
                },
            )
            .optional()?;
        }

        if preserve_rowids {
            const ROW_ID_NAMES: [&str; 3] = ["rowid", "_row_id_", "oid"];

            for row_id_name in ROW_ID_NAMES {
                let col_name_taken = cols.iter().any(|col| col == row_id_name);

                if !col_name_taken {
                    let table_name_cstr = CString::new(table).unwrap();
                    let row_id_name_cstr = CString::new(row_id_name).unwrap();
                    let rc = unsafe {
                        sqlite3_table_column_metadata(
                            txn.handle(),
                            std::ptr::null_mut(),
                            table_name_cstr.as_ptr(),
                            row_id_name_cstr.as_ptr(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                        )
                    };

                    if rc == SQLITE_OK {
                        row_id_col = Some(row_id_name.to_owned());
                        break;
                    }
                }
            }
        }

        Ok((row_id_col, cols))
    }
}

fn write_value_ref<W: Write>(mut w: W, val: ValueRef) -> anyhow::Result<()> {
    match val {
        ValueRef::Null => write!(w, "NULL")?,
        ValueRef::Integer(i) => write!(w, "{i}")?,
        ValueRef::Real(x) => {
            let as_u64 = x as u64;
            if as_u64 == 0x7ff0000000000000 {
                write!(w, "1e999")?;
            } else if as_u64 == 0xfff0000000000000 {
                write!(w, "-1e999")?;
            } else {
                write!(w, "{x}")?;
            }
        }
        ValueRef::Text(s) => {
            write!(w, "{}", Escaped(std::str::from_utf8(s)?))?;
        }
        ValueRef::Blob(data) => {
            write!(w, "{}", Blob(data))?;
        }
    }

    Ok(())
}

/// Perform quoting as per sqlite algorithm.
/// from sqlite:
/// > Attempt to determine if identifier self.0 needs to be quoted, either
/// > because it contains non-alphanumeric characters, or because it is an
/// > SQLite keyword.  Be conservative in this estimate:  When in doubt assume
/// > that quoting is required.
///
/// > Return '"' if quoting is required.  Return 0 if no quoting is required.
struct Quoted<'a>(&'a str);

impl Display for Quoted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = &self.0;
        let Some(first) = s.chars().next() else {
            write!(f, "{s}")?;
            return Ok(());
        };
        if !first.is_alphabetic() && first != '_' {
            write!(f, r#""{s}""#)?;
            return Ok(());
        }

        for c in s.chars() {
            if !c.is_alphanumeric() && c != '_' {
                write!(f, r#""{s}""#)?;
                return Ok(());
            }
        }

        unsafe {
            if sqlite3_keyword_check(s.as_ptr() as _, s.len() as _) != 0 {
                write!(f, r#""{s}""#)?;
                Ok(())
            } else {
                write!(f, "{s}")?;
                Ok(())
            }
        }
    }
}

struct Escaped<'a>(&'a str);

impl Display for Escaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let i = self
            .0
            .chars()
            .take_while(|c| !['\'', '\n', '\r'].contains(c))
            .count();
        if i == self.0.chars().count() {
            // nothing to escape
            write!(f, "'{}'", self.0)?;
        } else {
            let (num_nl, num_cr) = self.0.chars().fold((0, 0), |(nnl, ncl), c| {
                if c == '\n' {
                    (nnl + 1, ncl)
                } else if c == '\r' {
                    (nnl, ncl + 1)
                } else {
                    (nnl, ncl)
                }
            });

            let mut num_nl_replace = None;
            if num_nl != 0 {
                write!(f, "replace(")?;
                num_nl_replace = Some(find_unused_str(self.0, "\\n", "\\012"));
            }

            let mut num_cr_replace = None;
            if num_cr != 0 {
                write!(f, "replace(")?;
                num_cr_replace = Some(find_unused_str(self.0, "\\r", "\\015"));
            }

            write!(f, "'")?;

            let mut s = self.0;
            while !s.is_empty() {
                let mut chars = s.chars();
                chars
                    .by_ref()
                    .take_while(|c| !['\'', '\n', '\r'].contains(c))
                    .last();
                let remaining = chars.as_str();
                let start_len = s.len() - remaining.len();
                let start = &s[..start_len];
                let mut start_chars = start.chars();
                match start_chars.next_back() {
                    Some('\n') => {
                        write!(
                            f,
                            "{}{}",
                            start_chars.as_str(),
                            num_nl_replace.as_ref().unwrap()
                        )?;
                    }
                    Some('\r') => {
                        write!(
                            f,
                            "{}{}",
                            start_chars.as_str(),
                            num_cr_replace.as_ref().unwrap()
                        )?;
                    }
                    Some('\'') => {
                        write!(f, "{start}'")?;
                    }
                    Some(_) => {
                        write!(f, "{start}")?;
                    }
                    None => (),
                }

                s = remaining;
            }

            write!(f, "'")?;

            if let Some(token) = num_cr_replace {
                write!(f, ",'{token}',char(13))")?;
            }

            if let Some(token) = num_nl_replace {
                write!(f, ",'{token}',char(10))")?;
            }
        }

        Ok(())
    }
}

struct Blob<'a>(&'a [u8]);

impl Display for Blob<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "X'")?;
        const ALPHABET: &[u8] = b"0123456789abcdef";
        for byte in self.0 {
            let s = [
                ALPHABET[(*byte as usize >> 4) & 15],
                ALPHABET[(*byte as usize) & 15],
            ];

            write!(
                f,
                "{}",
                std::str::from_utf8(&s).map_err(|_| std::fmt::Error)?
            )?;
        }

        write!(f, "'")?;

        Ok(())
    }
}

fn find_unused_str(haystack: &str, needle1: &str, needle2: &str) -> String {
    if !haystack.contains(needle1) {
        return needle1.to_string();
    }

    if !haystack.contains(needle2) {
        return needle2.to_string();
    }

    let mut i = 0;
    loop {
        let needle = format!("({needle1}{i})");
        if !haystack.contains(&needle) {
            return needle;
        }
        i += 1;
    }
}

pub fn export_dump(
    db: &mut rusqlite::Connection,
    writer: impl Write,
    preserve_rowids: bool,
) -> anyhow::Result<()> {
    let mut txn = db.transaction()?;
    txn.execute("PRAGMA writable_schema=ON", ())?;
    let savepoint = txn.savepoint_with_name("dump")?;
    let mut state = DumpState {
        writable_schema: false,
        writer,
    };

    writeln!(state.writer, "PRAGMA foreign_keys=OFF;")?;
    writeln!(state.writer, "BEGIN TRANSACTION;")?;

    // from sqlite:
    // > Set writable_schema=ON since doing so forces SQLite to initialize
    // > as much of the schema as it can even if the sqlite_schema table is
    // > corrupt.

    let q = "SELECT name, type, sql FROM sqlite_schema AS o 
WHERE type=='table' 
AND sql NOT NULL 
ORDER BY tbl_name='sqlite_sequence', rowid";
    state.run_schema_dump_query(&savepoint, q, preserve_rowids)?;

    let q = "SELECT sql FROM sqlite_schema AS o 
WHERE sql NOT NULL 
AND type IN ('index','trigger','view')";
    state.run_table_dump_query(&savepoint, q)?;

    if state.writable_schema {
        writeln!(state.writer, "PRAGMA writable_schema=OFF;")?;
    }

    writeln!(state.writer, "COMMIT;")?;

    let _ = savepoint.execute("PRAGMA writable_schema = OFF;", ());
    let _ = savepoint.finish();

    Ok(())
}

#[cfg(test)]
mod test {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn escape_formatter() {
        assert_eq!("'hello world'", Escaped("hello world").to_string());
        assert_eq!("'hello '' world'", Escaped("hello ' world").to_string());
        assert_eq!(
            "replace('hello\\nworld','\\n',char(10))",
            Escaped("hello\nworld").to_string()
        );
        assert_eq!(
            "replace('hello\\rworld','\\r',char(13))",
            Escaped("hello\rworld").to_string()
        );
        assert_eq!(
            "replace('hello\\n\\012world','\\012',char(10))",
            Escaped("hello\\n\nworld").to_string()
        );
    }

    #[test]
    fn blob_formatter() {
        assert_eq!("X'68656c6c6f0a'", Blob(b"hello\n").to_string());
        assert_eq!("X''", Blob(b"").to_string());
    }

    #[test]
    fn table_col_is_keyword() {
        let tmp = tempdir().unwrap();
        let mut conn = Connection::open(tmp.path().join("data")).unwrap();
        conn.execute(r#"create table test ("limit")"#, ()).unwrap();

        let mut out = Vec::new();
        export_dump(&mut conn, &mut out, false).unwrap();

        insta::assert_snapshot!(std::str::from_utf8(&out).unwrap());
    }

    #[test]
    fn table_preserve_rowids() {
        let tmp = tempdir().unwrap();
        let mut conn = Connection::open(tmp.path().join("data")).unwrap();
        conn.execute(r#"create table test ( id TEXT PRIMARY KEY )"#, ())
            .unwrap();
        conn.execute(r#"insert into test values ( 'a' ), ( 'b' ), ( 'c' )"#, ())
            .unwrap();
        conn.execute(r#"delete from test where id = 'a'"#, ())
            .unwrap();

        let mut out = Vec::new();
        export_dump(&mut conn, &mut out, true).unwrap();

        insta::assert_snapshot!(std::str::from_utf8(&out).unwrap());
    }

    #[test]
    fn virtual_table_sql_escaping() {
        let tmp = tempdir().unwrap();
        let mut conn = Connection::open(tmp.path().join("data")).unwrap();

        conn.execute(r#"CREATE VIRTUAL TABLE test_fts USING fts5(content)"#, ())
            .unwrap();

        conn.execute(
            r#"CREATE VIRTUAL TABLE test_vocab USING fts5vocab(test_fts, 'row')"#,
            (),
        )
        .unwrap();

        let mut out = Vec::new();
        export_dump(&mut conn, &mut out, false).unwrap();
        let dump_output = std::str::from_utf8(&out).unwrap();

        assert!(dump_output.contains("''row''"));
    }
}
