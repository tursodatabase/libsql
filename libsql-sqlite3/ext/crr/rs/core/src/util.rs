extern crate alloc;

use crate::{alloc::string::ToString, tableinfo::ColumnInfo};
use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::str::Utf8Error;
use sqlite::{sqlite3, ColumnType, Connection, ResultCode};
use sqlite_nostd as sqlite;

pub fn get_dflt_value(
    db: *mut sqlite3,
    table: &str,
    col: &str,
) -> Result<Option<String>, ResultCode> {
    let sql = "SELECT [dflt_value], [notnull] FROM pragma_table_info(?) WHERE name = ?";
    let stmt = db.prepare_v2(sql)?;
    stmt.bind_text(1, table, sqlite_nostd::Destructor::STATIC)?;
    stmt.bind_text(2, col, sqlite_nostd::Destructor::STATIC)?;
    let rc = stmt.step()?;
    if rc == ResultCode::DONE {
        // There should always be a row for a column in pragma_table_info
        return Err(ResultCode::DONE);
    }

    let notnull = stmt.column_int(1);
    let dflt_column_type = stmt.column_type(0)?;

    // if the column is nullable and no default value is specified
    // then the default value is null.
    if notnull == 0 && dflt_column_type == ColumnType::Null {
        return Ok(Some(String::from("NULL")));
    }

    if dflt_column_type == ColumnType::Null {
        // no default value specified
        // and the column is not nullable
        return Ok(None);
    }

    return Ok(Some(String::from(stmt.column_text(0)?)));
}

pub fn get_db_version_union_query(tbl_names: &Vec<String>) -> String {
    let unions_str = tbl_names
        .iter()
        .map(|tbl_name| {
            format!(
                "SELECT max(db_version) as version FROM \"{}\"",
                escape_ident(tbl_name),
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    return format!(
        "SELECT max(version) as version FROM ({} UNION SELECT value as
        version FROM crsql_master WHERE key = 'pre_compact_dbversion')",
        unions_str
    );
}

pub fn slab_rowid(idx: i32, rowid: sqlite::int64) -> sqlite::int64 {
    if idx < 0 {
        return -1;
    }

    let modulo = rowid % crate::consts::ROWID_SLAB_SIZE;
    return (idx as i64) * crate::consts::ROWID_SLAB_SIZE + modulo;
}

pub fn where_list(columns: &Vec<ColumnInfo>, prefix: Option<&str>) -> Result<String, Utf8Error> {
    let mut result = vec![];
    for c in columns {
        let name = &c.name;
        if let Some(prefix) = prefix {
            result.push(format!(
                "{prefix}\"{col_name}\" IS ?",
                prefix = prefix,
                col_name = crate::util::escape_ident(name)
            ));
        } else {
            result.push(format!(
                "\"{col_name}\" IS ?",
                col_name = crate::util::escape_ident(name)
            ));
        }
    }

    Ok(result.join(" AND "))
}

pub fn binding_list(num_slots: usize) -> String {
    core::iter::repeat('?')
        .take(num_slots)
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn as_identifier_list(
    columns: &Vec<ColumnInfo>,
    prefix: Option<&str>,
) -> Result<String, Utf8Error> {
    let mut result = vec![];
    for c in columns {
        result.push(if let Some(prefix) = prefix {
            format!("{}\"{}\"", prefix, crate::util::escape_ident(&c.name))
        } else {
            format!("\"{}\"", crate::util::escape_ident(&c.name))
        })
    }
    Ok(result.join(","))
}

pub fn escape_ident(ident: &str) -> String {
    return ident.replace("\"", "\"\"");
}

pub fn escape_ident_as_value(ident: &str) -> String {
    return ident.replace("'", "''");
}

pub trait Countable {
    fn count(self, sql: &str) -> Result<i32, ResultCode>;
}

impl Countable for *mut sqlite::sqlite3 {
    fn count(self, sql: &str) -> Result<i32, ResultCode> {
        let stmt = self.prepare_v2(sql)?;
        stmt.step()?;
        Ok(stmt.column_int(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slab_rowid() {
        let foo_slab = slab_rowid(0, 1);
        let bar_slab = slab_rowid(1, 2);
        let baz_slab = slab_rowid(2, 3);

        assert_eq!(foo_slab, 1);
        assert_eq!(bar_slab, 2 + crate::consts::ROWID_SLAB_SIZE);
        assert_eq!(baz_slab, 3 + crate::consts::ROWID_SLAB_SIZE * 2);
        assert_eq!(slab_rowid(0, crate::consts::ROWID_SLAB_SIZE), 0);
        assert_eq!(slab_rowid(0, crate::consts::ROWID_SLAB_SIZE + 1), 1);

        let foo_slab = slab_rowid(0, crate::consts::ROWID_SLAB_SIZE + 1);
        let bar_slab = slab_rowid(1, crate::consts::ROWID_SLAB_SIZE + 2);
        let baz_slab = slab_rowid(2, crate::consts::ROWID_SLAB_SIZE * 2 + 3);

        assert_eq!(foo_slab, 1);
        assert_eq!(bar_slab, 2 + crate::consts::ROWID_SLAB_SIZE);
        assert_eq!(baz_slab, 3 + crate::consts::ROWID_SLAB_SIZE * 2);
    }

    #[test]
    fn test_get_db_version_union_query() {
        let tbl_names = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let union = get_db_version_union_query(&tbl_names);
        assert_eq!(
            union,
            "SELECT max(version) as version FROM (SELECT max(db_version) as version FROM \"foo\" UNION ALL SELECT max(db_version) as version FROM \"bar\" UNION ALL SELECT max(db_version) as version FROM \"baz\" UNION SELECT value as\n        version FROM crsql_master WHERE key = 'pre_compact_dbversion')"
        );
    }
}
