extern crate alloc;

use crate::alloc::string::ToString;
use crate::c::crsql_ColumnInfo;
use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::{ffi::CStr, str::Utf8Error};
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

    let notnull = stmt.column_int(1)?;
    let dflt_column_type = stmt.column_type(0)?;

    // if the column is nullable and no default value is specified
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

pub fn slab_rowid(idx: i32, rowid: sqlite::int64) -> sqlite::int64 {
    if idx < 0 {
        return -1;
    }

    let modulo = rowid % crate::consts::ROWID_SLAB_SIZE;
    return (idx as i64) * crate::consts::ROWID_SLAB_SIZE + modulo;
}

pub fn pk_where_list(
    columns: &[crsql_ColumnInfo],
    rhs_prefix: Option<&str>,
) -> Result<String, Utf8Error> {
    let mut result = vec![];
    for c in columns {
        let name = unsafe { CStr::from_ptr(c.name) };
        result.push(if let Some(prefix) = rhs_prefix {
            format!(
                "\"{col_name}\" IS {prefix}\"{col_name}\"",
                prefix = prefix,
                col_name = crate::util::escape_ident(name.to_str()?)
            )
        } else {
            format!(
                "\"{col_name}\" = \"{col_name}\"",
                col_name = crate::util::escape_ident(name.to_str()?)
            )
        })
    }
    Ok(result.join(" AND "))
}

pub fn where_list(columns: &[crsql_ColumnInfo], prefix: Option<&str>) -> Result<String, Utf8Error> {
    let mut result = vec![];
    for c in columns {
        let name = unsafe { CStr::from_ptr(c.name) };
        if let Some(prefix) = prefix {
            result.push(format!(
                "{prefix}\"{col_name}\" IS ?",
                prefix = prefix,
                col_name = crate::util::escape_ident(name.to_str()?)
            ));
        } else {
            result.push(format!(
                "\"{col_name}\" IS ?",
                col_name = crate::util::escape_ident(name.to_str()?)
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
    columns: &[crsql_ColumnInfo],
    prefix: Option<&str>,
) -> Result<String, Utf8Error> {
    let mut result = vec![];
    for c in columns {
        let name = unsafe { CStr::from_ptr(c.name) };
        result.push(if let Some(prefix) = prefix {
            format!(
                "{}\"{}\"",
                prefix,
                crate::util::escape_ident(name.to_str()?)
            )
        } else {
            format!("\"{}\"", crate::util::escape_ident(name.to_str()?))
        })
    }
    Ok(result.join(","))
}

pub fn map_columns<F>(columns: &[crsql_ColumnInfo], map: F) -> Result<Vec<String>, Utf8Error>
where
    F: Fn(&str) -> String,
{
    let mut result = vec![];
    for c in columns {
        let name = unsafe { CStr::from_ptr(c.name) };
        result.push(map(name.to_str()?))
    }

    return Ok(result);
}

pub fn escape_ident(ident: &str) -> String {
    return ident.replace("\"", "\"\"");
}

pub fn escape_ident_as_value(ident: &str) -> String {
    return ident.replace("'", "''");
}
