extern crate alloc;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use sqlite_nostd::Value;
use sqlite_nostd::{self, Connection, Destructor, ResultCode};

pub fn where_predicates<T: AsRef<str>>(columns: &[T]) -> Result<String, ResultCode> {
    let mut predicates = String::new();
    for (i, column_name) in columns.iter().enumerate() {
        predicates.push_str(&format!(
            "\"{}\" = NEW.\"{}\"",
            column_name.as_ref(),
            column_name.as_ref()
        ));
        if i < columns.len() - 1 {
            predicates.push_str(" AND ");
        }
    }
    if columns.len() == 0 {
        predicates.push_str("1");
    }
    Ok(predicates)
}

pub fn collection_min_select(
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<String, ResultCode> {
    Ok(format!(
        "SELECT MIN(\"{order_col}\") FROM \"{table}\" WHERE {list_preds} AND \"{order_col}\" != -1 AND \"{order_col}\" != 1",
        order_col = escape_ident(order_by_column.text()),
        table = escape_ident(table),
        list_preds = where_predicates(collection_columns)?
    ))
}

pub fn collection_max_select(
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<String, ResultCode> {
    Ok(format!(
        "SELECT MAX(\"{order_col}\") FROM \"{table}\" WHERE {list_preds} AND \"{order_col}\" != -1 AND \"{order_col}\" != 1",
        order_col = escape_ident(order_by_column.text()),
        table = escape_ident(table),
        list_preds = where_predicates(collection_columns)?
    ))
}

/// Stmt is returned to the caller since all values become invalid as soon as the
/// statement is dropped.
pub fn extract_pk_columns(
    db: *mut sqlite_nostd::sqlite3,
    table: &str,
) -> Result<Vec<String>, ResultCode> {
    let sql = "SELECT \"name\" FROM pragma_table_info(?) WHERE \"pk\" > 0 ORDER BY \"pk\" ASC";
    let stmt = db.prepare_v2(&sql)?;
    stmt.bind_text(1, table, Destructor::STATIC)?;
    let mut columns = Vec::new();
    while stmt.step()? == ResultCode::ROW {
        columns.push(String::from(stmt.column_text(0)?));
    }
    Ok(columns)
}

/// Stmt is returned to the caller since all values become invalid as soon as the
/// statement is dropped.
pub fn extract_columns(
    db: *mut sqlite_nostd::sqlite3,
    table: &str,
) -> Result<Vec<String>, ResultCode> {
    let sql = "SELECT \"name\" FROM pragma_table_info(?)";
    let stmt = db.prepare_v2(&sql)?;
    stmt.bind_text(1, table, Destructor::STATIC)?;
    let mut columns = Vec::new();
    while stmt.step()? == ResultCode::ROW {
        columns.push(String::from(stmt.column_text(0)?));
    }
    Ok(columns)
}

pub fn escape_ident(ident: &str) -> String {
    return ident.replace("\"", "\"\"");
}

/// You should not use this for anything except defining triggers.
pub fn escape_arg(arg: &str) -> String {
    return arg.replace("'", "''");
}
