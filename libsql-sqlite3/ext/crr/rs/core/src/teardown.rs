use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};
extern crate alloc;
use alloc::format;

pub fn remove_crr_clock_table_if_exists(
    db: *mut sqlite::sqlite3,
    table: &str,
) -> Result<ResultCode, ResultCode> {
    let escaped_table = crate::util::escape_ident(table);
    db.exec_safe(&format!(
        "DROP TABLE IF EXISTS \"{table}__crsql_clock\"",
        table = escaped_table
    ))?;
    db.exec_safe(&format!(
        "DROP TABLE IF EXISTS \"{table}__crsql_pks\"",
        table = escaped_table
    ))
}

pub fn remove_crr_triggers_if_exist(
    db: *mut sqlite::sqlite3,
    table: &str,
) -> Result<ResultCode, ResultCode> {
    let escaped_table = crate::util::escape_ident(table);

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_itrig\"",
        table = escaped_table
    ))?;

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_utrig\"",
        table = escaped_table
    ))?;

    // get all columns of table
    // iterate pk cols
    // drop triggers against those pk cols
    let stmt = db.prepare_v2("SELECT name FROM pragma_table_info(?) WHERE pk > 0")?;
    stmt.bind_text(1, table, sqlite::Destructor::STATIC)?;
    while stmt.step()? == ResultCode::ROW {
        let col_name = stmt.column_text(0)?;
        db.exec_safe(&format!(
            "DROP TRIGGER IF EXISTS \"{tbl_name}_{col_name}__crsql_utrig\"",
            tbl_name = crate::util::escape_ident(table),
            col_name = crate::util::escape_ident(col_name),
        ))?;
    }

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_dtrig\"",
        table = escaped_table
    ))
}
