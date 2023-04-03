use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};
extern crate alloc;
use alloc::format;

pub fn remove_crr_clock_table_if_exists(
    db: *mut sqlite::sqlite3,
    table: &str,
) -> Result<ResultCode, ResultCode> {
    let escaped_table = crate::escape_ident(table);
    db.exec_safe(&format!(
        "DROP TABLE IF EXISTS \"{table}__crsql_clock\"",
        table = escaped_table
    ))
}

pub fn remove_crr_triggers_if_exist(
    db: *mut sqlite::sqlite3,
    table: &str,
) -> Result<ResultCode, ResultCode> {
    let escaped_table = crate::escape_ident(table);

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_itrig\"",
        table = escaped_table
    ))?;

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_utrig\"",
        table = escaped_table
    ))?;

    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_dtrig\"",
        table = escaped_table
    ))
}
