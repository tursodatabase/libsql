use alloc::format;
use sqlite::Connection;
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

/**
* Given a table name, returns whether or not it has already
* been upgraded to a CRR.
*/
pub fn is_crr(db: *mut sqlite::sqlite3, table: &str) -> Result<bool, ResultCode> {
    let stmt =
        db.prepare_v2("SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?")?;
    stmt.bind_text(
        1,
        &format!("{}__crsql_itrig", table),
        sqlite::Destructor::TRANSIENT,
    )?;
    stmt.step()?;
    let count = stmt.column_int(0);

    if count == 0 {
        Ok(false)
    } else {
        Ok(true)
    }
}
