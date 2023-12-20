use core::ffi::{c_char, c_int};

use crate::{consts, tableinfo::TableInfo};
use alloc::{ffi::CString, format};
use core::slice;
use sqlite::{sqlite3, Connection, Destructor, ResultCode};
use sqlite_nostd as sqlite;

fn uuid() -> [u8; 16] {
    let mut blob: [u8; 16] = [0; 16];
    sqlite::randomness(&mut blob);
    blob[6] = (blob[6] & 0x0f) + 0x40;
    blob[8] = (blob[8] & 0x3f) + 0x80;
    blob
}

#[no_mangle]
pub extern "C" fn crsql_init_site_id(db: *mut sqlite3, ret: *mut u8) -> c_int {
    let buffer: &mut [u8] = unsafe { slice::from_raw_parts_mut(ret, 16) };
    if let Ok(site_id) = init_site_id(db) {
        buffer.copy_from_slice(&site_id);
        ResultCode::OK as c_int
    } else {
        ResultCode::ERROR as c_int
    }
}

fn insert_site_id(db: *mut sqlite3) -> Result<[u8; 16], ResultCode> {
    let stmt = db.prepare_v2(&format!(
        "INSERT INTO \"{tbl}\" (site_id, ordinal) VALUES (?, 0)",
        tbl = consts::TBL_SITE_ID
    ))?;

    let site_id = uuid();
    stmt.bind_blob(1, &site_id, Destructor::STATIC)?;
    stmt.step()?;

    Ok(site_id)
}

fn create_site_id_and_site_id_table(db: *mut sqlite3) -> Result<[u8; 16], ResultCode> {
    db.exec_safe(&format!(
        "CREATE TABLE \"{tbl}\" (site_id BLOB NOT NULL, ordinal INTEGER PRIMARY KEY);
        CREATE UNIQUE INDEX {tbl}_site_id ON \"{tbl}\" (site_id);",
        tbl = consts::TBL_SITE_ID
    ))?;

    insert_site_id(db)
}

#[no_mangle]
pub extern "C" fn crsql_init_peer_tracking_table(db: *mut sqlite3) -> c_int {
    match db.exec_safe("CREATE TABLE IF NOT EXISTS crsql_tracked_peers (\"site_id\" BLOB NOT NULL, \"version\" INTEGER NOT NULL, \"seq\" INTEGER DEFAULT 0, \"tag\" INTEGER, \"event\" INTEGER, PRIMARY KEY (\"site_id\", \"tag\", \"event\")) STRICT;") {
      Ok(_) => ResultCode::OK as c_int,
      Err(code) => code as c_int
    }
}

fn has_table(db: *mut sqlite3, table_name: &str) -> Result<bool, ResultCode> {
    let stmt =
        db.prepare_v2("SELECT 1 FROM sqlite_master WHERE type = 'table' AND tbl_name = ?")?;
    stmt.bind_text(1, table_name, Destructor::STATIC)?;
    let tbl_exists_result = stmt.step()?;
    Ok(tbl_exists_result == ResultCode::ROW)
}

/**
 * Loads the siteId into memory. If a site id
 * cannot be found for the given database one is created
 * and saved to the site id table.
 */
fn init_site_id(db: *mut sqlite3) -> Result<[u8; 16], ResultCode> {
    if !has_table(db, consts::TBL_SITE_ID)? {
        return create_site_id_and_site_id_table(db);
    }

    let stmt = db.prepare_v2(&format!(
        "SELECT site_id FROM \"{}\" WHERE ordinal = 0",
        consts::TBL_SITE_ID
    ))?;
    let result_code = stmt.step()?;

    let ret = if result_code == ResultCode::DONE {
        insert_site_id(db)?
    } else {
        let site_id_from_table = stmt.column_blob(0)?;
        site_id_from_table.try_into()?
    };

    Ok(ret)
}

fn crsql_create_schema_table_if_not_exists(db: *mut sqlite3) -> Result<ResultCode, ResultCode> {
    db.exec_safe("SAVEPOINT crsql_create_schema_table;")?;

    if let Ok(_) = db.exec_safe(&format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\"key\" TEXT PRIMARY KEY, \"value\" ANY);",
        consts::TBL_SCHEMA
    )) {
        db.exec_safe("RELEASE crsql_create_schema_table;")
    } else {
        let _ = db.exec_safe("ROLLBACK");
        Err(ResultCode::ERROR)
    }
}

#[no_mangle]
pub extern "C" fn crsql_maybe_update_db(db: *mut sqlite3, err_msg: *mut *mut c_char) -> c_int {
    // No schema table? First time this DB has been opened with this extension.
    if let Ok(has_schema_table) = has_table(db, consts::TBL_SCHEMA) {
        if let Err(code) = crsql_create_schema_table_if_not_exists(db) {
            return code as c_int;
        }
        let r = db.exec_safe("SAVEPOINT crsql_maybe_update_db;");
        if let Err(code) = r {
            return code as c_int;
        }
        if let Ok(_) = maybe_update_db_inner(db, has_schema_table == false, err_msg) {
            let _ = db.exec_safe("RELEASE crsql_maybe_update_db;");
            return ResultCode::OK as c_int;
        } else {
            let _ = db.exec_safe("ROLLBACK;");
            return ResultCode::ERROR as c_int;
        }
    } else {
        return ResultCode::ERROR as c_int;
    }
}

fn maybe_update_db_inner(
    db: *mut sqlite3,
    is_blank_slate: bool,
    err_msg: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let mut recorded_version: i32 = 0;

    // Completely new DBs need no migrations.
    // We can set them to the current version.
    if is_blank_slate {
        recorded_version = consts::CRSQLITE_VERSION;
    } else {
        let stmt =
            db.prepare_v2("SELECT value FROM crsql_master WHERE key = 'crsqlite_version'")?;
        let step_result = stmt.step()?;
        if step_result == ResultCode::ROW {
            recorded_version = stmt.column_int(0);
        }
    }

    if recorded_version < consts::CRSQLITE_VERSION && !is_blank_slate {
        // todo: return an error message to the user that their version is
        // not supported
        let cstring = CString::new(format!("Opening a db created with cr-sqlite version {} is not supported. Upcoming release 0.15.0 is a breaking change.", recorded_version))?;
        unsafe {
            (*err_msg) = cstring.into_raw();
            return Err(ResultCode::ERROR);
        }
    }

    // if recorded_version < consts::CRSQLITE_VERSION_0_13_0 {
    //     update_to_0_13_0(db)?;
    // }

    // if recorded_version < consts::CRSQLITE_VERSION_0_15_0 {
    //     update_to_0_15_0(db)?;
    // }

    // write the db version if we migrated to a new one or we are a blank slate db
    if recorded_version < consts::CRSQLITE_VERSION || is_blank_slate {
        let stmt =
            db.prepare_v2("INSERT OR REPLACE INTO crsql_master VALUES ('crsqlite_version', ?)")?;
        stmt.bind_int(1, consts::CRSQLITE_VERSION)?;
        stmt.step()?;
    }

    Ok(ResultCode::OK)
}

/**
 * The clock table holds the versions for each column of a given row.
 *
 * These version are set to the dbversion at the time of the write to the
 * column.
 *
 * The dbversion is updated on transaction commit.
 * This allows us to find all columns written in the same transaction
 * albeit with caveats.
 *
 * The caveats being that two partiall overlapping transactions will
 * clobber the full transaction picture given we only keep latest
 * state and not a full causal history.
 *
 * @param tableInfo
 */
pub fn create_clock_table(
    db: *mut sqlite3,
    table_info: &TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let pk_list = crate::util::as_identifier_list(&table_info.pks, None)?;
    let table_name = &table_info.tbl_name;

    db.exec_safe(&format!(
        "CREATE TABLE IF NOT EXISTS \"{table_name}__crsql_clock\" (
      key INTEGER NOT NULL,
      col_name TEXT NOT NULL,
      col_version INTEGER NOT NULL,
      db_version INTEGER NOT NULL,
      site_id INTEGER NOT NULL DEFAULT 0,
      seq INTEGER NOT NULL,
      PRIMARY KEY (key, col_name)
    ) WITHOUT ROWID, STRICT",
        table_name = crate::util::escape_ident(table_name),
    ))?;

    db.exec_safe(
      &format!(
        "CREATE INDEX IF NOT EXISTS \"{table_name}__crsql_clock_dbv_idx\" ON \"{table_name}__crsql_clock\" (\"db_version\")",
        table_name = crate::util::escape_ident(table_name),
      ))?;
    db.exec_safe(
      &format!(
        "CREATE TABLE IF NOT EXISTS \"{table_name}__crsql_pks\" (__crsql_key INTEGER PRIMARY KEY, {pk_list})",
        table_name = table_name,
        pk_list = pk_list,
      )
    )?;
    db.exec_safe(
      &format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS \"{table_name}__crsql_pks_pks\" ON \"{table_name}__crsql_pks\" ({pk_list})",
        table_name = table_name,
        pk_list = pk_list
      )
    )
}
