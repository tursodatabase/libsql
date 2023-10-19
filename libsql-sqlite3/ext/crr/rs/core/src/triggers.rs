extern crate alloc;
use alloc::format;
use alloc::string::String;
use alloc::vec;
use sqlite::Connection;

use core::{
    ffi::{c_char, c_int, CStr},
    slice,
    str::Utf8Error,
};

use crate::c::crsql_TableInfo;
use sqlite::{sqlite3, ResultCode};
use sqlite_nostd as sqlite;

#[no_mangle]
pub extern "C" fn crsql_create_crr_triggers(
    db: *mut sqlite3,
    table_info: *mut crsql_TableInfo,
    err: *mut *mut c_char,
) -> c_int {
    match create_triggers(db, table_info, err) {
        Ok(code) => code as c_int,
        Err(code) => code as c_int,
    }
}

fn create_triggers(
    db: *mut sqlite3,
    table_info: *mut crsql_TableInfo,
    err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    create_insert_trigger(db, table_info, err)?;
    create_update_trigger(db, table_info, err)?;
    create_delete_trigger(db, table_info, err)
}

fn create_insert_trigger(
    db: *mut sqlite3,
    table_info: *mut crsql_TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let table_name = unsafe { CStr::from_ptr((*table_info).tblName).to_str()? };
    let pk_columns =
        unsafe { slice::from_raw_parts((*table_info).pks, (*table_info).pksLen as usize) };
    let pk_list = crate::util::as_identifier_list(pk_columns, None)?;
    let pk_new_list = crate::util::as_identifier_list(pk_columns, Some("NEW."))?;
    let pk_where_list = crate::util::pk_where_list(pk_columns, Some("NEW."))?;
    let trigger_body =
        insert_trigger_body(table_info, table_name, pk_list, pk_new_list, pk_where_list)?;

    let create_trigger_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_itrig\"
      AFTER INSERT ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
      BEGIN
        {trigger_body}
      END;",
        table_name = crate::util::escape_ident(table_name),
        trigger_body = trigger_body
    );

    db.exec_safe(&create_trigger_sql)
}

fn insert_trigger_body(
    table_info: *mut crsql_TableInfo,
    table_name: &str,
    pk_list: String,
    pk_new_list: String,
    pk_where_list: String,
) -> Result<String, Utf8Error> {
    let non_pk_columns =
        unsafe { slice::from_raw_parts((*table_info).nonPks, (*table_info).nonPksLen as usize) };
    let mut trigger_components = vec![];

    if non_pk_columns.len() == 0 {
        // a table that only has primary keys.
        // we'll need to record a create record in this case.
        trigger_components.push(format!(
          "INSERT INTO \"{table_name}__crsql_clock\" (
            {pk_list},
            __crsql_col_name,
            __crsql_col_version,
            __crsql_db_version,
            __crsql_seq,
            __crsql_site_id
            ) SELECT
            {pk_new_list},
            '{col_name}',
            1,
            crsql_next_db_version(),
            crsql_increment_and_get_seq(),
            NULL
          ON CONFLICT DO UPDATE SET
            __crsql_col_version = CASE __crsql_col_version % 2 WHEN 0 THEN __crsql_col_version + 1 ELSE __crsql_col_version + 2 END,
            __crsql_db_version = crsql_next_db_version(),
            __crsql_seq = crsql_get_seq() - 1,
            __crsql_site_id = NULL;",
          table_name = crate::util::escape_ident(table_name),
          pk_list = pk_list,
          pk_new_list = pk_new_list,
          col_name = crate::c::INSERT_SENTINEL
      ));
    } else {
        // only update the create record if it exists.
        // this is an optimization so as not to create create records
        // for things that don't strictly need them.
        trigger_components.push(format!(
          "UPDATE \"{table_name}__crsql_clock\" SET
            __crsql_col_version = CASE __crsql_col_version % 2 WHEN 0 THEN __crsql_col_version + 1 ELSE __crsql_col_version + 2 END,
            __crsql_db_version = crsql_next_db_version(),
            __crsql_seq = crsql_increment_and_get_seq(),
            __crsql_site_id = NULL
          WHERE {pk_where_list} AND __crsql_col_name = '{col_name}';",
          table_name = crate::util::escape_ident(table_name),
          pk_where_list = pk_where_list,
          col_name = crate::c::INSERT_SENTINEL
        ));
    }

    for col in non_pk_columns {
        let col_name = unsafe { CStr::from_ptr(col.name).to_str()? };
        trigger_components.push(format_insert_trigger_component(
            table_name,
            &pk_list,
            &pk_new_list,
            col_name,
        ))
    }

    Ok(trigger_components.join("\n"))
}

fn format_insert_trigger_component(
    table_name: &str,
    pk_list: &str,
    pk_new_list: &str,
    col_name: &str,
) -> String {
    format!(
        "INSERT INTO \"{table_name}__crsql_clock\" (
          {pk_list},
          __crsql_col_name,
          __crsql_col_version,
          __crsql_db_version,
          __crsql_seq,
          __crsql_site_id
        ) SELECT
          {pk_new_list},
          '{col_name}',
          1,
          crsql_next_db_version(),
          crsql_increment_and_get_seq(),
          NULL
        ON CONFLICT DO UPDATE SET
          __crsql_col_version = __crsql_col_version + 1,
          __crsql_db_version = crsql_next_db_version(),
          __crsql_seq = crsql_get_seq() - 1,
          __crsql_site_id = NULL;",
        table_name = crate::util::escape_ident(table_name),
        pk_list = pk_list,
        pk_new_list = pk_new_list,
        col_name = crate::util::escape_ident_as_value(col_name)
    )
}

fn create_update_trigger(
    db: *mut sqlite3,
    table_info: *mut crsql_TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let table_name = unsafe { CStr::from_ptr((*table_info).tblName).to_str()? };
    let pk_columns =
        unsafe { slice::from_raw_parts((*table_info).pks, (*table_info).pksLen as usize) };
    let pk_list = crate::util::as_identifier_list(pk_columns, None)?;
    let pk_new_list = crate::util::as_identifier_list(pk_columns, Some("NEW."))?;
    let pk_old_list = crate::util::as_identifier_list(pk_columns, Some("OLD."))?;
    let pk_where_list = crate::util::pk_where_list(pk_columns, Some("OLD."))?;
    let mut any_pk_differs = vec![];
    for c in pk_columns {
        let name = unsafe { CStr::from_ptr(c.name).to_str()? };
        any_pk_differs.push(format!(
            "NEW.\"{col_name}\" IS NOT OLD.\"{col_name}\"",
            col_name = crate::util::escape_ident(name)
        ));
    }
    let any_pk_differs = any_pk_differs.join(" OR ");

    // Changing a primary key to a new value is the same as deleting the row previously
    // identified by that primary key. TODO: share this code with `create_delete_trigger`
    for col in pk_columns {
        let col_name = unsafe { CStr::from_ptr(col.name).to_str()? };
        db.exec_safe(&format!(
            "CREATE TRIGGER IF NOT EXISTS \"{tbl_name}_{col_name}__crsql_utrig\"
          AFTER UPDATE OF \"{col_name}\" ON \"{tbl_name}\"
          WHEN crsql_internal_sync_bit() = 0 AND NEW.\"{col_name}\" IS NOT OLD.\"{col_name}\"
          BEGIN
            INSERT INTO \"{table_name}__crsql_clock\" (
              {pk_list},
              __crsql_col_name,
              __crsql_col_version,
              __crsql_db_version,
              __crsql_seq,
              __crsql_site_id
            ) SELECT
              {pk_old_list},
              '{sentinel}',
              2,
              crsql_next_db_version(),
              crsql_increment_and_get_seq(),
              NULL WHERE true
            ON CONFLICT DO UPDATE SET
              __crsql_col_version = 1 + __crsql_col_version,
              __crsql_db_version = crsql_next_db_version(),
              __crsql_seq = crsql_get_seq() - 1,
              __crsql_site_id = NULL;
            DELETE FROM \"{table_name}__crsql_clock\"
              WHERE {pk_where_list} AND __crsql_col_name != '{sentinel}';
          END;",
            tbl_name = crate::util::escape_ident(table_name),
            col_name = crate::util::escape_ident(col_name),
            pk_list = pk_list,
            pk_old_list = pk_old_list,
            sentinel = crate::c::DELETE_SENTINEL,
        ))?;
    }

    let trigger_body =
        update_trigger_body(table_info, table_name, pk_list, pk_new_list, any_pk_differs)?;

    let create_trigger_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_utrig\"
      AFTER UPDATE ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
      BEGIN
        {trigger_body}
      END;",
        table_name = crate::util::escape_ident(table_name),
        trigger_body = trigger_body
    );

    db.exec_safe(&create_trigger_sql)
}

fn update_trigger_body(
    table_info: *mut crsql_TableInfo,
    table_name: &str,
    pk_list: String,
    pk_new_list: String,
    any_pk_differs: String,
) -> Result<String, Utf8Error> {
    let non_pk_columns =
        unsafe { slice::from_raw_parts((*table_info).nonPks, (*table_info).nonPksLen as usize) };
    let mut trigger_components = vec![];

    // If any PK is different, record a create for the row
    // as setting a PK to a _new value_ is like insertion or creating a row.
    // If we have CL and we conflict.. and pk is not _dead_, ignore?
    trigger_components.push(format!(
        "INSERT INTO \"{table_name}__crsql_clock\" (
          {pk_list},
          __crsql_col_name,
          __crsql_col_version,
          __crsql_db_version,
          __crsql_seq,
          __crsql_site_id
        ) SELECT
          {pk_new_list},
          '{sentinel}',
          1,
          crsql_next_db_version(),
          crsql_increment_and_get_seq(),
          NULL
        WHERE {any_pk_differs}
        ON CONFLICT DO UPDATE SET
          __crsql_col_version = CASE __crsql_col_version % 2 WHEN 0 THEN __crsql_col_version + 1 ELSE __crsql_col_version + 2 END,
          __crsql_db_version = crsql_next_db_version(),
          __crsql_seq = crsql_get_seq() - 1,
          __crsql_site_id = NULL;",
        table_name = crate::util::escape_ident(table_name),
        pk_list = pk_list,
        pk_new_list = pk_new_list,
        sentinel = crate::c::INSERT_SENTINEL,
        any_pk_differs = any_pk_differs
    ));

    for col in non_pk_columns {
        let col_name = unsafe { CStr::from_ptr(col.name).to_str()? };
        trigger_components.push(format!(
            "INSERT INTO \"{table_name}__crsql_clock\" (
          {pk_list},
          __crsql_col_name,
          __crsql_col_version,
          __crsql_db_version,
          __crsql_seq,
          __crsql_site_id
        ) SELECT
          {pk_new_list},
          '{col_name_val}',
          1,
          crsql_next_db_version(),
          crsql_increment_and_get_seq(),
          NULL
        WHERE NEW.\"{col_name_ident}\" IS NOT OLD.\"{col_name_ident}\"
        ON CONFLICT DO UPDATE SET
          __crsql_col_version = __crsql_col_version + 1,
          __crsql_db_version = crsql_next_db_version(),
          __crsql_seq = crsql_get_seq() - 1,
          __crsql_site_id = NULL;",
            table_name = crate::util::escape_ident(table_name),
            pk_list = pk_list,
            pk_new_list = pk_new_list,
            col_name_val = crate::util::escape_ident_as_value(col_name),
            col_name_ident = crate::util::escape_ident(col_name)
        ))
    }

    Ok(trigger_components.join("\n"))
}

fn create_delete_trigger(
    db: *mut sqlite3,
    table_info: *mut crsql_TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let table_name = unsafe { CStr::from_ptr((*table_info).tblName).to_str()? };
    let pk_columns =
        unsafe { slice::from_raw_parts((*table_info).pks, (*table_info).pksLen as usize) };
    let pk_list = crate::util::as_identifier_list(pk_columns, None)?;
    let pk_old_list = crate::util::as_identifier_list(pk_columns, Some("OLD."))?;
    let pk_where_list = crate::util::pk_where_list(pk_columns, Some("OLD."))?;

    let create_trigger_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_dtrig\"
    AFTER DELETE ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
    BEGIN
      INSERT INTO \"{table_name}__crsql_clock\" (
        {pk_list},
        __crsql_col_name,
        __crsql_col_version,
        __crsql_db_version,
        __crsql_seq,
        __crsql_site_id
      ) SELECT
        {pk_old_list},
        '{sentinel}',
        2,
        crsql_next_db_version(),
        crsql_increment_and_get_seq(),
        NULL WHERE true
      ON CONFLICT DO UPDATE SET
        __crsql_col_version = 1 + __crsql_col_version,
        __crsql_db_version = crsql_next_db_version(),
        __crsql_seq = crsql_get_seq() - 1,
        __crsql_site_id = NULL;
      DELETE FROM \"{table_name}__crsql_clock\"
        WHERE {pk_where_list} AND __crsql_col_name != '{sentinel}';
    END;",
        table_name = crate::util::escape_ident(table_name),
        sentinel = crate::c::DELETE_SENTINEL,
        pk_where_list = pk_where_list,
        pk_old_list = pk_old_list
    );

    db.exec_safe(&create_trigger_sql)
}
