use sqlite_nostd::{sqlite3, Connection, Destructor, ManagedStmt, ResultCode};
extern crate alloc;
use crate::tableinfo::ColumnInfo;
use crate::util::get_dflt_value;
use alloc::format;
use alloc::string::String;
use alloc::{vec, vec::Vec};
use sqlite_nostd as sqlite;

/**
 * Backfills rows in a table with clock values.
 */
pub fn backfill_table(
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<ColumnInfo>,
    non_pk_cols: &Vec<ColumnInfo>,
    is_commit_alter: bool,
    no_tx: bool,
) -> Result<ResultCode, ResultCode> {
    if !no_tx {
        db.exec_safe("SAVEPOINT backfill")?;
    }

    let sql = format!(
        "SELECT {pk_cols} FROM \"{table}\" AS t1
        EXCEPT SELECT {pk_cols} FROM \"{table}__crsql_pks\" AS t2",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("\"{}\"", crate::util::escape_ident(&f.name)))
            .collect::<Vec<_>>()
            .join(", "),
    );
    let stmt = db.prepare_v2(&sql);

    let non_pk_cols_refs = non_pk_cols.iter().collect::<Vec<_>>();
    let result = match stmt {
        Ok(stmt) => create_clock_rows_from_stmt(
            stmt,
            db,
            table,
            pk_cols,
            &non_pk_cols_refs,
            is_commit_alter,
        ),
        Err(e) => Err(e),
    };

    if let Err(e) = result {
        if !no_tx {
            db.exec_safe("ROLLBACK")?;
        }

        return Err(e);
    }

    if let Err(e) = backfill_missing_columns(db, table, pk_cols, non_pk_cols, is_commit_alter) {
        if !no_tx {
            db.exec_safe("ROLLBACK")?;
        }

        return Err(e);
    }

    if !no_tx {
        db.exec_safe("RELEASE backfill")
    } else {
        Ok(ResultCode::OK)
    }
}

/**
* Given a statement that returns rows in the source table not present
* in the clock table, create those rows in the clock table.
*/
fn create_clock_rows_from_stmt(
    read_stmt: ManagedStmt,
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<ColumnInfo>,
    non_pk_cols: &Vec<&ColumnInfo>,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    let select_key = db.prepare_v2(&format!(
        "SELECT __crsql_key FROM \"{table}__crsql_pks\" WHERE {pk_where_conditions}",
        table = crate::util::escape_ident(table),
        pk_where_conditions = crate::util::where_list(pk_cols, None)?
    ))?;
    let create_key = db.prepare_v2(&format!(
        "INSERT INTO \"{table}__crsql_pks\" ({pk_cols}) VALUES ({pk_values}) RETURNING __crsql_key",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("\"{}\"", crate::util::escape_ident(&f.name)))
            .collect::<Vec<_>>()
            .join(", "),
        pk_values = pk_cols.iter().map(|_| "?").collect::<Vec<_>>().join(", "),
    ))?;
    // We do not grab nextdbversion on migration.
    // The idea is that other nodes will apply the same migration
    // in the future so if they have already seen this node up
    // to the current db version then the migration will place them into the correct
    // state. No need to re-sync post migration.
    // or-ignore since we do not drop sentinel values during compaction as they act as our metadata
    // to determine if rows should resurrect on a future insertion event provided by a peer.
    let sql = format!(
        "INSERT OR IGNORE INTO \"{table}__crsql_clock\"
          (key, col_name, col_version, db_version, seq) VALUES
          (?, ?, 1, {dbversion_getter}, crsql_increment_and_get_seq())",
        table = crate::util::escape_ident(table),
        dbversion_getter = if is_commit_alter {
            "crsql_db_version()"
        } else {
            "crsql_next_db_version()"
        }
    );
    let write_stmt = db.prepare_v2(&sql)?;

    while read_stmt.step()? == ResultCode::ROW {
        let key = get_or_create_key(&select_key, &create_key, pk_cols, &read_stmt)?;
        write_stmt.bind_int64(1, key)?;

        for col in non_pk_cols.iter() {
            // We even backfill default values since we can't differentiate between an explicit
            // reset to a default vs an implicit set to default on create. Do we? I don't think we do set defaults.
            write_stmt.bind_text(2, &col.name, Destructor::STATIC)?;
            write_stmt.step()?;
            write_stmt.reset()?;
        }
        if non_pk_cols.len() == 0 {
            write_stmt.bind_text(2, crate::c::INSERT_SENTINEL, Destructor::STATIC)?;
            write_stmt.step()?;
            write_stmt.reset()?;
        }
    }

    Ok(ResultCode::OK)
}

fn get_or_create_key(
    select_stmt: &ManagedStmt,
    create_stmt: &ManagedStmt,
    pk_cols: &Vec<ColumnInfo>,
    read_stmt: &ManagedStmt,
) -> Result<sqlite::int64, ResultCode> {
    for (i, _name) in pk_cols.iter().enumerate() {
        let value = read_stmt.column_value(i as i32)?;
        // TODO: ok to bind into to places at once?
        select_stmt.bind_value(i as i32 + 1, value)?;
        create_stmt.bind_value(i as i32 + 1, value)?;
    }

    if let Ok(ResultCode::ROW) = select_stmt.step() {
        let key = select_stmt.column_int64(0);
        create_stmt.clear_bindings()?;
        select_stmt.reset()?;
        return Ok(key);
    }
    select_stmt.reset()?;

    if let Ok(ResultCode::ROW) = create_stmt.step() {
        let key = create_stmt.column_int64(0);
        create_stmt.reset()?;
        return Ok(key);
    }
    create_stmt.reset()?;

    return Err(ResultCode::ERROR);
}

/**
* For each column, make sure there was a clock table entry.
* If not, fill the data in for it for each row.
*
* Can we optimize and skip cases where it is equivalent to the default value?
* E.g., adding a new column set to default values should not require a backfill...
*/
fn backfill_missing_columns(
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<ColumnInfo>,
    non_pk_cols: &Vec<ColumnInfo>,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    for non_pk_col in non_pk_cols {
        fill_column(db, table, pk_cols, &non_pk_col, is_commit_alter)?;
    }

    Ok(ResultCode::OK)
}

// This doesn't fill compeltely new columns...
// Wel... does it not? The on condition x left join should do it.
fn fill_column(
    db: *mut sqlite3,
    table: &str,
    pk_cols: &Vec<ColumnInfo>,
    non_pk_col: &ColumnInfo,
    is_commit_alter: bool,
) -> Result<ResultCode, ResultCode> {
    // Only fill rows for which
    // - a row does not exist for that pk combo _and_ the cid in the clock table.
    // - the value is not the default value for that column.
    let dflt_value = get_dflt_value(db, table, &non_pk_col.name)?;
    let sql = format!(
        "SELECT {pk_cols} FROM {table} as t1
          JOIN \"{table}__crsql_pks\" as t2 ON {pk_on_conditions}
          LEFT JOIN \"{table}__crsql_clock\" as t3 ON t3.key = t2.__crsql_key AND t3.col_name = ?
          WHERE t3.key IS NULL {dflt_value_condition}",
        table = crate::util::escape_ident(table),
        pk_cols = pk_cols
            .iter()
            .map(|f| format!("t1.\"{}\"", crate::util::escape_ident(&f.name)))
            .collect::<Vec<_>>()
            .join(", "),
        pk_on_conditions = pk_cols
            .iter()
            .map(|f| format!(
                "t1.\"{}\" = t2.\"{}\"",
                crate::util::escape_ident(&f.name),
                crate::util::escape_ident(&f.name)
            ))
            .collect::<Vec<_>>()
            .join(" AND "),
        dflt_value_condition = if let Some(dflt) = dflt_value {
            format!("AND t1.\"{}\" IS NOT {}", &non_pk_col.name, dflt)
        } else {
            String::from("")
        },
    );
    let read_stmt = db.prepare_v2(&sql)?;
    read_stmt.bind_text(1, &non_pk_col.name, Destructor::STATIC)?;

    // TODO: rm clone?
    let non_pk_cols = vec![non_pk_col];
    create_clock_rows_from_stmt(read_stmt, db, table, pk_cols, &non_pk_cols, is_commit_alter)
}
