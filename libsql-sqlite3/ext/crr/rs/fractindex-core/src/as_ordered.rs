use sqlite_nostd::{context, sqlite3, Connection, Context, Destructor, ResultCode, Value};
extern crate alloc;
use alloc::format;
use alloc::vec::Vec;

use crate::{
    fractindex_view::create_fract_view_and_view_triggers,
    util::{collection_max_select, collection_min_select, escape_ident},
};

// TODO: do validation and suggest indices? collection and order should be indexed as compound index
// with col columns first.
pub fn as_ordered(
    context: *mut context,
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &[*mut sqlite_nostd::value],
) {
    // 0. we should drop all triggers and views if they already exist
    // or be fancy and track schema versions to know if this is needed.
    let rc = db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"__crsql_{table}_fractindex_pend_trig\";",
        table = escape_ident(table)
    ));
    if rc.is_err() {
        context.result_error("Failed dropping prior incarnation of fractindex triggers");
    }

    let rc = db.exec_safe(&format!(
        "DROP VIEW IF EXISTS \"{table}_fractindex\";",
        table = escape_ident(table)
    ));
    if rc.is_err() {
        context.result_error("Failed dropping prior incarnation of fractindex views");
    }

    // 1. ensure that all columns exist in the target table
    let mut provided_cols = collection_columns.to_vec();
    provided_cols.push(order_by_column);
    let rc = table_has_all_columns(db, table, &provided_cols);

    if rc.is_err() {
        context.result_error("Failed determining if all columns are present in the base table");
        return;
    }
    if let Ok(false) = rc {
        context.result_error("all columns are not present in the base table");
        return;
    }

    if let Err(_) = db.exec_safe("SAVEPOINT as_ordered;") {
        return;
    }

    let collection_column_names = collection_columns
        .iter()
        .map(|c| c.text())
        .collect::<Vec<_>>();
    // 2. set up triggers to allow for append and pre-pend insertions
    if let Err(_) = create_pend_trigger(db, table, order_by_column, &collection_column_names) {
        let _ = db.exec_safe("ROLLBACK;");
        context.result_error("Failed creating triggers for the base table");
        return;
    }

    if let Err(_) = create_simple_move_trigger(db, table, order_by_column, &collection_column_names)
    {
        let _ = db.exec_safe("ROLLBACK;");
        context.result_error("Failed creating simple move trigger");
        return;
    }

    // 4. create fract view for insert after and move operations
    if let Err(_) =
        create_fract_view_and_view_triggers(db, table, order_by_column, &collection_column_names)
    {
        let _ = db.exec_safe("ROLLBACK;");
        context.result_error("Failed creating view for the base table");
        return;
    }

    let _ = db.exec_safe("RELEASE as_ordered;");
}

fn table_has_all_columns(
    db: *mut sqlite3,
    table: &str,
    columns: &Vec<*mut sqlite_nostd::value>,
) -> Result<bool, ResultCode> {
    let bindings = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let sql = format!(
        "SELECT count(*) FROM pragma_table_info(?) WHERE \"name\" IN ({})",
        bindings
    );
    let stmt = db.prepare_v2(&sql)?;
    stmt.bind_text(1, table, Destructor::STATIC)?;
    for (i, col) in columns.iter().enumerate() {
        stmt.bind_value((i + 2) as i32, *col)?;
    }

    let step_code = stmt.step()?;
    if step_code == ResultCode::ROW {
        let count = stmt.column_int(0);
        if count != columns.len() as i32 {
            return Ok(false);
        }
    }

    Ok(true)
}

fn create_pend_trigger(
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<ResultCode, ResultCode> {
    let trigger = format!(
        "CREATE TRIGGER IF NOT EXISTS \"__crsql_{table}_fractindex_pend_trig\" AFTER INSERT ON \"{table}\"
        WHEN CAST(NEW.\"{order_by_column}\" AS INTEGER) = -1 OR CAST(NEW.\"{order_by_column}\" AS INTEGER) = 1 BEGIN
            UPDATE \"{table}\" SET \"{order_by_column}\" = CASE CAST(NEW.\"{order_by_column}\" AS INTEGER)
            WHEN -1 THEN crsql_fract_key_between(NULL, ({min_select}))
            WHEN 1 THEN crsql_fract_key_between(({max_select}), NULL)
            END
            WHERE _rowid_ = NEW._rowid_;
        END;",
        table = escape_ident(table),
        order_by_column = escape_ident(order_by_column.text()),
        min_select = collection_min_select(table, order_by_column, collection_columns)?,
        max_select = collection_max_select(table, order_by_column, collection_columns)?
    );
    db.exec_safe(&trigger)
}

fn create_simple_move_trigger(
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<ResultCode, ResultCode> {
    // simple move allows moving a thing to the start or end of the list
    let trigger = format!(
      "CREATE TRIGGER IF NOT EXISTS \"__crsql_{table}_fractindex_ezmove\" AFTER UPDATE OF \"{order_col}\" ON \"{table}\"
      WHEN NEW.\"{order_col}\" = -1 OR NEW.\"{order_col}\" = 1 BEGIN
        UPDATE \"{table}\" SET \"{order_col}\" = CASE NEW.\"{order_col}\"
        WHEN -1 THEN crsql_fract_key_between(NULL, ({min_select}))
        WHEN 1 THEN crsql_fract_key_between(({max_select}), NULL)
        END
        WHERE _rowid_ = NEW._rowid_;
      END;
      ",
      table = escape_ident(table),
      order_col = escape_ident(order_by_column.text()),
      min_select = collection_min_select(table, order_by_column, collection_columns)?,
      max_select = collection_max_select(table, order_by_column, collection_columns)?
    );
    db.exec_safe(&trigger)
}
