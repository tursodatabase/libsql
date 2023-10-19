use sqlite_nostd::{context, sqlite3, Connection, Context, ResultCode, Value};
extern crate alloc;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use crate::{
    key_between,
    util::{escape_arg, escape_ident, extract_columns, extract_pk_columns, where_predicates},
};

pub fn create_fract_view_and_view_triggers(
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<ResultCode, ResultCode> {
    // extract pk information from pragma table_info
    let pks = extract_pk_columns(db, table)?;

    let after_pk_defs = pks
        .iter()
        .map(|pk| format!("NULL AS \"after_{}\"", escape_ident(pk)))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "CREATE VIEW IF NOT EXISTS \"{table}_fractindex\" AS
        SELECT *, {after_pk_defs}
        FROM \"{table}\"",
        table = escape_ident(table),
        after_pk_defs = after_pk_defs
    );

    db.exec_safe(&sql)?;

    create_instead_of_insert_trigger(db, table, order_by_column, collection_columns)?;
    create_instead_of_update_trigger(db, table, order_by_column, collection_columns)?;

    Ok(ResultCode::OK)
}

fn create_instead_of_insert_trigger(
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<ResultCode, ResultCode> {
    let columns = extract_columns(db, table)?;
    let columns_ex_order = columns
        .iter()
        .filter(|col| col != &order_by_column.text())
        .collect::<Vec<_>>();

    let col_names_ex_order = columns_ex_order
        .iter()
        .map(|col| format!("\"{}\"", escape_ident(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let col_values_ex_order = columns_ex_order
        .iter()
        .map(|col| format!("NEW.\"{}\"", escape_ident(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let (after_pk_values, list_predicates, after_predicates, list_name_args, pk_names_args) =
        create_common_inputs(db, table, collection_columns)?;

    let sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table}_fractindex_insert_trig\"
        INSTEAD OF INSERT ON \"{table}_fractindex\"
        BEGIN
            INSERT INTO \"{table}\"
              ({col_names_ex_order}, \"{order_col}\")
            VALUES
              (
                {col_values_ex_order},
                CASE (
                  SELECT count(*) FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" = (SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates})
                )
                  WHEN 1 THEN crsql_fract_key_between(
                    (SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}),
                    (SELECT \"{order_col}\" FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" >
                      (SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates})
                    ORDER BY \"{order_col}\" ASC LIMIT 1)
                  )
                  WHEN 0 THEN -1
                  ELSE crsql_fract_fix_conflict_return_old_key(
                    '{table_arg}', '{order_col_arg}', {list_name_args}{maybe_comma} -1, {pk_names_args}, {after_pk_values}
                  )
                END
              );
        END;",
        table = escape_ident(table),
        col_names_ex_order = col_names_ex_order,
        col_values_ex_order = col_values_ex_order,
        order_col = escape_ident(order_by_column.text()),
        list_predicates = list_predicates,
        list_name_args = list_name_args,
        maybe_comma = if list_name_args.len() > 0 { ", " } else { "" },
        after_predicates = after_predicates,
        after_pk_values = after_pk_values,
        order_col_arg = escape_arg(order_by_column.text()),
        table_arg = escape_arg(table),
        pk_names_args = pk_names_args,
    );

    let stmt = db.prepare_v2(&sql)?;
    stmt.step()
}

fn create_instead_of_update_trigger(
    db: *mut sqlite3,
    table: &str,
    order_by_column: *mut sqlite_nostd::value,
    collection_columns: &Vec<&str>,
) -> Result<ResultCode, ResultCode> {
    let columns = extract_columns(db, table)?;
    let base_sets_ex_order = columns
        .iter()
        .filter(|col| col != &order_by_column.text())
        .map(|col| format!("\"{col}\" = NEW.\"{col}\"", col = col))
        .collect::<Vec<_>>()
        .join(",\n");

    let (after_pk_values, list_predicates, after_predicates, list_name_args, pk_names_args) =
        create_common_inputs(db, table, collection_columns)?;
    let pks = extract_pk_columns(db, table)?;

    let sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table}_fractindex_update_trig\"
      INSTEAD OF UPDATE ON \"{table}_fractindex\"
      BEGIN
        UPDATE \"{table}\" SET
          {base_sets_ex_order},
          \"{order_col}\" = CASE (
            SELECT count(*) FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" = (
              SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}
            )
          )
          WHEN 1 THEN crsql_fract_key_between(
            (SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}),
            (SELECT \"{order_col}\" FROM \"{table}\" WHERE {list_predicates} AND \"{order_col}\" > (
              SELECT \"{order_col}\" FROM \"{table}\" WHERE {after_predicates}
            ) ORDER BY \"{order_col}\" ASC LIMIT 1)
          )
          WHEN 0 THEN -1
          ELSE crsql_fract_fix_conflict_return_old_key(
            '{table_arg}', '{order_col_arg}', {list_name_args}{maybe_comma} -1, {pk_names_args}, {after_pk_values}
          )
          END
        WHERE {pk_predicates};
      END;",
        table = table,
        base_sets_ex_order = base_sets_ex_order,
        order_col = order_by_column.text(),
        list_predicates = list_predicates,
        after_predicates = after_predicates,
        maybe_comma = if list_name_args.len() > 0 { ", " } else { "" },
        after_pk_values = after_pk_values,
        order_col_arg = escape_arg(order_by_column.text()),
        table_arg = escape_arg(table),
        pk_names_args = pk_names_args,
        pk_predicates = where_predicates(&pks)?,
    );
    let stmt = db.prepare_v2(&sql)?;
    stmt.step()
}

fn create_common_inputs(
    db: *mut sqlite3,
    table: &str,
    collection_columns: &Vec<&str>,
) -> Result<(String, String, String, String, String), ResultCode> {
    let pks = extract_pk_columns(db, table)?;

    let after_pk_values = pks
        .iter()
        .map(|pk| format!("NEW.\"after_{}\"", escape_ident(pk)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_name_args = pks
        .iter()
        .map(|pk| format!("'{}'", escape_arg(pk)))
        .collect::<Vec<_>>()
        .join(", ");

    let list_predicates = where_predicates(collection_columns)?;

    let after_predicates = pks
        .iter()
        .map(|pk| format!("\"{pk}\" = NEW.\"after_{pk}\"", pk = escape_ident(pk)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let list_name_args = collection_columns
        .iter()
        .map(|c| format!("'{}'", escape_arg(c)))
        .collect::<Vec<_>>()
        .join(", ");

    Ok((
        after_pk_values,
        list_predicates,
        after_predicates,
        list_name_args,
        pk_name_args,
    ))
}

// TODO: rather than return old key we should return midpoint between old key and after point.
pub fn fix_conflict_return_old_key(
    ctx: *mut context,
    table: &str,
    order_col: *mut sqlite_nostd::value,
    collection_columns: &[*mut sqlite_nostd::value],
    pk_names: &[*mut sqlite_nostd::value],
    pk_values: &[*mut sqlite_nostd::value],
) -> Result<ResultCode, ResultCode> {
    let db = ctx.db_handle();
    let pk_predicates = pk_names
        .iter()
        .enumerate()
        .map(|(i, pk_name)| format!("\"{}\" = ?{}", escape_ident(pk_name.text()), i + 1))
        .collect::<Vec<_>>()
        .join(", AND");

    // Get the order of the row that we are inserting after.
    // This row had collisions.
    // This row needs to be moved down.
    // We'll use the order of this row for the order of the new row being inserted.
    let sql = format!(
        "SELECT \"{order_col}\" FROM \"{table}\" WHERE {pk_predicates}",
        order_col = escape_ident(order_col.text()),
        table = escape_ident(table),
        pk_predicates = pk_predicates
    );
    let stmt = db.prepare_v2(&sql)?;
    for (i, val) in pk_values.iter().enumerate() {
        stmt.bind_value(i as i32 + 1, *val)?;
    }
    let code = stmt.step()?;
    if code != ResultCode::ROW {
        // this should be impossible
        return Err(ResultCode::ERROR);
    }

    let target_order = stmt.column_value(0)?;

    let list_columns = collection_columns
        .iter()
        .map(|c| format!("\"{}\"", escape_ident(c.text())))
        .collect::<Vec<_>>()
        .join(", ");
    let list_join_predicates = collection_columns
        .iter()
        .map(|col| {
            format!(
                "\"{table}\".\"{col}\" = t.\"{col}\"",
                table = escape_ident(table),
                col = escape_ident(col.text())
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    // could do returning `order_col` and calculate new midpoint after
    let sql = format!(
        "UPDATE \"{table}\" SET \"{order_col}\" = crsql_fract_key_between(
        (
          SELECT \"{order_col}\" FROM \"{table}\"
          {maybe_join} WHERE \"{order_col}\" < ?{target_order_slot} ORDER BY \"{order_col}\" DESC LIMIT 1
        ),
        ?{target_order_slot}
      ) WHERE {pk_predicates} RETURNING \"{order_col}\"",
        table = escape_ident(table),
        order_col = escape_ident(order_col.text()),
        pk_predicates = pk_predicates,
        target_order_slot = pk_values.len() + 1,
        maybe_join = if list_columns.len() > 0 {
          format!(
            "JOIN (SELECT {list_columns} FROM \"{table}\" WHERE {pk_predicates}) as t
            ON {list_join_predicates}",
            list_columns = list_columns, pk_predicates = pk_predicates, table = escape_ident(table), list_join_predicates = list_join_predicates)
        } else {
          format!("")
        }
    );

    let stmt = db.prepare_v2(&sql)?;
    // bind pk_predicates
    for (i, val) in pk_values.iter().enumerate() {
        stmt.bind_value(i as i32 + 1, *val)?;
    }
    // bind target_order
    stmt.bind_value(pk_values.len() as i32 + 1, target_order)?;
    stmt.step()?;
    let new_target_order = stmt.column_text(0)?;
    let ret = key_between(Some(new_target_order), Some(target_order.text()));

    if let Ok(Some(ret)) = ret {
        ctx.result_text_transient(&ret);
        Ok(ResultCode::OK)
    } else {
        Err(ResultCode::ERROR)
    }
}
