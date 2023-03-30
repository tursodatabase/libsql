use sqlite::ColumnType;
use sqlite::Destructor;
use sqlite::ManagedConnection;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

integration_utils::counter_setup!(2);

#[test]
fn create_pkonlytable() {
    // just expecting not to throw
    create_pkonlytable_impl().unwrap();
    decrement_counter();
}

#[test]
fn insert_pkonly_row() {
    insert_pkonly_row_impl().unwrap();
    decrement_counter();
}

#[test]
fn modify_pkonly_row() {
    // inserts then updates then syncs the value of a pk column
    // inserts, syncs, then updates then syncs
    //
    // repeat for single column keys and compound
    // modify_pkonly_row_impl().unwrap()
}

#[test]
/// Test a common configuration of a junction/edge table (with no edge data)
/// to relate two relations.
fn junction_table() {
    // junction_table_impl().unwrap();
}

// https://discord.com/channels/989870439897653248/989870440585494530/1081084118680485938
#[test]
fn dicord_report_1() {
    discord_report_1_impl().unwrap();
}

fn sync_left_to_right(
    l: &dyn Connection,
    r: &dyn Connection,
    since: sqlite::int64,
) -> Result<ResultCode, ResultCode> {
    let siteid_stmt = r.prepare_v2("SELECT crsql_siteid()")?;
    siteid_stmt.step()?;
    let siteid = siteid_stmt.column_blob(0)?;

    let stmt_l =
        l.prepare_v2("SELECT * FROM crsql_changes WHERE db_version > ? AND site_id IS NOT ?")?;
    stmt_l.bind_int64(1, since)?;
    stmt_l.bind_blob(2, siteid, Destructor::STATIC)?;

    while stmt_l.step()? == ResultCode::ROW {
        let stmt_r = r.prepare_v2("INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?)")?;
        for x in 0..7 {
            stmt_r.bind_value(x + 1, stmt_l.column_value(x)?)?;
        }
        stmt_r.step()?;
    }
    Ok(ResultCode::OK)
}

// fn print_changes(
//     db: &dyn Connection,
//     for_db: Option<&dyn Connection>,
// ) -> Result<ResultCode, ResultCode> {
//     let stmt = if let Some(for_db) = for_db {
//         let siteid_stmt = for_db.prepare_v2("SELECT crsql_siteid()")?;
//         siteid_stmt.step()?;
//         let siteid = siteid_stmt.column_blob(0)?;
//         let stmt = db.prepare_v2(
//           "SELECT [table], [pk], [cid], [val], [col_version], [db_version], quote([site_id]) FROM crsql_changes WHERE site_id IS NOT ?",
//         )?;
//         stmt.bind_blob(1, siteid, Destructor::STATIC)?;
//         stmt
//     } else {
//         db.prepare_v2(
//           "SELECT [table], [pk], [cid], [val], [col_version], [db_version], quote([site_id]) FROM crsql_changes",
//         )?
//     };

//     while stmt.step()? == ResultCode::ROW {
//         println!(
//             "{}, {}, {}, {}, {}, {}, {}",
//             stmt.column_text(0)?,
//             stmt.column_text(1)?,
//             stmt.column_text(2)?,
//             if stmt.column_type(3)? == ColumnType::Null {
//                 ""
//             } else {
//                 stmt.column_text(3)?
//             },
//             stmt.column_int64(4)?,
//             stmt.column_int64(5)?,
//             stmt.column_text(6)?,
//         );
//     }
//     Ok(sqlite::ResultCode::OK)
// }

fn setup_schema(db: &ManagedConnection) -> Result<ResultCode, ResultCode> {
    db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY);")?;
    db.exec_safe("SELECT crsql_as_crr('foo');")
}

fn create_pkonlytable_impl() -> Result<(), ResultCode> {
    let db_a = integration_utils::opendb()?;

    setup_schema(&db_a.db)?;
    Ok(())
}

fn insert_pkonly_row_impl() -> Result<(), ResultCode> {
    let db_a = integration_utils::opendb()?;
    let db_b = integration_utils::opendb()?;

    fn setup_schema(db: &ManagedConnection) -> Result<ResultCode, ResultCode> {
        db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY);")?;
        db.exec_safe("SELECT crsql_as_crr('foo');")
    }

    setup_schema(&db_a.db)?;
    setup_schema(&db_b.db)?;

    let stmt = db_a.db.prepare_v2("INSERT INTO foo (id) VALUES (?);")?;
    stmt.bind_int(1, 1)?;
    stmt.step()?;

    let stmt = db_a.db.prepare_v2("SELECT * FROM crsql_changes;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);

    sync_left_to_right(&db_a.db, &db_b.db, -1)?;

    let stmt = db_b.db.prepare_v2("SELECT * FROM foo;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id = stmt.column_int(0)?;
    assert_eq!(id, 1);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);
    Ok(())
}

fn modify_pkonly_row_impl() -> Result<(), ResultCode> {
    let db_a = integration_utils::opendb()?;
    let db_b = integration_utils::opendb()?;

    fn setup_schema(db: &ManagedConnection) -> Result<ResultCode, ResultCode> {
        db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY);")?;
        db.exec_safe("SELECT crsql_as_crr('foo');")
    }

    setup_schema(&db_a.db)?;
    setup_schema(&db_b.db)?;

    let stmt = db_a.db.prepare_v2("INSERT INTO foo (id) VALUES (1);")?;
    stmt.step()?;

    let stmt = db_a.db.prepare_v2("UPDATE foo SET id = 2 WHERE id = 1;")?;
    stmt.step()?;

    sync_left_to_right(&db_a.db, &db_b.db, -1)?;

    let stmt = db_b.db.prepare_v2("SELECT * FROM foo;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id = stmt.column_int(0)?;
    assert_eq!(id, 2);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);

    Ok(())
}

// Current issue with this test is that we're not recording the actual
// delete event on update of primary key. We're creating a synthetic one
// on read from `changes` when the target row is missing.
fn junction_table_impl() -> Result<(), ResultCode> {
    let db_a = integration_utils::opendb()?;
    let db_b = integration_utils::opendb()?;

    fn setup_schema(db: &ManagedConnection) -> Result<ResultCode, ResultCode> {
        db.exec_safe("CREATE TABLE jx (id1, id2, PRIMARY KEY(id1, id2));")?;
        db.exec_safe("SELECT crsql_as_crr('jx');")
    }

    setup_schema(&db_a.db)?;
    setup_schema(&db_b.db)?;

    db_a.db
        .prepare_v2("INSERT INTO jx VALUES (1, 2);")?
        .step()?;
    db_a.db
        .prepare_v2("UPDATE jx SET id2 = 3 WHERE id1 = 1 AND id2 = 2")?
        .step()?;

    sync_left_to_right(&db_a.db, &db_b.db, -1)?;
    let stmt = db_b.db.prepare_v2("SELECT * FROM jx;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id1 = stmt.column_int(0)?;
    let id2 = stmt.column_int(1)?;
    assert_eq!(id1, 1);
    assert_eq!(id2, 3);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);

    db_b.db
        .prepare_v2("UPDATE jx SET id1 = 2 WHERE id1 = 1 AND id2 = 3")?
        .step()?;

    println!("A before sync");
    // print_changes(&db_a, None)?;

    sync_left_to_right(&db_b.db, &db_a.db, -1)?;

    println!("B");
    // print_changes(&db_b, None)?;
    println!("A after sync");
    // print_changes(&db_a, None)?;

    let stmt = db_a.db.prepare_v2("SELECT * FROM jx;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id1 = stmt.column_int(0)?;
    let id2 = stmt.column_int(1)?;
    assert_eq!(id1, 1);
    assert_eq!(id2, 3);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);

    // insert an edge
    // check it
    // modify the edge to point to something new
    // check it
    // change source of edge
    // check it
    // delete the edge
    // check it
    Ok(())
}

fn discord_report_1_impl() -> Result<(), ResultCode> {
    let db_a = integration_utils::opendb()?;
    db_a.db
        .exec_safe("CREATE TABLE IF NOT EXISTS data (id NUMBER PRIMARY KEY);")?;
    db_a.db.exec_safe("SELECT crsql_as_crr('data')")?;
    db_a.db
        .exec_safe("INSERT INTO data VALUES (42) ON CONFLICT DO NOTHING;")?;

    let stmt = db_a.db.prepare_v2("SELECT * FROM crsql_changes")?;

    assert_eq!(stmt.step()?, ResultCode::ROW);

    let table = stmt.column_text(0)?;
    assert_eq!(table, "data");
    let pk_val = stmt.column_text(1)?;
    assert_eq!(pk_val, "42");
    let cid = stmt.column_text(2)?;
    assert_eq!(cid, "__crsql_pko");
    let val_type = stmt.column_type(3)?;
    assert_eq!(val_type, ColumnType::Null);
    let col_version = stmt.column_int64(4)?;
    assert_eq!(col_version, 1);
    let db_version = stmt.column_int64(5)?;
    assert_eq!(db_version, 1);

    assert_eq!(stmt.step()?, ResultCode::DONE);

    Ok(())
}
