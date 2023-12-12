extern crate crsql_bundle;
use sqlite::ColumnType;
use sqlite::Destructor;
use sqlite::ManagedConnection;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

fn sync_left_to_right(l: &dyn Connection, r: &dyn Connection, since: sqlite::int64) {
    let siteid_stmt = r.prepare_v2("SELECT crsql_site_id()").expect("prepared");
    siteid_stmt.step().expect("stepped");
    let siteid = siteid_stmt.column_blob(0).expect("got site id");

    let stmt_l = l
        .prepare_v2("SELECT * FROM crsql_changes WHERE db_version > ? AND site_id IS NOT ?")
        .expect("prepared select changes");
    stmt_l.bind_int64(1, since).expect("bound db version");
    stmt_l
        .bind_blob(2, siteid, Destructor::STATIC)
        .expect("bound site id");

    while stmt_l.step().expect("pulled change set") == ResultCode::ROW {
        let stmt_r = r
            .prepare_v2("INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .expect("prepared insert changes");
        for x in 0..9 {
            stmt_r
                .bind_value(x + 1, stmt_l.column_value(x).expect("got changeset value"))
                .expect("bound value");
        }
        stmt_r.step().expect("inserted change");
    }
}

// fn print_changes(
//     db: &dyn Connection,
//     for_db: Option<&dyn Connection>,
// ) -> Result<ResultCode, ResultCode> {
//     let stmt = if let Some(for_db) = for_db {
//         let siteid_stmt = for_db.prepare_v2("SELECT crsql_site_id()")?;
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

fn setup_schema(db: &ManagedConnection) {
    db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL);")
        .expect("created foo");
    db.exec_safe("SELECT crsql_as_crr('foo');")
        .expect("converted to crr");
}

fn create_pkonlytable() -> Result<(), ResultCode> {
    let db_a = crate::opendb()?;

    setup_schema(&db_a.db);
    Ok(())
}

fn insert_pkonly_row() {
    let db_a = crate::opendb().expect("open db a");
    let db_b = crate::opendb().expect("open db b");

    fn setup_schema(db: &ManagedConnection) {
        db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL);")
            .expect("created foo");
        db.exec_safe("SELECT crsql_as_crr('foo');")
            .expect("upgraded to crr");
    }

    setup_schema(&db_a.db);
    setup_schema(&db_b.db);

    let stmt = db_a
        .db
        .prepare_v2("INSERT INTO foo (id) VALUES (?);")
        .expect("prepared insert to foo");
    stmt.bind_int(1, 1).expect("bound values");
    stmt.step().expect("inserted");

    let stmt = db_a
        .db
        .prepare_v2("SELECT * FROM crsql_changes;")
        .expect("prepared select changes");
    let result = stmt.step().expect("stepped");
    assert_eq!(result, ResultCode::ROW);

    sync_left_to_right(&db_a.db, &db_b.db, -1);

    let stmt = db_b
        .db
        .prepare_v2("SELECT * FROM foo;")
        .expect("prepared select foo");
    let result = stmt.step().expect("stepped");
    assert_eq!(result, ResultCode::ROW);
    let id = stmt.column_int(0);
    assert_eq!(id, 1);
    let result = stmt.step().expect("stepped");
    assert_eq!(result, ResultCode::DONE);
}

fn modify_pkonly_row() -> Result<(), ResultCode> {
    let db_a = crate::opendb().expect("open db a");
    let db_b = crate::opendb().expect("open db b");

    fn setup_schema(db: &ManagedConnection) {
        db.exec_safe("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL);")
            .expect("create foo");
        db.exec_safe("SELECT crsql_as_crr('foo');")
            .expect("upgrade foo");
    }

    setup_schema(&db_a.db);
    setup_schema(&db_b.db);

    let stmt = db_a
        .db
        .prepare_v2("INSERT INTO foo (id) VALUES (1);")
        .expect("prepare insert to foo");
    stmt.step().expect("step insert to foo");

    let stmt = db_a
        .db
        .prepare_v2("UPDATE foo SET id = 2 WHERE id = 1;")
        .expect("prepare set to foo");
    stmt.step().expect("step update to foo");

    sync_left_to_right(&db_a.db, &db_b.db, -1);

    let stmt = db_b
        .db
        .prepare_v2("SELECT * FROM foo;")
        .expect("prepare select all from foo");
    let result = stmt.step().expect("step select all from foo");
    assert_eq!(result, ResultCode::ROW);
    let id = stmt.column_int(0);
    assert_eq!(id, 2);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);

    Ok(())
}

// Current issue with this test is that we're not recording the actual
// delete event on update of primary key. We're creating a synthetic one
// on read from `changes` when the target row is missing.
fn junction_table() -> Result<(), ResultCode> {
    let db_a = crate::opendb()?;
    let db_b = crate::opendb()?;

    fn setup_schema(db: &ManagedConnection) -> Result<ResultCode, ResultCode> {
        db.exec_safe("CREATE TABLE jx (id1 NOT NULL, id2 NOT NULL, PRIMARY KEY(id1, id2));")?;
        db.exec_safe("SELECT crsql_as_crr('jx');")
    }

    setup_schema(&db_a.db).expect("created schema");
    setup_schema(&db_b.db).expect("created schema");

    db_a.db
        .prepare_v2("INSERT INTO jx VALUES (1, 2);")?
        .step()?;
    db_a.db
        .prepare_v2("UPDATE jx SET id2 = 3 WHERE id1 = 1 AND id2 = 2")?
        .step()?;

    sync_left_to_right(&db_a.db, &db_b.db, -1);
    let stmt = db_b.db.prepare_v2("SELECT * FROM jx;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id1 = stmt.column_int(0);
    let id2 = stmt.column_int(1);
    assert_eq!(id1, 1);
    assert_eq!(id2, 3);
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::DONE);

    db_b.db
        .prepare_v2("UPDATE jx SET id1 = 2 WHERE id1 = 1 AND id2 = 3")?
        .step()?;

    sync_left_to_right(&db_b.db, &db_a.db, -1);

    let stmt = db_a.db.prepare_v2("SELECT * FROM jx;")?;
    let result = stmt.step()?;
    assert_eq!(result, ResultCode::ROW);
    let id1 = stmt.column_int(0);
    let id2 = stmt.column_int(1);
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

// https://discord.com/channels/989870439897653248/989870440585494530/1081084118680485938
fn discord_report_1() -> Result<(), ResultCode> {
    let db_a = crate::opendb()?;
    db_a.db
        .exec_safe("CREATE TABLE IF NOT EXISTS data (id NUMBER PRIMARY KEY NOT NULL);")?;
    db_a.db.exec_safe("SELECT crsql_as_crr('data')")?;
    db_a.db
        .exec_safe("INSERT INTO data VALUES (42) ON CONFLICT DO NOTHING;")?;

    let stmt = db_a.db.prepare_v2("SELECT * FROM crsql_changes")?;

    assert_eq!(stmt.step()?, ResultCode::ROW);

    let table = stmt.column_text(0)?;
    assert_eq!(table, "data");
    let pk_val = stmt.column_blob(1)?;
    assert_eq!(pk_val, [0x01, 0x09, 0x2A]);
    let cid = stmt.column_text(2)?;
    assert_eq!(cid, "-1");
    let val_type = stmt.column_type(3)?;
    assert_eq!(val_type, ColumnType::Null);
    let col_version = stmt.column_int64(4);
    assert_eq!(col_version, 1);
    let db_version = stmt.column_int64(5);
    assert_eq!(db_version, 1);

    assert_eq!(stmt.step()?, ResultCode::DONE);

    Ok(())
}

pub fn run_suite() {
    create_pkonlytable().expect("created pk only table");
    insert_pkonly_row();
    modify_pkonly_row().expect("modified pk only row");
    // TODO: get this test working.
    // junction_table()?;
    discord_report_1().expect("ran discord report");
}
