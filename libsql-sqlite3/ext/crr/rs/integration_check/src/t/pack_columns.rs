use crsql_bundle::test_exports::pack_columns::unpack_columns;
use crsql_bundle::test_exports::pack_columns::ColumnValue;
use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

// The rust test is mainly to check with valgrind and ensure we're correctly
// freeing data as we do some passing of destructors from rust to SQLite.
// Complete property based tests for encode & decode exist in python.
fn test_pack_columns() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe("CREATE TABLE foo (id PRIMARY KEY, x, y)")?;
    let insert_stmt = db.db.prepare_v2("INSERT INTO foo VALUES (?, ?, ?)")?;
    let blob: [u8; 3] = [1, 2, 3];

    insert_stmt.bind_int(1, 12)?;
    insert_stmt.bind_text(2, "str", sqlite::Destructor::STATIC)?;
    insert_stmt.bind_blob(3, &blob, sqlite::Destructor::STATIC)?;
    insert_stmt.step()?;

    let select_stmt = db
        .db
        .prepare_v2("SELECT quote(crsql_pack_columns(id, x, y)) FROM foo")?;
    select_stmt.step()?;
    let result = select_stmt.column_text(0)?;
    assert!(result == "X'03090C0B037374720C03010203'");
    // 03 09 0C 0B 03 73 74 72 0C 03 01 02 03
    // cols: 03
    // type & intlen: 09 -> 0b00001001 -> 01 type & 01 intlen
    // value: 0C -> 12
    // type & intlen: 0B -> 0b00001011 -> 03 type & 01 intlen
    // 03 -> len
    // 73 74 72 -> str
    // type & intlen: 0C ->  0b00001100 -> 04 type & 01 intlen
    // len: 03
    // bytes: 01 02 3
    // voila, done in 13 bytes! < 18 byte string < 26 byte binary w/o varints

    // Before variable length encoding:
    // 03 01 00 00 00 00 00 00 00 0C 03 00 00 00 03 73 74 72 04 00 00 00 03 01 02 03
    // cols:03
    // type: 01 (integer)
    // value: 00 00 00 00 00 00 00 0C (12) TODO: encode as variable length integers to save space?
    // type: 03 (text)
    // len: 00 00 00 03 (3)
    // byes: 73 (s) 74 (t) 72 (r)
    // type: 04 (blob)
    // len: 00 00 00 03 (3)
    // bytes: 01 02 03
    // vs string:
    // 12|'str'|x'010203'
    // ^ 18 bytes via string
    // vs
    // 26 bytes via binary
    // 13 bytes are wasted due to not using variable length encoding for integers
    // So.. do variable length ints?

    let select_stmt = db
        .db
        .prepare_v2("SELECT crsql_pack_columns(id, x, y) FROM foo")?;
    select_stmt.step()?;
    let result = select_stmt.column_blob(0)?;
    assert!(result.len() == 13);
    let unpacked = unpack_columns(result)?;
    assert!(unpacked.len() == 3);

    if let ColumnValue::Integer(i) = unpacked[0] {
        assert!(i == 12);
    } else {
        assert!("unexpected type" == "");
    }
    if let ColumnValue::Text(s) = &unpacked[1] {
        assert!(s == "str")
    } else {
        assert!("unexpected type" == "");
    }
    if let ColumnValue::Blob(b) = &unpacked[2] {
        assert!(b[..] == blob);
    } else {
        assert!("unexpected type" == "");
    }

    db.db.exec_safe("DELETE FROM foo")?;
    let insert_stmt = db.db.prepare_v2("INSERT INTO foo VALUES (?, ?, ?)")?;

    insert_stmt.bind_int(1, 0)?;
    insert_stmt.bind_int(2, 10000000)?;
    insert_stmt.bind_int(3, -2500000)?;
    insert_stmt.step()?;

    let select_stmt = db
        .db
        .prepare_v2("SELECT crsql_pack_columns(id, x, y) FROM foo")?;
    select_stmt.step()?;
    let result = select_stmt.column_blob(0)?;
    let unpacked = unpack_columns(result)?;
    assert!(unpacked.len() == 3);

    if let ColumnValue::Integer(i) = unpacked[0] {
        assert!(i == 0);
    } else {
        assert!("unexpected type" == "");
    }
    if let ColumnValue::Integer(i) = unpacked[1] {
        assert!(i == 10000000)
    } else {
        assert!("unexpected type" == "");
    }
    if let ColumnValue::Integer(i) = unpacked[2] {
        assert!(i == -2500000);
    } else {
        assert!("unexpected type" == "");
    }

    Ok(())
}

fn test_unpack_columns() -> Result<(), ResultCode> {
    let db = crate::opendb().unwrap();
    db.db.exec_safe("CREATE TABLE foo (id PRIMARY KEY, x, y)")?;
    let insert_stmt = db.db.prepare_v2("INSERT INTO foo VALUES (?, ?, ?)")?;
    let blob: [u8; 3] = [1, 2, 3];

    insert_stmt.bind_int(1, 12)?;
    insert_stmt.bind_text(2, "str", sqlite::Destructor::STATIC)?;
    insert_stmt.bind_blob(3, &blob, sqlite::Destructor::STATIC)?;
    insert_stmt.step()?;

    let select_stmt = db
        .db
        .prepare_v2("SELECT cell FROM crsql_unpack_columns WHERE package = (SELECT crsql_pack_columns(id, x, y) FROM foo)")?;
    select_stmt.step()?;
    assert!(select_stmt.column_int(0) == 12);
    select_stmt.step()?;
    assert!(select_stmt.column_text(0)? == "str");
    select_stmt.step()?;
    assert!(select_stmt.column_blob(0)? == blob);

    Ok(())
}

pub fn run_suite() -> Result<(), ResultCode> {
    test_pack_columns()?;
    test_unpack_columns()
}
