use crate::user_defined_functions_src::{
    concat3_src, contains_src, fib_src, get_null_src, reverse_blob_src,
};
use libsql_sys::rusqlite::Connection;

fn fib(n: i32) -> i32 {
    match n {
        0 | 1 => n,
        _ => fib(n - 1) + fib(n - 2),
    }
}

#[test]
fn test_create_drop_fib() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t (id)", ()).unwrap();
    for i in 1..7 {
        conn.execute("INSERT INTO t(id) VALUES (?)", (i,)).unwrap();
    }

    conn.execute(
        &format!("CREATE FUNCTION fib LANGUAGE wasm AS x'{}'", fib_src()),
        (),
    )
    .unwrap();
    let mut stmt = conn.prepare("SELECT id, fib(id) FROM t").unwrap();

    let fibs: Vec<(i32, i32)> = stmt
        .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
        .unwrap()
        .map(|e| e.unwrap())
        .collect();

    let expected_fibs: Vec<(i32, i32)> = (1..7).map(|n| (n, fib(n))).collect();
    assert_eq!(fibs, expected_fibs);

    std::mem::drop(stmt);
    conn.execute("DROP FUNCTION fib", ()).unwrap();
    assert!(conn.prepare("SELECT id, fib(id) FROM t").is_err());
}

#[test]
fn test_contains() {
    use itertools::Itertools;

    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t (a, b)", ()).unwrap();
    for perm in vec!["eenie", "meenie", "miny", "mo", "m", "o"]
        .into_iter()
        .permutations(2)
    {
        conn.execute("INSERT INTO t(a, b) VALUES (?, ?)", (perm[0], perm[1]))
            .unwrap();
    }

    conn.execute(
        &format!(
            "CREATE FUNCTION contains LANGUAGE wasm AS x'{}'",
            contains_src()
        ),
        (),
    )
    .unwrap();
    let mut stmt = conn.prepare("SELECT a, b, contains(a, b) FROM t").unwrap();

    let results: Vec<(String, String, bool)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            ))
        })
        .unwrap()
        .map(|e| e.unwrap())
        .collect();

    for (a, b, res) in results {
        assert_eq!(a.contains(&b), res);
        if res {
            println!("{} contains {}", a, b);
        }
    }

    assert!(conn.execute("SELECT contains(a) FROM t", ()).is_err());
    assert!(conn.execute("SELECT contains(a, b, c) FROM t", ()).is_err());
    assert!(conn.execute("SELECT contains(a, 7) FROM t", ()).is_err());
    assert!(conn.execute("SELECT contains(7, a) FROM t", ()).is_err());
    assert!(conn.execute("SELECT contains(7, null) FROM t", ()).is_err());

    std::mem::drop(stmt);
    conn.execute("DROP FUNCTION contains", ()).unwrap();
    assert!(conn.execute("SELECT contains(a, b) FROM t", ()).is_err());
}

#[test]
fn test_concat3() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t (a, b)", ()).unwrap();
    conn.execute("INSERT INTO t(a, b) VALUES ('hello', 'world')", ())
        .unwrap();

    conn.execute(
        &format!(
            "CREATE FUNCTION concat3 LANGUAGE wasm AS x'{}'",
            concat3_src()
        ),
        (),
    )
    .unwrap();

    let result: (String, String) = conn
        .query_row("SELECT concat3(a, ', ', b), concat3('x', a, concat3('z', 'y', concat3(b, b, b))) FROM t", [], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
        .unwrap();

    assert_eq!(
        result,
        (
            "hello, world".to_string(),
            "xhellozyworldworldworld".to_string()
        )
    );

    assert!(conn.execute("SELECT concat3(a) FROM t", ()).is_err());
    assert!(conn.execute("SELECT concat3(a, b) FROM t", ()).is_err());
    assert!(conn
        .execute("SELECT concat3(a, b, c, d) FROM t", ())
        .is_err());
    assert!(conn.execute("SELECT concat3(a, b, 7) FROM t", ()).is_err());
    assert!(conn.execute("SELECT concat3(1, 2, 3)", ()).is_err());

    conn.execute("DROP FUNCTION concat3", ()).unwrap();
    assert!(conn.execute("SELECT concat3(a, b, '') FROM t", ()).is_err());
}

#[test]
fn test_reverse_blob() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE t (id)", ()).unwrap();
    for vec in [
        vec![1, 2, 3, 4, 42],
        vec![7; 65536],
        vec![1],
        vec![0, 5, 0, 5],
    ] {
        conn.execute("INSERT INTO t(id) VALUES (?)", (vec,))
            .unwrap();
    }

    conn.execute(
        &format!(
            "CREATE FUNCTION reverse_blob LANGUAGE wasm AS x'{}'",
            reverse_blob_src()
        ),
        (),
    )
    .unwrap();
    let mut stmt = conn.prepare("SELECT id, reverse_blob(id) FROM t").unwrap();

    for (mut blob, rev) in stmt
        .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
        .unwrap()
        .map(|e: libsql_sys::rusqlite::Result<(Vec<u8>, Vec<u8>)>| e.unwrap())
    {
        blob.reverse();
        assert_eq!(blob, rev)
    }

    std::mem::drop(stmt);
    conn.execute("DROP FUNCTION reverse_blob", ()).unwrap();
    assert!(conn.prepare("SELECT id, reverse_blob(id) FROM t").is_err());
}

#[test]
fn test_get_null() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute(
        &format!(
            "CREATE FUNCTION get_null LANGUAGE wasm AS x'{}'",
            get_null_src()
        ),
        (),
    )
    .unwrap();

    let result: Option<String> = conn
        .query_row("SELECT get_null()", [], |row| Ok(row.get(0).unwrap()))
        .unwrap();

    assert!(result.is_none());

    conn.execute("DROP FUNCTION get_null", ()).unwrap();
    assert!(conn.prepare("SELECT id, get_null(id) FROM t").is_err());
}

#[test]
fn test_explain() {
    let conn = Connection::open_in_memory().unwrap();

    let mut create_stmt = conn
        .prepare("EXPLAIN CREATE FUNCTION mj LANGUAGE wasm AS 'hee-hee'")
        .unwrap();
    assert!(create_stmt
        .query_map([], |row| Ok(row.get::<_, String>(1).unwrap()))
        .unwrap()
        .any(|e| e.unwrap() == "CreateWasmFunc"));

    let mut drop_stmt = conn.prepare("EXPLAIN DROP FUNCTION mj").unwrap();
    assert!(drop_stmt
        .query_map([], |row| Ok(row.get::<_, String>(1).unwrap()))
        .unwrap()
        .any(|e| e.unwrap() == "DropWasmFunc"));
}
