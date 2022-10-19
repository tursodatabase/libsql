use rusqlite::Connection;

static FIB_SRC: &str = r#"
(module 
    (type (;0;) (func (param i64) (result i64))) 
    (func $fib (type 0) (param i64) (result i64) 
    (local i64) 
    i64.const 0 
    local.set 1 
    block ;; label = @1 
    local.get 0 
    i64.const 2 
    i64.lt_u 
    br_if 0 (;@1;) 
    i64.const 0 
    local.set 1 
    loop ;; label = @2 
    local.get 0 
    i64.const -1 
    i64.add 
    call $fib 
    local.get 1 
    i64.add 
    local.set 1 
    local.get 0 
    i64.const -2 
    i64.add 
    local.tee 0 
    i64.const 1 
    i64.gt_u 
    br_if 0 (;@2;) 
    end 
    end 
    local.get 0 
    local.get 1 
    i64.add) 
    (memory (;0;) 16) 
    (global $__stack_pointer (mut i32) (i32.const 1048576)) 
    (global (;1;) i32 (i32.const 1048576)) 
    (global (;2;) i32 (i32.const 1048576)) 
    (export "memory" (memory 0)) 
    (export "fib" (func $fib)))
"#;

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
        &format!("CREATE FUNCTION fib LANGUAGE wasm AS '{}'", FIB_SRC),
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

    conn.execute("DROP FUNCTION fib", ()).unwrap();

    assert!(conn.prepare("SELECT id, fib(id) FROM t").is_err());
}
