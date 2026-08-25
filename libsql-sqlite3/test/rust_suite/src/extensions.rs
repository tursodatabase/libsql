use libsql_sys::rusqlite::{Connection, params, LoadExtensionGuard};

#[test]
fn test_sqlite_vss() {
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        let _guard = LoadExtensionGuard::new(&conn).unwrap();
        conn.load_extension("src/vector0", None).unwrap();
        conn.load_extension("src/vss0", None).unwrap();
    }
    conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS vss_demo USING vss0(a(2))", ())
        .unwrap();
    conn.execute("INSERT INTO vss_demo(rowid, a) VALUES (1, '[1.0, 2.0]'), (2, '[2.0, 2.0]'), (3, '[3.0, 2.0]')", ()).unwrap();
    
    let mut stmt = conn.prepare("SELECT rowid, distance FROM vss_demo WHERE vss_search(?, ?) LIMIT 3").unwrap();
    let mut rows = stmt.query(params![1.0, 2.0]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let rowid: i32 = row.get(0).unwrap();
        let distance: f64 = row.get(1).unwrap();
        println!("Row ID: {}, Distance: {}", rowid, distance);
    }
}

#[test]
fn test_sqlite_math() {
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        let _guard = LoadExtensionGuard::new(&conn).unwrap();
        conn.load_extension("src/math0", None).unwrap();
    }
    
    let result: f64 = conn.query_row("SELECT sin(PI() / 2)", [], |row| row.get(0)).unwrap();
    assert!((result - 1.0).abs() < 1e-9, "Expected sin(PI/2) to be close to 1.0");
}

#[test]
fn test_sqlite_fuzzy() {
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        let _guard = LoadExtensionGuard::new(&conn).unwrap();
        conn.load_extension("src/fuzzy0", None).unwrap();
    }
    
    let result: i32 = conn.query_row("SELECT levenshtein('kitten', 'sitting')", [], |row| row.get(0)).unwrap();
    assert_eq!(result, 3, "Expected levenshtein distance between 'kitten' and 'sitting' to be 3");
}

#[test]
fn test_sqlite_stats() {
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        let _guard = LoadExtensionGuard::new(&conn).unwrap();
        conn.load_extension("src/stats0", None).unwrap();
    }
    
    conn.execute("CREATE TABLE data(value REAL)", ()).unwrap();
    conn.execute("INSERT INTO data(value) VALUES (1.0), (2.0), (3.0), (4.0), (5.0)", ()).unwrap();
    
    let avg: f64 = conn.query_row("SELECT avg(value) FROM data", [], |row| row.get(0)).unwrap();
    assert!((avg - 3.0).abs() < 1e-9, "Expected average of values 1.0 to 5.0 to be 3.0");
}
