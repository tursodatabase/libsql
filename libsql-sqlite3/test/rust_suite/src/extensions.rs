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
    conn.execute(
        "SELECT rowid, distance FROM vss_demo WHERE vss_search(?, ?) LIMIT 3",
        params![1.0, 2.0],
    ).unwrap();
}