#[cfg(test)]
mod tests {
    use libsql_sys::rusqlite::Connection;

    // Test that RANDOM ROWID tables indeed generate rowid values in a pseudorandom way
    #[test]
    fn test_random_rowid_distribution() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute("CREATE TABLE t(id)", ()).unwrap();
        conn.execute("CREATE TABLE tr(id) RANDOM ROWID", ())
            .unwrap();
        for _ in 1..=1024 {
            conn.execute("INSERT INTO t(id) VALUES (42)", ()).unwrap();
            conn.execute("INSERT INTO tr(id) VALUES (42)", ()).unwrap();
        }

        let seq_rowids: Vec<i64> = conn
            .prepare("SELECT rowid FROM t")
            .unwrap()
            .query_map([], |row| Ok(row.get_unwrap(0)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(seq_rowids, (1..=1024_i64).collect::<Vec<i64>>());

        let random_rowids: Vec<i64> = conn
            .prepare("SELECT rowid FROM tr")
            .unwrap()
            .query_map([], |row| Ok(row.get_unwrap(0)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        // This assertion is technically just probabilistic, but in practice
        // precise enough to ~never cause false positives
        assert_ne!(random_rowids, (1..=1024_i64).collect::<Vec<i64>>())
    }

    // Test that RANDOM ROWID can only be used in specific context - table creation
    #[test]
    fn test_random_rowid_validate_create() {
        let conn = Connection::open_in_memory().unwrap();

        for wrong in [
            "CREATE TABLE t(id) RANDOM ROWID WITHOUT ROWID",
            "CREATE TABLE t(id int PRIMARY KEY AUTOINCREMENT) RANDOM ROWID",
            "CREATE TABLE t(id) RANDOM ROW_ID",
            "CREATE TABLE t(id) RANDO ROWID",
        ] {
            assert!(conn.execute(wrong, ()).is_err());
        }
    }

    // Test that providing rowid value explicitly still works
    // and is respected with higher priority
    #[test]
    fn test_random_rowid_explicit_rowid() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute("CREATE TABLE t(id) RANDOM ROWID", ()).unwrap();
        conn.execute("INSERT INTO t(rowid) VALUES (42)", ())
            .unwrap();
        let rowid: i64 = conn
            .query_row("SELECT rowid FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rowid, 42);
    }
}
