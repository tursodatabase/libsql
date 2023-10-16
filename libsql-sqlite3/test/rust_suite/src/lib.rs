#[cfg(test)]
mod alter_column;
mod random_rowid;
mod virtual_wal;

#[cfg(all(test, feature = "udf"))]
mod user_defined_functions;
#[cfg(all(test, feature = "udf"))]
mod user_defined_functions_src;

#[cfg(test)]
mod tests {
    extern "C" {
        fn libsql_close_hook(
            db: *mut rusqlite::ffi::sqlite3,
            callback: Option<
                unsafe fn(arg: *mut std::ffi::c_void, db: *mut rusqlite::ffi::sqlite3),
            >,
            arg: *mut std::ffi::c_void,
        );
    }

    use rusqlite::Connection;

    #[derive(Debug, PartialEq)]
    struct Person {
        name: String,
        data: Option<Vec<u8>>,
    }

    #[test]
    fn test_insert_steven() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute(
            "CREATE TABLE person (
                name  TEXT NOT NULL,
                data  BLOB
            )",
            (),
        )
        .unwrap();
        let steven = Person {
            name: "Steven".to_string(),
            data: Some(vec![4, 2]),
        };
        conn.execute(
            "INSERT INTO person (name, data) VALUES (?1, ?2)",
            (&steven.name, &steven.data),
        )
        .unwrap();

        let mut stmt = conn.prepare("SELECT name, data FROM person").unwrap();
        let mut person_iter = stmt
            .query_map([], |row| {
                Ok(Person {
                    name: row.get(0).unwrap(),
                    data: row.get(1).unwrap(),
                })
            })
            .unwrap();

        let also_steven = person_iter.next().unwrap().unwrap();
        println!("Read {also_steven:#?}");
        assert!(also_steven == steven);
        assert!(person_iter.next().is_none())
    }

    fn get_read_written(conn: &Connection, stmt: &str) -> (i32, i32) {
        const STMT_ROWS_READ: i32 = 1024 + 1;
        const STMT_ROWS_WRITTEN: i32 = 1024 + 2;
        let mut stmt = conn.prepare(stmt).unwrap();
        let mut rows = stmt.query(()).unwrap();
        while let Ok(Some(_)) = rows.next() {}
        drop(rows);
        let mut rows_read = rusqlite::StatementStatus::FullscanStep;
        let mut rows_written = rusqlite::StatementStatus::FullscanStep;
        // FIXME: there's no API for ROWS_READ/WRITTEN yet, so let's rewrite to checking ROWS_* instead
        unsafe {
            std::ptr::copy(
                &[STMT_ROWS_READ] as *const i32,
                &mut rows_read as *mut _ as *mut i32,
                4,
            )
        }
        unsafe {
            std::ptr::copy(
                &[STMT_ROWS_WRITTEN] as *const i32,
                &mut rows_written as *mut _ as *mut i32,
                4,
            )
        }
        (stmt.get_status(rows_read), stmt.get_status(rows_written))
    }

    #[test]
    fn test_rows_read_written() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE test(id)", ()).unwrap();
        for _ in 0..16 {
            conn.execute("INSERT INTO test values (1)", ()).unwrap();
        }
        assert_eq!(get_read_written(&conn, "SELECT * FROM test"), (16, 0));
        assert_eq!(get_read_written(&conn, "SELECT count(*) FROM test"), (1, 0));
        assert_eq!(
            get_read_written(&conn, "SELECT min(id), max(id) FROM test where 1 = 1"),
            (16, 0)
        );
        assert_eq!(
            get_read_written(&conn, "SELECT * FROM test LIMIT 3"),
            (3, 0)
        );
        assert_eq!(
            get_read_written(&conn, "SELECT * FROM test LIMIT 3"),
            (3, 0)
        );
        assert_eq!(
            get_read_written(&conn, "SELECT * FROM test WHERE id = 2"),
            (16, 0)
        );
        assert_eq!(
            get_read_written(&conn, "SELECT * FROM test WHERE id = 2 ORDER BY rowid DESC"),
            (16, 0)
        );
        assert_eq!(
            get_read_written(&conn, "SELECT * FROM test WHERE rowid = 1"),
            (1, 0)
        );
        assert_eq!(
            get_read_written(&conn, "INSERT INTO test VALUES (1)"),
            (0, 1)
        );
        assert_eq!(
            get_read_written(&conn, "INSERT INTO test(id) SELECT id FROM test"),
            (34, 17)
        );
        assert_eq!(
            get_read_written(
                &conn,
                "SELECT * FROM test WHERE id IN (SELECT id FROM test)"
            ),
            (68, 0)
        );
        assert_eq!(
            get_read_written(&conn, "INSERT INTO test VALUES (1), (2), (3), (4)"),
            (0, 4)
        );
        assert_eq!(get_read_written(&conn, "SELECT COUNT(*) FROM test"), (1, 0));
    }

    #[test]
    fn test_close_hook() {
        let conn = Connection::open_in_memory().unwrap();
        let mut closed = false;
        unsafe {
            libsql_close_hook(
                conn.handle(),
                Some(|closed, _db| {
                    println!("Close hook called!");
                    let closed = &mut *(closed as *mut bool);
                    *closed = true;
                }),
                &mut closed as *mut _ as *mut _,
            );
        }
        assert!(!closed);
        drop(conn);
        assert!(closed);
    }
}
