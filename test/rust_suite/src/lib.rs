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
        assert_eq!(
            get_read_written(&conn, "SELECT count(*) FROM test"),
            (16, 0)
        );
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
            (34, 34)
        );
        assert_eq!(
            get_read_written(&conn, "INSERT INTO test VALUES (1), (2), (3), (4)"),
            (0, 4)
        );
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

#[test]
fn test_books_by_author() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();

    conn.execute(
        "CREATE TABLE author (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
        (),
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE book (
            id         INTEGER PRIMARY KEY,
            title      TEXT NOT NULL,
            author_id  INTEGER NOT NULL,
            FOREIGN KEY (author_id) REFERENCES author (id)
        )",
        (),
    )
    .unwrap();

    let authors = vec![
        Author { id: 1, name: "J.K. Rowling".to_string() },
        Author { id: 2, name: "George Orwell".to_string() },
    ];

    for author in &authors {
        conn.execute(
            "INSERT INTO author (id, name) VALUES (?1, ?2)",
            (&author.id, &author.name),
        )
        .unwrap();
    }

    let books = vec![
        Book { id: 1, title: "Harry Potter and the Sorcerer's Stone".to_string(), author_id: 1 },
        Book { id: 2, title: "1984".to_string(), author_id: 2 },
        Book { id: 3, title: "Animal Farm".to_string(), author_id: 2 },
    ];

    for book in &books {
        conn.execute(
            "INSERT INTO book (id, title, author_id) VALUES (?1, ?2, ?3)",
            (&book.id, &book.title, &book.author_id),
        )
        .unwrap();
    }

    let author_name = "George Orwell";
    let mut stmt = conn.prepare(
        "SELECT b.title FROM book b
         INNER JOIN author a ON b.author_id = a.id
         WHERE a.name = ?1",
    )
    .unwrap();

    let book_titles: Result<Vec<String>, rusqlite::Error> = stmt.query_map([author_name], |row| {
        Ok(row.get(0)?)
    })
    .unwrap()
    .map(|res| res.unwrap())
    .collect();

    let expected_titles = vec!["1984".to_string(), "Animal Farm".to_string()];
    assert_eq!(book_titles.unwrap(), expected_titles);
}
