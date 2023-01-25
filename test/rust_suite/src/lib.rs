mod random_rowid;
mod virtual_wal;

#[cfg(all(test, feature = "udf"))]
mod user_defined_functions;
#[cfg(all(test, feature = "udf"))]
mod user_defined_functions_src;

#[cfg(test)]
mod tests {
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
        println!("Read {:#?}", also_steven);
        assert!(also_steven == steven);
        assert!(person_iter.next().is_none())
    }
}
