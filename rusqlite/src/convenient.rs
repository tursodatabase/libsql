use {Error, Result, Statement};
use types::ToSql;

impl<'conn> Statement<'conn> {
    /// Execute an INSERT and return the ROWID.
    ///
    /// # Failure
    /// Will return `Err` if no row is inserted or many rows are inserted.
    pub fn insert(&mut self, params: &[&ToSql]) -> Result<i64> {
        // Some non-insertion queries could still return 1 change (an UPDATE, for example), so
        // to guard against that we can check that the connection's last_insert_rowid() changes
        // after we execute the statement.
        let prev_rowid = self.conn.last_insert_rowid();
        let changes = try!(self.execute(params));
        let new_rowid = self.conn.last_insert_rowid();
        match changes {
            1 if prev_rowid != new_rowid => Ok(new_rowid),
            1 if prev_rowid == new_rowid => Err(Error::StatementFailedToInsertRow),
            _ => Err(Error::StatementChangedRows(changes))
        }
    }

    /// Return `true` if a query in the SQL statement it executes returns one or more rows
    /// and `false` if the SQL returns an empty set.
    pub fn exists(&mut self, params: &[&ToSql]) -> Result<bool> {
        self.reset_if_needed();
        let mut rows = try!(self.query(params));
        match rows.next() {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod test {
    use {Connection, Error};

    #[test]
    fn test_insert() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER UNIQUE)").unwrap();
        let mut stmt = db.prepare("INSERT OR IGNORE INTO foo (x) VALUES (?)").unwrap();
        assert_eq!(stmt.insert(&[&1i32]).unwrap(), 1);
        assert_eq!(stmt.insert(&[&2i32]).unwrap(), 2);
        match stmt.insert(&[&1i32]).unwrap_err() {
            Error::StatementChangedRows(0) => (),
            err => panic!("Unexpected error {}", err),
        }
        let mut multi = db.prepare("INSERT INTO foo (x) SELECT 3 UNION ALL SELECT 4").unwrap();
        match multi.insert(&[]).unwrap_err() {
            Error::StatementChangedRows(2) => (),
            err => panic!("Unexpected error {}", err),
        }
    }

    #[test]
    fn test_insert_failures() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(x INTEGER UNIQUE)").unwrap();
        let mut insert = db.prepare("INSERT INTO foo (x) VALUES (?)").unwrap();
        let mut update = db.prepare("UPDATE foo SET x = ?").unwrap();

        assert_eq!(insert.insert(&[&1i32]).unwrap(), 1);

        match update.insert(&[&2i32]) {
            Err(Error::StatementFailedToInsertRow) => (),
            r => panic!("Unexpected result {:?}", r),
        }
    }

    #[test]
    fn test_exists() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   END;";
        db.execute_batch(sql).unwrap();
        let mut stmt = db.prepare("SELECT 1 FROM foo WHERE x = ?").unwrap();
        assert!(stmt.exists(&[&1i32]).unwrap());
        assert!(stmt.exists(&[&2i32]).unwrap());
        assert!(!stmt.exists(&[&0i32]).unwrap());
    }
}
