use super::ffi;

use {Error, Result, Statement};
use types::ToSql;

impl<'conn> Statement<'conn> {
    /// Execute an INSERT and return the ROWID.
    ///
    /// # Failure
    /// Will return `Err` if no row is inserted or many rows are inserted.
    pub fn insert(&mut self, params: &[&ToSql]) -> Result<i64> {
        let changes = try!(self.execute(params));
        match changes {
            1 => Ok(self.conn.last_insert_rowid()),
            _ => Err(Error::QueryInsertedRows(changes))
        }
    }

    /// Return `true` if a query in the SQL statement it executes returns one or more rows
    /// and `false` if the SQL returns an empty set.
    pub fn exists(&mut self, params: &[&ToSql]) -> Result<bool> {
        self.reset_if_needed();
        unsafe {
            try!(self.bind_parameters(params));
            let r = ffi::sqlite3_step(self.stmt);
            ffi::sqlite3_reset(self.stmt);
            match r {
                ffi::SQLITE_DONE => Ok(false),
                ffi::SQLITE_ROW => Ok(true),
                _ => Err(self.conn.decode_result(r).unwrap_err()),
            }
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
            Error::QueryInsertedRows(0) => (),
            err => panic!("Unexpected error {}", err),
        }
        let mut multi = db.prepare("INSERT INTO foo (x) SELECT 3 UNION ALL SELECT 4").unwrap();
        match multi.insert(&[]).unwrap_err() {
            Error::QueryInsertedRows(2) => (),
            err => panic!("Unexpected error {}", err),
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