use {SqliteResult, SqliteConnection};

pub enum SqliteTransactionBehavior {
    SqliteTransactionDeferred,
    SqliteTransactionImmediate,
    SqliteTransactionExclusive,
}

pub struct SqliteTransaction<'conn> {
    conn: &'conn SqliteConnection,
    depth: u32,
    commit: bool,
    finished: bool,
}

impl<'conn> SqliteTransaction<'conn> {
    pub fn new(conn: &SqliteConnection,
               behavior: SqliteTransactionBehavior) -> SqliteResult<SqliteTransaction> {
        let query = match behavior {
            SqliteTransactionDeferred => "BEGIN DEFERRED",
            SqliteTransactionImmediate => "BEGIN IMMEDIATE",
            SqliteTransactionExclusive => "BEGIN EXCLUSIVE",
        };
        conn.execute_batch(query).map(|_| {
            SqliteTransaction{ conn: conn, depth: 0, commit: false, finished: false }
        })
    }

    pub fn savepoint<'a>(&'a self) -> SqliteResult<SqliteTransaction<'a>> {
        self.conn.execute_batch("SAVEPOINT sp").map(|_| {
            SqliteTransaction{
                conn: self.conn, depth: self.depth + 1, commit: false, finished: false
            }
        })
    }

    pub fn will_commit(&self) -> bool {
        self.commit
    }

    pub fn will_rollback(&self) -> bool {
        !self.commit
    }

    pub fn set_commit(&mut self) {
        self.commit = true
    }

    pub fn set_rollback(&mut self) {
        self.commit = false
    }

    pub fn commit(mut self) -> SqliteResult<()> {
        self.commit_()
    }

    fn commit_(&mut self) -> SqliteResult<()> {
        self.finished = true;
        self.conn.execute_batch(if self.depth == 0 { "COMMIT" } else { "RELEASE sp" })
    }

    pub fn rollback(mut self) -> SqliteResult<()> {
        self.rollback_()
    }

    fn rollback_(&mut self) -> SqliteResult<()> {
        self.finished = true;
        self.conn.execute_batch(if self.depth == 0 { "ROLLBACK" } else { "ROLLBACK TO sp" })
    }

    pub fn finish(mut self) -> SqliteResult<()> {
        self.finish_()
    }

    fn finish_(&mut self) -> SqliteResult<()> {
        match (self.finished, self.commit) {
            (true, _) => Ok(()),
            (false, true) => self.commit_(),
            (false, false) => self.rollback_(),
        }
    }
}

#[unsafe_destructor]
#[allow(unused_must_use)]
impl<'conn> Drop for SqliteTransaction<'conn> {
    fn drop(&mut self) {
        self.finish_();
    }
}

#[cfg(test)]
mod test {
    extern crate test;
    use SqliteConnection;

    fn checked_memory_handle() -> SqliteConnection {
        let db = SqliteConnection::open(":memory:").unwrap();
        db.execute_batch("CREATE TABLE foo (x INTEGER)").unwrap();
        db
    }

    #[test]
    fn test_drop() {
        let db = checked_memory_handle();
        {
            let _tx = db.transaction().unwrap();
            db.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
            // default: rollback
        }
        {
            let mut tx = db.transaction().unwrap();
            db.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
            tx.set_commit()
        }
        {
            let _tx = db.transaction().unwrap();
            assert_eq!(2i32, db.query_row("SELECT SUM(x) FROM foo", [], |r| r.unwrap().get(0)));
        }
    }

    #[test]
    fn test_explicit_rollback_commit() {
        let db = checked_memory_handle();
        {
            let tx = db.transaction().unwrap();
            db.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
            tx.rollback().unwrap();
        }
        {
            let tx = db.transaction().unwrap();
            db.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
            tx.commit().unwrap();
        }
        {
            let _tx = db.transaction().unwrap();
            assert_eq!(2i32, db.query_row("SELECT SUM(x) FROM foo", [], |r| r.unwrap().get(0)));
        }
    }

    #[test]
    fn test_savepoint() {
        let db = checked_memory_handle();
        {
            let mut tx = db.transaction().unwrap();
            db.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
            tx.set_commit();
            {
                let mut sp1 = tx.savepoint().unwrap();
                db.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
                sp1.set_commit();
                {
                    let sp2 = sp1.savepoint().unwrap();
                    db.execute_batch("INSERT INTO foo VALUES(4)").unwrap();
                    // will rollback sp2
                    {
                        let sp3 = sp2.savepoint().unwrap();
                        db.execute_batch("INSERT INTO foo VALUES(8)").unwrap();
                        sp3.commit().unwrap();
                        // committed sp3, but will be erased by sp2 rollback
                    }
                }
            }
        }
        assert_eq!(3i32, db.query_row("SELECT SUM(x) FROM foo", [], |r| r.unwrap().get(0)));
    }

    #[bench]
    fn test_no_transaction_insert(bencher: &mut test::Bencher) {
        let db = checked_memory_handle();

        let mut stmt = db.prepare("INSERT INTO foo VALUES(1)").unwrap();

        bencher.iter(|| {
            for _ in range(0i32, 1000) {
                stmt.execute([]).unwrap();
            }
        })
    }

    #[bench]
    fn test_transaction_insert(bencher: &mut test::Bencher) {
        let db = checked_memory_handle();

        let mut stmt = db.prepare("INSERT INTO foo VALUES(1)").unwrap();

        bencher.iter(|| {
            let mut tx = db.transaction().unwrap();
            tx.set_commit();
            for _ in range(0i32, 1000) {
                stmt.execute([]).unwrap();
            }
        })
    }
}
