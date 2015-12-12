use {Result, Connection};

pub use SqliteTransactionBehavior::{SqliteTransactionDeferred, SqliteTransactionImmediate,
                                    SqliteTransactionExclusive};

/// Options for transaction behavior. See [BEGIN
/// TRANSACTION](http://www.sqlite.org/lang_transaction.html) for details.
#[derive(Copy,Clone)]
pub enum SqliteTransactionBehavior {
    SqliteTransactionDeferred,
    SqliteTransactionImmediate,
    SqliteTransactionExclusive,
}

/// Represents a transaction on a database connection.
///
/// ## Note
///
/// Transactions will roll back by default. Use the `set_commit` or `commit` methods to commit the
/// transaction.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result};
/// # fn do_queries_part_1(conn: &Connection) -> Result<()> { Ok(()) }
/// # fn do_queries_part_2(conn: &Connection) -> Result<()> { Ok(()) }
/// fn perform_queries(conn: &Connection) -> Result<()> {
///     let tx = try!(conn.transaction());
///
///     try!(do_queries_part_1(conn)); // tx causes rollback if this fails
///     try!(do_queries_part_2(conn)); // tx causes rollback if this fails
///
///     tx.commit()
/// }
/// ```
pub struct SqliteTransaction<'conn> {
    conn: &'conn Connection,
    depth: u32,
    commit: bool,
    finished: bool,
}

impl<'conn> SqliteTransaction<'conn> {
    /// Begin a new transaction. Cannot be nested; see `savepoint` for nested transactions.
    pub fn new(conn: &Connection,
               behavior: SqliteTransactionBehavior)
               -> Result<SqliteTransaction> {
        let query = match behavior {
            SqliteTransactionDeferred => "BEGIN DEFERRED",
            SqliteTransactionImmediate => "BEGIN IMMEDIATE",
            SqliteTransactionExclusive => "BEGIN EXCLUSIVE",
        };
        conn.execute_batch(query).map(|_| {
            SqliteTransaction {
                conn: conn,
                depth: 0,
                commit: false,
                finished: false,
            }
        })
    }

    /// Starts a new [savepoint](http://www.sqlite.org/lang_savepoint.html), allowing nested
    /// transactions.
    ///
    /// ## Note
    ///
    /// Just like outer level transactions, savepoint transactions rollback by default.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # fn perform_queries_part_1_succeeds(conn: &Connection) -> bool { true }
    /// fn perform_queries(conn: &Connection) -> Result<()> {
    ///     let tx = try!(conn.transaction());
    ///
    ///     {
    ///         let sp = try!(tx.savepoint());
    ///         if perform_queries_part_1_succeeds(conn) {
    ///             try!(sp.commit());
    ///         }
    ///         // otherwise, sp will rollback
    ///     }
    ///
    ///     tx.commit()
    /// }
    /// ```
    pub fn savepoint<'a>(&'a self) -> Result<SqliteTransaction<'a>> {
        self.conn.execute_batch("SAVEPOINT sp").map(|_| {
            SqliteTransaction {
                conn: self.conn,
                depth: self.depth + 1,
                commit: false,
                finished: false,
            }
        })
    }

    /// Returns whether or not the transaction is currently set to commit.
    pub fn will_commit(&self) -> bool {
        self.commit
    }

    /// Returns whether or not the transaction is currently set to rollback.
    pub fn will_rollback(&self) -> bool {
        !self.commit
    }

    /// Set the transaction to commit at its completion.
    pub fn set_commit(&mut self) {
        self.commit = true
    }

    /// Set the transaction to rollback at its completion.
    pub fn set_rollback(&mut self) {
        self.commit = false
    }

    /// A convenience method which consumes and commits a transaction.
    pub fn commit(mut self) -> Result<()> {
        self.commit_()
    }

    fn commit_(&mut self) -> Result<()> {
        self.finished = true;
        self.conn.execute_batch(if self.depth == 0 {
            "COMMIT"
        } else {
            "RELEASE sp"
        })
    }

    /// A convenience method which consumes and rolls back a transaction.
    pub fn rollback(mut self) -> Result<()> {
        self.rollback_()
    }

    fn rollback_(&mut self) -> Result<()> {
        self.finished = true;
        self.conn.execute_batch(if self.depth == 0 {
            "ROLLBACK"
        } else {
            "ROLLBACK TO sp"
        })
    }

    /// Consumes the transaction, committing or rolling back according to the current setting
    /// (see `will_commit`, `will_rollback`).
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows callers to see any
    /// errors that occur.
    pub fn finish(mut self) -> Result<()> {
        self.finish_()
    }

    fn finish_(&mut self) -> Result<()> {
        match (self.finished, self.commit) {
            (true, _) => Ok(()),
            (false, true) => self.commit_(),
            (false, false) => self.rollback_(),
        }
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for SqliteTransaction<'conn> {
    fn drop(&mut self) {
        self.finish_();
    }
}

#[cfg(test)]
mod test {
    use Connection;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
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
            assert_eq!(2i32,
                       db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0)).unwrap());
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
            assert_eq!(2i32,
                       db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0)).unwrap());
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
        assert_eq!(3i32,
                   db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0)).unwrap());
    }
}
