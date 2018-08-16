use std::ops::Deref;
use {Connection, Result};

/// Old name for `TransactionBehavior`. `SqliteTransactionBehavior` is
/// deprecated.
#[deprecated(since = "0.6.0", note = "Use TransactionBehavior instead")]
pub type SqliteTransactionBehavior = TransactionBehavior;

/// Options for transaction behavior. See [BEGIN
/// TRANSACTION](http://www.sqlite.org/lang_transaction.html) for details.
#[derive(Copy, Clone)]
pub enum TransactionBehavior {
    Deferred,
    Immediate,
    Exclusive,
}

/// Options for how a Transaction or Savepoint should behave when it is dropped.
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum DropBehavior {
    /// Roll back the changes. This is the default.
    Rollback,

    /// Commit the changes.
    Commit,

    /// Do not commit or roll back changes - this will leave the transaction or
    /// savepoint open, so should be used with care.
    Ignore,

    /// Panic. Used to enforce intentional behavior during development.
    Panic,
}

/// Old name for `Transaction`. `SqliteTransaction` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Transaction instead")]
pub type SqliteTransaction<'conn> = Transaction<'conn>;

/// Represents a transaction on a database connection.
///
/// ## Note
///
/// Transactions will roll back by default. Use `commit` method to explicitly
/// commit the transaction, or use `set_drop_behavior` to change what happens
/// when the transaction is dropped.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result};
/// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
/// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
/// fn perform_queries(conn: &mut Connection) -> Result<()> {
///     let tx = try!(conn.transaction());
///
///     try!(do_queries_part_1(&tx)); // tx causes rollback if this fails
///     try!(do_queries_part_2(&tx)); // tx causes rollback if this fails
///
///     tx.commit()
/// }
/// ```
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    drop_behavior: DropBehavior,
}

/// Represents a savepoint on a database connection.
///
/// ## Note
///
/// Savepoints will roll back by default. Use `commit` method to explicitly
/// commit the savepoint, or use `set_drop_behavior` to change what happens
/// when the savepoint is dropped.
///
/// ## Example
///
/// ```rust,no_run
/// # use rusqlite::{Connection, Result};
/// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
/// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
/// fn perform_queries(conn: &mut Connection) -> Result<()> {
///     let sp = try!(conn.savepoint());
///
///     try!(do_queries_part_1(&sp)); // sp causes rollback if this fails
///     try!(do_queries_part_2(&sp)); // sp causes rollback if this fails
///
///     sp.commit()
/// }
/// ```
pub struct Savepoint<'conn> {
    conn: &'conn Connection,
    name: String,
    depth: u32,
    drop_behavior: DropBehavior,
    committed: bool,
}

impl<'conn> Transaction<'conn> {
    /// Begin a new transaction. Cannot be nested; see `savepoint` for nested
    /// transactions.
    // Even though we don't mutate the connection, we take a `&mut Connection`
    // so as to prevent nested or concurrent transactions on the same
    // connection.
    pub fn new(conn: &mut Connection, behavior: TransactionBehavior) -> Result<Transaction> {
        let query = match behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
        };
        conn.execute_batch(query).map(move |_| Transaction {
            conn,
            drop_behavior: DropBehavior::Rollback,
        })
    }

    /// Starts a new [savepoint](http://www.sqlite.org/lang_savepoint.html), allowing nested
    /// transactions.
    ///
    /// ## Note
    ///
    /// Just like outer level transactions, savepoint transactions rollback by
    /// default.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # fn perform_queries_part_1_succeeds(_conn: &Connection) -> bool { true }
    /// fn perform_queries(conn: &mut Connection) -> Result<()> {
    ///     let mut tx = try!(conn.transaction());
    ///
    ///     {
    ///         let sp = try!(tx.savepoint());
    ///         if perform_queries_part_1_succeeds(&sp) {
    ///             try!(sp.commit());
    ///         }
    ///         // otherwise, sp will rollback
    ///     }
    ///
    ///     tx.commit()
    /// }
    /// ```
    pub fn savepoint(&mut self) -> Result<Savepoint> {
        Savepoint::with_depth(self.conn, 1)
    }

    /// Create a new savepoint with a custom savepoint name. See `savepoint()`.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint> {
        Savepoint::with_depth_and_name(self.conn, 1, name)
    }

    /// Get the current setting for what happens to the transaction when it is
    /// dropped.
    pub fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    /// Configure the transaction to perform the specified action when it is
    /// dropped.
    pub fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior
    }

    /// A convenience method which consumes and commits a transaction.
    pub fn commit(mut self) -> Result<()> {
        self.commit_()
    }

    fn commit_(&mut self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    /// A convenience method which consumes and rolls back a transaction.
    pub fn rollback(mut self) -> Result<()> {
        self.rollback_()
    }

    fn rollback_(&mut self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }

    /// Consumes the transaction, committing or rolling back according to the
    /// current setting (see `drop_behavior`).
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    pub fn finish(mut self) -> Result<()> {
        self.finish_()
    }

    fn finish_(&mut self) -> Result<()> {
        if self.conn.is_autocommit() {
            return Ok(());
        }
        match self.drop_behavior() {
            DropBehavior::Commit => self.commit_().or_else(|_| self.rollback_()),
            DropBehavior::Rollback => self.rollback_(),
            DropBehavior::Ignore => Ok(()),
            DropBehavior::Panic => panic!("Transaction dropped unexpectedly."),
        }
    }
}

impl<'conn> Deref for Transaction<'conn> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for Transaction<'conn> {
    fn drop(&mut self) {
        self.finish_();
    }
}

impl<'conn> Savepoint<'conn> {
    fn with_depth_and_name<T: Into<String>>(
        conn: &Connection,
        depth: u32,
        name: T,
    ) -> Result<Savepoint> {
        let name = name.into();
        conn.execute_batch(&format!("SAVEPOINT {}", name))
            .map(|_| Savepoint {
                conn,
                name,
                depth,
                drop_behavior: DropBehavior::Rollback,
                committed: false,
            })
    }

    fn with_depth(conn: &Connection, depth: u32) -> Result<Savepoint> {
        let name = format!("_rusqlite_sp_{}", depth);
        Savepoint::with_depth_and_name(conn, depth, name)
    }

    /// Begin a new savepoint. Can be nested.
    pub fn new(conn: &mut Connection) -> Result<Savepoint> {
        Savepoint::with_depth(conn, 0)
    }

    /// Begin a new savepoint with a user-provided savepoint name.
    pub fn with_name<T: Into<String>>(conn: &mut Connection, name: T) -> Result<Savepoint> {
        Savepoint::with_depth_and_name(conn, 0, name)
    }

    /// Begin a nested savepoint.
    pub fn savepoint(&mut self) -> Result<Savepoint> {
        Savepoint::with_depth(self.conn, self.depth + 1)
    }

    /// Begin a nested savepoint with a user-provided savepoint name.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint> {
        Savepoint::with_depth_and_name(self.conn, self.depth + 1, name)
    }

    /// Get the current setting for what happens to the savepoint when it is
    /// dropped.
    pub fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    /// Configure the savepoint to perform the specified action when it is
    /// dropped.
    pub fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior
    }

    /// A convenience method which consumes and commits a savepoint.
    pub fn commit(mut self) -> Result<()> {
        self.commit_()
    }

    fn commit_(&mut self) -> Result<()> {
        self.conn.execute_batch(&format!("RELEASE {}", self.name))?;
        self.committed = true;
        Ok(())
    }

    /// A convenience method which rolls back a savepoint.
    ///
    /// ## Note
    ///
    /// Unlike `Transaction`s, savepoints remain active after they have been
    /// rolled back, and can be rolled back again or committed.
    pub fn rollback(&mut self) -> Result<()> {
        self.conn
            .execute_batch(&format!("ROLLBACK TO {}", self.name))
    }

    /// Consumes the savepoint, committing or rolling back according to the
    /// current setting (see `drop_behavior`).
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    pub fn finish(mut self) -> Result<()> {
        self.finish_()
    }

    fn finish_(&mut self) -> Result<()> {
        if self.committed {
            return Ok(());
        }
        match self.drop_behavior() {
            DropBehavior::Commit => self.commit_().or_else(|_| self.rollback()),
            DropBehavior::Rollback => self.rollback(),
            DropBehavior::Ignore => Ok(()),
            DropBehavior::Panic => panic!("Savepoint dropped unexpectedly."),
        }
    }
}

impl<'conn> Deref for Savepoint<'conn> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for Savepoint<'conn> {
    fn drop(&mut self) {
        self.finish_();
    }
}

impl Connection {
    /// Begin a new transaction with the default behavior (DEFERRED).
    ///
    /// The transaction defaults to rolling back when it is dropped. If you
    /// want the transaction to commit, you must call `commit` or
    /// `set_drop_behavior(DropBehavior::Commit)`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
    /// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
    /// fn perform_queries(conn: &mut Connection) -> Result<()> {
    ///     let tx = try!(conn.transaction());
    ///
    ///     try!(do_queries_part_1(&tx)); // tx causes rollback if this fails
    ///     try!(do_queries_part_2(&tx)); // tx causes rollback if this fails
    ///
    ///     tx.commit()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn transaction(&mut self) -> Result<Transaction> {
        Transaction::new(self, TransactionBehavior::Deferred)
    }

    /// Begin a new transaction with a specified behavior.
    ///
    /// See `transaction`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn transaction_with_behavior(
        &mut self,
        behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        Transaction::new(self, behavior)
    }

    /// Begin a new savepoint with the default behavior (DEFERRED).
    ///
    /// The savepoint defaults to rolling back when it is dropped. If you want
    /// the savepoint to commit, you must call `commit` or
    /// `set_drop_behavior(DropBehavior::Commit)`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
    /// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
    /// fn perform_queries(conn: &mut Connection) -> Result<()> {
    ///     let sp = try!(conn.savepoint());
    ///
    ///     try!(do_queries_part_1(&sp)); // sp causes rollback if this fails
    ///     try!(do_queries_part_2(&sp)); // sp causes rollback if this fails
    ///
    ///     sp.commit()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn savepoint(&mut self) -> Result<Savepoint> {
        Savepoint::new(self)
    }

    /// Begin a new savepoint with a specified name.
    ///
    /// See `savepoint`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn savepoint_with_name<T: Into<String>>(&mut self, name: T) -> Result<Savepoint> {
        Savepoint::with_name(self, name)
    }
}

#[cfg(test)]
mod test {
    use super::DropBehavior;
    use Connection;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (x INTEGER)").unwrap();
        db
    }

    #[test]
    fn test_drop() {
        let mut db = checked_memory_handle();
        {
            let tx = db.transaction().unwrap();
            tx.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
            // default: rollback
        }
        {
            let mut tx = db.transaction().unwrap();
            tx.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
            tx.set_drop_behavior(DropBehavior::Commit)
        }
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                2i32,
                tx.query_row::<i32, _>("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_explicit_rollback_commit() {
        let mut db = checked_memory_handle();
        {
            let mut tx = db.transaction().unwrap();
            {
                let mut sp = tx.savepoint().unwrap();
                sp.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
                sp.rollback().unwrap();
                sp.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
                sp.commit().unwrap();
            }
            tx.commit().unwrap();
        }
        {
            let tx = db.transaction().unwrap();
            tx.execute_batch("INSERT INTO foo VALUES(4)").unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                6i32,
                tx.query_row::<i32, _>("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_savepoint() {
        let mut db = checked_memory_handle();
        {
            let mut tx = db.transaction().unwrap();
            tx.execute_batch("INSERT INTO foo VALUES(1)").unwrap();
            assert_current_sum(1, &tx);
            tx.set_drop_behavior(DropBehavior::Commit);
            {
                let mut sp1 = tx.savepoint().unwrap();
                sp1.execute_batch("INSERT INTO foo VALUES(2)").unwrap();
                assert_current_sum(3, &sp1);
                // will rollback sp1
                {
                    let mut sp2 = sp1.savepoint().unwrap();
                    sp2.execute_batch("INSERT INTO foo VALUES(4)").unwrap();
                    assert_current_sum(7, &sp2);
                    // will rollback sp2
                    {
                        let sp3 = sp2.savepoint().unwrap();
                        sp3.execute_batch("INSERT INTO foo VALUES(8)").unwrap();
                        assert_current_sum(15, &sp3);
                        sp3.commit().unwrap();
                        // committed sp3, but will be erased by sp2 rollback
                    }
                    assert_current_sum(15, &sp2);
                }
                assert_current_sum(3, &sp1);
            }
            assert_current_sum(1, &tx);
        }
        assert_current_sum(1, &db);
    }

    #[test]
    fn test_ignore_drop_behavior() {
        let mut db = checked_memory_handle();

        let mut tx = db.transaction().unwrap();
        {
            let mut sp1 = tx.savepoint().unwrap();
            insert(1, &sp1);
            sp1.rollback().unwrap();
            insert(2, &sp1);
            {
                let mut sp2 = sp1.savepoint().unwrap();
                sp2.set_drop_behavior(DropBehavior::Ignore);
                insert(4, &sp2);
            }
            assert_current_sum(6, &sp1);
            sp1.commit().unwrap();
        }
        assert_current_sum(6, &tx);
    }

    #[test]
    fn test_savepoint_names() {
        let mut db = checked_memory_handle();

        {
            let mut sp1 = db.savepoint_with_name("my_sp").unwrap();
            insert(1, &sp1);
            assert_current_sum(1, &sp1);
            {
                let mut sp2 = sp1.savepoint_with_name("my_sp").unwrap();
                sp2.set_drop_behavior(DropBehavior::Commit);
                insert(2, &sp2);
                assert_current_sum(3, &sp2);
                sp2.rollback().unwrap();
                assert_current_sum(1, &sp2);
                insert(4, &sp2);
            }
            assert_current_sum(5, &sp1);
            sp1.rollback().unwrap();
            {
                let mut sp2 = sp1.savepoint_with_name("my_sp").unwrap();
                sp2.set_drop_behavior(DropBehavior::Ignore);
                insert(8, &sp2);
            }
            assert_current_sum(8, &sp1);
            sp1.commit().unwrap();
        }
        assert_current_sum(8, &db);
    }

    fn insert(x: i32, conn: &Connection) {
        conn.execute("INSERT INTO foo VALUES(?)", &[&x]).unwrap();
    }

    fn assert_current_sum(x: i32, conn: &Connection) {
        let i = conn
            .query_row::<i32, _>("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
            .unwrap();
        assert_eq!(x, i);
    }
}
