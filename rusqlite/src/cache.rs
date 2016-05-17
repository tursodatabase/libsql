//! Prepared statements cache for faster execution.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use {Result, Connection, Statement};
use raw_statement::RawStatement;

impl Connection {
    /// Prepare a SQL statement for execution, returning a previously prepared (but
    /// not currently in-use) statement if one is available. The returned statement
    /// will be cached for reuse by future calls to `prepare_cached` once it is
    /// dropped.
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     {
    ///         let mut stmt = try!(conn.prepare_cached("INSERT INTO People (name) VALUES (?)"));
    ///         try!(stmt.execute(&[&"Joe Smith"]));
    ///     }
    ///     {
    ///         // This will return the same underlying SQLite statement handle without
    ///         // having to prepare it again.
    ///         let mut stmt = try!(conn.prepare_cached("INSERT INTO People (name) VALUES (?)"));
    ///         try!(stmt.execute(&[&"Bob Jones"]));
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn prepare_cached<'a>(&'a self, sql: &str) -> Result<CachedStatement<'a>> {
        self.cache.get(&self, sql)
    }
}

/// Prepared statements LRU cache.
#[derive(Debug)]
pub struct StatementCache {
    cache: RefCell<VecDeque<RawStatement>>, // back = LRU
}

/// Cacheable statement.
///
/// Statement will return automatically to the cache by default.
/// If you want the statement to be discarded, call `discard()` on it.
pub struct CachedStatement<'conn> {
    stmt: Option<Statement<'conn>>,
    cache: &'conn StatementCache,
}

impl<'conn> Deref for CachedStatement<'conn> {
    type Target = Statement<'conn>;

    fn deref(&self) -> &Statement<'conn> {
        self.stmt.as_ref().unwrap()
    }
}

impl<'conn> DerefMut for CachedStatement<'conn> {
    fn deref_mut(&mut self) -> &mut Statement<'conn> {
        self.stmt.as_mut().unwrap()
    }
}

impl<'conn> Drop for CachedStatement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            self.cache.cache_stmt(stmt.into());
        }
    }
}

impl<'conn> CachedStatement<'conn> {
    fn new(stmt: Statement<'conn>, cache: &'conn StatementCache) -> CachedStatement<'conn> {
        CachedStatement {
            stmt: Some(stmt),
            cache: cache,
        }
    }

    pub fn discard(mut self) {
        self.stmt = None;
    }
}

impl StatementCache {
    /// Create a statement cache.
    pub fn with_capacity(capacity: usize) -> StatementCache {
        StatementCache { cache: RefCell::new(VecDeque::with_capacity(capacity)) }
    }

    /// Search the cache for a prepared-statement object that implements `sql`.
    /// If no such prepared-statement can be found, allocate and prepare a new one.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no cached statement can be found and the underlying SQLite prepare
    /// call fails.
    pub fn get<'conn>(&'conn self,
                      conn: &'conn Connection,
                      sql: &str)
                      -> Result<CachedStatement<'conn>> {
        let mut cache = self.cache.borrow_mut();
        let stmt = match cache.iter()
            .rposition(|entry| entry.sql().to_bytes().eq(sql.as_bytes())) {
            Some(index) => {
                let raw_stmt = cache.swap_remove_front(index).unwrap(); // FIXME Not LRU compliant
                Ok(Statement::new(conn, raw_stmt))
            }
            _ => conn.prepare(sql),
        };
        stmt.map(|stmt| CachedStatement::new(stmt, self))
    }

    // Return a statement to the cache.
    fn cache_stmt(&self, stmt: RawStatement) {
        let mut cache = self.cache.borrow_mut();
        if cache.capacity() == cache.len() {
            // is full
            cache.pop_back(); // LRU dropped
        }
        stmt.clear_bindings();
        cache.push_front(stmt)
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use super::StatementCache;

    impl StatementCache {
        fn clear(&self) {
            self.cache.borrow_mut().clear();
        }

        fn len(&self) -> usize {
            self.cache.borrow().len()
        }

        fn capacity(&self) -> usize {
            self.cache.borrow().capacity()
        }
    }

    #[test]
    fn test_cache() {
        let db = Connection::open_in_memory().unwrap();
        let cache = &db.cache;
        let initial_capacity = cache.capacity();
        assert_eq!(0, cache.len());
        assert!(initial_capacity > 0);

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(0,
                       stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i32, i64>(0));
        }
        assert_eq!(1, cache.len());

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(0,
                       stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i32, i64>(0));
        }
        assert_eq!(1, cache.len());

        cache.clear();
        assert_eq!(0, cache.len());
        assert_eq!(initial_capacity, cache.capacity());
    }

    #[test]
    fn test_discard() {
        let db = Connection::open_in_memory().unwrap();
        let cache = &db.cache;

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(0,
                       stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i32, i64>(0));
            stmt.discard();
        }
        assert_eq!(0, cache.len());
    }
}
