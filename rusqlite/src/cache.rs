//! Prepared statements cache for faster execution.
extern crate lru_cache;

use std::cell::RefCell;
use {Result, Connection, Statement};
use self::lru_cache::LruCache;

/// Prepared statements cache.
///
/// FIXME limitation: the same SQL can be cached only once...
#[derive(Debug)]
pub struct StatementCache<'conn> {
    conn: &'conn Connection,
    cache: LruCache<String, Statement<'conn>>,
}

pub struct CachedStatement<'conn> {
    stmt: Statement<'conn>,
    cache: RefCell<StatementCache<'conn>>,
    pub cacheable : bool,
}

impl<'conn> Drop for CachedStatement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.cacheable {
            // FIXME: cannot move out of type `cache::CachedStatement<'conn>`, which defines the `Drop` trait [E0509]
            //self.cache.borrow_mut().release(self.stmt, false);
        } else {
            self.stmt.finalize_();
        }
    }
}

impl<'conn> StatementCache<'conn> {
    /// Create a statement cache.
    pub fn new(conn: &'conn Connection, capacity: usize) -> StatementCache<'conn> {
        StatementCache {
            conn: conn,
            cache: LruCache::new(capacity),
        }
    }

    /// Search the cache for a prepared-statement object that implements `sql`.
    // If no such prepared-statement can be found, allocate and prepare a new one.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no cached statement can be found and the underlying SQLite prepare call fails.
    pub fn get(&mut self, sql: &str) -> Result<Statement<'conn>> {
        let stmt = self.cache.remove(sql);
        match stmt {
            Some(stmt) => Ok(stmt),
            _ => self.conn.prepare(sql),
        }
    }

    /// If `discard` is true, then the statement is deleted immediately.
    /// Otherwise it is added to the LRU list and may be returned
    /// by a subsequent call to `get()`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `stmt` (or the already cached statement implementing the same SQL) statement is `discard`ed
    /// and the underlying SQLite finalize call fails.
    pub fn release(&mut self, mut stmt: Statement<'conn>, discard: bool) -> Result<()> {
        if discard {
            return stmt.finalize();
        }
        stmt.reset_if_needed();
        stmt.clear_bindings();
        self.cache.insert(stmt.sql(), stmt).map_or(Ok(()), |stmt| stmt.finalize())
    }

    /// Flush the prepared statement cache
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Return current cache size.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Return maximum cache size.
    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }

    /// Set the maximum number of cached statements.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.cache.set_capacity(capacity);
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use super::StatementCache;

    #[test]
    fn test_cache() {
        let db = Connection::open_in_memory().unwrap();
        let mut cache = StatementCache::new(&db, 10);
        assert_eq!(0, cache.len());
        assert_eq!(10, cache.capacity());

        let sql = "PRAGMA schema_version";
        let mut stmt = cache.get(sql).unwrap();
        assert_eq!(0, cache.len());
        assert_eq!(0, stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i64>(0));

        // println!("NEW {:?}", stmt);
        cache.release(stmt, false).unwrap();
        assert_eq!(1, cache.len());

        stmt = cache.get(sql).unwrap();
        assert_eq!(0, cache.len());
        assert_eq!(0, stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i64>(0));

        // println!("CACHED {:?}", stmt);
        cache.release(stmt, true).unwrap();
        assert_eq!(0, cache.len());

        cache.clear();
        assert_eq!(0, cache.len());
        assert_eq!(10, cache.capacity());
    }
}
