//! Prepared statements cache for faster execution.

use std::cell::RefCell;
use std::collections::VecDeque;
use {Result, Connection, Statement};

/// Prepared statements cache.
#[derive(Debug)]
pub struct StatementCache<'conn> {
    conn: &'conn Connection,
    cache: VecDeque<Statement<'conn>>, // back = LRU
}

pub struct CachedStatement<'conn> {
    stmt: Option<Statement<'conn>>,
    cache: RefCell<StatementCache<'conn>>,
    pub cacheable: bool,
}

impl<'conn> Drop for CachedStatement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.cacheable {
            self.cache.borrow_mut().release(self.stmt.take().unwrap());
        } else {
            self.stmt.take().unwrap().finalize();
        }
    }
}

impl<'conn> StatementCache<'conn> {
    /// Create a statement cache.
    pub fn new(conn: &'conn Connection, capacity: usize) -> StatementCache<'conn> {
        StatementCache {
            conn: conn,
            cache: VecDeque::with_capacity(capacity),
        }
    }

    /// Search the cache for a prepared-statement object that implements `sql`.
    // If no such prepared-statement can be found, allocate and prepare a new one.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no cached statement can be found and the underlying SQLite prepare call fails.
    pub fn get(&mut self, sql: &str) -> Result<Statement<'conn>> {
        match self.cache.iter().rposition(|entry| entry.eq(sql)) {
            Some(index) => Ok(self.cache.swap_remove_front(index).unwrap()), // FIXME Not LRU compliant
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
    fn release(&mut self, mut stmt: Statement<'conn>) {
        if self.cache.capacity() == self.cache.len() { // is full
            self.cache.pop_back(); // LRU dropped
        }
        stmt.reset_if_needed();
        stmt.clear_bindings();
        self.cache.push_front(stmt)
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
}

#[cfg(test)]
mod test {
    use Connection;
    use super::StatementCache;

    #[test]
    fn test_cache() {
        let db = Connection::open_in_memory().unwrap();
        let mut cache = StatementCache::new(&db, 15);
        assert_eq!(0, cache.len());
        assert_eq!(15, cache.capacity());

        let sql = "PRAGMA schema_version";
        let mut stmt = cache.get(sql).unwrap();
        assert_eq!(0, cache.len());
        assert_eq!(0,
                   stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i64>(0));

        // println!("NEW {:?}", stmt);
        cache.release(stmt);
        assert_eq!(1, cache.len());

        stmt = cache.get(sql).unwrap();
        assert_eq!(0, cache.len());
        assert_eq!(0,
                   stmt.query(&[]).unwrap().get_expected_row().unwrap().get::<i64>(0));

        // println!("CACHED {:?}", stmt);
        cache.release(stmt);
        assert_eq!(1, cache.len());

        cache.clear();
        assert_eq!(0, cache.len());
        assert_eq!(15, cache.capacity());
    }
}
