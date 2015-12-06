//! Prepared statements cache for faster execution.
extern crate lru_cache;

use {SqliteResult, SqliteConnection, SqliteStatement};
use self::lru_cache::LruCache;

/// Prepared statements cache.
///
/// FIXME limitation: the same SQL can be cached only once...
#[derive(Debug)]
pub struct StatementCache<'conn> {
    pub conn: &'conn SqliteConnection,
    cache: LruCache<String, SqliteStatement<'conn>>,
}

impl<'conn> StatementCache<'conn> {
    /// Create a statement cache.
    pub fn new(conn: &'conn SqliteConnection, capacity: usize) -> StatementCache<'conn> {
        StatementCache{ conn: conn,  cache: LruCache::new(capacity) }
    }

    /// Search the cache for a prepared-statement object that implements `sql`.
    // If no such prepared-statement can be found, allocate and prepare a new one.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no cached statement can be found and the underlying SQLite prepare call fails.
    pub fn get(&mut self, sql: &str) -> SqliteResult<SqliteStatement<'conn>> {
        let stmt = self.cache.remove(sql);
        match stmt {
            Some(stmt) => Ok(stmt),
            _ => self.conn.prepare(sql)
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
    pub fn release(&mut self, stmt: SqliteStatement<'conn>, discard: bool) -> SqliteResult<()> {
        if discard {
            return stmt.finalize();
        }
        stmt.reset_if_needed();
        // clear bindings ???
        self.cache.insert(stmt.sql(), stmt).map_or(Ok(()), |stmt| stmt.finalize())
    }

    /// Flush the prepared statement cache
    pub fn flush(&mut self) {
        self.cache.clear();
    }

    /// Return (current, max) sizes.
    pub fn size(&self) -> (usize, usize) {
        (self.cache.len(), self.cache.capacity())
    }

    /// Set the maximum number of cached statements.
    pub fn set_size(&mut self, capacity: usize) {
        self.cache.set_capacity(capacity);
    }
}

#[cfg(test)]
mod test {
    use SqliteConnection;
    use super::StatementCache;

   #[test]
    fn test_cache() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let mut cache = StatementCache::new(&db, 10);
        let sql = "PRAGMA schema_version";
        let mut stmt = cache.get(sql).unwrap();
        //println!("NEW {:?}", stmt);
        cache.release(stmt, false).unwrap();
        stmt = cache.get(sql).unwrap();
        //println!("CACHED {:?}", stmt);
        cache.release(stmt, true).unwrap();
        cache.flush();
    }
}
