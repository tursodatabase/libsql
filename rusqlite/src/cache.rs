//! Prepared statements cache for faster execution.

use crate::raw_statement::RawStatement;
use crate::{Connection, Result, Statement};
use lru_cache::LruCache;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

impl Connection {
    /// Prepare a SQL statement for execution, returning a previously prepared
    /// (but not currently in-use) statement if one is available. The
    /// returned statement will be cached for reuse by future calls to
    /// `prepare_cached` once it is dropped.
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     {
    ///         let mut stmt = conn.prepare_cached("INSERT INTO People (name) VALUES (?)")?;
    ///         stmt.execute(&["Joe Smith"])?;
    ///     }
    ///     {
    ///         // This will return the same underlying SQLite statement handle without
    ///         // having to prepare it again.
    ///         let mut stmt = conn.prepare_cached("INSERT INTO People (name) VALUES (?)")?;
    ///         stmt.execute(&["Bob Jones"])?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn prepare_cached(&self, sql: &str) -> Result<CachedStatement<'_>> {
        self.cache.get(self, sql)
    }

    /// Set the maximum number of cached prepared statements this connection
    /// will hold. By default, a connection will hold a relatively small
    /// number of cached statements. If you need more, or know that you
    /// will not use cached statements, you
    /// can set the capacity manually using this method.
    pub fn set_prepared_statement_cache_capacity(&self, capacity: usize) {
        self.cache.set_capacity(capacity)
    }

    /// Remove/finalize all prepared statements currently in the cache.
    pub fn flush_prepared_statement_cache(&self) {
        self.cache.flush()
    }
}

/// Prepared statements LRU cache.
#[derive(Debug)]
pub struct StatementCache(RefCell<LruCache<Arc<str>, RawStatement>>);

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

impl Drop for CachedStatement<'_> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            self.cache.cache_stmt(stmt.into());
        }
    }
}

impl CachedStatement<'_> {
    fn new<'conn>(stmt: Statement<'conn>, cache: &'conn StatementCache) -> CachedStatement<'conn> {
        CachedStatement {
            stmt: Some(stmt),
            cache,
        }
    }

    /// Discard the statement, preventing it from being returned to its
    /// `Connection`'s collection of cached statements.
    pub fn discard(mut self) {
        self.stmt = None;
    }
}

impl StatementCache {
    /// Create a statement cache.
    pub fn with_capacity(capacity: usize) -> StatementCache {
        StatementCache(RefCell::new(LruCache::new(capacity)))
    }

    fn set_capacity(&self, capacity: usize) {
        self.0.borrow_mut().set_capacity(capacity)
    }

    // Search the cache for a prepared-statement object that implements `sql`.
    // If no such prepared-statement can be found, allocate and prepare a new one.
    //
    // # Failure
    //
    // Will return `Err` if no cached statement can be found and the underlying
    // SQLite prepare call fails.
    fn get<'conn>(
        &'conn self,
        conn: &'conn Connection,
        sql: &str,
    ) -> Result<CachedStatement<'conn>> {
        let trimmed = sql.trim();
        let mut cache = self.0.borrow_mut();
        let stmt = match cache.remove(trimmed) {
            Some(raw_stmt) => Ok(Statement::new(conn, raw_stmt)),
            None => conn.prepare(trimmed),
        };
        stmt.map(|mut stmt| {
            stmt.stmt.set_statement_cache_key(trimmed);
            CachedStatement::new(stmt, self)
        })
    }

    // Return a statement to the cache.
    fn cache_stmt(&self, stmt: RawStatement) {
        if stmt.is_null() {
            return;
        }
        let mut cache = self.0.borrow_mut();
        stmt.clear_bindings();
        if let Some(sql) = stmt.statement_cache_key() {
            cache.insert(sql, stmt);
        } else {
            debug_assert!(
                false,
                "bug in statement cache code, statement returned to cache that without key"
            );
        }
    }

    fn flush(&self) {
        let mut cache = self.0.borrow_mut();
        cache.clear()
    }
}

#[cfg(test)]
mod test {
    use super::StatementCache;
    use crate::{Connection, NO_PARAMS};
    use fallible_iterator::FallibleIterator;

    impl StatementCache {
        fn clear(&self) {
            self.0.borrow_mut().clear();
        }

        fn len(&self) -> usize {
            self.0.borrow().len()
        }

        fn capacity(&self) -> usize {
            self.0.borrow().capacity()
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
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());

        cache.clear();
        assert_eq!(0, cache.len());
        assert_eq!(initial_capacity, cache.capacity());
    }

    #[test]
    fn test_set_capacity() {
        let db = Connection::open_in_memory().unwrap();
        let cache = &db.cache;

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());

        db.set_prepared_statement_cache_capacity(0);
        assert_eq!(0, cache.len());

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(0, cache.len());

        db.set_prepared_statement_cache_capacity(8);
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());
    }

    #[test]
    fn test_discard() {
        let db = Connection::open_in_memory().unwrap();
        let cache = &db.cache;

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
            stmt.discard();
        }
        assert_eq!(0, cache.len());
    }

    #[test]
    fn test_ddl() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            r#"
            CREATE TABLE foo (x INT);
            INSERT INTO foo VALUES (1);
        "#,
        )
        .unwrap();

        let sql = "SELECT * FROM foo";

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(
                Ok(Some(1i32)),
                stmt.query(NO_PARAMS).unwrap().map(|r| r.get(0)).next()
            );
        }

        db.execute_batch(
            r#"
            ALTER TABLE foo ADD COLUMN y INT;
            UPDATE foo SET y = 2;
        "#,
        )
        .unwrap();

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(
                Ok(Some((1i32, 2i32))),
                stmt.query(NO_PARAMS)
                    .unwrap()
                    .map(|r| Ok((r.get(0)?, r.get(1)?)))
                    .next()
            );
        }
    }

    #[test]
    fn test_connection_close() {
        let conn = Connection::open_in_memory().unwrap();
        conn.prepare_cached("SELECT * FROM sqlite_master;").unwrap();

        conn.close().expect("connection not closed");
    }

    #[test]
    fn test_cache_key() {
        let db = Connection::open_in_memory().unwrap();
        let cache = &db.cache;
        assert_eq!(0, cache.len());

        //let sql = " PRAGMA schema_version; -- comment";
        let sql = "PRAGMA schema_version; ";
        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());

        {
            let mut stmt = db.prepare_cached(sql).unwrap();
            assert_eq!(0, cache.len());
            assert_eq!(
                0,
                stmt.query_row(NO_PARAMS, |r| r.get::<_, i64>(0)).unwrap()
            );
        }
        assert_eq!(1, cache.len());
    }

    #[test]
    fn test_empty_stmt() {
        let conn = Connection::open_in_memory().unwrap();
        conn.prepare_cached("").unwrap();
    }
}
