//! Prepared statements cache for faster execution.

use crate::raw_statement::RawStatement;
use crate::{Connection, Result, Statement};
use hashlink::LruCache;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

impl Connection {
    /// Prepare a SQL statement for execution, returning a previously prepared
    /// (but not currently in-use) statement if one is available. The
    /// returned statement will be cached for reuse by future calls to
    /// [`prepare_cached`](Connection::prepare_cached) once it is dropped.
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     {
    ///         let mut stmt = conn.prepare_cached("INSERT INTO People (name) VALUES (?1)")?;
    ///         stmt.execute(["Joe Smith"])?;
    ///     }
    ///     {
    ///         // This will return the same underlying SQLite statement handle without
    ///         // having to prepare it again.
    ///         let mut stmt = conn.prepare_cached("INSERT INTO People (name) VALUES (?1)")?;
    ///         stmt.execute(["Bob Jones"])?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[inline]
    pub fn prepare_cached(&self, sql: &str) -> Result<CachedStatement<'_>> {
        self.cache.get(self, sql)
    }

    /// Set the maximum number of cached prepared statements this connection
    /// will hold. By default, a connection will hold a relatively small
    /// number of cached statements. If you need more, or know that you
    /// will not use cached statements, you
    /// can set the capacity manually using this method.
    #[inline]
    pub fn set_prepared_statement_cache_capacity(&self, capacity: usize) {
        self.cache.set_capacity(capacity);
    }

    /// Remove/finalize all prepared statements currently in the cache.
    #[inline]
    pub fn flush_prepared_statement_cache(&self) {
        self.cache.flush();
    }
}

/// Prepared statements LRU cache.
// #[derive(Debug)] // FIXME: https://github.com/kyren/hashlink/pull/4
pub struct StatementCache(RefCell<LruCache<Arc<str>, RawStatement>>);

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for StatementCache {}

/// Cacheable statement.
///
/// Statement will return automatically to the cache by default.
/// If you want the statement to be discarded, call
/// [`discard()`](CachedStatement::discard) on it.
pub struct CachedStatement<'conn> {
    stmt: Option<Statement<'conn>>,
    cache: &'conn StatementCache,
}

impl<'conn> Deref for CachedStatement<'conn> {
    type Target = Statement<'conn>;

    #[inline]
    fn deref(&self) -> &Statement<'conn> {
        self.stmt.as_ref().unwrap()
    }
}

impl<'conn> DerefMut for CachedStatement<'conn> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Statement<'conn> {
        self.stmt.as_mut().unwrap()
    }
}

impl Drop for CachedStatement<'_> {
    #[allow(unused_must_use)]
    #[inline]
    fn drop(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            self.cache.cache_stmt(unsafe { stmt.into_raw() });
        }
    }
}

impl CachedStatement<'_> {
    #[inline]
    fn new<'conn>(stmt: Statement<'conn>, cache: &'conn StatementCache) -> CachedStatement<'conn> {
        CachedStatement {
            stmt: Some(stmt),
            cache,
        }
    }

    /// Discard the statement, preventing it from being returned to its
    /// [`Connection`]'s collection of cached statements.
    #[inline]
    pub fn discard(mut self) {
        self.stmt = None;
    }
}

impl StatementCache {
    /// Create a statement cache.
    #[inline]
    pub fn with_capacity(capacity: usize) -> StatementCache {
        StatementCache(RefCell::new(LruCache::new(capacity)))
    }

    #[inline]
    fn set_capacity(&self, capacity: usize) {
        self.0.borrow_mut().set_capacity(capacity);
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

    #[inline]
    fn flush(&self) {
        let mut cache = self.0.borrow_mut();
        cache.clear();
    }
}

#[cfg(test)]
mod test {
    use super::StatementCache;
    use crate::{Connection, Result};
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
    fn test_cache() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let cache = &db.cache;
        let initial_capacity = cache.capacity();
        assert_eq!(0, cache.len());
        assert!(initial_capacity > 0);

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());

        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());

        cache.clear();
        assert_eq!(0, cache.len());
        assert_eq!(initial_capacity, cache.capacity());
        Ok(())
    }

    #[test]
    fn test_set_capacity() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let cache = &db.cache;

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());

        db.set_prepared_statement_cache_capacity(0);
        assert_eq!(0, cache.len());

        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(0, cache.len());

        db.set_prepared_statement_cache_capacity(8);
        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());
        Ok(())
    }

    #[test]
    fn test_discard() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let cache = &db.cache;

        let sql = "PRAGMA schema_version";
        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
            stmt.discard();
        }
        assert_eq!(0, cache.len());
        Ok(())
    }

    #[test]
    fn test_ddl() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            r#"
            CREATE TABLE foo (x INT);
            INSERT INTO foo VALUES (1);
        "#,
        )?;

        let sql = "SELECT * FROM foo";

        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(Ok(Some(1i32)), stmt.query([])?.map(|r| r.get(0)).next());
        }

        db.execute_batch(
            r#"
            ALTER TABLE foo ADD COLUMN y INT;
            UPDATE foo SET y = 2;
        "#,
        )?;

        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(
                Ok(Some((1i32, 2i32))),
                stmt.query([])?.map(|r| Ok((r.get(0)?, r.get(1)?))).next()
            );
        }
        Ok(())
    }

    #[test]
    fn test_connection_close() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.prepare_cached("SELECT * FROM sqlite_master;")?;

        conn.close().expect("connection not closed");
        Ok(())
    }

    #[test]
    fn test_cache_key() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let cache = &db.cache;
        assert_eq!(0, cache.len());

        //let sql = " PRAGMA schema_version; -- comment";
        let sql = "PRAGMA schema_version; ";
        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());

        {
            let mut stmt = db.prepare_cached(sql)?;
            assert_eq!(0, cache.len());
            assert_eq!(0, stmt.query_row([], |r| r.get::<_, i64>(0))?);
        }
        assert_eq!(1, cache.len());
        Ok(())
    }

    #[test]
    fn test_empty_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.prepare_cached("")?;
        Ok(())
    }
}
