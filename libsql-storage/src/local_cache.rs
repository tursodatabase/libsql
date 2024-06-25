use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql_sys::rusqlite::{params, Connection, Error, Result};

pub struct LocalCache {
    conn: Connection,
    path: Arc<Path>,
}

impl LocalCache {
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let path: Arc<Path> = Arc::from(PathBuf::from(db_path));
        let local_cache = LocalCache { conn, path };
        local_cache.create_table()?;
        Ok(local_cache)
    }

    fn create_table(&self) -> Result<()> {
        self.conn.pragma_update(None, "journal_mode", &"WAL")?;
        self.conn.pragma_update(None, "synchronous", &"NORMAL")?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS frames (
                namespace TEXT NOT NULL,
                frame_no INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (namespace, frame_no)
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT PRIMARY KEY NOT NULL,
                page_no INTEGER NOT NULL,
                data BLOB NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    pub fn insert_frame(&self, namespace: &str, frame_no: u64, frame_data: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO frames (namespace, frame_no, data) VALUES (?1, ?2, ?3)",
            params![namespace, frame_no, frame_data],
        )?;
        Ok(())
    }

    pub fn get_frame(&self, namespace: &str, frame_no: u64) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT data FROM frames WHERE namespace = ?1 AND frame_no = ?2")?;
        match stmt.query_row(params![namespace, frame_no], |row| row.get(0)) {
            Ok(frame_data) => Ok(Some(frame_data)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn insert_page(&self, txn_id: &str, page_no: u32, frame_data: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO transactions (txn_id, page_no, data) VALUES (?1, ?2, ?3)",
            params![txn_id, page_no, frame_data],
        )?;
        Ok(())
    }

    pub fn get_page(&self, txn_id: &str, page_no: u32) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT data FROM transactions WHERE txn_id = ?1 AND page_no = ?2")?;
        match stmt.query_row(params![txn_id, page_no], |row| row.get(0)) {
            Ok(frame_data) => Ok(Some(frame_data)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_all_pages(&self, txn_id: &str) -> Result<Vec<(u32, Vec<u8>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT page_no, data FROM transactions WHERE txn_id = ?1")?;
        let pages: Result<Vec<(u32, Vec<u8>)>> = stmt
            .query_map(params![txn_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect();
        self.conn.execute(
            "DELETE FROM transactions WHERE txn_id = ?1",
            params![txn_id],
        )?;
        pages
    }
}

impl Clone for LocalCache {
    fn clone(&self) -> Self {
        let conn = Connection::open(&*self.path).expect("failed to open database");
        LocalCache {
            conn,
            path: Arc::clone(&self.path),
        }
    }
}
