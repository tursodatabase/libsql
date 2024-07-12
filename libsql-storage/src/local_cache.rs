use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::rpc::Frame;
use libsql_sys::rusqlite::{ffi, params, Connection, Error, Result};

/// We use LocalCache to cache frames and transaction state. Each namespace gets its own cache
/// which is currently stored in a SQLite DB file, along with the main database file.
///
/// Frames Cache:
///     Frames are immutable. So we can cache all the frames locally, and it does not require them
///     to be fetched from the storage server. We cache the frame data with frame_no being the key.
///
/// Transaction State:
///     Whenever a transaction reads any pages from storage server, we cache them in the transaction
///     state. Since we want to provide a consistent view of the database, for the next reads we can
///     serve the pages from the cache. Any writes a transaction makes are cached too. At the time of
///     commit they are removed from the cache and sent to the storage server.

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
                frame_no INTEGER PRIMARY KEY NOT NULL,
                page_no INTEGER NOT NULL,
                data BLOB NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_page_no_frame_no ON frames (page_no, frame_no)",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT NOT NULL,
                page_no INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (txn_id, page_no)
            )",
            [],
        )?;
        Ok(())
    }

    pub fn insert_frame(&self, frame_no: u64, page_no: u32, frame_data: &[u8]) -> Result<()> {
        match self.conn.execute(
            "INSERT INTO frames (frame_no, page_no, data) VALUES (?1, ?2, ?3)",
            params![frame_no, page_no, frame_data],
        ) {
            Ok(_) => Ok(()),
            Err(Error::SqliteFailure(e, _)) if e.code == ffi::ErrorCode::ConstraintViolation => {
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert_frames(&mut self, frame_no: u64, frames: Vec<Frame>) -> Result<()> {
        let tx = self.conn.transaction().unwrap();
        {
            let mut stmt =
                tx.prepare("INSERT INTO frames (frame_no, page_no, data) VALUES (?1, ?2, ?3)")?;
            let mut frame_no = frame_no;
            for f in frames {
                frame_no += 1;
                stmt.execute(params![frame_no, f.page_no, f.data]).unwrap();
            }
        }
        tx.commit().unwrap();
        Ok(())
    }

    pub fn get_frame_by_page(&self, page_no: u32, max_frame_no: u64) -> Result<Option<Vec<u8>>> {
        let mut stmt = self.conn.prepare(
            "SELECT data FROM frames WHERE page_no=?1 AND frame_no <= ?2
            ORDER BY frame_no DESC LIMIT 1",
        )?;
        match stmt.query_row(params![page_no, max_frame_no], |row| row.get(0)) {
            Ok(frame_data) => Ok(Some(frame_data)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_max_frame_num(&self) -> Result<u64> {
        match self
            .conn
            .query_row("SELECT MAX(frame_no) from frames", (), |row| {
                row.get::<_, Option<u64>>(0)
            }) {
            Ok(Some(frame_no)) => Ok(frame_no),
            Ok(None) | Err(Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(e),
        }
    }

    pub fn insert_page(&self, txn_id: &str, page_no: u32, frame_data: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO transactions (txn_id, page_no, data) VALUES (?1, ?2, ?3)
                 ON CONFLICT(txn_id, page_no) DO UPDATE SET data = ?3",
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
