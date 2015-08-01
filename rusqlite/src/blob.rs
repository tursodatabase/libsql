use std::mem;
use std::ptr;

use super::ffi;
use {SqliteError, SqliteResult, SqliteConnection};

pub struct SqliteBlob<'conn> {
    conn: &'conn SqliteConnection,
    blob: *mut ffi::sqlite3_blob,
    pos: i32,
}

pub enum SeekFrom {
    Start(i32),
    End(i32),
    Current(i32),
}

impl SqliteConnection {
    pub fn blob_open<'a>(&'a self, db: &str, table: &str, column: &str, row: i64, read_only: bool) -> SqliteResult<SqliteBlob<'a>> {
        let mut c = self.db.borrow_mut();
        let mut blob = ptr::null_mut();
        let db = try!(super::str_to_cstring(db));
        let table = try!(super::str_to_cstring(table));
        let column = try!(super::str_to_cstring(column));
        let rc = unsafe{ ffi::sqlite3_blob_open(c.db(), db.as_ptr(), table.as_ptr(), column.as_ptr(), row, if read_only { 0 } else { 1 }, &mut blob) };
        c.decode_result(rc).map(|_| {
            SqliteBlob{ conn: self, blob: blob, pos: 0 }
        })
    }
}

impl<'conn> SqliteBlob<'conn> {
    pub fn reopen(&mut self, row: i64) -> SqliteResult<()> {
        let rc = unsafe{ ffi::sqlite3_blob_reopen(self.blob, row) };
        if rc != ffi::SQLITE_OK {
            return self.conn.decode_result(rc);
        }
        self.pos = 0;
        Ok(())
    }

    pub fn size(&self) -> i32 {
        unsafe{ ffi::sqlite3_blob_bytes(self.blob) }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> SqliteResult<i32> {
        let mut n = buf.len() as i32;
        let size = self.size();
        if self.pos + n > size {
            n = size - self.pos;
        }
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe { ffi::sqlite3_blob_read(self.blob, mem::transmute(buf.as_ptr()), n, self.pos) };
        self.conn.decode_result(rc).map(|_| {
            self.pos += n;
            n
        })
    }

    pub fn write(&mut self, buf: &[u8]) -> SqliteResult<i32> {
        let n = buf.len() as i32;
        let size = self.size();
        if self.pos + n > size {
            return Err(SqliteError{code: ffi::SQLITE_MISUSE, message: format!("pos = {} + n = {} > size = {}", self.pos, n, size)});
        }
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe { ffi::sqlite3_blob_write(self.blob, mem::transmute(buf.as_ptr()), n, self.pos) };
        self.conn.decode_result(rc).map(|_| {
            self.pos += n;
            n
        })
    }

    pub fn seek(&mut self, pos: SeekFrom) {
        self.pos  = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self.pos + offset,
            SeekFrom::End(offset) => self.size() + offset
        };
    }

    pub fn close(mut self) -> SqliteResult<()> {
        self.close_()
    }

    fn close_(&mut self) -> SqliteResult<()> {
        let rc = unsafe { ffi::sqlite3_blob_close(self.blob) };
        self.blob = ptr::null_mut();
        self.conn.decode_result(rc)
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for SqliteBlob<'conn> {
    fn drop(&mut self) {
        self.close_();
    }
}

#[cfg(test)]
mod test {
    use SqliteConnection;

   #[test]
    fn test_blob() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let sql = "BEGIN;
                CREATE TABLE test (content BLOB);
                INSERT INTO test VALUES (ZEROBLOB(10));
                END;";
        db.execute_batch(sql).unwrap();
        let rowid = db.last_insert_rowid();

        let mut blob = db.blob_open("main", "test", "content", rowid, false).unwrap();
        blob.write(b"Clob").unwrap();
        let err = blob.write(b"5678901");
        //writeln!(io::stderr(), "{:?}", err);
        assert!(err.is_err());

        assert!(blob.reopen(rowid).is_ok());
        assert!(blob.close().is_ok());

        blob = db.blob_open("main", "test", "content", rowid, true).unwrap();
        let mut bytes = [0u8; 5];
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(0, blob.read(&mut bytes[..]).unwrap());

        assert!(blob.reopen(rowid).is_ok());
        blob.seek(super::SeekFrom::Start(0));
    }
}