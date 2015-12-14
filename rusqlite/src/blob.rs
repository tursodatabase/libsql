//! incremental BLOB I/O
use std::io;
use std::mem;
use std::ptr;

use super::ffi;
use {Error, Result, Connection, DatabaseName};

/// Handle to an open BLOB
pub struct Blob<'conn> {
    conn: &'conn Connection,
    blob: *mut ffi::sqlite3_blob,
    pos: i32,
}

impl Connection {
    /// Open a handle to the BLOB located in `row`, `column`, `table` in database `db`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `db`/`table`/`column` cannot be converted to a C-compatible string or if the
    /// underlying SQLite BLOB open call fails.
    pub fn blob_open<'a>(&'a self,
                         db: DatabaseName,
                         table: &str,
                         column: &str,
                         row: i64,
                         read_only: bool)
                         -> Result<Blob<'a>> {
        let mut c = self.db.borrow_mut();
        let mut blob = ptr::null_mut();
        let db = try!(db.to_cstring());
        let table = try!(super::str_to_cstring(table));
        let column = try!(super::str_to_cstring(column));
        let rc = unsafe {
            ffi::sqlite3_blob_open(c.db(),
                                   db.as_ptr(),
                                   table.as_ptr(),
                                   column.as_ptr(),
                                   row,
                                   if read_only {
                                       0
                                   } else {
                                       1
                                   },
                                   &mut blob)
        };
        c.decode_result(rc).map(|_| {
            Blob {
                conn: self,
                blob: blob,
                pos: 0,
            }
        })
    }
}

impl<'conn> Blob<'conn> {
    /// Move a BLOB handle to a new row
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite BLOB reopen call fails.
    pub fn reopen(&mut self, row: i64) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_blob_reopen(self.blob, row) };
        if rc != ffi::SQLITE_OK {
            return self.conn.decode_result(rc);
        }
        self.pos = 0;
        Ok(())
    }

    /// Return the size in bytes of the BLOB
    pub fn size(&self) -> i32 {
        unsafe { ffi::sqlite3_blob_bytes(self.blob) }
    }

    /// Close a BLOB handle
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite close call fails.
    pub fn close(mut self) -> Result<()> {
        self.close_()
    }

    fn close_(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_blob_close(self.blob) };
        self.blob = ptr::null_mut();
        self.conn.decode_result(rc)
    }
}

impl<'conn> io::Read for Blob<'conn> {
    /// Read data from a BLOB incrementally
    ///
    /// # Failure
    ///
    /// Will return `Err` if `buf` length > i32 max value or if the underlying SQLite read call fails.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.len() > ::std::i32::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      Error {
                                          code: ffi::SQLITE_TOOBIG,
                                          message: "buffer too long".to_string(),
                                      }));
        }
        let mut n = buf.len() as i32;
        let size = self.size();
        if self.pos + n > size {
            n = size - self.pos;
        }
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe {
            ffi::sqlite3_blob_read(self.blob, mem::transmute(buf.as_ptr()), n, self.pos)
        };
        self.conn
            .decode_result(rc)
            .map(|_| {
                self.pos += n;
                n as usize
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}

impl<'conn> io::Write for Blob<'conn> {
    /// Write data into a BLOB incrementally
    ///
    /// This function may only modify the contents of the BLOB; it is not possible to increase the size of a BLOB using this API.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `buf` length > i32 max value or if `buf` length + offset > BLOB size
    /// or if the underlying SQLite write call fails.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > ::std::i32::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      Error {
                                          code: ffi::SQLITE_TOOBIG,
                                          message: "buffer too long".to_string(),
                                      }));
        }
        let n = buf.len() as i32;
        let size = self.size();
        if self.pos + n > size {
            return Err(io::Error::new(io::ErrorKind::Other,
                                      Error {
                                          code: ffi::SQLITE_MISUSE,
                                          message: format!("pos = {} + n = {} > size = {}",
                                                           self.pos,
                                                           n,
                                                           size),
                                      }));
        }
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe {
            ffi::sqlite3_blob_write(self.blob, mem::transmute(buf.as_ptr()), n, self.pos)
        };
        self.conn
            .decode_result(rc)
            .map(|_| {
                self.pos += n;
                n as usize
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'conn> io::Seek for Blob<'conn> {
    /// Seek to an offset, in bytes, in BLOB.
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            io::SeekFrom::Start(offset) => offset as i64,
            io::SeekFrom::Current(offset) => self.pos as i64 + offset,
            io::SeekFrom::End(offset) => self.size() as i64 + offset,
        };

        if pos < 0 {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                               "invalid seek to negative position"))
        } else if pos > ::std::i32::MAX as i64 {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                               "invalid seek to position > i32::MAX"))
        } else {
            self.pos = pos as i32;
            Ok(pos as u64)
        }
    }
}

#[allow(unused_must_use)]
impl<'conn> Drop for Blob<'conn> {
    fn drop(&mut self) {
        self.close_();
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufReader, BufRead, Read, Write, Seek, SeekFrom};
    use {Connection, DatabaseName, Result};

    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn db_with_test_blob() -> Result<(Connection, i64)> {
        let db = try!(Connection::open_in_memory());
        let sql = "BEGIN;
                   CREATE TABLE test (content BLOB);
                   INSERT INTO test VALUES (ZEROBLOB(10));
                   END;";
        try!(db.execute_batch(sql));
        let rowid = db.last_insert_rowid();
        Ok((db, rowid))
    }

    #[test]
    fn test_blob() {
        let (db, rowid) = db_with_test_blob().unwrap();

        let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false).unwrap();
        assert_eq!(4, blob.write(b"Clob").unwrap());
        assert!(blob.write(b"5678901").is_err()); // cannot write past 10
        assert_eq!(4, blob.write(b"5678").unwrap());

        blob.reopen(rowid).unwrap();
        blob.close().unwrap();

        blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, true).unwrap();
        let mut bytes = [0u8; 5];
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"Clob5");
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"678\0\0");
        assert_eq!(0, blob.read(&mut bytes[..]).unwrap());

        blob.seek(SeekFrom::Start(2)).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"ob567");

        blob.seek(SeekFrom::Current(-6)).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"lob56");

        blob.seek(SeekFrom::End(-6)).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"5678\0");

        blob.reopen(rowid).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"Clob5");
    }

    #[test]
    fn test_blob_in_bufreader() {
        let (db, rowid) = db_with_test_blob().unwrap();

        let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false).unwrap();
        assert_eq!(8, blob.write(b"one\ntwo\n").unwrap());

        blob.reopen(rowid).unwrap();
        let mut reader = BufReader::new(blob);

        let mut line = String::new();
        assert_eq!(4, reader.read_line(&mut line).unwrap());
        assert_eq!("one\n", line);

        line.truncate(0);
        assert_eq!(4, reader.read_line(&mut line).unwrap());
        assert_eq!("two\n", line);

        line.truncate(0);
        assert_eq!(2, reader.read_line(&mut line).unwrap());
        assert_eq!("\0\0", line);
    }
}
