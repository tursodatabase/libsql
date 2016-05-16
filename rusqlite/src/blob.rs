//! Incremental BLOB I/O.
//!
//! Note that SQLite does not provide API-level access to change the size of a BLOB; that must
//! be performed through SQL statements.
//!
//! `Blob` conforms to `std::io::Read`, `std::io::Write`, and `std::io::Seek`, so it plays
//! nicely with other types that build on these (such as `std::io::BufReader` and
//! `std::io::BufWriter`). However, you must be careful with the size of the blob. For example,
//! when using a `BufWriter`, the `BufWriter` will accept more data than the `Blob` will allow,
//! so make sure to call `flush` and check for errors. (See the unit tests in this module for
//! an example.)
//!
//! ## Example
//!
//! ```rust
//! extern crate libsqlite3_sys;
//! extern crate rusqlite;
//!
//! use rusqlite::{Connection, DatabaseName};
//! use rusqlite::blob::ZeroBlob;
//! use std::io::{Read, Write, Seek, SeekFrom};
//!
//! fn main() {
//!     let db = Connection::open_in_memory().unwrap();
//!     db.execute_batch("CREATE TABLE test (content BLOB);").unwrap();
//!     db.execute("INSERT INTO test (content) VALUES (ZEROBLOB(10))", &[]).unwrap();
//!
//!     let rowid = db.last_insert_rowid();
//!     let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false).unwrap();
//!
//!     // Make sure to test that the number of bytes written matches what you expect;
//!     // if you try to write too much, the data will be truncated to the size of the BLOB.
//!     let bytes_written = blob.write(b"01234567").unwrap();
//!     assert_eq!(bytes_written, 8);
//!
//!     // Same guidance - make sure you check the number of bytes read!
//!     blob.seek(SeekFrom::Start(0)).unwrap();
//!     let mut buf = [0u8; 20];
//!     let bytes_read = blob.read(&mut buf[..]).unwrap();
//!     assert_eq!(bytes_read, 10); // note we read 10 bytes because the blob has size 10
//!
//!     db.execute("INSERT INTO test (content) VALUES (?)", &[&ZeroBlob(64)]).unwrap();
//!
//!     // given a new row ID, we can reopen the blob on that row
//!     let rowid = db.last_insert_rowid();
//!     blob.reopen(rowid).unwrap();
//!
//!     assert_eq!(blob.size(), 64);
//! }
//! ```
use std::io;
use std::cmp::min;
use std::mem;
use std::ptr;
use libc::c_int;

use super::ffi;
use super::types::ToSql;
use {Result, Connection, DatabaseName};

/// Handle to an open BLOB.
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
    /// Will return `Err` if `db`/`table`/`column` cannot be converted to a C-compatible string
    /// or if the underlying SQLite BLOB open call fails.
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
    /// Move a BLOB handle to a new row.
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

    /// Return the size in bytes of the BLOB.
    pub fn size(&self) -> i32 {
        unsafe { ffi::sqlite3_blob_bytes(self.blob) }
    }

    /// Close a BLOB handle.
    ///
    /// Calling `close` explicitly is not required (the BLOB will be closed when the
    /// `Blob` is dropped), but it is available so you can get any errors that occur.
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
    /// Read data from a BLOB incrementally. Will return Ok(0) if the end of the blob
    /// has been reached.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite read call fails.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let max_allowed_len = (self.size() - self.pos) as usize;
        let n = min(buf.len(), max_allowed_len) as i32;
        if n <= 0 {
            return Ok(0);
        }
        let rc =
            unsafe { ffi::sqlite3_blob_read(self.blob, mem::transmute(buf.as_ptr()), n, self.pos) };
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
    /// Write data into a BLOB incrementally. Will return `Ok(0)` if the end of the blob
    /// has been reached; consider using `Write::write_all(buf)` if you want to get an
    /// error if the entirety of the buffer cannot be written.
    ///
    /// This function may only modify the contents of the BLOB; it is not possible to increase
    /// the size of a BLOB using this API.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite write call fails.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let max_allowed_len = (self.size() - self.pos) as usize;
        let n = min(buf.len(), max_allowed_len) as i32;
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
        } else if pos > self.size() as i64 {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                               "invalid seek to position past end of blob"))
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

/// BLOB of length N that is filled with zeroes.
/// Zeroblobs are intended to serve as placeholders for BLOBs whose content is later written using incremental BLOB I/O routines.
/// A negative value for the zeroblob results in a zero-length BLOB.
#[derive(Copy,Clone)]
pub struct ZeroBlob(pub i32);

impl ToSql for ZeroBlob {
    unsafe fn bind_parameter(&self, stmt: *mut ffi::sqlite3_stmt, col: c_int) -> c_int {
        let ZeroBlob(length) = *self;
        ffi::sqlite3_bind_zeroblob(stmt, col, length)
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufReader, BufRead, BufWriter, Read, Write, Seek, SeekFrom};
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
        assert_eq!(6, blob.write(b"567890xxxxxx").unwrap()); // cannot write past 10
        assert_eq!(0, blob.write(b"5678").unwrap());         // still cannot write past 10

        blob.reopen(rowid).unwrap();
        blob.close().unwrap();

        blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, true).unwrap();
        let mut bytes = [0u8; 5];
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"Clob5");
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"67890");
        assert_eq!(0, blob.read(&mut bytes[..]).unwrap());

        blob.seek(SeekFrom::Start(2)).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"ob567");

        // only first 4 bytes of `bytes` should be read into
        blob.seek(SeekFrom::Current(-1)).unwrap();
        assert_eq!(4, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"78907");

        blob.seek(SeekFrom::End(-6)).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"56789");

        blob.reopen(rowid).unwrap();
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"Clob5");

        // should not be able to seek negative or past end
        assert!(blob.seek(SeekFrom::Current(-20)).is_err());
        assert!(blob.seek(SeekFrom::End(0)).is_ok());
        assert!(blob.seek(SeekFrom::Current(1)).is_err());

        // write_all should detect when we return Ok(0) because there is no space left,
        // and return a write error
        blob.reopen(rowid).unwrap();
        assert!(blob.write_all(b"0123456789x").is_err());
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

    #[test]
    fn test_blob_in_bufwriter() {
        let (db, rowid) = db_with_test_blob().unwrap();

        {
            let blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false).unwrap();
            let mut writer = BufWriter::new(blob);

            // trying to write too much and then flush should fail
            assert_eq!(8, writer.write(b"01234567").unwrap());
            assert_eq!(8, writer.write(b"01234567").unwrap());
            assert!(writer.flush().is_err());
        }

        {
            // ... but it should've written the first 10 bytes
            let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)
                .unwrap();
            let mut bytes = [0u8; 10];
            assert_eq!(10, blob.read(&mut bytes[..]).unwrap());
            assert_eq!(b"0123456701", &bytes);
        }

        {
            let blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false).unwrap();
            let mut writer = BufWriter::new(blob);

            // trying to write_all too much should fail
            writer.write_all(b"aaaaaaaaaabbbbb").unwrap();
            assert!(writer.flush().is_err());
        }

        {
            // ... but it should've written the first 10 bytes
            let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)
                .unwrap();
            let mut bytes = [0u8; 10];
            assert_eq!(10, blob.read(&mut bytes[..]).unwrap());
            assert_eq!(b"aaaaaaaaaa", &bytes);
        }
    }
}
