//! Incremental BLOB I/O.
//!
//! Note that SQLite does not provide API-level access to change the size of a
//! BLOB; that must be performed through SQL statements.
//!
//! There are two choices for how to perform IO on a [`Blob`].
//!
//! 1. The implementations it provides of the `std::io::Read`, `std::io::Write`,
//!    and `std::io::Seek` traits.
//!
//! 2. A positional IO API, e.g. [`Blob::read_at`], [`Blob::write_at`] and
//!    similar.
//!
//! Documenting these in order:
//!
//! ## 1. `std::io` trait implementations.
//!
//! `Blob` conforms to `std::io::Read`, `std::io::Write`, and `std::io::Seek`,
//! so it plays nicely with other types that build on these (such as
//! `std::io::BufReader` and `std::io::BufWriter`). However, you must be careful
//! with the size of the blob. For example, when using a `BufWriter`, the
//! `BufWriter` will accept more data than the `Blob` will allow, so make sure
//! to call `flush` and check for errors. (See the unit tests in this module for
//! an example.)
//!
//! ## 2. Positional IO
//!
//! `Blob`s also offer a `pread` / `pwrite`-style positional IO api in the form
//! of [`Blob::read_at`], [`Blob::write_at`], [`Blob::raw_read_at`],
//! [`Blob::read_at_exact`], and [`Blob::raw_read_at_exact`].
//!
//! These APIs all take the position to read from or write to from as a
//! parameter, instead of using an internal `pos` value.
//!
//! ### Positional IO Read Variants
//!
//! For the `read` functions, there are several functions provided:
//!
//! - [`Blob::read_at`]
//! - [`Blob::raw_read_at`]
//! - [`Blob::read_at_exact`]
//! - [`Blob::raw_read_at_exact`]
//!
//! These can be divided along two axes: raw/not raw, and exact/inexact:
//!
//! 1. Raw/not raw refers to the type of the destination buffer. The raw
//!    functions take a `&mut [MaybeUninit<u8>]` as the destination buffer,
//!    where the "normal" functions take a `&mut [u8]`.
//!
//!    Using `MaybeUninit` here can be more efficient in some cases, but is
//!    often inconvenient, so both are provided.
//!
//! 2. Exact/inexact refers to to whether or not the entire buffer must be
//!    filled in order for the call to be considered a success.
//!
//!    The "exact" functions require the provided buffer be entirely filled, or
//!    they return an error, whereas the "inexact" functions read as much out of
//!    the blob as is available, and return how much they were able to read.
//!
//!    The inexact functions are preferable if you do not know the size of the
//!    blob already, and the exact functions are preferable if you do.
//!
//! ### Comparison to using the `std::io` traits:
//!
//! In general, the positional methods offer the following Pro/Cons compared to
//! using the implementation `std::io::{Read, Write, Seek}` we provide for
//! `Blob`:
//!
//! 1. (Pro) There is no need to first seek to a position in order to perform IO
//!    on it as the position is a parameter.
//!
//! 2. (Pro) `Blob`'s positional read functions don't mutate the blob in any
//!    way, and take `&self`. No `&mut` access required.
//!
//! 3. (Pro) Positional IO functions return `Err(rusqlite::Error)` on failure,
//!    rather than `Err(std::io::Error)`. Returning `rusqlite::Error` is more
//!    accurate and convenient.
//!
//!    Note that for the `std::io` API, no data is lost however, and it can be
//!    recovered with `io_err.downcast::<rusqlite::Error>()` (this can be easy
//!    to forget, though).
//!
//! 4. (Pro, for now). A `raw` version of the read API exists which can allow
//!    reading into a `&mut [MaybeUninit<u8>]` buffer, which avoids a potential
//!    costly initialization step. (However, `std::io` traits will certainly
//!    gain this someday, which is why this is only a "Pro, for now").
//!
//! 5. (Con) The set of functions is more bare-bones than what is offered in
//!    `std::io`, which has a number of adapters, handy algorithms, further
//!    traits.
//!
//! 6. (Con) No meaningful interoperability with other crates, so if you need
//!    that you must use `std::io`.
//!
//! To generalize: the `std::io` traits are useful because they conform to a
//! standard interface that a lot of code knows how to handle, however that
//! interface is not a perfect fit for [`Blob`], so another small set of
//! functions is provided as well.
//!
//! # Example (`std::io`)
//!
//! ```rust
//! # use rusqlite::blob::ZeroBlob;
//! # use rusqlite::{Connection, DatabaseName};
//! # use std::error::Error;
//! # use std::io::{Read, Seek, SeekFrom, Write};
//! # fn main() -> Result<(), Box<dyn Error>> {
//! let db = Connection::open_in_memory()?;
//! db.execute_batch("CREATE TABLE test_table (content BLOB);")?;
//!
//! // Insert a BLOB into the `content` column of `test_table`. Note that the Blob
//! // I/O API provides no way of inserting or resizing BLOBs in the DB -- this
//! // must be done via SQL.
//! db.execute("INSERT INTO test_table (content) VALUES (ZEROBLOB(10))", [])?;
//!
//! // Get the row id off the BLOB we just inserted.
//! let rowid = db.last_insert_rowid();
//! // Open the BLOB we just inserted for IO.
//! let mut blob = db.blob_open(DatabaseName::Main, "test_table", "content", rowid, false)?;
//!
//! // Write some data into the blob. Make sure to test that the number of bytes
//! // written matches what you expect; if you try to write too much, the data
//! // will be truncated to the size of the BLOB.
//! let bytes_written = blob.write(b"01234567")?;
//! assert_eq!(bytes_written, 8);
//!
//! // Move back to the start and read into a local buffer.
//! // Same guidance - make sure you check the number of bytes read!
//! blob.seek(SeekFrom::Start(0))?;
//! let mut buf = [0u8; 20];
//! let bytes_read = blob.read(&mut buf[..])?;
//! assert_eq!(bytes_read, 10); // note we read 10 bytes because the blob has size 10
//!
//! // Insert another BLOB, this time using a parameter passed in from
//! // rust (potentially with a dynamic size).
//! db.execute(
//!     "INSERT INTO test_table (content) VALUES (?)",
//!     [ZeroBlob(64)],
//! )?;
//!
//! // given a new row ID, we can reopen the blob on that row
//! let rowid = db.last_insert_rowid();
//! blob.reopen(rowid)?;
//! // Just check that the size is right.
//! assert_eq!(blob.len(), 64);
//! # Ok(())
//! # }
//! ```
//!
//! # Example (Positional)
//!
//! ```rust
//! # use rusqlite::blob::ZeroBlob;
//! # use rusqlite::{Connection, DatabaseName};
//! # use std::error::Error;
//! # fn main() -> Result<(), Box<dyn Error>> {
//! let db = Connection::open_in_memory()?;
//! db.execute_batch("CREATE TABLE test_table (content BLOB);")?;
//! // Insert a blob into the `content` column of `test_table`. Note that the Blob
//! // I/O API provides no way of inserting or resizing blobs in the DB -- this
//! // must be done via SQL.
//! db.execute("INSERT INTO test_table (content) VALUES (ZEROBLOB(10))", [])?;
//! // Get the row id off the blob we just inserted.
//! let rowid = db.last_insert_rowid();
//! // Open the blob we just inserted for IO.
//! let mut blob = db.blob_open(DatabaseName::Main, "test_table", "content", rowid, false)?;
//! // Write some data into the blob.
//! blob.write_at(b"ABCDEF", 2)?;
//!
//! // Read the whole blob into a local buffer.
//! let mut buf = [0u8; 10];
//! blob.read_at_exact(&mut buf, 0)?;
//! assert_eq!(&buf, b"\0\0ABCDEF\0\0");
//!
//! // Insert another blob, this time using a parameter passed in from
//! // rust (potentially with a dynamic size).
//! db.execute(
//!     "INSERT INTO test_table (content) VALUES (?)",
//!     [ZeroBlob(64)],
//! )?;
//!
//! // given a new row ID, we can reopen the blob on that row
//! let rowid = db.last_insert_rowid();
//! blob.reopen(rowid)?;
//! assert_eq!(blob.len(), 64);
//! # Ok(())
//! # }
//! ```
use std::cmp::min;
use std::io;
use std::ptr;

use super::ffi;
use super::types::{ToSql, ToSqlOutput};
use crate::{Connection, DatabaseName, Result};

mod pos_io;

/// Handle to an open BLOB. See
/// [`rusqlite::blob`](crate::blob) documentation for in-depth discussion.
pub struct Blob<'conn> {
    conn: &'conn Connection,
    blob: *mut ffi::sqlite3_blob,
    // used by std::io implementations,
    pos: i32,
}

impl Connection {
    /// Open a handle to the BLOB located in `row_id`,
    /// `column`, `table` in database `db`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `db`/`table`/`column` cannot be converted to a
    /// C-compatible string or if the underlying SQLite BLOB open call
    /// fails.
    #[inline]
    pub fn blob_open<'a>(
        &'a self,
        db: DatabaseName<'_>,
        table: &str,
        column: &str,
        row_id: i64,
        read_only: bool,
    ) -> Result<Blob<'a>> {
        let c = self.db.borrow_mut();
        let mut blob = ptr::null_mut();
        let db = db.as_cstring()?;
        let table = super::str_to_cstring(table)?;
        let column = super::str_to_cstring(column)?;
        let rc = unsafe {
            ffi::sqlite3_blob_open(
                c.db(),
                db.as_ptr(),
                table.as_ptr(),
                column.as_ptr(),
                row_id,
                if read_only { 0 } else { 1 },
                &mut blob,
            )
        };
        c.decode_result(rc).map(|_| Blob {
            conn: self,
            blob,
            pos: 0,
        })
    }
}

impl Blob<'_> {
    /// Move a BLOB handle to a new row.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite BLOB reopen call fails.
    #[inline]
    pub fn reopen(&mut self, row: i64) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_blob_reopen(self.blob, row) };
        if rc != ffi::SQLITE_OK {
            return self.conn.decode_result(rc);
        }
        self.pos = 0;
        Ok(())
    }

    /// Return the size in bytes of the BLOB.
    #[inline]
    #[must_use]
    pub fn size(&self) -> i32 {
        unsafe { ffi::sqlite3_blob_bytes(self.blob) }
    }

    /// Return the current size in bytes of the BLOB.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        use std::convert::TryInto;
        self.size().try_into().unwrap()
    }

    /// Return true if the BLOB is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Close a BLOB handle.
    ///
    /// Calling `close` explicitly is not required (the BLOB will be closed
    /// when the `Blob` is dropped), but it is available so you can get any
    /// errors that occur.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite close call fails.
    #[inline]
    pub fn close(mut self) -> Result<()> {
        self.close_()
    }

    #[inline]
    fn close_(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_blob_close(self.blob) };
        self.blob = ptr::null_mut();
        self.conn.decode_result(rc)
    }
}

impl io::Read for Blob<'_> {
    /// Read data from a BLOB incrementally. Will return Ok(0) if the end of
    /// the blob has been reached.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite read call fails.
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let max_allowed_len = (self.size() - self.pos) as usize;
        let n = min(buf.len(), max_allowed_len) as i32;
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe { ffi::sqlite3_blob_read(self.blob, buf.as_mut_ptr().cast(), n, self.pos) };
        self.conn
            .decode_result(rc)
            .map(|_| {
                self.pos += n;
                n as usize
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}

impl io::Write for Blob<'_> {
    /// Write data into a BLOB incrementally. Will return `Ok(0)` if the end of
    /// the blob has been reached; consider using `Write::write_all(buf)`
    /// if you want to get an error if the entirety of the buffer cannot be
    /// written.
    ///
    /// This function may only modify the contents of the BLOB; it is not
    /// possible to increase the size of a BLOB using this API.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite write call fails.
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let max_allowed_len = (self.size() - self.pos) as usize;
        let n = min(buf.len(), max_allowed_len) as i32;
        if n <= 0 {
            return Ok(0);
        }
        let rc = unsafe { ffi::sqlite3_blob_write(self.blob, buf.as_ptr() as *mut _, n, self.pos) };
        self.conn
            .decode_result(rc)
            .map(|_| {
                self.pos += n;
                n as usize
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Seek for Blob<'_> {
    /// Seek to an offset, in bytes, in BLOB.
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            io::SeekFrom::Start(offset) => offset as i64,
            io::SeekFrom::Current(offset) => i64::from(self.pos) + offset,
            io::SeekFrom::End(offset) => i64::from(self.size()) + offset,
        };

        if pos < 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to negative position",
            ))
        } else if pos > i64::from(self.size()) {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to position past end of blob",
            ))
        } else {
            self.pos = pos as i32;
            Ok(pos as u64)
        }
    }
}

#[allow(unused_must_use)]
impl Drop for Blob<'_> {
    #[inline]
    fn drop(&mut self) {
        self.close_();
    }
}

/// BLOB of length N that is filled with zeroes.
///
/// Zeroblobs are intended to serve as placeholders for BLOBs whose content is
/// later written using incremental BLOB I/O routines.
///
/// A negative value for the zeroblob results in a zero-length BLOB.
#[derive(Copy, Clone)]
pub struct ZeroBlob(pub i32);

impl ToSql for ZeroBlob {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let ZeroBlob(length) = *self;
        Ok(ToSqlOutput::ZeroBlob(length))
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, DatabaseName, Result};
    use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

    fn db_with_test_blob() -> Result<(Connection, i64)> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE test (content BLOB);
                   INSERT INTO test VALUES (ZEROBLOB(10));
                   END;";
        db.execute_batch(sql)?;
        let rowid = db.last_insert_rowid();
        Ok((db, rowid))
    }

    #[test]
    fn test_blob() -> Result<()> {
        let (db, rowid) = db_with_test_blob()?;

        let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
        assert_eq!(4, blob.write(b"Clob").unwrap());
        assert_eq!(6, blob.write(b"567890xxxxxx").unwrap()); // cannot write past 10
        assert_eq!(0, blob.write(b"5678").unwrap()); // still cannot write past 10

        blob.reopen(rowid)?;
        blob.close()?;

        blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, true)?;
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

        blob.reopen(rowid)?;
        assert_eq!(5, blob.read(&mut bytes[..]).unwrap());
        assert_eq!(&bytes, b"Clob5");

        // should not be able to seek negative or past end
        assert!(blob.seek(SeekFrom::Current(-20)).is_err());
        assert!(blob.seek(SeekFrom::End(0)).is_ok());
        assert!(blob.seek(SeekFrom::Current(1)).is_err());

        // write_all should detect when we return Ok(0) because there is no space left,
        // and return a write error
        blob.reopen(rowid)?;
        assert!(blob.write_all(b"0123456789x").is_err());
        Ok(())
    }

    #[test]
    fn test_blob_in_bufreader() -> Result<()> {
        let (db, rowid) = db_with_test_blob()?;

        let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
        assert_eq!(8, blob.write(b"one\ntwo\n").unwrap());

        blob.reopen(rowid)?;
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
        Ok(())
    }

    #[test]
    fn test_blob_in_bufwriter() -> Result<()> {
        let (db, rowid) = db_with_test_blob()?;

        {
            let blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
            let mut writer = BufWriter::new(blob);

            // trying to write too much and then flush should fail
            assert_eq!(8, writer.write(b"01234567").unwrap());
            assert_eq!(8, writer.write(b"01234567").unwrap());
            assert!(writer.flush().is_err());
        }

        {
            // ... but it should've written the first 10 bytes
            let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
            let mut bytes = [0u8; 10];
            assert_eq!(10, blob.read(&mut bytes[..]).unwrap());
            assert_eq!(b"0123456701", &bytes);
        }

        {
            let blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
            let mut writer = BufWriter::new(blob);

            // trying to write_all too much should fail
            writer.write_all(b"aaaaaaaaaabbbbb").unwrap();
            assert!(writer.flush().is_err());
        }

        {
            // ... but it should've written the first 10 bytes
            let mut blob = db.blob_open(DatabaseName::Main, "test", "content", rowid, false)?;
            let mut bytes = [0u8; 10];
            assert_eq!(10, blob.read(&mut bytes[..]).unwrap());
            assert_eq!(b"aaaaaaaaaa", &bytes);
            Ok(())
        }
    }
}
