//! CSV Virtual Table
extern crate csv;
use std::fs::File;
use std::mem;
use libc;

use {Connection, Error, Result};
use ffi;
use vtab::declare_vtab;

use self::csv::Reader;

pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("csv", &CSV_MODULE, aux)
}

init_module!(CSV_MODULE, CSVTab, CSVTabCursor,
    csv_create, csv_best_index, csv_destroy,
    csv_open, csv_close,
    csv_filter, csv_next, csv_eof,
    csv_column, csv_rowid);

#[repr(C)]
struct CSVTab {
    /// Base class
    base: ffi::sqlite3_vtab,
    reader: csv::Reader<File>,
    offset_first_row: u64,
}

impl CSVTab {
    fn create(db: *mut ffi::sqlite3,
              aux: *mut libc::c_void,
              _argc: libc::c_int,
              _argv: *const *const libc::c_char)
              -> Result<CSVTab> {
        let reader = try!(csv::Reader::from_file("FIXME"));
        let vtab = CSVTab {
            base: Default::default(),
            reader: reader,
            offset_first_row: 0,
        };
        unimplemented!();
        try!(declare_vtab(db, "CREATE TABLE x FIXME"));
        Ok(vtab)
    }

    fn best_index(&self, _info: *mut ffi::sqlite3_index_info) {}

    fn open(&self) -> Result<CSVTabCursor> {
        Ok(CSVTabCursor::new())
    }
}


#[repr(C)]
struct CSVTabCursor {
    /// Base class
    base: ffi::sqlite3_vtab_cursor,
    /// Current cursor position
    row_number: usize,
}

impl CSVTabCursor {
    fn new() -> CSVTabCursor {
        CSVTabCursor {
            base: Default::default(),
            row_number: 0,
        }
    }

    fn vtab(&self) -> &mut CSVTab {
        unsafe { &mut *(self.base.pVtab as *mut CSVTab) }
    }

    fn filter(&mut self) -> Result<()> {
        {
            let vtab = self.vtab();
            vtab.reader.seek(vtab.offset_first_row); // FIXME Result ignore
        }
        self.row_number = 0;
        self.next()
    }
    fn next(&mut self) -> Result<()> {
        let vtab = self.vtab();
        if vtab.reader.done() {
            return Err(Error::SqliteFailure(ffi::Error::new(ffi::SQLITE_ERROR), None));
        }
        unimplemented!();
        // self.row_number = self.row_number + 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        let vtab = self.vtab();
        unsafe { (*vtab).reader.done() }
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, _i: libc::c_int) -> Result<()> {
        let vtab = self.vtab();
        unimplemented!();
        // TODO.set_result(ctx);
        Ok(())
    }
    fn rowid(&self) -> Result<i64> {
        Ok(self.row_number as i64)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        use std::error::Error as StdError;
        Error::ModuleError(String::from(err.description()))
    }
}
