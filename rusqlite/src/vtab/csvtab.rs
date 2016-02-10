//! CSV Virtual Table
extern crate csv;
use std::fs::File;
use std::mem;
use libc;

use {Connection, Error, Result};
use ffi;
use types::Null;
use vtab::{declare_vtab, VTab, VTabCursor};

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
    cols: Vec<String>,
}

impl VTab<CSVTabCursor> for CSVTab {
    fn create(db: *mut ffi::sqlite3,
              aux: *mut libc::c_void,
              argc: libc::c_int,
              _argv: *const *const libc::c_char)
              -> Result<CSVTab> {
        if argc < 4 {
            return Err(Error::ModuleError(format!("no CSV file specified")));
        }
        //let filename = ;
        let reader = try!(csv::Reader::from_file("FIXME"));
        let vtab = CSVTab {
            base: Default::default(),
            reader: reader,
            offset_first_row: 0,
            cols: vec![], // FIXME
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
}

impl VTabCursor<CSVTab> for CSVTabCursor {
    fn vtab(&self) -> &mut CSVTab {
        unsafe { &mut *(self.base.pVtab as *mut CSVTab) }
    }

    fn filter(&mut self) -> Result<()> {
        {
            let vtab = self.vtab();
            try!(vtab.reader.seek(vtab.offset_first_row));
        }
        self.row_number = 0;
        self.next()
    }
    fn next(&mut self) -> Result<()> {
        let vtab = self.vtab();
        if vtab.reader.done() {
            return Err(Error::ModuleError(format!("eof")));
        }
        unimplemented!();
        // self.row_number = self.row_number + 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        let vtab = self.vtab();
        unsafe { (*vtab).reader.done() }
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, col: libc::c_int) -> Result<()> {
        use functions::ToResult;
        let vtab = self.vtab();
        if col < 0 || col as usize >= vtab.cols.len() {
            return Err(Error::ModuleError(format!("column index out of bounds: {}", col)));
        }
        if vtab.cols.is_empty() {
            unsafe { Null.set_result(ctx) };
            return Ok(());
        }
        // TODO Affinity
        unsafe { vtab.cols[col as usize].set_result(ctx) };
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
