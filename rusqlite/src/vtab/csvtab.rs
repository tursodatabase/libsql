//! CSV Virtual Table
extern crate csv;
use std::ffi::CStr;
use std::fs::File;
use std::mem;
use std::str;
use libc;

use {Connection, Error, Result};
use ffi;
use types::Null;
use vtab::{declare_vtab, escape_double_quote, VTab, VTabCursor};

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
    eof: bool,
}

impl VTab<CSVTabCursor> for CSVTab {
    fn create(db: *mut ffi::sqlite3,
              _aux: *mut libc::c_void,
              args: &[*const libc::c_char])
              -> Result<CSVTab> {
        if args.len() < 4 {
            return Err(Error::ModuleError(format!("no CSV file specified")));
        }
        // pull out name of csv file (remove quotes)
        let mut c_filename = unsafe { CStr::from_ptr(args[3]).to_bytes() };
        if c_filename[0] == b'\'' {
            c_filename = &c_filename[1..c_filename.len() - 1];
        }
        let filename = try!(str::from_utf8(c_filename));
        let mut reader = try!(csv::Reader::from_file(filename)).has_headers(false); // TODO flexible ?
        let mut cols: Vec<String> = Vec::new();

        let args = &args[4..];
        for c_arg in args {
            let c_slice = unsafe { CStr::from_ptr(*c_arg).to_bytes() };
            if c_slice.len() == 1 {
                reader = reader.delimiter(c_slice[0]);
            } else if c_slice.len() == 3 && c_slice[0] == b'\'' {
                reader = reader.delimiter(c_slice[1]);
            } else {
                let arg = try!(str::from_utf8(c_slice));
                let uc = arg.to_uppercase();
                if uc.contains("HEADER") {
                    reader = reader.has_headers(true);
                } else if uc.contains("NO_QUOTE") {
                    reader = reader.quote(0);
                } else {
                    cols.push(escape_double_quote(arg).into_owned());
                }
            }
        }

        let mut offset_first_row = 0;
        if reader.has_headers {
            let headers = try!(reader.headers());
            offset_first_row = reader.byte_offset();
            // headers ignored if cols is not empty
            if cols.is_empty() {
                cols = headers;
            }
        }

        if cols.is_empty() {
            return Err(Error::ModuleError(format!("no column name specified")));
        }

        let mut sql = String::from("CREATE TABLE x(");
        for (i, col) in cols.iter().enumerate() {
            if col.is_empty() {
                return Err(Error::ModuleError(format!("no column name found")));
            }
            sql.push('"');
            sql.push_str(col);
            sql.push('"');
            if i == cols.len() - 1 {
                sql.push_str(");");
            } else {
                sql.push_str(", ");
            }
        }

        let vtab = CSVTab {
            base: Default::default(),
            reader: reader,
            offset_first_row: offset_first_row,
            cols: cols,
            eof: false,
        };
        try!(declare_vtab(db, &sql));
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

    fn filter(&mut self,
              _idx_num: libc::c_int,
              _idx_str: *const libc::c_char,
              _argc: libc::c_int,
              _argv: *mut *mut ffi::sqlite3_value)
              -> Result<()> {
        {
            let vtab = self.vtab();
            try!(vtab.reader.seek(vtab.offset_first_row));
        }
        self.row_number = 0;
        self.next()
    }
    fn next(&mut self) -> Result<()> {
        {
            let vtab = self.vtab();
            vtab.eof = vtab.reader.done();
            if vtab.eof {
                return Ok(());
            }

            vtab.cols.clear();
            while let Some(col) = vtab.reader.next_str().into_iter_result() {
                vtab.cols.push(String::from(try!(col)));
            }
        }

        self.row_number = self.row_number + 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        let vtab = self.vtab();
        vtab.eof
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

#[cfg(test)]
mod test {
    use Connection;
    use vtab::csvtab;

    #[test]
    fn test_csv_module() {
        let db = Connection::open_in_memory().unwrap();
        csvtab::load_module(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE vtab USING csv('test.csv', HAS_HEADERS)").unwrap();

        let mut s = db.prepare("SELECT rowid, * FROM vtab").unwrap();
        {
            let headers = s.column_names();
            assert_eq!(vec!["rowid", "colA", "colB", "colC"], headers);
        }

        let rows = s.query(&[]).unwrap();
        let mut sum = 0;
        for row in rows {
            let row = row.unwrap();
            let id: i64 = row.get(0);
            // println!("{}, {:?}, {:?}, {:?}", id, row.get::<i32, Value>(1), row.get::<i32, Value>(2), row.get::<i32, Value>(3));
            sum = sum + id;
        }
        assert_eq!(sum, 15);

        db.execute_batch("DROP TABLE vtab").unwrap();
    }
}
