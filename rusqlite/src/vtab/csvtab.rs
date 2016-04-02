//! CSV Virtual Table
extern crate csv;
use std::ffi::CStr;
use std::fs::File;
use std::path::Path;
use std::result;
use std::str;
use libc;

use {Connection, Error, Result};
use ffi;
use types::Null;
use vtab::{declare_vtab, escape_double_quote, VTab, VTabCursor};

/// Register the "csv" module.
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
    filename: String,
    has_headers: bool,
    delimiter: u8,
    quote: u8,
    offset_first_row: u64,
}

impl CSVTab {
    fn reader(&self) -> result::Result<csv::Reader<File>, csv::Error> {
        csv::Reader::from_file(&self.filename).map(|reader| {
            reader.has_headers(self.has_headers)
                  .delimiter(self.delimiter)
                  .quote(self.quote)
        })
    }
}

impl VTab<CSVTabCursor> for CSVTab {
    fn create(db: *mut ffi::sqlite3,
              _aux: *mut libc::c_void,
              args: &[*const libc::c_char])
              -> Result<CSVTab> {
        if args.len() < 4 {
            return Err(Error::ModuleError("no CSV file specified".to_owned()));
        }
        // pull out name of csv file (remove quotes)
        let mut c_filename = unsafe { CStr::from_ptr(args[3]).to_bytes() };
        if c_filename[0] == b'\'' {
            c_filename = &c_filename[1..c_filename.len() - 1];
        }
        let filename = try!(str::from_utf8(c_filename));
        if !Path::new(filename).exists() {
            return Err(Error::ModuleError(format!("file '{}' does not exist", filename)));
        }
        let mut vtab = CSVTab {
            base: Default::default(),
            filename: String::from(filename),
            has_headers: false,
            delimiter: b',',
            quote: b'"',
            offset_first_row: 0,
        };
        let mut cols: Vec<String> = Vec::new();

        let args = &args[4..];
        for c_arg in args {
            let c_slice = unsafe { CStr::from_ptr(*c_arg).to_bytes() };
            if c_slice.len() == 1 {
                vtab.delimiter = c_slice[0];
            } else if c_slice.len() == 3 && c_slice[0] == b'\'' {
                vtab.delimiter = c_slice[1];
            } else {
                let arg = try!(str::from_utf8(c_slice));
                let uc = arg.to_uppercase();
                if uc.contains("HEADER") {
                    vtab.has_headers = true;
                } else if uc.contains("NO_QUOTE") {
                    vtab.quote = 0;
                } else {
                    cols.push(escape_double_quote(arg).into_owned());
                }
            }
        }

        if vtab.has_headers {
            let mut reader = try!(vtab.reader());
            let headers = try!(reader.headers());
            vtab.offset_first_row = reader.byte_offset();
            // headers ignored if cols is not empty
            if cols.is_empty() {
                cols = headers;
            }
        }

        if cols.is_empty() {
            return Err(Error::ModuleError("no column name specified".to_owned()));
        }

        let mut sql = String::from("CREATE TABLE x(");
        for (i, col) in cols.iter().enumerate() {
            if col.is_empty() {
                return Err(Error::ModuleError("no column name found".to_owned()));
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

        try!(declare_vtab(db, &sql));
        Ok(vtab)
    }

    fn best_index(&self, _info: *mut ffi::sqlite3_index_info) {}

    fn open(&self) -> Result<CSVTabCursor> {
        Ok(CSVTabCursor::new(try!(self.reader())))
    }
}


#[repr(C)]
struct CSVTabCursor {
    /// Base class
    base: ffi::sqlite3_vtab_cursor,
    reader: csv::Reader<File>,
    /// Current cursor position
    row_number: usize,
    cols: Vec<String>,
    eof: bool,
}

impl CSVTabCursor {
    fn new(reader: csv::Reader<File>) -> CSVTabCursor {
        CSVTabCursor {
            base: Default::default(),
            reader: reader,
            row_number: 0,
            cols: Vec::new(),
            eof: false,
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
            let offset_first_row = self.vtab().offset_first_row;
            try!(self.reader.seek(offset_first_row));
        }
        self.row_number = 0;
        self.next()
    }
    fn next(&mut self) -> Result<()> {
        {
            self.eof = self.reader.done();
            if self.eof {
                return Ok(());
            }

            self.cols.clear();
            while let Some(col) = self.reader.next_str().into_iter_result() {
                self.cols.push(String::from(try!(col)));
            }
        }

        self.row_number = self.row_number + 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        self.eof
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, col: libc::c_int) -> Result<()> {
        use functions::ToResult;
        if col < 0 || col as usize >= self.cols.len() {
            return Err(Error::ModuleError(format!("column index out of bounds: {}", col)));
        }
        if self.cols.is_empty() {
            unsafe { Null.set_result(ctx) };
            return Ok(());
        }
        // TODO Affinity
        unsafe { self.cols[col as usize].set_result(ctx) };
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

        {
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
                sum = sum + id;
            }
            assert_eq!(sum, 15);
        }
        db.execute_batch("DROP TABLE vtab").unwrap();
    }

    #[test]
    fn test_csv_cursor() {
        let db = Connection::open_in_memory().unwrap();
        csvtab::load_module(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE vtab USING csv('test.csv', HAS_HEADERS)").unwrap();

        {
            let mut s = db.prepare("SELECT v1.rowid, v1.* FROM vtab v1 NATURAL JOIN vtab v2 \
                                    WHERE v1.rowid < v2.rowid")
                          .unwrap();

            let row = s.query(&[]).unwrap().next().unwrap().unwrap();
            assert_eq!(row.get::<i32, i32>(0), 2);
        }
        db.execute_batch("DROP TABLE vtab").unwrap();
    }
}
