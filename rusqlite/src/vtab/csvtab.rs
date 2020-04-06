//! `feature = "csvtab"` CSV Virtual Table.
//!
//! Port of [csv](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/csv.c) C
//! extension: https://www.sqlite.org/csv.html
//!
//! # Example
//!
//! ```rust,no_run
//! # use rusqlite::{Connection, Result};
//! fn example() -> Result<()> {
//!     // Note: This should be done once (usually when opening the DB).
//!     let db = Connection::open_in_memory()?;
//!     rusqlite::vtab::csvtab::load_module(&db)?;
//!     // Assum3e my_csv.csv
//!     let schema = "
//!         CREATE VIRTUAL TABLE my_csv_data
//!         USING csv(filename = 'my_csv.csv')
//!     ";
//!     db.execute_batch(schema)?;
//!     // Now the `my_csv_data` (virtual) table can be queried as normal...
//!     Ok(())
//! }
//! ```
use std::fs::File;
use std::os::raw::c_int;
use std::path::Path;
use std::str;

use crate::ffi;
use crate::types::Null;
use crate::vtab::{
    dequote, escape_double_quote, parse_boolean, read_only_module, Context, CreateVTab, IndexInfo,
    Module, VTab, VTabConnection, VTabCursor, Values,
};
use crate::{Connection, Error, Result};

/// `feature = "csvtab"` Register the "csv" module.
/// ```sql
/// CREATE VIRTUAL TABLE vtab USING csv(
///   filename=FILENAME -- Name of file containing CSV content
///   [, schema=SCHEMA] -- Alternative CSV schema. 'CREATE TABLE x(col1 TEXT NOT NULL, col2 INT, ...);'
///   [, header=YES|NO] -- First row of CSV defines the names of columns if "yes". Default "no".
///   [, columns=N] -- Assume the CSV file contains N columns.
///   [, delimiter=C] -- CSV delimiter. Default ','.
///   [, quote=C] -- CSV quote. Default '"'. 0 means no quote.
/// );
/// ```
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("csv", &CSV_MODULE, aux)
}

lazy_static::lazy_static! {
    static ref CSV_MODULE: Module<CSVTab> = read_only_module::<CSVTab>(1);
}

/// An instance of the CSV virtual table
#[repr(C)]
struct CSVTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
    /// Name of the CSV file
    filename: String,
    has_headers: bool,
    delimiter: u8,
    quote: u8,
    /// Offset to start of data
    offset_first_row: csv::Position,
}

impl CSVTab {
    fn reader(&self) -> Result<csv::Reader<File>, csv::Error> {
        csv::ReaderBuilder::new()
            .has_headers(self.has_headers)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_path(&self.filename)
    }

    fn parameter(c_slice: &[u8]) -> Result<(&str, &str)> {
        let arg = str::from_utf8(c_slice)?.trim();
        let mut split = arg.split('=');
        if let Some(key) = split.next() {
            if let Some(value) = split.next() {
                let param = key.trim();
                let value = dequote(value);
                return Ok((param, value));
            }
        }
        Err(Error::ModuleError(format!("illegal argument: '{}'", arg)))
    }

    fn parse_byte(arg: &str) -> Option<u8> {
        if arg.len() == 1 {
            arg.bytes().next()
        } else {
            None
        }
    }
}

impl VTab for CSVTab {
    type Aux = ();
    type Cursor = CSVTabCursor;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        args: &[&[u8]],
    ) -> Result<(String, CSVTab)> {
        if args.len() < 4 {
            return Err(Error::ModuleError("no CSV file specified".to_owned()));
        }

        let mut vtab = CSVTab {
            base: ffi::sqlite3_vtab::default(),
            filename: "".to_owned(),
            has_headers: false,
            delimiter: b',',
            quote: b'"',
            offset_first_row: csv::Position::new(),
        };
        let mut schema = None;
        let mut n_col = None;

        let args = &args[3..];
        for c_slice in args {
            let (param, value) = CSVTab::parameter(c_slice)?;
            match param {
                "filename" => {
                    if !Path::new(value).exists() {
                        return Err(Error::ModuleError(format!(
                            "file '{}' does not exist",
                            value
                        )));
                    }
                    vtab.filename = value.to_owned();
                }
                "schema" => {
                    schema = Some(value.to_owned());
                }
                "columns" => {
                    if let Ok(n) = value.parse::<u16>() {
                        if n_col.is_some() {
                            return Err(Error::ModuleError(
                                "more than one 'columns' parameter".to_owned(),
                            ));
                        } else if n == 0 {
                            return Err(Error::ModuleError(
                                "must have at least one column".to_owned(),
                            ));
                        }
                        n_col = Some(n);
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'columns': {}",
                            value
                        )));
                    }
                }
                "header" => {
                    if let Some(b) = parse_boolean(value) {
                        vtab.has_headers = b;
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'header': {}",
                            value
                        )));
                    }
                }
                "delimiter" => {
                    if let Some(b) = CSVTab::parse_byte(value) {
                        vtab.delimiter = b;
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'delimiter': {}",
                            value
                        )));
                    }
                }
                "quote" => {
                    if let Some(b) = CSVTab::parse_byte(value) {
                        if b == b'0' {
                            vtab.quote = 0;
                        } else {
                            vtab.quote = b;
                        }
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'quote': {}",
                            value
                        )));
                    }
                }
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unrecognized parameter '{}'",
                        param
                    )));
                }
            }
        }

        if vtab.filename.is_empty() {
            return Err(Error::ModuleError("no CSV file specified".to_owned()));
        }

        let mut cols: Vec<String> = Vec::new();
        if vtab.has_headers || (n_col.is_none() && schema.is_none()) {
            let mut reader = vtab.reader()?;
            if vtab.has_headers {
                {
                    let headers = reader.headers()?;
                    // headers ignored if cols is not empty
                    if n_col.is_none() && schema.is_none() {
                        cols = headers
                            .into_iter()
                            .map(|header| escape_double_quote(&header).into_owned())
                            .collect();
                    }
                }
                vtab.offset_first_row = reader.position().clone();
            } else {
                let mut record = csv::ByteRecord::new();
                if reader.read_byte_record(&mut record)? {
                    for (i, _) in record.iter().enumerate() {
                        cols.push(format!("c{}", i));
                    }
                }
            }
        } else if let Some(n_col) = n_col {
            for i in 0..n_col {
                cols.push(format!("c{}", i));
            }
        }

        if cols.is_empty() && schema.is_none() {
            return Err(Error::ModuleError("no column specified".to_owned()));
        }

        if schema.is_none() {
            let mut sql = String::from("CREATE TABLE x(");
            for (i, col) in cols.iter().enumerate() {
                sql.push('"');
                sql.push_str(col);
                sql.push_str("\" TEXT");
                if i == cols.len() - 1 {
                    sql.push_str(");");
                } else {
                    sql.push_str(", ");
                }
            }
            schema = Some(sql);
        }

        Ok((schema.unwrap(), vtab))
    }

    // Only a forward full table scan is supported.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.set_estimated_cost(1_000_000.);
        Ok(())
    }

    fn open(&self) -> Result<CSVTabCursor> {
        Ok(CSVTabCursor::new(self.reader()?))
    }
}

impl CreateVTab for CSVTab {}

/// A cursor for the CSV virtual table
#[repr(C)]
struct CSVTabCursor {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// The CSV reader object
    reader: csv::Reader<File>,
    /// Current cursor position used as rowid
    row_number: usize,
    /// Values of the current row
    cols: csv::StringRecord,
    eof: bool,
}

impl CSVTabCursor {
    fn new(reader: csv::Reader<File>) -> CSVTabCursor {
        CSVTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            reader,
            row_number: 0,
            cols: csv::StringRecord::new(),
            eof: false,
        }
    }

    /// Accessor to the associated virtual table.
    fn vtab(&self) -> &CSVTab {
        unsafe { &*(self.base.pVtab as *const CSVTab) }
    }
}

impl VTabCursor for CSVTabCursor {
    // Only a full table scan is supported.  So `filter` simply rewinds to
    // the beginning.
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> Result<()> {
        {
            let offset_first_row = self.vtab().offset_first_row.clone();
            self.reader.seek(offset_first_row)?;
        }
        self.row_number = 0;
        self.next()
    }

    fn next(&mut self) -> Result<()> {
        {
            self.eof = self.reader.is_done();
            if self.eof {
                return Ok(());
            }

            self.eof = !self.reader.read_record(&mut self.cols)?;
        }

        self.row_number += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        if col < 0 || col as usize >= self.cols.len() {
            return Err(Error::ModuleError(format!(
                "column index out of bounds: {}",
                col
            )));
        }
        if self.cols.is_empty() {
            return ctx.set_result(&Null);
        }
        // TODO Affinity
        ctx.set_result(&self.cols[col as usize].to_owned())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_number as i64)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        Error::ModuleError(err.to_string())
    }
}

#[cfg(test)]
mod test {
    use crate::vtab::csvtab;
    use crate::{Connection, Result, NO_PARAMS};
    use fallible_iterator::FallibleIterator;

    #[test]
    fn test_csv_module() {
        let db = Connection::open_in_memory().unwrap();
        csvtab::load_module(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE vtab USING csv(filename='test.csv', header=yes)")
            .unwrap();

        {
            let mut s = db.prepare("SELECT rowid, * FROM vtab").unwrap();
            {
                let headers = s.column_names();
                assert_eq!(vec!["rowid", "colA", "colB", "colC"], headers);
            }

            let ids: Result<Vec<i32>> = s
                .query(NO_PARAMS)
                .unwrap()
                .map(|row| row.get::<_, i32>(0))
                .collect();
            let sum = ids.unwrap().iter().sum::<i32>();
            assert_eq!(sum, 15);
        }
        db.execute_batch("DROP TABLE vtab").unwrap();
    }

    #[test]
    fn test_csv_cursor() {
        let db = Connection::open_in_memory().unwrap();
        csvtab::load_module(&db).unwrap();
        db.execute_batch("CREATE VIRTUAL TABLE vtab USING csv(filename='test.csv', header=yes)")
            .unwrap();

        {
            let mut s = db
                .prepare(
                    "SELECT v1.rowid, v1.* FROM vtab v1 NATURAL JOIN vtab v2 WHERE \
                     v1.rowid < v2.rowid",
                )
                .unwrap();

            let mut rows = s.query(NO_PARAMS).unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get_unwrap::<_, i32>(0), 2);
        }
        db.execute_batch("DROP TABLE vtab").unwrap();
    }
}
