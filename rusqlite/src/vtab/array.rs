//! `feature = "array"` Array Virtual Table.
//!
//! Note: `rarray`, not `carray` is the name of the table valued function we
//! define.
//!
//! Port of [carray](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/carray.c)
//! C extension: https://www.sqlite.org/carray.html
//!
//! # Example
//!
//! ```rust,no_run
//! # use rusqlite::{types::Value, Connection, Result, params};
//! # use std::rc::Rc;
//! fn example(db: &Connection) -> Result<()> {
//!     // Note: This should be done once (usually when opening the DB).
//!     rusqlite::vtab::array::load_module(&db)?;
//!     let v = [1i64, 2, 3, 4];
//!     // Note: A `Rc<Vec<Value>>` must be used as the parameter.
//!     let values = Rc::new(v.iter().copied().map(Value::from).collect::<Vec<Value>>());
//!     let mut stmt = db.prepare("SELECT value from rarray(?);")?;
//!     let rows = stmt.query_map(params![values], |row| row.get::<_, i64>(0))?;
//!     for value in rows {
//!         println!("{}", value?);
//!     }
//!     Ok(())
//! }
//! ```

use std::default::Default;
use std::os::raw::{c_char, c_int, c_void};
use std::rc::Rc;

use crate::ffi;
use crate::types::{ToSql, ToSqlOutput, Value};
use crate::vtab::{
    eponymous_only_module, Context, IndexConstraintOp, IndexInfo, Module, VTab, VTabConnection,
    VTabCursor, Values,
};
use crate::{Connection, Result};

// http://sqlite.org/bindptr.html

pub(crate) const ARRAY_TYPE: *const c_char = b"rarray\0" as *const u8 as *const c_char;

pub(crate) unsafe extern "C" fn free_array(p: *mut c_void) {
    let _: Array = Rc::from_raw(p as *const Vec<Value>);
}

pub type Array = Rc<Vec<Value>>;

impl ToSql for Array {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Array(self.clone()))
    }
}

/// `feature = "array"` Register the "rarray" module.
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("rarray", &ARRAY_MODULE, aux)
}

lazy_static::lazy_static! {
    static ref ARRAY_MODULE: Module<ArrayTab> = eponymous_only_module::<ArrayTab>(1);
}

// Column numbers
// const CARRAY_COLUMN_VALUE : c_int = 0;
const CARRAY_COLUMN_POINTER: c_int = 1;

/// An instance of the Array virtual table
#[repr(C)]
struct ArrayTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
}

impl VTab for ArrayTab {
    type Aux = ();
    type Cursor = ArrayTabCursor;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> Result<(String, ArrayTab)> {
        let vtab = ArrayTab {
            base: ffi::sqlite3_vtab::default(),
        };
        Ok(("CREATE TABLE x(value,pointer hidden)".to_owned(), vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Index of the pointer= constraint
        let mut ptr_idx = None;
        for (i, constraint) in info.constraints().enumerate() {
            if !constraint.is_usable() {
                continue;
            }
            if constraint.operator() != IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }
            if let CARRAY_COLUMN_POINTER = constraint.column() {
                ptr_idx = Some(i);
            }
        }
        if let Some(ptr_idx) = ptr_idx {
            {
                let mut constraint_usage = info.constraint_usage(ptr_idx);
                constraint_usage.set_argv_index(1);
                constraint_usage.set_omit(true);
            }
            info.set_estimated_cost(1f64);
            info.set_estimated_rows(100);
            info.set_idx_num(1);
        } else {
            info.set_estimated_cost(2_147_483_647f64);
            info.set_estimated_rows(2_147_483_647);
            info.set_idx_num(0);
        }
        Ok(())
    }

    fn open(&self) -> Result<ArrayTabCursor> {
        Ok(ArrayTabCursor::new())
    }
}

/// A cursor for the Array virtual table
#[repr(C)]
struct ArrayTabCursor {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    /// Pointer to the array of values ("pointer")
    ptr: Option<Array>,
}

impl ArrayTabCursor {
    fn new() -> ArrayTabCursor {
        ArrayTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            row_id: 0,
            ptr: None,
        }
    }

    fn len(&self) -> i64 {
        match self.ptr {
            Some(ref a) => a.len() as i64,
            _ => 0,
        }
    }
}
impl VTabCursor for ArrayTabCursor {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Values<'_>) -> Result<()> {
        if idx_num > 0 {
            self.ptr = args.get_array(0)?;
        } else {
            self.ptr = None;
        }
        self.row_id = 1;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_id > self.len()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        match i {
            CARRAY_COLUMN_POINTER => Ok(()),
            _ => {
                if let Some(ref array) = self.ptr {
                    let value = &array[(self.row_id - 1) as usize];
                    ctx.set_result(&value)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_id)
    }
}

#[cfg(test)]
mod test {
    use crate::types::Value;
    use crate::vtab::array;
    use crate::Connection;
    use std::rc::Rc;

    #[test]
    fn test_array_module() {
        let db = Connection::open_in_memory().unwrap();
        array::load_module(&db).unwrap();

        let v = vec![1i64, 2, 3, 4];
        let values: Vec<Value> = v.into_iter().map(Value::from).collect();
        let ptr = Rc::new(values);
        {
            let mut stmt = db.prepare("SELECT value from rarray(?);").unwrap();

            let rows = stmt.query_map(&[&ptr], |row| row.get::<_, i64>(0)).unwrap();
            assert_eq!(2, Rc::strong_count(&ptr));
            let mut count = 0;
            for (i, value) in rows.enumerate() {
                assert_eq!(i as i64, value.unwrap() - 1);
                count += 1;
            }
            assert_eq!(4, count);
        }
        assert_eq!(1, Rc::strong_count(&ptr));
    }
}
