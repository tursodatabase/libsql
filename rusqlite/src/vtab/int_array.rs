//! Int array virtual table.
use std::cell::RefCell;
use std::default::Default;
use std::mem;
use std::rc::Rc;
use libc;

use {Connection, Error, Result};
use ffi;
use vtab::{declare_vtab, escape_double_quote, VTab, VTabCursor};

/// Create a specific instance of an intarray object.
/// The new intarray object is returned.
///
/// Each intarray object corresponds to a virtual table in the TEMP table
/// with the specified `name`.
pub fn create_int_array(conn: &Connection, name: &str) -> Result<Rc<RefCell<Vec<i64>>>> {
    let array = Rc::new(RefCell::new(Vec::new()));
    try!(conn.create_module(name, &INT_ARRAY_MODULE, Some(array.clone())));
    try!(conn.execute_batch(&format!("CREATE VIRTUAL TABLE temp.\"{0}\" USING \"{0}\"",
                                     escape_double_quote(name))));
    Ok(array)
}

/// Destroy the intarray object by dropping the virtual table.
/// If not done explicitly by the application, the virtual table will be dropped implicitly
/// by the system when the database connection is closed.
pub fn drop_int_array(conn: &Connection, name: &str) -> Result<()> {
    conn.execute_batch(&format!("DROP TABLE temp.\"{0}\"", escape_double_quote(name)))
}

init_module!(INT_ARRAY_MODULE,
             IntArrayVTab,
             IntArrayVTabCursor,
             int_array_create,
             int_array_best_index,
             int_array_destroy,
             int_array_open,
             int_array_close,
             int_array_filter,
             int_array_next,
             int_array_eof,
             int_array_column,
             int_array_rowid);

#[repr(C)]
struct IntArrayVTab {
    /// Base class
    base: ffi::sqlite3_vtab,
    array: *const Rc<RefCell<Vec<i64>>>,
}

impl VTab<IntArrayVTabCursor> for IntArrayVTab {
    fn create(db: *mut ffi::sqlite3,
              aux: *mut libc::c_void,
              _args: &[*const libc::c_char])
              -> Result<IntArrayVTab> {
        let array = unsafe { mem::transmute(aux) };
        let vtab = IntArrayVTab {
            base: Default::default(),
            array: array,
        };
        try!(declare_vtab(db, "CREATE TABLE x(value INTEGER PRIMARY KEY)"));
        Ok(vtab)
    }

    fn best_index(&self, _info: *mut ffi::sqlite3_index_info) {}

    fn open(&self) -> Result<IntArrayVTabCursor> {
        Ok(IntArrayVTabCursor::new())
    }
}

#[derive(Default)]
#[repr(C)]
struct IntArrayVTabCursor {
    /// Base class
    base: ffi::sqlite3_vtab_cursor,
    /// Current cursor position
    i: usize,
}

impl IntArrayVTabCursor {
    fn new() -> IntArrayVTabCursor {
        IntArrayVTabCursor {
            base: Default::default(),
            i: 0,
        }
    }
}

impl VTabCursor<IntArrayVTab> for IntArrayVTabCursor {
    fn vtab(&self) -> &mut IntArrayVTab {
        unsafe { &mut *(self.base.pVtab as *mut IntArrayVTab) }
    }
    fn filter(&mut self,
              _idx_num: libc::c_int,
              _idx_str: *const libc::c_char,
              _argc: libc::c_int,
              _argv: *mut *mut ffi::sqlite3_value)
              -> Result<()> {
        self.i = 0;
        Ok(())
    }
    fn next(&mut self) -> Result<()> {
        self.i = self.i + 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        let vtab = self.vtab();
        unsafe {
            let array = (*vtab.array).borrow();
            self.i >= array.len()
        }
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, _i: libc::c_int) -> Result<()> {
        use functions::ToResult;
        let vtab = self.vtab();
        unsafe {
            let array = (*vtab.array).borrow();
            array[self.i].set_result(ctx);
        }
        Ok(())
    }
    fn rowid(&self) -> Result<i64> {
        Ok(self.i as i64)
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use vtab::int_array;

    #[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn test_int_array_module() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE t1 (x INT);
                INSERT INTO t1 VALUES (1);
                INSERT INTO t1 VALUES (3);
                CREATE TABLE t2 (y INT);
                INSERT INTO t2 VALUES (11);
                CREATE TABLE t3 (z INT);
                INSERT INTO t3 VALUES (-5);").unwrap();
        let p1 = int_array::create_int_array(&db, "ex1").unwrap();
        let p2 = int_array::create_int_array(&db, "ex2").unwrap();
        let p3 = int_array::create_int_array(&db, "ex3").unwrap();

        let mut s = db.prepare("SELECT * FROM t1, t2, t3
                WHERE t1.x IN ex1
                AND t2.y IN ex2
                AND t3.z IN ex3").unwrap();

        p1.borrow_mut().append(&mut vec![1, 2, 3, 4]);
        p2.borrow_mut().append(&mut vec![5, 6, 7, 8, 9, 10, 11]);
        p3.borrow_mut().append(&mut vec![-1, -5, -10]);

        {
            let rows = s.query(&[]).unwrap();
            for row in rows {
                let row = row.unwrap();
                let i1: i64 = row.get(0);
                assert!(i1 == 1 || i1 == 3);
                assert_eq!(11, row.get(1));
                assert_eq!(-5, row.get(2));
            }
        }

        s.reset_if_needed();
        p1.borrow_mut().clear();
        p2.borrow_mut().clear();
        p3.borrow_mut().clear();
        p1.borrow_mut().append(&mut vec![1]);
        p2.borrow_mut().append(&mut vec![7, 11]);
        p3.borrow_mut().append(&mut vec![-5, -10]);

        {
            let row = s.query(&[]).unwrap().next().unwrap().unwrap();
            assert_eq!(1, row.get(0));
            assert_eq!(11, row.get(1));
            assert_eq!(-5, row.get(2));
        }

        s.reset_if_needed();
        p2.borrow_mut().clear();
        p3.borrow_mut().clear();
        p2.borrow_mut().append(&mut vec![3, 4, 5]);
        p3.borrow_mut().append(&mut vec![0, -5]);
        assert!(s.query(&[]).unwrap().next().is_none());

        int_array::drop_int_array(&db, "ex1").unwrap();
        int_array::drop_int_array(&db, "ex2").unwrap();
        int_array::drop_int_array(&db, "ex3").unwrap();
    }
}
