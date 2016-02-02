//! Create virtual tables.
//! (See http://sqlite.org/vtab.html)
use std::cell::RefCell;
use std::default::Default;
use std::error::Error as StdError;
use std::ffi::CString;
use std::mem;
use std::rc::Rc;
use libc;

use {Connection, Error, Result, InnerConnection, str_to_cstring};
use error::error_from_sqlite_code;
use ffi;
use functions::ToResult;

// let conn: Connection = ...;
// let mod: Module = ...; // VTab builder
// conn.create_module("module", mod);
//
// conn.execute("CREATE VIRTUAL TABLE foo USING module(...)");
// \-> Module::xcreate
//  |-> let vtab: VTab = ...; // on the heap
//  \-> conn.declare_vtab("CREATE TABLE foo (...)");
// conn = Connection::open(...);
// \-> Module::xconnect
//  |-> let vtab: VTab = ...; // on the heap
//  \-> conn.declare_vtab("CREATE TABLE foo (...)");
//
// conn.close();
// \-> vtab.xdisconnect
// conn.execute("DROP TABLE foo");
// \-> vtab.xDestroy
//
// let stmt = conn.prepare("SELECT ... FROM foo WHERE ...");
// \-> vtab.xbestindex
// stmt.quey().next();
// \-> vtab.xopen
//  |-> let cursor: Cursor = ...; // on the heap
//  |-> cursor.xfilter or xnext
//  |-> cursor.xeof
//  \-> if not eof { cursor.column or xrowid } else { cursor.xclose }
//

impl Connection {
    /// Register a virtual table implementation.
    pub fn create_module<A>(&self,
                            module_name: &str,
                            module: *const ffi::sqlite3_module,
                            aux: A)
                            -> Result<()> {
        self.db
            .borrow_mut()
            .create_module(module_name, module, aux)
    }
}

impl InnerConnection {
    fn create_module<A>(&mut self,
                        module_name: &str,
                        module: *const ffi::sqlite3_module,
                        aux: A)
                        -> Result<()> {
        let boxed_aux: *mut A = Box::into_raw(Box::new(aux));
        let c_name = try!(str_to_cstring(module_name));
        let r = unsafe {
            ffi::sqlite3_create_module_v2(self.db(),
                                          c_name.as_ptr(),
                                          module,
                                          mem::transmute(boxed_aux),
                                          Some(mem::transmute(free_boxed_value::<A>)))
        };
        self.decode_result(r)
    }
}

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut libc::c_void) {
    let _: Box<T> = Box::from_raw(mem::transmute(p));
}

pub fn create_int_array(conn: &Connection, name: &str) -> Result<Rc<RefCell<Vec<i64>>>> {
    let array = Rc::new(RefCell::new(Vec::new()));
    try!(conn.create_module(name, &INT_ARRAY_MODULE, array.clone()));
    try!(conn.execute_batch(&format!("CREATE VIRTUAL TABLE temp.{0} USING {0}",
                                     escape_quote(name.to_string()))));
    Ok(array)
}

pub fn drop_int_array(conn: &Connection, name: &str) -> Result<()> {
    conn.execute_batch(&format!("DROP TABLE temp.{0}", escape_quote(name.to_string())))
}

fn escape_quote(identifier: String) -> String {
    if identifier.contains('"') {
        // escape quote by doubling them
        identifier.replace('"', "\"\"")
    } else {
        identifier
    }
}

#[macro_export]
macro_rules! init_module {
    ($module_name: ident, $vtab: ident, $cursor: ty,
        $create: ident, $best_index: ident, $destroy: ident,
        $open: ident, $close: ident,
        $filter: ident, $next: ident, $eof: ident,
        $column: ident, $rowid: ident) => {

static $module_name: ffi::sqlite3_module = ffi::sqlite3_module {
    iVersion: 1,
    xCreate: Some($create),
    xConnect: Some($create), /* A virtual table is eponymous if its xCreate method is the exact same function as the xConnect method */
    xBestIndex: Some($best_index),
    xDisconnect: Some($destroy),
    xDestroy: Some($destroy),
    xOpen: Some($open),
    xClose: Some($close),
    xFilter: Some($filter),
    xNext: Some($next),
    xEof: Some($eof),
    xColumn: Some($column),
    xRowid: Some($rowid),
    xUpdate: None, // TODO
    xBegin: None,
    xSync: None,
    xCommit: None,
    xRollback: None,
    xFindFunction: None,
    xRename: None,
    xSavepoint: None,
    xRelease: None,
    xRollbackTo: None,
};

unsafe extern "C" fn $create(db: *mut ffi::sqlite3,
                              aux: *mut libc::c_void,
                              argc: libc::c_int,
                              argv: *const *const libc::c_char,
                              pp_vtab: *mut *mut ffi::sqlite3_vtab,
                              err_msg: *mut *mut libc::c_char)
                              -> libc::c_int {
    match $vtab::create(db, aux, argc, argv) {
        Ok(vtab) => {
            let boxed_vtab: *mut $vtab = Box::into_raw(Box::new(vtab));
            *pp_vtab = boxed_vtab as *mut ffi::sqlite3_vtab;
            ffi::SQLITE_OK
        }
        Err(err) => {
            match err {
                Error::SqliteFailure(err, s) => {
                    if let Some(s) = s {
                        *err_msg = mprintf(&s);
                    }
                    err.extended_code
                }
                _ => {
                    *err_msg = mprintf(err.description());
                    ffi::SQLITE_ERROR
                }
            }
        }
    }
}
unsafe extern "C" fn $best_index(vtab: *mut ffi::sqlite3_vtab,
                                  info: *mut ffi::sqlite3_index_info)
                                  -> libc::c_int {
    let vtab = vtab as *mut $vtab;
    (*vtab).best_index(info);
    ffi::SQLITE_OK
}
unsafe extern "C" fn $destroy(vtab: *mut ffi::sqlite3_vtab) -> libc::c_int {
    let vtab = vtab as *mut $vtab;
    let _: Box<$vtab> = Box::from_raw(mem::transmute(vtab));
    ffi::SQLITE_OK
}

unsafe extern "C" fn $open(vtab: *mut ffi::sqlite3_vtab,
                            pp_cursor: *mut *mut ffi::sqlite3_vtab_cursor)
                            -> libc::c_int {
    let vtab = vtab as *mut $vtab;
    let cursor = (*vtab).open();
    let boxed_cursor: *mut $cursor = Box::into_raw(Box::new(cursor));
    *pp_cursor = boxed_cursor as *mut ffi::sqlite3_vtab_cursor;
    ffi::SQLITE_OK
}
unsafe extern "C" fn $close(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    let _: Box<$cursor> = Box::from_raw(mem::transmute(cursor));
    ffi::SQLITE_OK
}

unsafe extern "C" fn $filter(cursor: *mut ffi::sqlite3_vtab_cursor,
                              _idx_num: libc::c_int,
                              _idx_str: *const libc::c_char,
                              _argc: libc::c_int,
                              _argv: *mut *mut ffi::sqlite3_value)
                              -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    (*cursor).filter()
}
unsafe extern "C" fn $next(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    (*cursor).next()
}
unsafe extern "C" fn $eof(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    (*cursor).eof() as libc::c_int
}
unsafe extern "C" fn $column(cursor: *mut ffi::sqlite3_vtab_cursor,
                              ctx: *mut ffi::sqlite3_context,
                              i: libc::c_int)
                              -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    (*cursor).column(ctx, i)
}
unsafe extern "C" fn $rowid(cursor: *mut ffi::sqlite3_vtab_cursor,
                             p_rowid: *mut ffi::sqlite3_int64)
                             -> libc::c_int {
    let cursor = cursor as *mut $cursor;
    *p_rowid = (*cursor).rowid();
    ffi::SQLITE_OK
}
    }
}

init_module!(INT_ARRAY_MODULE, IntArrayVTab, IntArrayVTabCursor,
    int_array_create, int_array_best_index, int_array_destroy,
    int_array_open, int_array_close,
    int_array_filter, int_array_next, int_array_eof,
    int_array_column, int_array_rowid);

#[repr(C)]
struct IntArrayVTab {
    /// Base class
    base: ffi::sqlite3_vtab,
    array: *const Rc<RefCell<Vec<i64>>>,
}

impl IntArrayVTab {
    fn create(db: *mut ffi::sqlite3,
              aux: *mut libc::c_void,
              _argc: libc::c_int,
              _argv: *const *const libc::c_char)
              -> Result<IntArrayVTab> {
        let array = unsafe { mem::transmute(aux) };
        let vtab = IntArrayVTab {
            base: Default::default(),
            array: array,
        };
        try!(IntArrayVTab::declare_vtab(db, "CREATE TABLE x(value INTEGER PRIMARY KEY)"));
        Ok(vtab)
    }
    fn declare_vtab(db: *mut ffi::sqlite3, sql: &str) -> Result<()> {
        let c_sql = try!(CString::new(sql));
        let rc = unsafe { ffi::sqlite3_declare_vtab(db, c_sql.as_ptr()) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(error_from_sqlite_code(rc, None))
        }
    }

    fn best_index(&self, _info: *mut ffi::sqlite3_index_info) {
        // unimplemented!()
    }

    fn open(&self) -> IntArrayVTabCursor {
        IntArrayVTabCursor::new()
    }

    fn set_err_msg(&mut self, err_msg: &str) {
        if !self.base.zErrMsg.is_null() {
            unsafe {
                ffi::sqlite3_free(self.base.zErrMsg as *mut libc::c_void);
            }
        }
        self.base.zErrMsg = mprintf(err_msg);
    }
}

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

    fn vtab(&self) -> *mut IntArrayVTab {
        self.base.pVtab as *mut IntArrayVTab
    }

    fn filter(&mut self) -> libc::c_int {
        self.i = 0;
        ffi::SQLITE_OK
    }
    fn next(&mut self) -> libc::c_int {
        self.i = self.i + 1;
        ffi::SQLITE_OK
    }
    fn eof(&self) -> bool {
        let vtab = self.vtab();
        unsafe {
            let array = (*(*vtab).array).borrow();
            self.i >= array.len()
        }
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, _i: libc::c_int) -> libc::c_int {
        let vtab = self.vtab();
        unsafe {
            let array = (*(*vtab).array).borrow();
            array[self.i].set_result(ctx);
        }
        ffi::SQLITE_OK
    }
    fn rowid(&self) -> i64 {
        self.i as i64
    }
}

unsafe fn result_error(ctx: *mut ffi::sqlite3_context, err: Error) -> libc::c_int {
    match err {
        Error::SqliteFailure(err, s) => {
            ffi::sqlite3_result_error_code(ctx, err.extended_code);
            if let Some(Ok(cstr)) = s.map(|s| str_to_cstring(&s)) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
            err.extended_code
        }
        _ => {
            ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_CORRUPT_VTAB);
            if let Ok(cstr) = str_to_cstring(err.description()) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
            ffi::SQLITE_CORRUPT_VTAB
        }
    }
}

// Space to hold this error message string must be obtained from an SQLite memory allocation function.
fn mprintf(err_msg: &str) -> *mut ::libc::c_char {
    let c_format = CString::new("%s").unwrap();
    let c_err = CString::new(err_msg).unwrap();
    unsafe { ffi::sqlite3_mprintf(c_format.as_ptr(), c_err.as_ptr()) }
}

#[cfg(test)]
mod test {
    use Connection;
    use vtab;

    #[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn test_int_array_module() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE t1 (x INT);
                INSERT INTO t1 VALUES (1), (3);
                CREATE TABLE t2 (y INT);
                INSERT INTO t2 VALUES (11);
                CREATE TABLE t3 (z INT);
                INSERT INTO t3 VALUES (-5);").unwrap();
        let p1 = vtab::create_int_array(&db, "ex1").unwrap();
        let p2 = vtab::create_int_array(&db, "ex2").unwrap();
        let p3 = vtab::create_int_array(&db, "ex3").unwrap();

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

        vtab::drop_int_array(&db, "ex1").unwrap();
        vtab::drop_int_array(&db, "ex2").unwrap();
        vtab::drop_int_array(&db, "ex3").unwrap();
    }
}
