//! Create virtual tables.
//! (See http://sqlite.org/vtab.html)
use std::default::Default;
use std::error::Error as StdError;
use std::ffi::CString;
use std::mem;
use libc;

use {Connection, Error, Result, InnerConnection, str_to_cstring};
use error::error_from_sqlite_code;
use ffi;
use functions::{ToResult, NoMem, TooBig};

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

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut libc::c_void) {
    let _: Box<T> = Box::from_raw(mem::transmute(p));
}

static RUST_MODULE: ffi::sqlite3_module = ffi::sqlite3_module {
    iVersion: 1,
    xCreate: Some(x_create),
    xConnect: Some(x_create), /* A virtual table is eponymous if its xCreate method is the exact same function as the xConnect method */
    xBestIndex: Some(x_best_index),
    xDisconnect: Some(x_destroy),
    xDestroy: Some(x_destroy),
    xOpen: Some(x_open),
    xClose: Some(x_close),
    xFilter: Some(x_filter),
    xNext: Some(x_next),
    xEof: Some(x_eof),
    xColumn: Some(x_column),
    xRowid: Some(x_rowid),
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

unsafe extern "C" fn x_create(db: *mut ffi::sqlite3,
                              aux: *mut libc::c_void,
                              argc: libc::c_int,
                              argv: *const *const libc::c_char,
                              pp_vtab: *mut *mut ffi::sqlite3_vtab,
                              err_msg: *mut *mut libc::c_char)
                              -> libc::c_int {
    match VTab::new(db, aux, argc, argv) {
        Ok(vtab) => {
            let boxed_vtab: *mut VTab = Box::into_raw(Box::new(vtab));
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

unsafe extern "C" fn x_destroy(vtab: *mut ffi::sqlite3_vtab) -> libc::c_int {
    let vtab = vtab as *mut VTab;
    let _: Box<VTab> = Box::from_raw(mem::transmute(vtab));
    ffi::SQLITE_OK
}

#[repr(C)]
struct VTab {
    /// Base class
    base: ffi::sqlite3_vtab,
}

impl VTab {
    fn new(db: *mut ffi::sqlite3,
           aux: *mut libc::c_void,
           argc: libc::c_int,
           argv: *const *const libc::c_char)
           -> Result<VTab> {
        unimplemented!()
    }
    fn open(&self) -> VTabCursor {
        VTabCursor::new()
    }
    unsafe fn declare_vtab(db: *mut ffi::sqlite3, sql: &str) -> Result<()> {
        let c_sql = try!(CString::new(sql));
        let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(error_from_sqlite_code(rc, None))
        }
    }

    unsafe fn set_err_msg(&mut self, err_msg: &str) -> libc::c_int {
        if !self.base.zErrMsg.is_null() {
            ffi::sqlite3_free(self.base.zErrMsg as *mut libc::c_void)
        }
        self.base.zErrMsg = mprintf(err_msg);
        return ffi::SQLITE_ERROR;
    }
}

unsafe extern "C" fn x_best_index(_vtab: *mut ffi::sqlite3_vtab,
                                  _info: *mut ffi::sqlite3_index_info)
                                  -> libc::c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_open(vtab: *mut ffi::sqlite3_vtab,
                            pp_cursor: *mut *mut ffi::sqlite3_vtab_cursor)
                            -> libc::c_int {
    let vtab = vtab as *mut VTab;
    let cursor = (*vtab).open();
    let boxed_cursor: *mut VTabCursor = Box::into_raw(Box::new(cursor));
    *pp_cursor = boxed_cursor as *mut ffi::sqlite3_vtab_cursor;
    ffi::SQLITE_OK
}
unsafe extern "C" fn x_close(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    let _: Box<VTabCursor> = Box::from_raw(mem::transmute(cursor));
    ffi::SQLITE_OK
}

#[repr(C)]
struct VTabCursor {
    /// Base class
    base: ffi::sqlite3_vtab_cursor,
    /// Current cursor position
    i: usize,
}

impl VTabCursor {
    fn new() -> VTabCursor {
        VTabCursor {
            base: Default::default(),
            i: 0,
        }
    }

    unsafe fn vtab(&self) -> *mut VTab {
        self.base.pVtab as *mut VTab
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
        unimplemented!()
    }
    fn column(&self, ctx: *mut ffi::sqlite3_context, i: libc::c_int) -> libc::c_int {
        // FIXME Result<()>
        unimplemented!()
    }
    fn rowid(&self) -> i64 {
        self.i as i64
    }
}

unsafe extern "C" fn x_filter(cursor: *mut ffi::sqlite3_vtab_cursor,
                              _idx_num: libc::c_int,
                              _idx_str: *const libc::c_char,
                              _argc: libc::c_int,
                              _argv: *mut *mut ffi::sqlite3_value)
                              -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    (*cursor).filter()
}
unsafe extern "C" fn x_next(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    (*cursor).next()
}
unsafe extern "C" fn x_eof(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    (*cursor).eof() as libc::c_int
}
unsafe extern "C" fn x_column(cursor: *mut ffi::sqlite3_vtab_cursor,
                              ctx: *mut ffi::sqlite3_context,
                              i: libc::c_int)
                              -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    (*cursor).column(ctx, i)
}
unsafe extern "C" fn x_rowid(cursor: *mut ffi::sqlite3_vtab_cursor,
                             p_rowid: *mut ffi::sqlite3_int64)
                             -> libc::c_int {
    let cursor = cursor as *mut VTabCursor;
    *p_rowid = (*cursor).rowid();
    ffi::SQLITE_OK
}

unsafe extern "C" fn result_error(ctx: *mut ffi::sqlite3_context, err: Error) -> libc::c_int {
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
unsafe fn mprintf(err_msg: &str) -> *mut ::libc::c_char {
    let c_format = CString::new("%s").unwrap();
    let c_err = CString::new(err_msg).unwrap();
    ffi::sqlite3_mprintf(c_format.as_ptr(), c_err.as_ptr())
}

impl InnerConnection {
    fn create_module<A>(&mut self,
                        module_name: &str,
                        module: *const ffi::sqlite3_module,
                        aux: A)
                        -> Result<()> {
        // FIXME Both rust_module and aux need to be boxed
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

#[cfg(test)]
mod test {

}
