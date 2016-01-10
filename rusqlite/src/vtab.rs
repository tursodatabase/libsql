//! Create virtual tables.
//! (See http://sqlite.org/vtab.html)
use std::mem;
use std::ptr;
use libc::c_void;

use {Connection, Result, InnerConnection, str_to_cstring};
use functions::ToResult;
use ffi;

/// Virtual table module.
/// (See http://sqlite.org/c3ref/module.html)
// (partial/minimal implementation)
pub trait Module: Drop {
    type V: VTab;

    /// http://sqlite.org/vtab.html#xconnect
    fn connect(conn: &Connection, args: &[&str]) -> Result<Self::V>; // xcreate() == xconnect()
    // drop() = destroy (http://sqlite.org/c3ref/create_module.html)
}

/// VTab describes a particular instance of a virtual table.
/// (See http://sqlite.org/c3ref/vtab.html)
// (partial/minimal implementation)
pub trait VTab: Drop {
    type C: Cursor;

    /// http://sqlite.org/vtab.html#xopen
    fn open(&self) -> Result<Self::C>;
    /// http://sqlite.org/vtab.html#xbestindex
    fn best_index(&self) -> Result<()> {
        // TODO sqlite3_index_info*
        Ok(())
    }
// drop() = xdisconnect == xDestroy (http://sqlite.org/vtab.html#xdisconnect)
}

/// Cursor that points into the virtual table and is used to loop through the virtual table.
/// (See http://sqlite.org/c3ref/vtab_cursor.html)
pub trait Cursor: Drop {
    /// http://sqlite.org/vtab.html#xfilter
    fn filter(&self) -> Result<()>; // TODO int idxNum, const char *idxStr, int argc, sqlite3_value **argv
    /// http://sqlite.org/vtab.html#xnext
    fn next(&self) -> Result<()>;
    /// http://sqlite.org/vtab.html#xeof
    fn eof(&self) -> bool;
    /// http://sqlite.org/vtab.html#xcolumn
    fn column<T: ToResult + Sized>(&self, col: i32) -> Result<T>;
    /// http://sqlite.org/vtab.html#xrowid
    fn rowid(&self) -> i64; // TODO vs Result<i64>
    // drop() == http://sqlite.org/vtab.html#xclose
}

impl Connection {
    /// Register a virtual table implementation.
    pub fn create_module<M>(&self, module_name: &str, module: M) -> Result<()>
        where M: Module
    {
        self.db
            .borrow_mut()
            .create_module(module_name, module)
    }

    /// Declare the schema of a virtual table.
    pub fn declare_vtab(&self, sql: &str) -> Result<()> {
        self.db
            .borrow_mut()
            .declare_vtab(sql)
    }
}

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut libc::c_void) {
    let _: Box<T> = Box::from_raw(mem::transmute(p));
}

static RUST_MODULE: ffi::sqlite3_module = ffi::sqlite3_module {
    iVersion: 1,
    xCreate: Some(xCreate),
    xConnect: Some(xCreate), /* A virtual table is eponymous if its xCreate method is the exact same function as the xConnect method */
    xBestIndex: Some(xBestIndex),
    xDisconnect: Some(xDestroy),
    xDestroy: Some(xDestroy),
    xOpen: Some(xOpen),
    xClose: Some(xClose),
    xFilter: Some(xFilter),
    xNext: Some(xNext),
    xEof: Some(xEof),
    xColumn: Some(xColumn),
    xRowid: Some(xRowid),
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

unsafe extern "C" fn xCreate(arg1: *mut ffi::sqlite3,
                             pAux: *mut libc::c_void,
                             argc: libc::c_int,
                             argv: *const *const libc::c_char,
                             ppVTab: *mut *mut ffi::sqlite3_vtab,
                             arg2: *mut *mut libc::c_char)
                             -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xBestIndex(pVTab: *mut ffi::sqlite3_vtab,
                                info: *mut ffi::sqlite3_index_info)
                                -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xDestroy(pVTab: *mut ffi::sqlite3_vtab) -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xOpen(pVTab: *mut ffi::sqlite3_vtab,
                           ppCursor: *mut *mut ffi::sqlite3_vtab_cursor)
                           -> libc::c_int {
    unimplemented!()
}

unsafe extern "C" fn xClose(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xFilter(cursor: *mut ffi::sqlite3_vtab_cursor,
                             idxNum: libc::c_int,
                             idxStr: *const libc::c_char,
                             argc: libc::c_int,
                             argv: *mut *mut ffi::sqlite3_value)
                             -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xNext(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xEof(cursor: *mut ffi::sqlite3_vtab_cursor) -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xColumn(cursor: *mut ffi::sqlite3_vtab_cursor,
                             ctx: *mut ffi::sqlite3_context,
                             i: libc::c_int)
                             -> libc::c_int {
    unimplemented!()
}
unsafe extern "C" fn xRowid(cursor: *mut ffi::sqlite3_vtab_cursor,
                            pRowid: *mut ffi::sqlite3_int64)
                            -> libc::c_int {
    unimplemented!()
}

unsafe extern "C" fn set_err_msg(cursor: *mut ffi::sqlite3_vtab_cursor,
                                 err_msg: *mut libc::c_char)
                                 -> libc::c_int {
    let pVtab = (*cursor).pVtab;
    if !(*pVtab).zErrMsg.is_null() {
        ffi::sqlite3_free((*pVtab).zErrMsg as *mut libc::c_void)
    }
    (*pVtab).zErrMsg = err_msg;
    return ffi::SQLITE_ERROR;
}

impl InnerConnection {
    fn create_module<M>(&mut self, module_name: &str, module: M) -> Result<()>
        where M: Module
    {
        let boxed_mod: *mut M = Box::into_raw(Box::new(module));
        let c_name = try!(str_to_cstring(module_name));
        let r = unsafe {
            ffi::sqlite3_create_module_v2(self.db(),
                                          c_name.as_ptr(),
                                          ptr::null_mut(), // FIXME *const sqlite3_module
                                          mem::transmute(boxed_mod),
                                          Some(mem::transmute(free_boxed_value::<M>)))
        };
        self.decode_result(r)
    }

    fn declare_vtab(&mut self, sql: &str) -> Result<()> {
        let c_sql = try!(str_to_cstring(sql));
        let r = unsafe { ffi::sqlite3_declare_vtab(self.db(), c_sql.as_ptr()) };
        self.decode_result(r)
    }
}

// A intarray table
// #[repr(C)]
// struct IntArrayVTab {
// Base class
// base: ffi::sqlite3_vtab,
// }
//
// impl VTab for IntArrayVTab {
//
// }
//
// impl Drop for IntArrayVTab {
// fn drop(&mut self) {
// FIXME
// }
// }
//
// A intarray cursor
// #[repr(C)]
// struct IntArrayCursor {
// Base class
// base: ffi::sqlite3_vtab_cursor,
// i: usize
// }
//
// impl Cursor for IntArrayCursor {
// type V = IntArrayVTab;
//
// fn filter(&mut self) -> Result<()> {
// self.i = 0;
// Ok(())
// }
// fn next(&mut self) -> Result<()> {
// self.i = self.i + 1;
// Ok(())
// }
// fn eof(&self) -> bool {
// false // FIXME
// }
// fn column(&self, ctx: *mut ffi::sqlite3_context, _: i32) -> Result<()> {
// let v: i64 = 0; // FIXME
// unsafe {
// v.set_result(ctx);
// }
// Ok(())
// }
// fn rowid(&self) -> i64 {
// self.i as i64
// }
// }
//
// impl Drop for IntArrayCursor {
// fn drop(&mut self) {
// FIXME
// }
// }

// #[repr(C)]
// #[derive(Copy)]
// pub struct Struct_sqlite3_vtab {
// pub pModule: *const sqlite3_module,
// pub nRef: libc::c_int,
// pub zErrMsg: *mut libc::c_char,
// }
// #[repr(C)]
// #[derive(Copy)]
// pub struct Struct_sqlite3_vtab_cursor {
// pub pVtab: *mut sqlite3_vtab,
// }
//

// file:///usr/local/share/doc/rust/html/book/ffi.html#targeting-callbacks-to-rust-objects
//
// #[repr(C)]
// struct RustObject {
// a: i32,
// other members
// }
//
// file:///usr/local/share/doc/rust/html/book/associated-types.html#implementing-associated-types
// file:///usr/local/share/doc/rust/html/book/closures.html
//

#[cfg(test)]
mod test {

}
