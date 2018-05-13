//! Create virtual tables.
//! (See http://sqlite.org/vtab.html)
use std::borrow::Cow::{self, Borrowed, Owned};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

use context::{report_error, set_result};
use error::error_from_sqlite_code;
use ffi;
use types::{FromSql, FromSqlError, ToSql, ValueRef};
use {str_to_cstring, Connection, Error, InnerConnection, Result};

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
// stmt.query().next();
// \-> vtab.xopen
//  |-> let cursor: VTabCursor = ...; // on the heap
//  |-> cursor.xfilter or xnext
//  |-> cursor.xeof
//  \-> if not eof { cursor.column or xrowid } else { cursor.xclose }
//

/// Virtual table instance trait.
pub trait VTab: Sized {
    type Aux;
    type Cursor: VTabCursor;

    /// Create a new instance of a virtual table in response to a CREATE VIRTUAL TABLE statement.
    /// The `db` parameter is a pointer to the SQLite database connection that is executing
    /// the CREATE VIRTUAL TABLE statement.
    unsafe fn create(db: *mut ffi::sqlite3, aux: *mut Self::Aux, args: &[&[u8]]) -> Result<Self> {
        Self::connect(db, aux, args)
    }
    /// Similar to `create`. The difference is that `connect` is called to establish a new connection
    /// to an _existing_ virtual table whereas `create` is called to create a new virtual table from scratch.
    unsafe fn connect(db: *mut ffi::sqlite3, aux: *mut Self::Aux, args: &[&[u8]]) -> Result<Self>;
    /// Determine the best way to access the virtual table.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;
    /// Create a new cursor used for accessing a virtual table.
    fn open(&self) -> Result<Self::Cursor>;
}

bitflags! {
    #[doc = "Index constraint operator."]
    #[repr(C)]
    pub struct IndexConstraintOp: ::std::os::raw::c_uchar {
        const SQLITE_INDEX_CONSTRAINT_EQ    = 2;
        const SQLITE_INDEX_CONSTRAINT_GT    = 4;
        const SQLITE_INDEX_CONSTRAINT_LE    = 8;
        const SQLITE_INDEX_CONSTRAINT_LT    = 16;
        const SQLITE_INDEX_CONSTRAINT_GE    = 32;
        const SQLITE_INDEX_CONSTRAINT_MATCH = 64;
    }
}

pub struct IndexInfo(*mut ffi::sqlite3_index_info);

impl IndexInfo {
    pub fn constraints(&self) -> IndexConstraintIter {
        let constraints =
            unsafe { slice::from_raw_parts((*self.0).aConstraint, (*self.0).nConstraint as usize) };
        IndexConstraintIter {
            iter: constraints.iter(),
        }
    }

    /// Number of terms in the ORDER BY clause
    pub fn num_of_order_by(&self) -> usize {
        unsafe { (*self.0).nOrderBy as usize }
    }
    /// Column number
    pub fn order_by_column(&self, order_by_idx: usize) -> c_int {
        unsafe {
            let order_bys = slice::from_raw_parts((*self.0).aOrderBy, (*self.0).nOrderBy as usize);
            order_bys[order_by_idx].iColumn
        }
    }
    /// True for DESC.  False for ASC.
    pub fn is_order_by_desc(&self, order_by_idx: usize) -> bool {
        unsafe {
            let order_bys = slice::from_raw_parts((*self.0).aOrderBy, (*self.0).nOrderBy as usize);
            order_bys[order_by_idx].desc != 0
        }
    }

    /// if `argv_index` > 0, constraint is part of argv to xFilter
    pub fn constraint_usage(&mut self, constraint_idx: usize) -> IndexConstraintUsage {
        let constraint_usages = unsafe {
            slice::from_raw_parts_mut((*self.0).aConstraintUsage, (*self.0).nConstraint as usize)
        };
        IndexConstraintUsage(&mut constraint_usages[constraint_idx])
    }

    /// Number used to identify the index
    pub fn set_idx_num(&mut self, idx_num: c_int) {
        unsafe {
            (*self.0).idxNum = idx_num;
        }
    }
    /// True if output is already ordered
    pub fn set_order_by_consumed(&mut self, order_by_consumed: bool) {
        unsafe {
            (*self.0).orderByConsumed = if order_by_consumed { 1 } else { 0 };
        }
    }
    /// Estimated cost of using this index
    pub fn set_estimated_cost(&mut self, estimated_ost: f64) {
        unsafe {
            (*self.0).estimatedCost = estimated_ost;
        }
    }

    /// Estimated number of rows returned
    #[cfg(feature = "bundled")] // SQLite >= 3.8.2
    pub fn set_estimated_rows(&mut self, estimated_rows: i64) {
        unsafe {
            (*self.0).estimatedRows = estimated_rows;
        }
    }
}
pub struct IndexConstraintIter<'a> {
    iter: slice::Iter<'a, ffi::sqlite3_index_constraint>,
}

impl<'a> Iterator for IndexConstraintIter<'a> {
    type Item = IndexConstraint<'a>;

    fn next(&mut self) -> Option<IndexConstraint<'a>> {
        self.iter.next().map(|raw| IndexConstraint(raw))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

pub struct IndexConstraint<'a>(&'a ffi::sqlite3_index_constraint);

impl<'a> IndexConstraint<'a> {
    /// Column constrained.  -1 for ROWID
    pub fn column(&self) -> c_int {
        self.0.iColumn
    }
    /// Constraint operator
    pub fn operator(&self) -> IndexConstraintOp {
        IndexConstraintOp::from_bits_truncate(self.0.op)
    }
    /// True if this constraint is usable
    pub fn is_usable(&self) -> bool {
        self.0.usable != 0
    }
}

pub struct IndexConstraintUsage<'a>(&'a mut ffi::sqlite3_index_constraint_usage);

impl<'a> IndexConstraintUsage<'a> {
    /// if `argv_index` > 0, constraint is part of argv to xFilter
    pub fn set_argv_index(&mut self, argv_index: c_int) {
        self.0.argvIndex = argv_index;
    }
    /// if `omit`, do not code a test for this constraint
    pub fn set_omit(&mut self, omit: bool) {
        self.0.omit = if omit { 1 } else { 0 };
    }
}

/// Virtual table cursor trait.
pub trait VTabCursor: Sized {
    type Table: VTab;
    /// Accessor to the associated virtual table.
    fn vtab(&self) -> &Self::Table;
    /// Begin a search of a virtual table.
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values) -> Result<()>;
    /// Advance cursor to the next row of a result set initiated by `filter`.
    fn next(&mut self) -> Result<()>;
    /// Must return `false` if the cursor currently points to a valid row of data,
    /// or `true` otherwise.
    fn eof(&self) -> bool;
    /// Find the value for the `i`-th column of the current row.
    /// `i` is zero-based so the first column is numbered 0.
    /// May return its result back to SQLite using one of the specified `ctx`.
    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()>;
    /// Return the rowid of row that the cursor is currently pointing at.
    fn rowid(&self) -> Result<i64>;
}

pub struct Context(*mut ffi::sqlite3_context);

impl Context {
    pub fn set_result<T: ToSql>(&mut self, value: &T) {
        let t = value.to_sql();
        match t {
            Ok(ref value) => unsafe { set_result(self.0, value) },
            Err(err) => unsafe { report_error(self.0, &err) },
        }
    }
}

pub struct Values<'a> {
    args: &'a [*mut ffi::sqlite3_value],
}

impl<'a> Values<'a> {
    pub fn len(&self) -> usize {
        self.args.len()
    }

    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    pub fn get<T: FromSql>(&self, idx: usize) -> Result<T> {
        let arg = self.args[idx];
        let value = unsafe { ValueRef::from_value(arg) };
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => Error::InvalidFilterParameterType(idx, value.data_type()),
            FromSqlError::Other(err) => {
                Error::FromSqlConversionFailure(idx, value.data_type(), err)
            }
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx as c_int, i),
        })
    }

    pub fn iter(&self) -> ValueIter {
        ValueIter {
            iter: self.args.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Values<'a> {
    type Item = ValueRef<'a>;
    type IntoIter = ValueIter<'a>;

    fn into_iter(self) -> ValueIter<'a> {
        self.iter()
    }
}

pub struct ValueIter<'a> {
    iter: slice::Iter<'a, *mut ffi::sqlite3_value>,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = ValueRef<'a>;

    fn next(&mut self) -> Option<ValueRef<'a>> {
        self.iter
            .next()
            .map(|&raw| unsafe { ValueRef::from_value(raw) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl Connection {
    /// Register a virtual table implementation.
    pub fn create_module<A>(
        &self,
        module_name: &str,
        module: *const ffi::sqlite3_module,
        aux: Option<A>,
    ) -> Result<()> {
        self.db.borrow_mut().create_module(module_name, module, aux)
    }
}

impl InnerConnection {
    fn create_module<A>(
        &mut self,
        module_name: &str,
        module: *const ffi::sqlite3_module,
        aux: Option<A>,
    ) -> Result<()> {
        let c_name = try!(str_to_cstring(module_name));
        let r = match aux {
            Some(aux) => {
                let boxed_aux: *mut A = Box::into_raw(Box::new(aux));
                unsafe {
                    ffi::sqlite3_create_module_v2(
                        self.db(),
                        c_name.as_ptr(),
                        module,
                        boxed_aux as *mut c_void,
                        Some(free_boxed_value::<A>),
                    )
                }
            }
            None => unsafe {
                ffi::sqlite3_create_module_v2(
                    self.db(),
                    c_name.as_ptr(),
                    module,
                    ptr::null_mut(),
                    None,
                )
            },
        };
        self.decode_result(r)
    }
}

/// Declare the schema of a virtual table.
pub unsafe fn declare_vtab(db: *mut ffi::sqlite3, sql: &str) -> Result<()> {
    let c_sql = try!(CString::new(sql));
    let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(error_from_sqlite_code(rc, None))
    }
}

/// Escape double-quote (`"`) character occurences by doubling them (`""`).
pub fn escape_double_quote(identifier: &str) -> Cow<str> {
    if identifier.contains('"') {
        // escape quote by doubling them
        Owned(identifier.replace("\"", "\"\""))
    } else {
        Borrowed(identifier)
    }
}
/// Dequote string
pub fn dequote(s: &str) -> &str {
    if s.len() < 2 {
        return s;
    }
    match s.bytes().next() {
        Some(b) if b == b'"' || b == b'\'' => match s.bytes().rev().next() {
            Some(e) if e == b => &s[1..s.len() - 1],
            _ => s,
        },
        _ => s,
    }
}
/// The boolean can be one of:
/// ```text
/// 1 yes true on
/// 0 no false off
/// ```
pub fn parse_boolean(s: &str) -> Option<bool> {
    if s.eq_ignore_ascii_case("yes") || s.eq_ignore_ascii_case("on")
        || s.eq_ignore_ascii_case("true") || s.eq("1")
    {
        Some(true)
    } else if s.eq_ignore_ascii_case("no") || s.eq_ignore_ascii_case("off")
        || s.eq_ignore_ascii_case("false") || s.eq("0")
    {
        Some(false)
    } else {
        None
    }
}

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    let _: Box<T> = Box::from_raw(p as *mut T);
}

#[macro_export]
macro_rules! init_module {
    (
        $module_name:ident,
        $vtab:ident,
        $aux:ty,
        $cursor:ty,
        $create:ident,
        $connect:ident,
        $best_index:ident,
        $disconnect:ident,
        $destroy:ident,
        $open:ident,
        $close:ident,
        $filter:ident,
        $next:ident,
        $eof:ident,
        $column:ident,
        $rowid:ident
    ) => {
        static $module_name: ffi::sqlite3_module = ffi::sqlite3_module {
            iVersion: 1,
            xCreate: Some($create),
            xConnect: Some($connect),
            xBestIndex: Some($best_index),
            xDisconnect: Some($disconnect),
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

        // The xConnect and xCreate methods do the same thing, but they must be
        // different so that the virtual table is not an eponymous virtual table.
        create_or_connect!($vtab, $aux, $create, create);
        common_decl!(
            $vtab,
            $aux,
            $cursor,
            $connect,
            $best_index,
            $disconnect,
            $destroy,
            $open,
            $close,
            $filter,
            $next,
            $eof,
            $column,
            $rowid
        );
    };
} // init_module macro end

#[macro_export]
macro_rules! eponymous_module {
    (
        $module_name:ident,
        $vtab:ident,
        $aux:ty,
        $cursor:ty,
        $create:expr,
        $connect:ident,
        $best_index:ident,
        $disconnect:ident,
        $destroy:expr,
        $open:ident,
        $close:ident,
        $filter:ident,
        $next:ident,
        $eof:ident,
        $column:ident,
        $rowid:ident
    ) => {
        static $module_name: ffi::sqlite3_module = ffi::sqlite3_module {
            iVersion: 1,
            xCreate: $create, /* For eponymous-only virtual tables, the xCreate method is NULL */
            xConnect: Some($connect), /* A virtual table is eponymous if its xCreate method is
                                                 the exact same function as the xConnect method */
            xBestIndex: Some($best_index),
            xDisconnect: Some($disconnect),
            xDestroy: $destroy,
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

        common_decl!(
            $vtab,
            $aux,
            $cursor,
            $connect,
            $best_index,
            $disconnect,
            $destroy,
            $open,
            $close,
            $filter,
            $next,
            $eof,
            $column,
            $rowid
        );
    };
} // eponymous_module macro end

macro_rules! create_or_connect {
    ($vtab:ident, $aux:ty, $create_or_connect:ident, $vtab_func:ident) => {
        unsafe extern "C" fn $create_or_connect(
            db: *mut ffi::sqlite3,
            aux: *mut c_void,
            argc: c_int,
            argv: *const *const c_char,
            pp_vtab: *mut *mut ffi::sqlite3_vtab,
            err_msg: *mut *mut c_char,
        ) -> c_int {
            use std::error::Error as StdError;
            use std::ffi::CStr;
            use std::slice;
            use vtab::mprintf;

            let aux = aux as *mut $aux;
            let args = slice::from_raw_parts(argv, argc as usize);
            let vec = args.iter()
                .map(|&cs| CStr::from_ptr(cs).to_bytes())
                .collect::<Vec<_>>();
            match $vtab::$vtab_func(db, aux, &vec[..]) {
                Ok(vtab) => {
                    let boxed_vtab: *mut $vtab = Box::into_raw(Box::new(vtab));
                    *pp_vtab = boxed_vtab as *mut ffi::sqlite3_vtab;
                    ffi::SQLITE_OK
                }
                Err(Error::SqliteFailure(err, s)) => {
                    if let Some(s) = s {
                        *err_msg = mprintf(&s);
                    }
                    err.extended_code
                }
                Err(err) => {
                    *err_msg = mprintf(err.description());
                    ffi::SQLITE_ERROR
                }
            }
        }
    };
} // create_or_connect macro end

macro_rules! common_decl {
    (
        $vtab:ident,
        $aux:ty,
        $cursor:ty,
        $connect:ident,
        $best_index:ident,
        $disconnect:ident,
        $destroy:expr,
        $open:ident,
        $close:ident,
        $filter:ident,
        $next:ident,
        $eof:ident,
        $column:ident,
        $rowid:ident
    ) => {
        create_or_connect!($vtab, $aux, $connect, connect);
        unsafe extern "C" fn $best_index(
            vtab: *mut ffi::sqlite3_vtab,
            info: *mut ffi::sqlite3_index_info,
        ) -> c_int {
            use std::error::Error as StdError;
            use vtab::set_err_msg;
            let vt = vtab as *mut $vtab;
            let mut idx_info = IndexInfo(info);
            match (*vt).best_index(&mut idx_info) {
                Ok(_) => ffi::SQLITE_OK,
                Err(Error::SqliteFailure(err, s)) => {
                    if let Some(err_msg) = s {
                        set_err_msg(vtab, &err_msg);
                    }
                    err.extended_code
                }
                Err(err) => {
                    set_err_msg(vtab, err.description());
                    ffi::SQLITE_ERROR
                }
            }
        }
        unsafe extern "C" fn $disconnect(vtab: *mut ffi::sqlite3_vtab) -> c_int {
            let vtab = vtab as *mut $vtab;
            let _: Box<$vtab> = Box::from_raw(vtab);
            ffi::SQLITE_OK
        }

        unsafe extern "C" fn $open(
            vtab: *mut ffi::sqlite3_vtab,
            pp_cursor: *mut *mut ffi::sqlite3_vtab_cursor,
        ) -> c_int {
            use std::error::Error as StdError;
            use vtab::set_err_msg;
            let vt = vtab as *mut $vtab;
            match (*vt).open() {
                Ok(cursor) => {
                    let boxed_cursor: *mut $cursor = Box::into_raw(Box::new(cursor));
                    *pp_cursor = boxed_cursor as *mut ffi::sqlite3_vtab_cursor;
                    ffi::SQLITE_OK
                }
                Err(Error::SqliteFailure(err, s)) => {
                    if let Some(err_msg) = s {
                        set_err_msg(vtab, &err_msg);
                    }
                    err.extended_code
                }
                Err(err) => {
                    set_err_msg(vtab, err.description());
                    ffi::SQLITE_ERROR
                }
            }
        }
        unsafe extern "C" fn $close(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int {
            let cr = cursor as *mut $cursor;
            let _: Box<$cursor> = Box::from_raw(cr);
            ffi::SQLITE_OK
        }

        unsafe extern "C" fn $filter(
            cursor: *mut ffi::sqlite3_vtab_cursor,
            idx_num: c_int,
            idx_str: *const c_char,
            argc: c_int,
            argv: *mut *mut ffi::sqlite3_value,
        ) -> c_int {
            use std::ffi::CStr;
            use std::slice;
            use std::str;
            use vtab::{cursor_error, Values};
            let idx_name = if idx_str.is_null() {
                None
            } else {
                let c_slice = CStr::from_ptr(idx_str).to_bytes();
                Some(str::from_utf8_unchecked(c_slice))
            };
            let args = slice::from_raw_parts_mut(argv, argc as usize);
            let values = Values { args: args };
            let cr = cursor as *mut $cursor;
            cursor_error(cursor, (*cr).filter(idx_num, idx_name, &values))
        }
        unsafe extern "C" fn $next(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int {
            use vtab::cursor_error;
            let cr = cursor as *mut $cursor;
            cursor_error(cursor, (*cr).next())
        }
        unsafe extern "C" fn $eof(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int {
            let cr = cursor as *mut $cursor;
            (*cr).eof() as c_int
        }
        unsafe extern "C" fn $column(
            cursor: *mut ffi::sqlite3_vtab_cursor,
            ctx: *mut ffi::sqlite3_context,
            i: c_int,
        ) -> c_int {
            use vtab::{result_error, Context};
            let cr = cursor as *mut $cursor;
            let mut ctxt = Context(ctx);
            result_error(ctx, (*cr).column(&mut ctxt, i))
        }
        unsafe extern "C" fn $rowid(
            cursor: *mut ffi::sqlite3_vtab_cursor,
            p_rowid: *mut ffi::sqlite3_int64,
        ) -> c_int {
            use vtab::cursor_error;
            let cr = cursor as *mut $cursor;
            match (*cr).rowid() {
                Ok(rowid) => {
                    *p_rowid = rowid;
                    ffi::SQLITE_OK
                }
                err => cursor_error(cursor, err),
            }
        }
    };
} // common_decl macro end

/// Virtual table cursors can set an error message by assigning a string to `zErrMsg`.
pub unsafe fn cursor_error<T>(cursor: *mut ffi::sqlite3_vtab_cursor, result: Result<T>) -> c_int {
    use std::error::Error as StdError;
    match result {
        Ok(_) => ffi::SQLITE_OK,
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(err_msg) = s {
                set_err_msg((*cursor).pVtab, &err_msg);
            }
            err.extended_code
        }
        Err(err) => {
            set_err_msg((*cursor).pVtab, err.description());
            ffi::SQLITE_ERROR
        }
    }
}

/// Virtual tables methods can set an error message by assigning a string to `zErrMsg`.
pub unsafe fn set_err_msg(vtab: *mut ffi::sqlite3_vtab, err_msg: &str) {
    if !(*vtab).zErrMsg.is_null() {
        ffi::sqlite3_free((*vtab).zErrMsg as *mut c_void);
    }
    (*vtab).zErrMsg = mprintf(err_msg);
}

/// To raise an error, the `column` method should use this method to set the error message
/// and return the error code.
unsafe fn result_error<T>(ctx: *mut ffi::sqlite3_context, result: Result<T>) -> c_int {
    use std::error::Error as StdError;
    match result {
        Ok(_) => ffi::SQLITE_OK,
        Err(Error::SqliteFailure(err, s)) => {
            match err.extended_code {
                ffi::SQLITE_TOOBIG => {
                    ffi::sqlite3_result_error_toobig(ctx);
                }
                ffi::SQLITE_NOMEM => {
                    ffi::sqlite3_result_error_nomem(ctx);
                }
                code => {
                    ffi::sqlite3_result_error_code(ctx, code);
                    if let Some(Ok(cstr)) = s.map(|s| str_to_cstring(&s)) {
                        ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
                    }
                }
            };
            err.extended_code
        }
        Err(err) => {
            ffi::sqlite3_result_error_code(ctx, ffi::SQLITE_ERROR);
            if let Ok(cstr) = str_to_cstring(err.description()) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
            ffi::SQLITE_ERROR
        }
    }
}

// Space to hold this error message string must be obtained
// from an SQLite memory allocation function.
pub fn mprintf(err_msg: &str) -> *mut c_char {
    let c_format = CString::new("%s").unwrap();
    let c_err = CString::new(err_msg).unwrap();
    unsafe { ffi::sqlite3_mprintf(c_format.as_ptr(), c_err.as_ptr()) }
}

#[cfg(feature = "csvtab")]
pub mod csvtab;
pub mod int_array;
#[cfg(feature = "bundled")]
pub mod series; // SQLite >= 3.9.0

#[cfg(test)]
mod test {
    #[test]
    fn test_dequote() {
        assert_eq!("", super::dequote(""));
        assert_eq!("'", super::dequote("'"));
        assert_eq!("\"", super::dequote("\""));
        assert_eq!("'\"", super::dequote("'\""));
        assert_eq!("", super::dequote("''"));
        assert_eq!("", super::dequote("\"\""));
        assert_eq!("x", super::dequote("'x'"));
        assert_eq!("x", super::dequote("\"x\""));
        assert_eq!("x", super::dequote("x"));
    }
    #[test]
    fn test_parse_boolean() {
        assert_eq!(None, super::parse_boolean(""));
        assert_eq!(Some(true), super::parse_boolean("1"));
        assert_eq!(Some(true), super::parse_boolean("yes"));
        assert_eq!(Some(true), super::parse_boolean("on"));
        assert_eq!(Some(true), super::parse_boolean("true"));
        assert_eq!(Some(false), super::parse_boolean("0"));
        assert_eq!(Some(false), super::parse_boolean("no"));
        assert_eq!(Some(false), super::parse_boolean("off"));
        assert_eq!(Some(false), super::parse_boolean("false"));
    }
}
