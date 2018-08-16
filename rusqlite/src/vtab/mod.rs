//! Create virtual tables.
//!
//! Follow these steps to create your own virtual table:
//! 1. Write implemenation of `VTab` and `VTabCursor` traits.
//! 2. Create an instance of the `Module` structure specialized for `VTab` impl. from step 1.
//! 3. Register your `Module` structure using `Connection.create_module`.
//! 4. Run a `CREATE VIRTUAL TABLE` command that specifies the new module in the `USING` clause.
//!
//! (See [SQLite doc](http://sqlite.org/vtab.html))
use std::borrow::Cow::{self, Borrowed, Owned};
use std::ffi::CString;
use std::marker::PhantomData;
use std::marker::Sync;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

use context::set_result;
use error::error_from_sqlite_code;
use ffi;
pub use ffi::{sqlite3_vtab, sqlite3_vtab_cursor};
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

// db: *mut ffi::sqlite3 => VTabConnection
// module: *const ffi::sqlite3_module => Module
// aux: *mut c_void => Module::Aux
// ffi::sqlite3_vtab => VTab
// ffi::sqlite3_vtab_cursor => VTabCursor

/// Virtual table module
///
/// (See [SQLite doc](https://sqlite.org/c3ref/module.html))
#[repr(C)]
pub struct Module<T: VTab> {
    base: ffi::sqlite3_module,
    phantom: PhantomData<T>,
}

unsafe impl<T: VTab> Sync for Module<T> {}

/// Create a read-only virtual table implementation.
///
/// Step 2 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
pub fn read_only_module<T: CreateVTab>(version: c_int) -> Module<T> {
    // The xConnect and xCreate methods do the same thing, but they must be
    // different so that the virtual table is not an eponymous virtual table.
    let ffi_module = ffi::sqlite3_module {
        iVersion: version,
        xCreate: Some(rust_create::<T>),
        xConnect: Some(rust_connect::<T>),
        xBestIndex: Some(rust_best_index::<T>),
        xDisconnect: Some(rust_disconnect::<T>),
        xDestroy: Some(rust_destroy::<T>),
        xOpen: Some(rust_open::<T>),
        xClose: Some(rust_close::<T::Cursor>),
        xFilter: Some(rust_filter::<T::Cursor>),
        xNext: Some(rust_next::<T::Cursor>),
        xEof: Some(rust_eof::<T::Cursor>),
        xColumn: Some(rust_column::<T::Cursor>),
        xRowid: Some(rust_rowid::<T::Cursor>),
        xUpdate: None,
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
    Module {
        base: ffi_module,
        phantom: PhantomData::<T>,
    }
}

/// Create an eponymous only virtual table implementation.
///
/// Step 2 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
pub fn eponymous_only_module<T: VTab>(version: c_int) -> Module<T> {
    // A virtual table is eponymous if its xCreate method is the exact same function as the xConnect method
    // For eponymous-only virtual tables, the xCreate method is NULL
    let ffi_module = ffi::sqlite3_module {
        iVersion: version,
        xCreate: None,
        xConnect: Some(rust_connect::<T>),
        xBestIndex: Some(rust_best_index::<T>),
        xDisconnect: Some(rust_disconnect::<T>),
        xDestroy: None,
        xOpen: Some(rust_open::<T>),
        xClose: Some(rust_close::<T::Cursor>),
        xFilter: Some(rust_filter::<T::Cursor>),
        xNext: Some(rust_next::<T::Cursor>),
        xEof: Some(rust_eof::<T::Cursor>),
        xColumn: Some(rust_column::<T::Cursor>),
        xRowid: Some(rust_rowid::<T::Cursor>),
        xUpdate: None,
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
    Module {
        base: ffi_module,
        phantom: PhantomData::<T>,
    }
}

pub struct VTabConnection(*mut ffi::sqlite3);

impl VTabConnection {
    // TODO sqlite3_vtab_config (http://sqlite.org/c3ref/vtab_config.html)

    // TODO sqlite3_vtab_on_conflict (http://sqlite.org/c3ref/vtab_on_conflict.html)

    /// Get access to the underlying SQLite database connection handle.
    ///
    /// # Warning
    ///
    /// You should not need to use this function. If you do need to, please [open an issue
    /// on the rusqlite repository](https://github.com/jgallagher/rusqlite/issues) and describe
    /// your use case. This function is unsafe because it gives you raw access to the SQLite
    /// connection, and what you do with it could impact the safety of this `Connection`.
    pub unsafe fn handle(&mut self) -> *mut ffi::sqlite3 {
        self.0
    }
}

/// Virtual table instance trait.
///
/// Implementations must be like:
/// ```rust,ignore
/// #[repr(C)]
/// struct MyTab {
///    /// Base class. Must be first
///    base: ffi::sqlite3_vtab,
///    /* Virtual table implementations will typically add additional fields */
/// }
/// ```
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab.html))
pub trait VTab: Sized {
    type Aux;
    type Cursor: VTabCursor;

    /// Establish a new connection to an existing virtual table.
    ///
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xconnect_method))
    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)>;

    /// Determine the best way to access the virtual table.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xbestindex_method))
    fn best_index(&self, info: &mut IndexInfo) -> Result<()>;

    /// Create a new cursor used for accessing a virtual table.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xopen_method))
    fn open(&self) -> Result<Self::Cursor>;
}

/// Non-eponymous virtual table instance trait.
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab.html))
pub trait CreateVTab: VTab {
    /// Create a new instance of a virtual table in response to a CREATE VIRTUAL TABLE statement.
    /// The `db` parameter is a pointer to the SQLite database connection that is executing
    /// the CREATE VIRTUAL TABLE statement.
    ///
    /// Call `connect` by default.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xcreate_method))
    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }

    /// Destroy the underlying table implementation. This method undoes the work of `create`.
    ///
    /// Do nothing by default.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xdestroy_method))
    fn destroy(&self) -> Result<()> {
        Ok(())
    }
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

/// Pass information into and receive the reply from the `VTab.best_index` method.
///
/// (See [SQLite doc](http://sqlite.org/c3ref/index_info.html))
pub struct IndexInfo(*mut ffi::sqlite3_index_info);

impl IndexInfo {
    /// Record WHERE clause constraints.
    pub fn constraints(&self) -> IndexConstraintIter {
        let constraints =
            unsafe { slice::from_raw_parts((*self.0).aConstraint, (*self.0).nConstraint as usize) };
        IndexConstraintIter {
            iter: constraints.iter(),
        }
    }

    /// Information about the ORDER BY clause.
    pub fn order_bys(&self) -> OrderByIter {
        let order_bys =
            unsafe { slice::from_raw_parts((*self.0).aOrderBy, (*self.0).nOrderBy as usize) };
        OrderByIter {
            iter: order_bys.iter(),
        }
    }

    /// Number of terms in the ORDER BY clause
    pub fn num_of_order_by(&self) -> usize {
        unsafe { (*self.0).nOrderBy as usize }
    }

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

    // TODO idxFlags
    // TODO colUsed

    // TODO sqlite3_vtab_collation (http://sqlite.org/c3ref/vtab_collation.html)
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

/// WHERE clause constraint
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

/// Information about what parameters to pass to `VTabCursor.filter`.
pub struct IndexConstraintUsage<'a>(&'a mut ffi::sqlite3_index_constraint_usage);

impl<'a> IndexConstraintUsage<'a> {
    /// if `argv_index` > 0, constraint is part of argv to `VTabCursor.filter`
    pub fn set_argv_index(&mut self, argv_index: c_int) {
        self.0.argvIndex = argv_index;
    }

    /// if `omit`, do not code a test for this constraint
    pub fn set_omit(&mut self, omit: bool) {
        self.0.omit = if omit { 1 } else { 0 };
    }
}

pub struct OrderByIter<'a> {
    iter: slice::Iter<'a, ffi::sqlite3_index_info_sqlite3_index_orderby>,
}

impl<'a> Iterator for OrderByIter<'a> {
    type Item = OrderBy<'a>;

    fn next(&mut self) -> Option<OrderBy<'a>> {
        self.iter.next().map(|raw| OrderBy(raw))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// A column of the ORDER BY clause.
pub struct OrderBy<'a>(&'a ffi::sqlite3_index_info_sqlite3_index_orderby);

impl<'a> OrderBy<'a> {
    /// Column number
    pub fn column(&self) -> c_int {
        self.0.iColumn
    }

    /// True for DESC.  False for ASC.
    pub fn is_order_by_desc(&self) -> bool {
        self.0.desc != 0
    }
}

/// Virtual table cursor trait.
///
/// Implementations must be like:
/// ```rust,ignore
/// #[repr(C)]
/// struct MyTabCursor {
///    /// Base class. Must be first
///    base: ffi::sqlite3_vtab_cursor,
///    /* Virtual table implementations will typically add additional fields */
/// }
/// ```
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab_cursor.html))
pub trait VTabCursor: Sized {
    /// Begin a search of a virtual table.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xfilter_method))
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values) -> Result<()>;
    /// Advance cursor to the next row of a result set initiated by `filter`.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xnext_method))
    fn next(&mut self) -> Result<()>;
    /// Must return `false` if the cursor currently points to a valid row of data,
    /// or `true` otherwise.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xeof_method))
    fn eof(&self) -> bool;
    /// Find the value for the `i`-th column of the current row.
    /// `i` is zero-based so the first column is numbered 0.
    /// May return its result back to SQLite using one of the specified `ctx`.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xcolumn_method))
    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()>;
    /// Return the rowid of row that the cursor is currently pointing at.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xrowid_method))
    fn rowid(&self) -> Result<i64>;
}

/// Context is used by `VTabCursor.column`` to specify the cell value.
pub struct Context(*mut ffi::sqlite3_context);

impl Context {
    pub fn set_result<T: ToSql>(&mut self, value: &T) -> Result<()> {
        let t = value.to_sql()?;
        unsafe { set_result(self.0, &t) };
        Ok(())
    }

    // TODO sqlite3_vtab_nochange (http://sqlite.org/c3ref/vtab_nochange.html)
}

/// Wrapper to `VTabCursor.filter` arguments, the values requested by `VTab.best_index`.
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
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
        })
    }

    // `sqlite3_value_type` returns `SQLITE_NULL` for pointer.
    // So it seems not possible to enhance `ValueRef::from_value`.
    #[cfg(feature = "array")]
    pub(crate) fn get_array(&self, idx: usize) -> Result<Option<array::Array>> {
        use types::Value;
        let arg = self.args[idx];
        let ptr = unsafe { ffi::sqlite3_value_pointer(arg, array::ARRAY_TYPE) };
        if ptr.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe {
                let rc = array::Array::from_raw(ptr as *const Vec<Value>);
                let array = rc.clone();
                array::Array::into_raw(rc); // don't consume it
                array
            }))
        }
    }

    pub fn iter(&self) -> ValueIter {
        ValueIter {
            iter: self.args.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Values<'a> {
    type IntoIter = ValueIter<'a>;
    type Item = ValueRef<'a>;

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
    ///
    /// Step 3 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
    pub fn create_module<T: VTab>(
        &self,
        module_name: &str,
        module: &Module<T>,
        aux: Option<T::Aux>,
    ) -> Result<()> {
        self.db.borrow_mut().create_module(module_name, module, aux)
    }
}

impl InnerConnection {
    fn create_module<T: VTab>(
        &mut self,
        module_name: &str,
        module: &Module<T>,
        aux: Option<T::Aux>,
    ) -> Result<()> {
        let c_name = try!(str_to_cstring(module_name));
        let r = match aux {
            Some(aux) => {
                let boxed_aux: *mut T::Aux = Box::into_raw(Box::new(aux));
                unsafe {
                    ffi::sqlite3_create_module_v2(
                        self.db(),
                        c_name.as_ptr(),
                        &module.base,
                        boxed_aux as *mut c_void,
                        Some(free_boxed_value::<T::Aux>),
                    )
                }
            }
            None => unsafe {
                ffi::sqlite3_create_module_v2(
                    self.db(),
                    c_name.as_ptr(),
                    &module.base,
                    ptr::null_mut(),
                    None,
                )
            },
        };
        self.decode_result(r)
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
    if s.eq_ignore_ascii_case("yes")
        || s.eq_ignore_ascii_case("on")
        || s.eq_ignore_ascii_case("true")
        || s.eq("1")
    {
        Some(true)
    } else if s.eq_ignore_ascii_case("no")
        || s.eq_ignore_ascii_case("off")
        || s.eq_ignore_ascii_case("false")
        || s.eq("0")
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

unsafe extern "C" fn rust_create<T>(
    db: *mut ffi::sqlite3,
    aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut ffi::sqlite3_vtab,
    err_msg: *mut *mut c_char,
) -> c_int
where
    T: CreateVTab,
{
    use std::error::Error as StdError;
    use std::ffi::CStr;
    use std::slice;

    let mut conn = VTabConnection(db);
    let aux = aux as *mut T::Aux;
    let args = slice::from_raw_parts(argv, argc as usize);
    let vec = args
        .iter()
        .map(|&cs| CStr::from_ptr(cs).to_bytes()) // FIXME .to_str() -> Result<&str, Utf8Error>
        .collect::<Vec<_>>();
    match T::create(&mut conn, aux.as_ref(), &vec[..]) {
        Ok((sql, vtab)) => match ::std::ffi::CString::new(sql) {
            Ok(c_sql) => {
                let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
                if rc == ffi::SQLITE_OK {
                    let boxed_vtab: *mut T = Box::into_raw(Box::new(vtab));
                    *pp_vtab = boxed_vtab as *mut ffi::sqlite3_vtab;
                    ffi::SQLITE_OK
                } else {
                    let err = error_from_sqlite_code(rc, None);
                    *err_msg = mprintf(err.description());
                    rc
                }
            }
            Err(err) => {
                *err_msg = mprintf(err.description());
                ffi::SQLITE_ERROR
            }
        },
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

unsafe extern "C" fn rust_connect<T>(
    db: *mut ffi::sqlite3,
    aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut ffi::sqlite3_vtab,
    err_msg: *mut *mut c_char,
) -> c_int
where
    T: VTab,
{
    use std::error::Error as StdError;
    use std::ffi::CStr;
    use std::slice;

    let mut conn = VTabConnection(db);
    let aux = aux as *mut T::Aux;
    let args = slice::from_raw_parts(argv, argc as usize);
    let vec = args
        .iter()
        .map(|&cs| CStr::from_ptr(cs).to_bytes()) // FIXME .to_str() -> Result<&str, Utf8Error>
        .collect::<Vec<_>>();
    match T::connect(&mut conn, aux.as_ref(), &vec[..]) {
        Ok((sql, vtab)) => match ::std::ffi::CString::new(sql) {
            Ok(c_sql) => {
                let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
                if rc == ffi::SQLITE_OK {
                    let boxed_vtab: *mut T = Box::into_raw(Box::new(vtab));
                    *pp_vtab = boxed_vtab as *mut ffi::sqlite3_vtab;
                    ffi::SQLITE_OK
                } else {
                    let err = error_from_sqlite_code(rc, None);
                    *err_msg = mprintf(err.description());
                    rc
                }
            }
            Err(err) => {
                *err_msg = mprintf(err.description());
                ffi::SQLITE_ERROR
            }
        },
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

unsafe extern "C" fn rust_best_index<T>(
    vtab: *mut ffi::sqlite3_vtab,
    info: *mut ffi::sqlite3_index_info,
) -> c_int
where
    T: VTab,
{
    use std::error::Error as StdError;
    let vt = vtab as *mut T;
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

unsafe extern "C" fn rust_disconnect<T>(vtab: *mut ffi::sqlite3_vtab) -> c_int
where
    T: VTab,
{
    if vtab.is_null() {
        return ffi::SQLITE_OK;
    }
    let vtab = vtab as *mut T;
    let _: Box<T> = Box::from_raw(vtab);
    ffi::SQLITE_OK
}

unsafe extern "C" fn rust_destroy<T>(vtab: *mut ffi::sqlite3_vtab) -> c_int
where
    T: CreateVTab,
{
    use std::error::Error as StdError;
    if vtab.is_null() {
        return ffi::SQLITE_OK;
    }
    let vt = vtab as *mut T;
    match (*vt).destroy() {
        Ok(_) => {
            let _: Box<T> = Box::from_raw(vt);
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

unsafe extern "C" fn rust_open<T>(
    vtab: *mut ffi::sqlite3_vtab,
    pp_cursor: *mut *mut ffi::sqlite3_vtab_cursor,
) -> c_int
where
    T: VTab,
{
    use std::error::Error as StdError;
    let vt = vtab as *mut T;
    match (*vt).open() {
        Ok(cursor) => {
            let boxed_cursor: *mut T::Cursor = Box::into_raw(Box::new(cursor));
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

unsafe extern "C" fn rust_close<C>(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor as *mut C;
    let _: Box<C> = Box::from_raw(cr);
    ffi::SQLITE_OK
}

unsafe extern "C" fn rust_filter<C>(
    cursor: *mut ffi::sqlite3_vtab_cursor,
    idx_num: c_int,
    idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) -> c_int
where
    C: VTabCursor,
{
    use std::ffi::CStr;
    use std::slice;
    use std::str;
    let idx_name = if idx_str.is_null() {
        None
    } else {
        let c_slice = CStr::from_ptr(idx_str).to_bytes();
        Some(str::from_utf8_unchecked(c_slice))
    };
    let args = slice::from_raw_parts_mut(argv, argc as usize);
    let values = Values { args };
    let cr = cursor as *mut C;
    cursor_error(cursor, (*cr).filter(idx_num, idx_name, &values))
}

unsafe extern "C" fn rust_next<C>(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor as *mut C;
    cursor_error(cursor, (*cr).next())
}

unsafe extern "C" fn rust_eof<C>(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor as *mut C;
    (*cr).eof() as c_int
}

unsafe extern "C" fn rust_column<C>(
    cursor: *mut ffi::sqlite3_vtab_cursor,
    ctx: *mut ffi::sqlite3_context,
    i: c_int,
) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor as *mut C;
    let mut ctxt = Context(ctx);
    result_error(ctx, (*cr).column(&mut ctxt, i))
}

unsafe extern "C" fn rust_rowid<C>(
    cursor: *mut ffi::sqlite3_vtab_cursor,
    p_rowid: *mut ffi::sqlite3_int64,
) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor as *mut C;
    match (*cr).rowid() {
        Ok(rowid) => {
            *p_rowid = rowid;
            ffi::SQLITE_OK
        }
        err => cursor_error(cursor, err),
    }
}

/// Virtual table cursors can set an error message by assigning a string to `zErrMsg`.
unsafe fn cursor_error<T>(cursor: *mut ffi::sqlite3_vtab_cursor, result: Result<T>) -> c_int {
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
unsafe fn set_err_msg(vtab: *mut ffi::sqlite3_vtab, err_msg: &str) {
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
fn mprintf(err_msg: &str) -> *mut c_char {
    let c_format = CString::new("%s").unwrap();
    let c_err = CString::new(err_msg).unwrap();
    unsafe { ffi::sqlite3_mprintf(c_format.as_ptr(), c_err.as_ptr()) }
}

#[cfg(feature = "array")]
pub mod array;
#[cfg(feature = "csvtab")]
pub mod csvtab;
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
