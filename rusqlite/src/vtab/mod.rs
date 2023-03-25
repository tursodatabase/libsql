//! Create virtual tables.
//!
//! Follow these steps to create your own virtual table:
//! 1. Write implementation of [`VTab`] and [`VTabCursor`] traits.
//! 2. Create an instance of the [`Module`] structure specialized for [`VTab`]
//! impl. from step 1.
//! 3. Register your [`Module`] structure using [`Connection::create_module`].
//! 4. Run a `CREATE VIRTUAL TABLE` command that specifies the new module in the
//! `USING` clause.
//!
//! (See [SQLite doc](http://sqlite.org/vtab.html))
use std::borrow::Cow::{self, Borrowed, Owned};
use std::marker::PhantomData;
use std::marker::Sync;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

use crate::context::set_result;
use crate::error::error_from_sqlite_code;
use crate::ffi;
pub use crate::ffi::{sqlite3_vtab, sqlite3_vtab_cursor};
use crate::types::{FromSql, FromSqlError, ToSql, ValueRef};
use crate::{str_to_cstring, Connection, Error, InnerConnection, Result};

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

/// Virtual table kind
pub enum VTabKind {
    /// Non-eponymous
    Default,
    /// [`create`](CreateVTab::create) == [`connect`](VTab::connect)
    ///
    /// See [SQLite doc](https://sqlite.org/vtab.html#eponymous_virtual_tables)
    Eponymous,
    /// No [`create`](CreateVTab::create) / [`destroy`](CreateVTab::destroy) or
    /// not used
    ///
    /// SQLite >= 3.9.0
    ///
    /// See [SQLite doc](https://sqlite.org/vtab.html#eponymous_only_virtual_tables)
    EponymousOnly,
}

/// Virtual table module
///
/// (See [SQLite doc](https://sqlite.org/c3ref/module.html))
#[repr(transparent)]
pub struct Module<'vtab, T: VTab<'vtab>> {
    base: ffi::sqlite3_module,
    phantom: PhantomData<&'vtab T>,
}

unsafe impl<'vtab, T: VTab<'vtab>> Send for Module<'vtab, T> {}
unsafe impl<'vtab, T: VTab<'vtab>> Sync for Module<'vtab, T> {}

union ModuleZeroHack {
    bytes: [u8; std::mem::size_of::<ffi::sqlite3_module>()],
    module: ffi::sqlite3_module,
}

// Used as a trailing initializer for sqlite3_module -- this way we avoid having
// the build fail if buildtime_bindgen is on. This is safe, as bindgen-generated
// structs are allowed to be zeroed.
const ZERO_MODULE: ffi::sqlite3_module = unsafe {
    ModuleZeroHack {
        bytes: [0_u8; std::mem::size_of::<ffi::sqlite3_module>()],
    }
    .module
};

macro_rules! module {
    ($lt:lifetime, $vt:ty, $ct:ty, $xc:expr, $xd:expr, $xu:expr) => {
    #[allow(clippy::needless_update)]
    &Module {
        base: ffi::sqlite3_module {
            // We don't use V3
            iVersion: 2,
            xCreate: $xc,
            xConnect: Some(rust_connect::<$vt>),
            xBestIndex: Some(rust_best_index::<$vt>),
            xDisconnect: Some(rust_disconnect::<$vt>),
            xDestroy: $xd,
            xOpen: Some(rust_open::<$vt>),
            xClose: Some(rust_close::<$ct>),
            xFilter: Some(rust_filter::<$ct>),
            xNext: Some(rust_next::<$ct>),
            xEof: Some(rust_eof::<$ct>),
            xColumn: Some(rust_column::<$ct>),
            xRowid: Some(rust_rowid::<$ct>), // FIXME optional
            xUpdate: $xu,
            xBegin: None,
            xSync: None,
            xCommit: None,
            xRollback: None,
            xFindFunction: None,
            xRename: None,
            xSavepoint: None,
            xRelease: None,
            xRollbackTo: None,
            ..ZERO_MODULE
        },
        phantom: PhantomData::<&$lt $vt>,
    }
    };
}

/// Create an modifiable virtual table implementation.
///
/// Step 2 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
#[must_use]
pub fn update_module<'vtab, T: UpdateVTab<'vtab>>() -> &'static Module<'vtab, T> {
    match T::KIND {
        VTabKind::EponymousOnly => {
            module!('vtab, T, T::Cursor, None, None, Some(rust_update::<T>))
        }
        VTabKind::Eponymous => {
            module!('vtab, T, T::Cursor, Some(rust_connect::<T>), Some(rust_disconnect::<T>), Some(rust_update::<T>))
        }
        _ => {
            module!('vtab, T, T::Cursor, Some(rust_create::<T>), Some(rust_destroy::<T>), Some(rust_update::<T>))
        }
    }
}

/// Create a read-only virtual table implementation.
///
/// Step 2 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
#[must_use]
pub fn read_only_module<'vtab, T: CreateVTab<'vtab>>() -> &'static Module<'vtab, T> {
    match T::KIND {
        VTabKind::EponymousOnly => eponymous_only_module(),
        VTabKind::Eponymous => {
            // A virtual table is eponymous if its xCreate method is the exact same function
            // as the xConnect method
            module!('vtab, T, T::Cursor, Some(rust_connect::<T>), Some(rust_disconnect::<T>), None)
        }
        _ => {
            // The xConnect and xCreate methods may do the same thing, but they must be
            // different so that the virtual table is not an eponymous virtual table.
            module!('vtab, T, T::Cursor, Some(rust_create::<T>), Some(rust_destroy::<T>), None)
        }
    }
}

/// Create an eponymous only virtual table implementation.
///
/// Step 2 of [Creating New Virtual Table Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
#[must_use]
pub fn eponymous_only_module<'vtab, T: VTab<'vtab>>() -> &'static Module<'vtab, T> {
    //  For eponymous-only virtual tables, the xCreate method is NULL
    module!('vtab, T, T::Cursor, None, None, None)
}

/// Virtual table configuration options
#[repr(i32)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum VTabConfig {
    /// Equivalent to SQLITE_VTAB_CONSTRAINT_SUPPORT
    ConstraintSupport = 1,
    /// Equivalent to SQLITE_VTAB_INNOCUOUS
    Innocuous = 2,
    /// Equivalent to SQLITE_VTAB_DIRECTONLY
    DirectOnly = 3,
}

/// `feature = "vtab"`
pub struct VTabConnection(*mut ffi::sqlite3);

impl VTabConnection {
    /// Configure various facets of the virtual table interface
    pub fn config(&mut self, config: VTabConfig) -> Result<()> {
        crate::error::check(unsafe { ffi::sqlite3_vtab_config(self.0, config as c_int) })
    }

    // TODO sqlite3_vtab_on_conflict (http://sqlite.org/c3ref/vtab_on_conflict.html) & xUpdate

    /// Get access to the underlying SQLite database connection handle.
    ///
    /// # Warning
    ///
    /// You should not need to use this function. If you do need to, please
    /// [open an issue on the rusqlite repository](https://github.com/rusqlite/rusqlite/issues) and describe
    /// your use case.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it gives you raw access
    /// to the SQLite connection, and what you do with it could impact the
    /// safety of this `Connection`.
    pub unsafe fn handle(&mut self) -> *mut ffi::sqlite3 {
        self.0
    }
}

/// Eponymous-only virtual table instance trait.
///
/// # Safety
///
/// The first item in a struct implementing `VTab` must be
/// `rusqlite::sqlite3_vtab`, and the struct must be `#[repr(C)]`.
///
/// ```rust,ignore
/// #[repr(C)]
/// struct MyTab {
///    /// Base class. Must be first
///    base: rusqlite::vtab::sqlite3_vtab,
///    /* Virtual table implementations will typically add additional fields */
/// }
/// ```
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab.html))
pub unsafe trait VTab<'vtab>: Sized {
    /// Client data passed to [`Connection::create_module`].
    type Aux;
    /// Specific cursor implementation
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
    fn open(&'vtab mut self) -> Result<Self::Cursor>;
}

/// Read-only virtual table instance trait.
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab.html))
pub trait CreateVTab<'vtab>: VTab<'vtab> {
    /// For [`EponymousOnly`](VTabKind::EponymousOnly),
    /// [`create`](CreateVTab::create) and [`destroy`](CreateVTab::destroy) are
    /// not called
    const KIND: VTabKind;
    /// Create a new instance of a virtual table in response to a CREATE VIRTUAL
    /// TABLE statement. The `db` parameter is a pointer to the SQLite
    /// database connection that is executing the CREATE VIRTUAL TABLE
    /// statement.
    ///
    /// Call [`connect`](VTab::connect) by default.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xcreate_method))
    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        Self::connect(db, aux, args)
    }

    /// Destroy the underlying table implementation. This method undoes the work
    /// of [`create`](CreateVTab::create).
    ///
    /// Do nothing by default.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xdestroy_method))
    fn destroy(&self) -> Result<()> {
        Ok(())
    }
}

/// Writable virtual table instance trait.
///
/// (See [SQLite doc](https://sqlite.org/vtab.html#xupdate))
pub trait UpdateVTab<'vtab>: CreateVTab<'vtab> {
    /// Delete rowid or PK
    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()>;
    /// Insert: `args[0] == NULL: old rowid or PK, args[1]: new rowid or PK,
    /// args[2]: ...`
    ///
    /// Return the new rowid.
    // TODO Make the distinction between argv[1] == NULL and argv[1] != NULL ?
    fn insert(&mut self, args: &Values<'_>) -> Result<i64>;
    /// Update: `args[0] != NULL: old rowid or PK, args[1]: new row id or PK,
    /// args[2]: ...`
    fn update(&mut self, args: &Values<'_>) -> Result<()>;
}

/// Index constraint operator.
/// See [Virtual Table Constraint Operator Codes](https://sqlite.org/c3ref/c_index_constraint_eq.html) for details.
#[derive(Debug, Eq, PartialEq)]
#[allow(non_snake_case, non_camel_case_types, missing_docs)]
#[allow(clippy::upper_case_acronyms)]
pub enum IndexConstraintOp {
    SQLITE_INDEX_CONSTRAINT_EQ,
    SQLITE_INDEX_CONSTRAINT_GT,
    SQLITE_INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_CONSTRAINT_GE,
    SQLITE_INDEX_CONSTRAINT_MATCH,
    SQLITE_INDEX_CONSTRAINT_LIKE,         // 3.10.0
    SQLITE_INDEX_CONSTRAINT_GLOB,         // 3.10.0
    SQLITE_INDEX_CONSTRAINT_REGEXP,       // 3.10.0
    SQLITE_INDEX_CONSTRAINT_NE,           // 3.21.0
    SQLITE_INDEX_CONSTRAINT_ISNOT,        // 3.21.0
    SQLITE_INDEX_CONSTRAINT_ISNOTNULL,    // 3.21.0
    SQLITE_INDEX_CONSTRAINT_ISNULL,       // 3.21.0
    SQLITE_INDEX_CONSTRAINT_IS,           // 3.21.0
    SQLITE_INDEX_CONSTRAINT_LIMIT,        // 3.38.0
    SQLITE_INDEX_CONSTRAINT_OFFSET,       // 3.38.0
    SQLITE_INDEX_CONSTRAINT_FUNCTION(u8), // 3.25.0
}

impl From<u8> for IndexConstraintOp {
    fn from(code: u8) -> IndexConstraintOp {
        match code {
            2 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ,
            4 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GT,
            8 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LE,
            16 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LT,
            32 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GE,
            64 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_MATCH,
            65 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LIKE,
            66 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GLOB,
            67 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_REGEXP,
            68 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_NE,
            69 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_ISNOT,
            70 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_ISNOTNULL,
            71 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_ISNULL,
            72 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_IS,
            73 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LIMIT,
            74 => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_OFFSET,
            v => IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_FUNCTION(v),
        }
    }
}

bitflags::bitflags! {
    /// Virtual table scan flags
    /// See [Function Flags](https://sqlite.org/c3ref/c_index_scan_unique.html) for details.
    #[repr(C)]
    pub struct IndexFlags: ::std::os::raw::c_int {
        /// Default
        const NONE     = 0;
        /// Scan visits at most 1 row.
        const SQLITE_INDEX_SCAN_UNIQUE  = ffi::SQLITE_INDEX_SCAN_UNIQUE;
    }
}

/// Pass information into and receive the reply from the
/// [`VTab::best_index`] method.
///
/// (See [SQLite doc](http://sqlite.org/c3ref/index_info.html))
#[derive(Debug)]
pub struct IndexInfo(*mut ffi::sqlite3_index_info);

impl IndexInfo {
    /// Iterate on index constraint and its associated usage.
    #[inline]
    pub fn constraints_and_usages(&mut self) -> IndexConstraintAndUsageIter<'_> {
        let constraints =
            unsafe { slice::from_raw_parts((*self.0).aConstraint, (*self.0).nConstraint as usize) };
        let constraint_usages = unsafe {
            slice::from_raw_parts_mut((*self.0).aConstraintUsage, (*self.0).nConstraint as usize)
        };
        IndexConstraintAndUsageIter {
            iter: constraints.iter().zip(constraint_usages.iter_mut()),
        }
    }

    /// Record WHERE clause constraints.
    #[inline]
    #[must_use]
    pub fn constraints(&self) -> IndexConstraintIter<'_> {
        let constraints =
            unsafe { slice::from_raw_parts((*self.0).aConstraint, (*self.0).nConstraint as usize) };
        IndexConstraintIter {
            iter: constraints.iter(),
        }
    }

    /// Information about the ORDER BY clause.
    #[inline]
    #[must_use]
    pub fn order_bys(&self) -> OrderByIter<'_> {
        let order_bys =
            unsafe { slice::from_raw_parts((*self.0).aOrderBy, (*self.0).nOrderBy as usize) };
        OrderByIter {
            iter: order_bys.iter(),
        }
    }

    /// Number of terms in the ORDER BY clause
    #[inline]
    #[must_use]
    pub fn num_of_order_by(&self) -> usize {
        unsafe { (*self.0).nOrderBy as usize }
    }

    /// Information about what parameters to pass to [`VTabCursor::filter`].
    #[inline]
    pub fn constraint_usage(&mut self, constraint_idx: usize) -> IndexConstraintUsage<'_> {
        let constraint_usages = unsafe {
            slice::from_raw_parts_mut((*self.0).aConstraintUsage, (*self.0).nConstraint as usize)
        };
        IndexConstraintUsage(&mut constraint_usages[constraint_idx])
    }

    /// Number used to identify the index
    #[inline]
    pub fn set_idx_num(&mut self, idx_num: c_int) {
        unsafe {
            (*self.0).idxNum = idx_num;
        }
    }

    /// String used to identify the index
    pub fn set_idx_str(&mut self, idx_str: &str) {
        unsafe {
            (*self.0).idxStr = alloc(idx_str);
            (*self.0).needToFreeIdxStr = 1;
        }
    }

    /// True if output is already ordered
    #[inline]
    pub fn set_order_by_consumed(&mut self, order_by_consumed: bool) {
        unsafe {
            (*self.0).orderByConsumed = order_by_consumed as c_int;
        }
    }

    /// Estimated cost of using this index
    #[inline]
    pub fn set_estimated_cost(&mut self, estimated_ost: f64) {
        unsafe {
            (*self.0).estimatedCost = estimated_ost;
        }
    }

    /// Estimated number of rows returned.
    #[inline]
    pub fn set_estimated_rows(&mut self, estimated_rows: i64) {
        unsafe {
            (*self.0).estimatedRows = estimated_rows;
        }
    }

    /// Mask of SQLITE_INDEX_SCAN_* flags.
    #[inline]
    pub fn set_idx_flags(&mut self, flags: IndexFlags) {
        unsafe { (*self.0).idxFlags = flags.bits() };
    }

    /// Mask of columns used by statement
    #[inline]
    pub fn col_used(&self) -> u64 {
        unsafe { (*self.0).colUsed }
    }

    /// Determine the collation for a virtual table constraint
    #[cfg(feature = "modern_sqlite")] // SQLite >= 3.22.0
    #[cfg_attr(docsrs, doc(cfg(feature = "modern_sqlite")))]
    pub fn collation(&self, constraint_idx: usize) -> Result<&str> {
        use std::ffi::CStr;
        let idx = constraint_idx as c_int;
        let collation = unsafe { ffi::sqlite3_vtab_collation(self.0, idx) };
        if collation.is_null() {
            return Err(Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_MISUSE),
                Some(format!("{constraint_idx} is out of range")),
            ));
        }
        Ok(unsafe { CStr::from_ptr(collation) }.to_str()?)
    }

    /*/// Determine if a virtual table query is DISTINCT
    #[cfg(feature = "modern_sqlite")] // SQLite >= 3.38.0
    #[cfg_attr(docsrs, doc(cfg(feature = "modern_sqlite")))]
    pub fn distinct(&self) -> c_int {
        unsafe { ffi::sqlite3_vtab_distinct(self.0) }
    }

    /// Constraint values
    #[cfg(feature = "modern_sqlite")] // SQLite >= 3.38.0
    #[cfg_attr(docsrs, doc(cfg(feature = "modern_sqlite")))]
    pub fn set_rhs_value(&mut self, constraint_idx: c_int, value: ValueRef) -> Result<()> {
        // TODO ValueRef to sqlite3_value
        crate::error::check(unsafe { ffi::sqlite3_vtab_rhs_value(self.O, constraint_idx, value) })
    }

    /// Identify and handle IN constraints
    #[cfg(feature = "modern_sqlite")] // SQLite >= 3.38.0
    #[cfg_attr(docsrs, doc(cfg(feature = "modern_sqlite")))]
    pub fn set_in_constraint(&mut self, constraint_idx: c_int, b_handle: c_int) -> bool {
        unsafe { ffi::sqlite3_vtab_in(self.0, constraint_idx, b_handle) != 0 }
    } // TODO sqlite3_vtab_in_first / sqlite3_vtab_in_next https://sqlite.org/c3ref/vtab_in_first.html
    */
}

/// Iterate on index constraint and its associated usage.
pub struct IndexConstraintAndUsageIter<'a> {
    iter: std::iter::Zip<
        slice::Iter<'a, ffi::sqlite3_index_constraint>,
        slice::IterMut<'a, ffi::sqlite3_index_constraint_usage>,
    >,
}

impl<'a> Iterator for IndexConstraintAndUsageIter<'a> {
    type Item = (IndexConstraint<'a>, IndexConstraintUsage<'a>);

    #[inline]
    fn next(&mut self) -> Option<(IndexConstraint<'a>, IndexConstraintUsage<'a>)> {
        self.iter
            .next()
            .map(|raw| (IndexConstraint(raw.0), IndexConstraintUsage(raw.1)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// `feature = "vtab"`
pub struct IndexConstraintIter<'a> {
    iter: slice::Iter<'a, ffi::sqlite3_index_constraint>,
}

impl<'a> Iterator for IndexConstraintIter<'a> {
    type Item = IndexConstraint<'a>;

    #[inline]
    fn next(&mut self) -> Option<IndexConstraint<'a>> {
        self.iter.next().map(IndexConstraint)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// WHERE clause constraint.
pub struct IndexConstraint<'a>(&'a ffi::sqlite3_index_constraint);

impl IndexConstraint<'_> {
    /// Column constrained.  -1 for ROWID
    #[inline]
    #[must_use]
    pub fn column(&self) -> c_int {
        self.0.iColumn
    }

    /// Constraint operator
    #[inline]
    #[must_use]
    pub fn operator(&self) -> IndexConstraintOp {
        IndexConstraintOp::from(self.0.op)
    }

    /// True if this constraint is usable
    #[inline]
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.0.usable != 0
    }
}

/// Information about what parameters to pass to
/// [`VTabCursor::filter`].
pub struct IndexConstraintUsage<'a>(&'a mut ffi::sqlite3_index_constraint_usage);

impl IndexConstraintUsage<'_> {
    /// if `argv_index` > 0, constraint is part of argv to
    /// [`VTabCursor::filter`]
    #[inline]
    pub fn set_argv_index(&mut self, argv_index: c_int) {
        self.0.argvIndex = argv_index;
    }

    /// if `omit`, do not code a test for this constraint
    #[inline]
    pub fn set_omit(&mut self, omit: bool) {
        self.0.omit = omit as std::os::raw::c_uchar;
    }
}

/// `feature = "vtab"`
pub struct OrderByIter<'a> {
    iter: slice::Iter<'a, ffi::sqlite3_index_orderby>,
}

impl<'a> Iterator for OrderByIter<'a> {
    type Item = OrderBy<'a>;

    #[inline]
    fn next(&mut self) -> Option<OrderBy<'a>> {
        self.iter.next().map(OrderBy)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// A column of the ORDER BY clause.
pub struct OrderBy<'a>(&'a ffi::sqlite3_index_orderby);

impl OrderBy<'_> {
    /// Column number
    #[inline]
    #[must_use]
    pub fn column(&self) -> c_int {
        self.0.iColumn
    }

    /// True for DESC.  False for ASC.
    #[inline]
    #[must_use]
    pub fn is_order_by_desc(&self) -> bool {
        self.0.desc != 0
    }
}

/// Virtual table cursor trait.
///
/// # Safety
///
/// Implementations must be like:
/// ```rust,ignore
/// #[repr(C)]
/// struct MyTabCursor {
///    /// Base class. Must be first
///    base: rusqlite::vtab::sqlite3_vtab_cursor,
///    /* Virtual table implementations will typically add additional fields */
/// }
/// ```
///
/// (See [SQLite doc](https://sqlite.org/c3ref/vtab_cursor.html))
pub unsafe trait VTabCursor: Sized {
    /// Begin a search of a virtual table.
    /// (See [SQLite doc](https://sqlite.org/vtab.html#the_xfilter_method))
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values<'_>) -> Result<()>;
    /// Advance cursor to the next row of a result set initiated by
    /// [`filter`](VTabCursor::filter). (See [SQLite doc](https://sqlite.org/vtab.html#the_xnext_method))
    fn next(&mut self) -> Result<()>;
    /// Must return `false` if the cursor currently points to a valid row of
    /// data, or `true` otherwise.
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

/// Context is used by [`VTabCursor::column`] to specify the
/// cell value.
pub struct Context(*mut ffi::sqlite3_context);

impl Context {
    /// Set current cell value
    #[inline]
    pub fn set_result<T: ToSql>(&mut self, value: &T) -> Result<()> {
        let t = value.to_sql()?;
        unsafe { set_result(self.0, &t) };
        Ok(())
    }

    // TODO sqlite3_vtab_nochange (http://sqlite.org/c3ref/vtab_nochange.html) // 3.22.0 & xColumn
}

/// Wrapper to [`VTabCursor::filter`] arguments, the values
/// requested by [`VTab::best_index`].
pub struct Values<'a> {
    args: &'a [*mut ffi::sqlite3_value],
}

impl Values<'_> {
    /// Returns the number of values.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.args.len()
    }

    /// Returns `true` if there is no value.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    /// Returns value at `idx`
    pub fn get<T: FromSql>(&self, idx: usize) -> Result<T> {
        let arg = self.args[idx];
        let value = unsafe { ValueRef::from_value(arg) };
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => Error::InvalidFilterParameterType(idx, value.data_type()),
            FromSqlError::Other(err) => {
                Error::FromSqlConversionFailure(idx, value.data_type(), err)
            }
            FromSqlError::InvalidBlobSize { .. } => {
                Error::FromSqlConversionFailure(idx, value.data_type(), Box::new(err))
            }
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
        })
    }

    // `sqlite3_value_type` returns `SQLITE_NULL` for pointer.
    // So it seems not possible to enhance `ValueRef::from_value`.
    #[cfg(feature = "array")]
    #[cfg_attr(docsrs, doc(cfg(feature = "array")))]
    fn get_array(&self, idx: usize) -> Option<array::Array> {
        use crate::types::Value;
        let arg = self.args[idx];
        let ptr = unsafe { ffi::sqlite3_value_pointer(arg, array::ARRAY_TYPE) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe {
                let rc = array::Array::from_raw(ptr as *const Vec<Value>);
                let array = rc.clone();
                array::Array::into_raw(rc); // don't consume it
                array
            })
        }
    }

    /// Turns `Values` into an iterator.
    #[inline]
    #[must_use]
    pub fn iter(&self) -> ValueIter<'_> {
        ValueIter {
            iter: self.args.iter(),
        }
    }
    // TODO sqlite3_vtab_in_first / sqlite3_vtab_in_next https://sqlite.org/c3ref/vtab_in_first.html & 3.38.0
}

impl<'a> IntoIterator for &'a Values<'a> {
    type IntoIter = ValueIter<'a>;
    type Item = ValueRef<'a>;

    #[inline]
    fn into_iter(self) -> ValueIter<'a> {
        self.iter()
    }
}

/// [`Values`] iterator.
pub struct ValueIter<'a> {
    iter: slice::Iter<'a, *mut ffi::sqlite3_value>,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = ValueRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<ValueRef<'a>> {
        self.iter
            .next()
            .map(|&raw| unsafe { ValueRef::from_value(raw) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl Connection {
    /// Register a virtual table implementation.
    ///
    /// Step 3 of [Creating New Virtual Table
    /// Implementations](https://sqlite.org/vtab.html#creating_new_virtual_table_implementations).
    #[inline]
    pub fn create_module<'vtab, T: VTab<'vtab>>(
        &self,
        module_name: &str,
        module: &'static Module<'vtab, T>,
        aux: Option<T::Aux>,
    ) -> Result<()> {
        self.db.borrow_mut().create_module(module_name, module, aux)
    }
}

impl InnerConnection {
    fn create_module<'vtab, T: VTab<'vtab>>(
        &mut self,
        module_name: &str,
        module: &'static Module<'vtab, T>,
        aux: Option<T::Aux>,
    ) -> Result<()> {
        use crate::version;
        if version::version_number() < 3_009_000 && module.base.xCreate.is_none() {
            return Err(Error::ModuleError(format!(
                "Eponymous-only virtual table not supported by SQLite version {}",
                version::version()
            )));
        }
        let c_name = str_to_cstring(module_name)?;
        let r = match aux {
            Some(aux) => {
                let boxed_aux: *mut T::Aux = Box::into_raw(Box::new(aux));
                unsafe {
                    ffi::sqlite3_create_module_v2(
                        self.db(),
                        c_name.as_ptr(),
                        &module.base,
                        boxed_aux.cast::<c_void>(),
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

/// Escape double-quote (`"`) character occurrences by
/// doubling them (`""`).
#[must_use]
pub fn escape_double_quote(identifier: &str) -> Cow<'_, str> {
    if identifier.contains('"') {
        // escape quote by doubling them
        Owned(identifier.replace('"', "\"\""))
    } else {
        Borrowed(identifier)
    }
}
/// Dequote string
#[must_use]
pub fn dequote(s: &str) -> &str {
    if s.len() < 2 {
        return s;
    }
    match s.bytes().next() {
        Some(b) if b == b'"' || b == b'\'' => match s.bytes().rev().next() {
            Some(e) if e == b => &s[1..s.len() - 1], // FIXME handle inner escaped quote(s)
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
#[must_use]
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

/// `<param_name>=['"]?<param_value>['"]?` => `(<param_name>, <param_value>)`
pub fn parameter(c_slice: &[u8]) -> Result<(&str, &str)> {
    let arg = std::str::from_utf8(c_slice)?.trim();
    let mut split = arg.split('=');
    if let Some(key) = split.next() {
        if let Some(value) = split.next() {
            let param = key.trim();
            let value = dequote(value);
            return Ok((param, value));
        }
    }
    Err(Error::ModuleError(format!("illegal argument: '{arg}'")))
}

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    drop(Box::from_raw(p.cast::<T>()));
}

unsafe extern "C" fn rust_create<'vtab, T>(
    db: *mut ffi::sqlite3,
    aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut ffi::sqlite3_vtab,
    err_msg: *mut *mut c_char,
) -> c_int
where
    T: CreateVTab<'vtab>,
{
    use std::ffi::CStr;

    let mut conn = VTabConnection(db);
    let aux = aux.cast::<T::Aux>();
    let args = slice::from_raw_parts(argv, argc as usize);
    let vec = args
        .iter()
        .map(|&cs| CStr::from_ptr(cs).to_bytes()) // FIXME .to_str() -> Result<&str, Utf8Error>
        .collect::<Vec<_>>();
    match T::create(&mut conn, aux.as_ref(), &vec[..]) {
        Ok((sql, vtab)) => match std::ffi::CString::new(sql) {
            Ok(c_sql) => {
                let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
                if rc == ffi::SQLITE_OK {
                    let boxed_vtab: *mut T = Box::into_raw(Box::new(vtab));
                    *pp_vtab = boxed_vtab.cast::<ffi::sqlite3_vtab>();
                    ffi::SQLITE_OK
                } else {
                    let err = error_from_sqlite_code(rc, None);
                    *err_msg = alloc(&err.to_string());
                    rc
                }
            }
            Err(err) => {
                *err_msg = alloc(&err.to_string());
                ffi::SQLITE_ERROR
            }
        },
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(s) = s {
                *err_msg = alloc(&s);
            }
            err.extended_code
        }
        Err(err) => {
            *err_msg = alloc(&err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

unsafe extern "C" fn rust_connect<'vtab, T>(
    db: *mut ffi::sqlite3,
    aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut ffi::sqlite3_vtab,
    err_msg: *mut *mut c_char,
) -> c_int
where
    T: VTab<'vtab>,
{
    use std::ffi::CStr;

    let mut conn = VTabConnection(db);
    let aux = aux.cast::<T::Aux>();
    let args = slice::from_raw_parts(argv, argc as usize);
    let vec = args
        .iter()
        .map(|&cs| CStr::from_ptr(cs).to_bytes()) // FIXME .to_str() -> Result<&str, Utf8Error>
        .collect::<Vec<_>>();
    match T::connect(&mut conn, aux.as_ref(), &vec[..]) {
        Ok((sql, vtab)) => match std::ffi::CString::new(sql) {
            Ok(c_sql) => {
                let rc = ffi::sqlite3_declare_vtab(db, c_sql.as_ptr());
                if rc == ffi::SQLITE_OK {
                    let boxed_vtab: *mut T = Box::into_raw(Box::new(vtab));
                    *pp_vtab = boxed_vtab.cast::<ffi::sqlite3_vtab>();
                    ffi::SQLITE_OK
                } else {
                    let err = error_from_sqlite_code(rc, None);
                    *err_msg = alloc(&err.to_string());
                    rc
                }
            }
            Err(err) => {
                *err_msg = alloc(&err.to_string());
                ffi::SQLITE_ERROR
            }
        },
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(s) = s {
                *err_msg = alloc(&s);
            }
            err.extended_code
        }
        Err(err) => {
            *err_msg = alloc(&err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

unsafe extern "C" fn rust_best_index<'vtab, T>(
    vtab: *mut ffi::sqlite3_vtab,
    info: *mut ffi::sqlite3_index_info,
) -> c_int
where
    T: VTab<'vtab>,
{
    let vt = vtab.cast::<T>();
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
            set_err_msg(vtab, &err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

unsafe extern "C" fn rust_disconnect<'vtab, T>(vtab: *mut ffi::sqlite3_vtab) -> c_int
where
    T: VTab<'vtab>,
{
    if vtab.is_null() {
        return ffi::SQLITE_OK;
    }
    let vtab = vtab.cast::<T>();
    drop(Box::from_raw(vtab));
    ffi::SQLITE_OK
}

unsafe extern "C" fn rust_destroy<'vtab, T>(vtab: *mut ffi::sqlite3_vtab) -> c_int
where
    T: CreateVTab<'vtab>,
{
    if vtab.is_null() {
        return ffi::SQLITE_OK;
    }
    let vt = vtab.cast::<T>();
    match (*vt).destroy() {
        Ok(_) => {
            drop(Box::from_raw(vt));
            ffi::SQLITE_OK
        }
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(err_msg) = s {
                set_err_msg(vtab, &err_msg);
            }
            err.extended_code
        }
        Err(err) => {
            set_err_msg(vtab, &err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

unsafe extern "C" fn rust_open<'vtab, T: 'vtab>(
    vtab: *mut ffi::sqlite3_vtab,
    pp_cursor: *mut *mut ffi::sqlite3_vtab_cursor,
) -> c_int
where
    T: VTab<'vtab>,
{
    let vt = vtab.cast::<T>();
    match (*vt).open() {
        Ok(cursor) => {
            let boxed_cursor: *mut T::Cursor = Box::into_raw(Box::new(cursor));
            *pp_cursor = boxed_cursor.cast::<ffi::sqlite3_vtab_cursor>();
            ffi::SQLITE_OK
        }
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(err_msg) = s {
                set_err_msg(vtab, &err_msg);
            }
            err.extended_code
        }
        Err(err) => {
            set_err_msg(vtab, &err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

unsafe extern "C" fn rust_close<C>(cursor: *mut ffi::sqlite3_vtab_cursor) -> c_int
where
    C: VTabCursor,
{
    let cr = cursor.cast::<C>();
    drop(Box::from_raw(cr));
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
    let cr = cursor.cast::<C>();
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
    let cr = cursor.cast::<C>();
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
    let cr = cursor.cast::<C>();
    match (*cr).rowid() {
        Ok(rowid) => {
            *p_rowid = rowid;
            ffi::SQLITE_OK
        }
        err => cursor_error(cursor, err),
    }
}

unsafe extern "C" fn rust_update<'vtab, T: 'vtab>(
    vtab: *mut ffi::sqlite3_vtab,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
    p_rowid: *mut ffi::sqlite3_int64,
) -> c_int
where
    T: UpdateVTab<'vtab>,
{
    assert!(argc >= 1);
    let args = slice::from_raw_parts_mut(argv, argc as usize);
    let vt = vtab.cast::<T>();
    let r = if args.len() == 1 {
        (*vt).delete(ValueRef::from_value(args[0]))
    } else if ffi::sqlite3_value_type(args[0]) == ffi::SQLITE_NULL {
        // TODO Make the distinction between argv[1] == NULL and argv[1] != NULL ?
        let values = Values { args };
        match (*vt).insert(&values) {
            Ok(rowid) => {
                *p_rowid = rowid;
                Ok(())
            }
            Err(e) => Err(e),
        }
    } else {
        let values = Values { args };
        (*vt).update(&values)
    };
    match r {
        Ok(_) => ffi::SQLITE_OK,
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(err_msg) = s {
                set_err_msg(vtab, &err_msg);
            }
            err.extended_code
        }
        Err(err) => {
            set_err_msg(vtab, &err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

/// Virtual table cursors can set an error message by assigning a string to
/// `zErrMsg`.
#[cold]
unsafe fn cursor_error<T>(cursor: *mut ffi::sqlite3_vtab_cursor, result: Result<T>) -> c_int {
    match result {
        Ok(_) => ffi::SQLITE_OK,
        Err(Error::SqliteFailure(err, s)) => {
            if let Some(err_msg) = s {
                set_err_msg((*cursor).pVtab, &err_msg);
            }
            err.extended_code
        }
        Err(err) => {
            set_err_msg((*cursor).pVtab, &err.to_string());
            ffi::SQLITE_ERROR
        }
    }
}

/// Virtual tables methods can set an error message by assigning a string to
/// `zErrMsg`.
#[cold]
unsafe fn set_err_msg(vtab: *mut ffi::sqlite3_vtab, err_msg: &str) {
    if !(*vtab).zErrMsg.is_null() {
        ffi::sqlite3_free((*vtab).zErrMsg.cast::<c_void>());
    }
    (*vtab).zErrMsg = alloc(err_msg);
}

/// To raise an error, the `column` method should use this method to set the
/// error message and return the error code.
#[cold]
unsafe fn result_error<T>(ctx: *mut ffi::sqlite3_context, result: Result<T>) -> c_int {
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
            if let Ok(cstr) = str_to_cstring(&err.to_string()) {
                ffi::sqlite3_result_error(ctx, cstr.as_ptr(), -1);
            }
            ffi::SQLITE_ERROR
        }
    }
}

// Space to hold this string must be obtained
// from an SQLite memory allocation function
fn alloc(s: &str) -> *mut c_char {
    crate::util::SqliteMallocString::from_str(s).into_raw()
}

#[cfg(feature = "array")]
#[cfg_attr(docsrs, doc(cfg(feature = "array")))]
pub mod array;
#[cfg(feature = "csvtab")]
#[cfg_attr(docsrs, doc(cfg(feature = "csvtab")))]
pub mod csvtab;
#[cfg(feature = "series")]
#[cfg_attr(docsrs, doc(cfg(feature = "series")))]
pub mod series; // SQLite >= 3.9.0
#[cfg(all(test, feature = "modern_sqlite"))]
mod vtablog;

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
