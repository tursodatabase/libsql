extern crate alloc;
use core::ffi::{c_char, c_int};
#[cfg(not(feature = "std"))]
use num_derive::FromPrimitive;

// Structs that still exist in C but will eventually be moved to Rust
// As well as functions re-defined in Rust but not yet deleted from C
use sqlite_nostd as sqlite;

pub static INSERT_SENTINEL: &str = "-1";
pub static DELETE_SENTINEL: &str = "-1";

#[derive(FromPrimitive, PartialEq, Debug)]
pub enum CrsqlChangesColumn {
    Tbl = 0,
    Pk = 1,
    Cid = 2,
    Cval = 3,
    ColVrsn = 4,
    DbVrsn = 5,
    SiteId = 6,
    Cl = 7,
    Seq = 8,
}

#[derive(FromPrimitive, PartialEq, Debug)]
pub enum ClockUnionColumn {
    Tbl = 0,
    Pks = 1,
    Cid = 2,
    ColVrsn = 3,
    DbVrsn = 4,
    SiteId = 5,
    RowId = 6,
    Seq = 7,
    Cl = 8,
}

#[derive(FromPrimitive, PartialEq, Debug)]
pub enum ChangeRowType {
    Update = 0,
    Delete = 1,
    PkOnly = 2,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct crsql_TableInfo {
    pub tblName: *mut ::core::ffi::c_char,
    pub baseCols: *mut crsql_ColumnInfo,
    pub baseColsLen: ::core::ffi::c_int,
    pub pks: *mut crsql_ColumnInfo,
    pub pksLen: ::core::ffi::c_int,
    pub nonPks: *mut crsql_ColumnInfo,
    pub nonPksLen: ::core::ffi::c_int,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct crsql_ColumnInfo {
    pub cid: ::core::ffi::c_int,
    pub name: *mut ::core::ffi::c_char,
    pub type_: *mut ::core::ffi::c_char,
    pub notnull: ::core::ffi::c_int,
    pub pk: ::core::ffi::c_int,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct crsql_ExtData {
    pub pPragmaSchemaVersionStmt: *mut sqlite::stmt,
    pub pPragmaDataVersionStmt: *mut sqlite::stmt,
    pub pragmaDataVersion: ::core::ffi::c_int,
    pub dbVersion: sqlite::int64,
    pub pendingDbVersion: sqlite::int64,
    pub pragmaSchemaVersion: ::core::ffi::c_int,
    pub pragmaSchemaVersionForTableInfos: ::core::ffi::c_int,
    pub siteId: *mut ::core::ffi::c_uchar,
    pub pDbVersionStmt: *mut sqlite::stmt,
    pub zpTableInfos: *mut *mut crsql_TableInfo,
    pub tableInfosLen: ::core::ffi::c_int,
    pub rowsImpacted: ::core::ffi::c_int,
    pub seq: ::core::ffi::c_int,
    pub pSetSyncBitStmt: *mut sqlite::stmt,
    pub pClearSyncBitStmt: *mut sqlite::stmt,
    pub pSetSiteIdOrdinalStmt: *mut sqlite::stmt,
    pub pSelectSiteIdOrdinalStmt: *mut sqlite::stmt,
    pub pStmtCache: *mut ::core::ffi::c_void,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct crsql_Changes_vtab {
    pub base: sqlite::vtab,
    pub db: *mut sqlite::sqlite3,
    pub pExtData: *mut crsql_ExtData,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct crsql_Changes_cursor {
    pub base: sqlite::vtab_cursor,
    pub pTab: *mut crsql_Changes_vtab,
    pub pChangesStmt: *mut sqlite::stmt,
    pub pRowStmt: *mut sqlite::stmt,
    pub dbVersion: sqlite::int64,
    pub rowType: ::core::ffi::c_int,
    pub changesRowid: sqlite::int64,
    pub tblInfoIdx: ::core::ffi::c_int,
}

extern "C" {
    pub fn crsql_indexofTableInfo(
        tblInfos: *mut *mut crsql_TableInfo,
        len: ::core::ffi::c_int,
        tblName: *const ::core::ffi::c_char,
    ) -> ::core::ffi::c_int;
    pub fn crsql_findTableInfo(
        tblInfos: *mut *mut crsql_TableInfo,
        len: c_int,
        tblName: *const c_char,
    ) -> *mut crsql_TableInfo;
    pub fn crsql_ensureTableInfosAreUpToDate(
        db: *mut sqlite::sqlite3,
        pExtData: *mut crsql_ExtData,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    pub fn crsql_getDbVersion(
        db: *mut sqlite::sqlite3,
        ext_data: *mut crsql_ExtData,
        err_msg: *mut *mut c_char,
    ) -> c_int;
    pub fn crsql_createCrr(
        db: *mut sqlite::sqlite3,
        schemaName: *const c_char,
        tblName: *const c_char,
        isCommitAlter: c_int,
        noTx: c_int,
        err: *mut *mut c_char,
    ) -> c_int;
}

#[test]
fn bindgen_test_layout_crsql_Changes_vtab() {
    const UNINIT: ::core::mem::MaybeUninit<crsql_Changes_vtab> = ::core::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::core::mem::size_of::<crsql_Changes_vtab>(),
        40usize,
        concat!("Size of: ", stringify!(crsql_Changes_vtab))
    );
    assert_eq!(
        ::core::mem::align_of::<crsql_Changes_vtab>(),
        8usize,
        concat!("Alignment of ", stringify!(crsql_Changes_vtab))
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).base) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_vtab),
            "::",
            stringify!(base)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).db) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_vtab),
            "::",
            stringify!(db)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pExtData) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_vtab),
            "::",
            stringify!(pExtData)
        )
    );
}

#[test]
fn bindgen_test_layout_crsql_Changes_cursor() {
    const UNINIT: ::core::mem::MaybeUninit<crsql_Changes_cursor> =
        ::core::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::core::mem::size_of::<crsql_Changes_cursor>(),
        64usize,
        concat!("Size of: ", stringify!(crsql_Changes_cursor))
    );
    assert_eq!(
        ::core::mem::align_of::<crsql_Changes_cursor>(),
        8usize,
        concat!("Alignment of ", stringify!(crsql_Changes_cursor))
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).base) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(base)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pTab) as usize - ptr as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(pTab)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pChangesStmt) as usize - ptr as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(pChangesStmt)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pRowStmt) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(pRowStmt)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).dbVersion) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(dbVersion)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).rowType) as usize - ptr as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(rowType)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).changesRowid) as usize - ptr as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(changesRowid)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).tblInfoIdx) as usize - ptr as usize },
        56usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_Changes_cursor),
            "::",
            stringify!(tblInfoIdx)
        )
    );
}

#[test]
#[allow(non_snake_case)]
fn bindgen_test_layout_crsql_ColumnInfo() {
    const UNINIT: ::core::mem::MaybeUninit<crsql_ColumnInfo> = ::core::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::core::mem::size_of::<crsql_ColumnInfo>(),
        32usize,
        concat!("Size of: ", stringify!(crsql_ColumnInfo))
    );
    assert_eq!(
        ::core::mem::align_of::<crsql_ColumnInfo>(),
        8usize,
        concat!("Alignment of ", stringify!(crsql_ColumnInfo))
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).cid) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ColumnInfo),
            "::",
            stringify!(cid)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).name) as usize - ptr as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ColumnInfo),
            "::",
            stringify!(name)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).type_) as usize - ptr as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ColumnInfo),
            "::",
            stringify!(type_)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).notnull) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ColumnInfo),
            "::",
            stringify!(notnull)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pk) as usize - ptr as usize },
        28usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ColumnInfo),
            "::",
            stringify!(pk)
        )
    );
}

#[test]
#[allow(non_snake_case)]
fn bindgen_test_layout_crsql_TableInfo() {
    const UNINIT: ::core::mem::MaybeUninit<crsql_TableInfo> = ::core::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::core::mem::size_of::<crsql_TableInfo>(),
        56usize,
        concat!("Size of: ", stringify!(crsql_TableInfo))
    );
    assert_eq!(
        ::core::mem::align_of::<crsql_TableInfo>(),
        8usize,
        concat!("Alignment of ", stringify!(crsql_TableInfo))
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).tblName) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(tblName)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).baseCols) as usize - ptr as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(baseCols)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).baseColsLen) as usize - ptr as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(baseColsLen)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pks) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(pks)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).pksLen) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(pksLen)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).nonPks) as usize - ptr as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(nonPks)
        )
    );
    assert_eq!(
        unsafe { ::core::ptr::addr_of!((*ptr).nonPksLen) as usize - ptr as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_TableInfo),
            "::",
            stringify!(nonPksLen)
        )
    );
}

#[test]
#[allow(non_snake_case)]
fn bindgen_test_layout_crsql_ExtData() {
    const UNINIT: ::std::mem::MaybeUninit<crsql_ExtData> = ::std::mem::MaybeUninit::uninit();
    let ptr = UNINIT.as_ptr();
    assert_eq!(
        ::std::mem::size_of::<crsql_ExtData>(),
        128usize,
        concat!("Size of: ", stringify!(crsql_ExtData))
    );
    assert_eq!(
        ::std::mem::align_of::<crsql_ExtData>(),
        8usize,
        concat!("Alignment of ", stringify!(crsql_ExtData))
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pPragmaSchemaVersionStmt) as usize - ptr as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pPragmaSchemaVersionStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pPragmaDataVersionStmt) as usize - ptr as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pPragmaDataVersionStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pragmaDataVersion) as usize - ptr as usize },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pragmaDataVersion)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).dbVersion) as usize - ptr as usize },
        24usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(dbVersion)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pendingDbVersion) as usize - ptr as usize },
        32usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pendingDbVersion)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pragmaSchemaVersion) as usize - ptr as usize },
        40usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pragmaSchemaVersion)
        )
    );
    assert_eq!(
        unsafe {
            ::std::ptr::addr_of!((*ptr).pragmaSchemaVersionForTableInfos) as usize - ptr as usize
        },
        44usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pragmaSchemaVersionForTableInfos)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).siteId) as usize - ptr as usize },
        48usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(siteId)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pDbVersionStmt) as usize - ptr as usize },
        56usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pDbVersionStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).zpTableInfos) as usize - ptr as usize },
        64usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(zpTableInfos)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).tableInfosLen) as usize - ptr as usize },
        72usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(tableInfosLen)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).rowsImpacted) as usize - ptr as usize },
        76usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(rowsImpacted)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).seq) as usize - ptr as usize },
        80usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(seq)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pSetSyncBitStmt) as usize - ptr as usize },
        88usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pSetSyncBitStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pClearSyncBitStmt) as usize - ptr as usize },
        96usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pClearSyncBitStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pSetSiteIdOrdinalStmt) as usize - ptr as usize },
        104usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pSetSiteIdOrdinalStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pSelectSiteIdOrdinalStmt) as usize - ptr as usize },
        112usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pSelectSiteIdOrdinalStmt)
        )
    );
    assert_eq!(
        unsafe { ::std::ptr::addr_of!((*ptr).pStmtCache) as usize - ptr as usize },
        120usize,
        concat!(
            "Offset of field: ",
            stringify!(crsql_ExtData),
            "::",
            stringify!(pStmtCache)
        )
    );
}
