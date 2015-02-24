#![allow(raw_pointer_derive, non_snake_case, non_camel_case_types)]
/* Running `target/bindgen /Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk/usr/include/sqlite3.h -I/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/6.0/include` */

#[derive(Copy)]
pub enum Struct_sqlite3 { }
pub type sqlite3 = Struct_sqlite3;
pub type sqlite_int64 = ::libc::c_longlong;
pub type sqlite_uint64 = ::libc::c_ulonglong;
pub type sqlite3_int64 = sqlite_int64;
pub type sqlite3_uint64 = sqlite_uint64;
pub type sqlite3_callback =
    ::std::option::Option<extern "C" fn
                              (arg1: *mut ::libc::c_void, arg2: ::libc::c_int,
                               arg3: *mut *mut ::libc::c_char,
                               arg4: *mut *mut ::libc::c_char)
                              -> ::libc::c_int>;
pub type sqlite3_file = Struct_sqlite3_file;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_file {
    pub pMethods: *const Struct_sqlite3_io_methods,
}
pub type sqlite3_io_methods = Struct_sqlite3_io_methods;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_io_methods {
    pub iVersion: ::libc::c_int,
    pub xClose: ::std::option::Option<extern "C" fn(arg1: *mut sqlite3_file)
                                          -> ::libc::c_int>,
    pub xRead: ::std::option::Option<extern "C" fn
                                         (arg1: *mut sqlite3_file,
                                          arg2: *mut ::libc::c_void,
                                          iAmt: ::libc::c_int,
                                          iOfst: sqlite3_int64)
                                         -> ::libc::c_int>,
    pub xWrite: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_file,
                                           arg2: *const ::libc::c_void,
                                           iAmt: ::libc::c_int,
                                           iOfst: sqlite3_int64)
                                          -> ::libc::c_int>,
    pub xTruncate: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_file,
                                              size: sqlite3_int64)
                                             -> ::libc::c_int>,
    pub xSync: ::std::option::Option<extern "C" fn
                                         (arg1: *mut sqlite3_file,
                                          flags: ::libc::c_int)
                                         -> ::libc::c_int>,
    pub xFileSize: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_file,
                                              pSize: *mut sqlite3_int64)
                                             -> ::libc::c_int>,
    pub xLock: ::std::option::Option<extern "C" fn
                                         (arg1: *mut sqlite3_file,
                                          arg2: ::libc::c_int)
                                         -> ::libc::c_int>,
    pub xUnlock: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_file,
                                            arg2: ::libc::c_int)
                                           -> ::libc::c_int>,
    pub xCheckReservedLock: ::std::option::Option<extern "C" fn
                                                      (arg1:
                                                           *mut sqlite3_file,
                                                       pResOut:
                                                           *mut ::libc::c_int)
                                                      -> ::libc::c_int>,
    pub xFileControl: ::std::option::Option<extern "C" fn
                                                (arg1: *mut sqlite3_file,
                                                 op: ::libc::c_int,
                                                 pArg: *mut ::libc::c_void)
                                                -> ::libc::c_int>,
    pub xSectorSize: ::std::option::Option<extern "C" fn
                                               (arg1: *mut sqlite3_file)
                                               -> ::libc::c_int>,
    pub xDeviceCharacteristics: ::std::option::Option<extern "C" fn
                                                          (arg1:
                                                               *mut sqlite3_file)
                                                          -> ::libc::c_int>,
    pub xShmMap: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_file,
                                            iPg: ::libc::c_int,
                                            pgsz: ::libc::c_int,
                                            arg2: ::libc::c_int,
                                            arg3: *mut *mut ::libc::c_void)
                                           -> ::libc::c_int>,
    pub xShmLock: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3_file,
                                             offset: ::libc::c_int,
                                             n: ::libc::c_int,
                                             flags: ::libc::c_int)
                                            -> ::libc::c_int>,
    pub xShmBarrier: ::std::option::Option<extern "C" fn
                                               (arg1: *mut sqlite3_file)>,
    pub xShmUnmap: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_file,
                                              deleteFlag: ::libc::c_int)
                                             -> ::libc::c_int>,
}
#[derive(Copy)]
pub enum Struct_sqlite3_mutex { }
pub type sqlite3_mutex = Struct_sqlite3_mutex;
pub type sqlite3_vfs = Struct_sqlite3_vfs;
pub type sqlite3_syscall_ptr = ::std::option::Option<extern "C" fn()>;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_vfs {
    pub iVersion: ::libc::c_int,
    pub szOsFile: ::libc::c_int,
    pub mxPathname: ::libc::c_int,
    pub pNext: *mut sqlite3_vfs,
    pub zName: *const ::libc::c_char,
    pub pAppData: *mut ::libc::c_void,
    pub xOpen: ::std::option::Option<extern "C" fn
                                         (arg1: *mut sqlite3_vfs,
                                          zName: *const ::libc::c_char,
                                          arg2: *mut sqlite3_file,
                                          flags: ::libc::c_int,
                                          pOutFlags: *mut ::libc::c_int)
                                         -> ::libc::c_int>,
    pub xDelete: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vfs,
                                            zName: *const ::libc::c_char,
                                            syncDir: ::libc::c_int)
                                           -> ::libc::c_int>,
    pub xAccess: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vfs,
                                            zName: *const ::libc::c_char,
                                            flags: ::libc::c_int,
                                            pResOut: *mut ::libc::c_int)
                                           -> ::libc::c_int>,
    pub xFullPathname: ::std::option::Option<extern "C" fn
                                                 (arg1: *mut sqlite3_vfs,
                                                  zName:
                                                      *const ::libc::c_char,
                                                  nOut: ::libc::c_int,
                                                  zOut: *mut ::libc::c_char)
                                                 -> ::libc::c_int>,
    pub xDlOpen: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vfs,
                                            zFilename: *const ::libc::c_char)
                                           -> *mut ::libc::c_void>,
    pub xDlError: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3_vfs,
                                             nByte: ::libc::c_int,
                                             zErrMsg: *mut ::libc::c_char)>,
    pub xDlSym: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_vfs,
                                           arg2: *mut ::libc::c_void,
                                           zSymbol: *const ::libc::c_char)
                                          ->
                                              ::std::option::Option<extern "C" fn
                                              ()>>,
    pub xDlClose: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3_vfs,
                                             arg2: *mut ::libc::c_void)>,
    pub xRandomness: ::std::option::Option<extern "C" fn
                                               (arg1: *mut sqlite3_vfs,
                                                nByte: ::libc::c_int,
                                                zOut: *mut ::libc::c_char)
                                               -> ::libc::c_int>,
    pub xSleep: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_vfs,
                                           microseconds: ::libc::c_int)
                                          -> ::libc::c_int>,
    pub xCurrentTime: ::std::option::Option<extern "C" fn
                                                (arg1: *mut sqlite3_vfs,
                                                 arg2: *mut ::libc::c_double)
                                                -> ::libc::c_int>,
    pub xGetLastError: ::std::option::Option<extern "C" fn
                                                 (arg1: *mut sqlite3_vfs,
                                                  arg2: ::libc::c_int,
                                                  arg3: *mut ::libc::c_char)
                                                 -> ::libc::c_int>,
    pub xCurrentTimeInt64: ::std::option::Option<extern "C" fn
                                                     (arg1: *mut sqlite3_vfs,
                                                      arg2:
                                                          *mut sqlite3_int64)
                                                     -> ::libc::c_int>,
    pub xSetSystemCall: ::std::option::Option<extern "C" fn
                                                  (arg1: *mut sqlite3_vfs,
                                                   zName:
                                                       *const ::libc::c_char,
                                                   arg2: sqlite3_syscall_ptr)
                                                  -> ::libc::c_int>,
    pub xGetSystemCall: ::std::option::Option<extern "C" fn
                                                  (arg1: *mut sqlite3_vfs,
                                                   zName:
                                                       *const ::libc::c_char)
                                                  -> sqlite3_syscall_ptr>,
    pub xNextSystemCall: ::std::option::Option<extern "C" fn
                                                   (arg1: *mut sqlite3_vfs,
                                                    zName:
                                                        *const ::libc::c_char)
                                                   -> *const ::libc::c_char>,
}
pub type sqlite3_mem_methods = Struct_sqlite3_mem_methods;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_mem_methods {
    pub xMalloc: ::std::option::Option<extern "C" fn(arg1: ::libc::c_int)
                                           -> *mut ::libc::c_void>,
    pub xFree: ::std::option::Option<extern "C" fn
                                         (arg1: *mut ::libc::c_void)>,
    pub xRealloc: ::std::option::Option<extern "C" fn
                                            (arg1: *mut ::libc::c_void,
                                             arg2: ::libc::c_int)
                                            -> *mut ::libc::c_void>,
    pub xSize: ::std::option::Option<extern "C" fn(arg1: *mut ::libc::c_void)
                                         -> ::libc::c_int>,
    pub xRoundup: ::std::option::Option<extern "C" fn(arg1: ::libc::c_int)
                                            -> ::libc::c_int>,
    pub xInit: ::std::option::Option<extern "C" fn(arg1: *mut ::libc::c_void)
                                         -> ::libc::c_int>,
    pub xShutdown: ::std::option::Option<extern "C" fn
                                             (arg1: *mut ::libc::c_void)>,
    pub pAppData: *mut ::libc::c_void,
}
#[derive(Copy)]
pub enum Struct_sqlite3_stmt { }
pub type sqlite3_stmt = Struct_sqlite3_stmt;
#[derive(Copy)]
pub enum Struct_Mem { }
pub type sqlite3_value = Struct_Mem;
#[derive(Copy)]
pub enum Struct_sqlite3_context { }
pub type sqlite3_context = Struct_sqlite3_context;
pub type sqlite3_destructor_type =
    ::std::option::Option<extern "C" fn(arg1: *mut ::libc::c_void)>;
pub type sqlite3_vtab = Struct_sqlite3_vtab;
pub type sqlite3_index_info = Struct_sqlite3_index_info;
pub type sqlite3_vtab_cursor = Struct_sqlite3_vtab_cursor;
pub type sqlite3_module = Struct_sqlite3_module;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_module {
    pub iVersion: ::libc::c_int,
    pub xCreate: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3,
                                            pAux: *mut ::libc::c_void,
                                            argc: ::libc::c_int,
                                            argv:
                                                *const *const ::libc::c_char,
                                            ppVTab: *mut *mut sqlite3_vtab,
                                            arg2: *mut *mut ::libc::c_char)
                                           -> ::libc::c_int>,
    pub xConnect: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3,
                                             pAux: *mut ::libc::c_void,
                                             argc: ::libc::c_int,
                                             argv:
                                                 *const *const ::libc::c_char,
                                             ppVTab: *mut *mut sqlite3_vtab,
                                             arg2: *mut *mut ::libc::c_char)
                                            -> ::libc::c_int>,
    pub xBestIndex: ::std::option::Option<extern "C" fn
                                              (pVTab: *mut sqlite3_vtab,
                                               arg1: *mut sqlite3_index_info)
                                              -> ::libc::c_int>,
    pub xDisconnect: ::std::option::Option<extern "C" fn
                                               (pVTab: *mut sqlite3_vtab)
                                               -> ::libc::c_int>,
    pub xDestroy: ::std::option::Option<extern "C" fn
                                            (pVTab: *mut sqlite3_vtab)
                                            -> ::libc::c_int>,
    pub xOpen: ::std::option::Option<extern "C" fn
                                         (pVTab: *mut sqlite3_vtab,
                                          ppCursor:
                                              *mut *mut sqlite3_vtab_cursor)
                                         -> ::libc::c_int>,
    pub xClose: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_vtab_cursor)
                                          -> ::libc::c_int>,
    pub xFilter: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vtab_cursor,
                                            idxNum: ::libc::c_int,
                                            idxStr: *const ::libc::c_char,
                                            argc: ::libc::c_int,
                                            argv: *mut *mut sqlite3_value)
                                           -> ::libc::c_int>,
    pub xNext: ::std::option::Option<extern "C" fn
                                         (arg1: *mut sqlite3_vtab_cursor)
                                         -> ::libc::c_int>,
    pub xEof: ::std::option::Option<extern "C" fn
                                        (arg1: *mut sqlite3_vtab_cursor)
                                        -> ::libc::c_int>,
    pub xColumn: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vtab_cursor,
                                            arg2: *mut sqlite3_context,
                                            arg3: ::libc::c_int)
                                           -> ::libc::c_int>,
    pub xRowid: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_vtab_cursor,
                                           pRowid: *mut sqlite3_int64)
                                          -> ::libc::c_int>,
    pub xUpdate: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_vtab,
                                            arg2: ::libc::c_int,
                                            arg3: *mut *mut sqlite3_value,
                                            arg4: *mut sqlite3_int64)
                                           -> ::libc::c_int>,
    pub xBegin: ::std::option::Option<extern "C" fn(pVTab: *mut sqlite3_vtab)
                                          -> ::libc::c_int>,
    pub xSync: ::std::option::Option<extern "C" fn(pVTab: *mut sqlite3_vtab)
                                         -> ::libc::c_int>,
    pub xCommit: ::std::option::Option<extern "C" fn(pVTab: *mut sqlite3_vtab)
                                           -> ::libc::c_int>,
    pub xRollback: ::std::option::Option<extern "C" fn
                                             (pVTab: *mut sqlite3_vtab)
                                             -> ::libc::c_int>,
    pub xFindFunction: ::std::option::Option<extern "C" fn
                                                 (pVtab: *mut sqlite3_vtab,
                                                  nArg: ::libc::c_int,
                                                  zName:
                                                      *const ::libc::c_char,
                                                  pxFunc:
                                                      *mut ::std::option::Option<extern "C" fn
                                                                                     (arg1:
                                                                                          *mut sqlite3_context,
                                                                                      arg2:
                                                                                          ::libc::c_int,
                                                                                      arg3:
                                                                                          *mut *mut sqlite3_value)>,
                                                  ppArg:
                                                      *mut *mut ::libc::c_void)
                                                 -> ::libc::c_int>,
    pub xRename: ::std::option::Option<extern "C" fn
                                           (pVtab: *mut sqlite3_vtab,
                                            zNew: *const ::libc::c_char)
                                           -> ::libc::c_int>,
    pub xSavepoint: ::std::option::Option<extern "C" fn
                                              (pVTab: *mut sqlite3_vtab,
                                               arg1: ::libc::c_int)
                                              -> ::libc::c_int>,
    pub xRelease: ::std::option::Option<extern "C" fn
                                            (pVTab: *mut sqlite3_vtab,
                                             arg1: ::libc::c_int)
                                            -> ::libc::c_int>,
    pub xRollbackTo: ::std::option::Option<extern "C" fn
                                               (pVTab: *mut sqlite3_vtab,
                                                arg1: ::libc::c_int)
                                               -> ::libc::c_int>,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_index_info {
    pub nConstraint: ::libc::c_int,
    pub aConstraint: *mut Struct_sqlite3_index_constraint,
    pub nOrderBy: ::libc::c_int,
    pub aOrderBy: *mut Struct_sqlite3_index_orderby,
    pub aConstraintUsage: *mut Struct_sqlite3_index_constraint_usage,
    pub idxNum: ::libc::c_int,
    pub idxStr: *mut ::libc::c_char,
    pub needToFreeIdxStr: ::libc::c_int,
    pub orderByConsumed: ::libc::c_int,
    pub estimatedCost: ::libc::c_double,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_index_constraint {
    pub iColumn: ::libc::c_int,
    pub op: ::libc::c_uchar,
    pub usable: ::libc::c_uchar,
    pub iTermOffset: ::libc::c_int,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_index_orderby {
    pub iColumn: ::libc::c_int,
    pub desc: ::libc::c_uchar,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_index_constraint_usage {
    pub argvIndex: ::libc::c_int,
    pub omit: ::libc::c_uchar,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_vtab {
    pub pModule: *const sqlite3_module,
    pub nRef: ::libc::c_int,
    pub zErrMsg: *mut ::libc::c_char,
}
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_vtab_cursor {
    pub pVtab: *mut sqlite3_vtab,
}
#[derive(Copy)]
pub enum Struct_sqlite3_blob { }
pub type sqlite3_blob = Struct_sqlite3_blob;
pub type sqlite3_mutex_methods = Struct_sqlite3_mutex_methods;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_mutex_methods {
    pub xMutexInit: ::std::option::Option<extern "C" fn() -> ::libc::c_int>,
    pub xMutexEnd: ::std::option::Option<extern "C" fn() -> ::libc::c_int>,
    pub xMutexAlloc: ::std::option::Option<extern "C" fn(arg1: ::libc::c_int)
                                               -> *mut sqlite3_mutex>,
    pub xMutexFree: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_mutex)>,
    pub xMutexEnter: ::std::option::Option<extern "C" fn
                                               (arg1: *mut sqlite3_mutex)>,
    pub xMutexTry: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_mutex)
                                             -> ::libc::c_int>,
    pub xMutexLeave: ::std::option::Option<extern "C" fn
                                               (arg1: *mut sqlite3_mutex)>,
    pub xMutexHeld: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_mutex)
                                              -> ::libc::c_int>,
    pub xMutexNotheld: ::std::option::Option<extern "C" fn
                                                 (arg1: *mut sqlite3_mutex)
                                                 -> ::libc::c_int>,
}
#[derive(Copy)]
pub enum Struct_sqlite3_pcache { }
pub type sqlite3_pcache = Struct_sqlite3_pcache;
pub type sqlite3_pcache_page = Struct_sqlite3_pcache_page;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_pcache_page {
    pub pBuf: *mut ::libc::c_void,
    pub pExtra: *mut ::libc::c_void,
}
pub type sqlite3_pcache_methods2 = Struct_sqlite3_pcache_methods2;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_pcache_methods2 {
    pub iVersion: ::libc::c_int,
    pub pArg: *mut ::libc::c_void,
    pub xInit: ::std::option::Option<extern "C" fn(arg1: *mut ::libc::c_void)
                                         -> ::libc::c_int>,
    pub xShutdown: ::std::option::Option<extern "C" fn
                                             (arg1: *mut ::libc::c_void)>,
    pub xCreate: ::std::option::Option<extern "C" fn
                                           (szPage: ::libc::c_int,
                                            szExtra: ::libc::c_int,
                                            bPurgeable: ::libc::c_int)
                                           -> *mut sqlite3_pcache>,
    pub xCachesize: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_pcache,
                                               nCachesize: ::libc::c_int)>,
    pub xPagecount: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_pcache)
                                              -> ::libc::c_int>,
    pub xFetch: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           key: ::libc::c_uint,
                                           createFlag: ::libc::c_int)
                                          -> *mut sqlite3_pcache_page>,
    pub xUnpin: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           arg2: *mut sqlite3_pcache_page,
                                           discard: ::libc::c_int)>,
    pub xRekey: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           arg2: *mut sqlite3_pcache_page,
                                           oldKey: ::libc::c_uint,
                                           newKey: ::libc::c_uint)>,
    pub xTruncate: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_pcache,
                                              iLimit: ::libc::c_uint)>,
    pub xDestroy: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3_pcache)>,
    pub xShrink: ::std::option::Option<extern "C" fn
                                           (arg1: *mut sqlite3_pcache)>,
}
pub type sqlite3_pcache_methods = Struct_sqlite3_pcache_methods;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_pcache_methods {
    pub pArg: *mut ::libc::c_void,
    pub xInit: ::std::option::Option<extern "C" fn(arg1: *mut ::libc::c_void)
                                         -> ::libc::c_int>,
    pub xShutdown: ::std::option::Option<extern "C" fn
                                             (arg1: *mut ::libc::c_void)>,
    pub xCreate: ::std::option::Option<extern "C" fn
                                           (szPage: ::libc::c_int,
                                            bPurgeable: ::libc::c_int)
                                           -> *mut sqlite3_pcache>,
    pub xCachesize: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_pcache,
                                               nCachesize: ::libc::c_int)>,
    pub xPagecount: ::std::option::Option<extern "C" fn
                                              (arg1: *mut sqlite3_pcache)
                                              -> ::libc::c_int>,
    pub xFetch: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           key: ::libc::c_uint,
                                           createFlag: ::libc::c_int)
                                          -> *mut ::libc::c_void>,
    pub xUnpin: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           arg2: *mut ::libc::c_void,
                                           discard: ::libc::c_int)>,
    pub xRekey: ::std::option::Option<extern "C" fn
                                          (arg1: *mut sqlite3_pcache,
                                           arg2: *mut ::libc::c_void,
                                           oldKey: ::libc::c_uint,
                                           newKey: ::libc::c_uint)>,
    pub xTruncate: ::std::option::Option<extern "C" fn
                                             (arg1: *mut sqlite3_pcache,
                                              iLimit: ::libc::c_uint)>,
    pub xDestroy: ::std::option::Option<extern "C" fn
                                            (arg1: *mut sqlite3_pcache)>,
}
#[derive(Copy)]
pub enum Struct_sqlite3_backup { }
pub type sqlite3_backup = Struct_sqlite3_backup;
pub type sqlite3_rtree_geometry = Struct_sqlite3_rtree_geometry;
#[repr(C)]
#[derive(Copy)]
pub struct Struct_sqlite3_rtree_geometry {
    pub pContext: *mut ::libc::c_void,
    pub nParam: ::libc::c_int,
    pub aParam: *mut ::libc::c_double,
    pub pUser: *mut ::libc::c_void,
    pub xDelUser: ::std::option::Option<extern "C" fn
                                            (arg1: *mut ::libc::c_void)>,
}
extern "C" {
    pub static mut sqlite3_version: *const ::libc::c_char;
    pub static mut sqlite3_temp_directory: *mut ::libc::c_char;
    pub static mut sqlite3_data_directory: *mut ::libc::c_char;
}
extern "C" {
    pub fn sqlite3_libversion() -> *const ::libc::c_char;
    pub fn sqlite3_sourceid() -> *const ::libc::c_char;
    pub fn sqlite3_libversion_number() -> ::libc::c_int;
    pub fn sqlite3_compileoption_used(zOptName: *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_compileoption_get(N: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_threadsafe() -> ::libc::c_int;
    pub fn sqlite3_close(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_exec(arg1: *mut sqlite3, sql: *const ::libc::c_char,
                        callback:
                            ::std::option::Option<extern "C" fn
                                                      (arg1: *mut sqlite3,
                                                       sql:
                                                           *const ::libc::c_char,
                                                       callback:
                                                           ::std::option::Option<extern "C" fn
                                                                                     (arg1:
                                                                                          *mut ::libc::c_void,
                                                                                      arg2:
                                                                                          ::libc::c_int,
                                                                                      arg3:
                                                                                          *mut *mut ::libc::c_char,
                                                                                      arg4:
                                                                                          *mut *mut ::libc::c_char)
                                                                                     ->
                                                                                         ::libc::c_int>,
                                                       arg2:
                                                           *mut ::libc::c_void,
                                                       errmsg:
                                                           *mut *mut ::libc::c_char)
                                                      -> ::libc::c_int>,
                        arg2: *mut ::libc::c_void,
                        errmsg: *mut *mut ::libc::c_char) -> ::libc::c_int;
    pub fn sqlite3_initialize() -> ::libc::c_int;
    pub fn sqlite3_shutdown() -> ::libc::c_int;
    pub fn sqlite3_os_init() -> ::libc::c_int;
    pub fn sqlite3_os_end() -> ::libc::c_int;
    pub fn sqlite3_config(arg1: ::libc::c_int, ...) -> ::libc::c_int;
    pub fn sqlite3_db_config(arg1: *mut sqlite3, op: ::libc::c_int, ...)
     -> ::libc::c_int;
    pub fn sqlite3_extended_result_codes(arg1: *mut sqlite3,
                                         onoff: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_last_insert_rowid(arg1: *mut sqlite3) -> sqlite3_int64;
    pub fn sqlite3_changes(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_total_changes(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_interrupt(arg1: *mut sqlite3);
    pub fn sqlite3_complete(sql: *const ::libc::c_char) -> ::libc::c_int;
    pub fn sqlite3_complete16(sql: *const ::libc::c_void) -> ::libc::c_int;
    pub fn sqlite3_busy_handler(arg1: *mut sqlite3,
                                arg2:
                                    ::std::option::Option<extern "C" fn
                                                              (arg1:
                                                                   *mut sqlite3,
                                                               arg2:
                                                                   ::std::option::Option<extern "C" fn
                                                                                             (arg1:
                                                                                                  *mut ::libc::c_void,
                                                                                              arg2:
                                                                                                  ::libc::c_int)
                                                                                             ->
                                                                                                 ::libc::c_int>,
                                                               arg3:
                                                                   *mut ::libc::c_void)
                                                              ->
                                                                  ::libc::c_int>,
                                arg3: *mut ::libc::c_void) -> ::libc::c_int;
    pub fn sqlite3_busy_timeout(arg1: *mut sqlite3, ms: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_get_table(db: *mut sqlite3, zSql: *const ::libc::c_char,
                             pazResult: *mut *mut *mut ::libc::c_char,
                             pnRow: *mut ::libc::c_int,
                             pnColumn: *mut ::libc::c_int,
                             pzErrmsg: *mut *mut ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_free_table(result: *mut *mut ::libc::c_char);
    pub fn sqlite3_mprintf(arg1: *const ::libc::c_char, ...)
     -> *mut ::libc::c_char;
    pub fn sqlite3_snprintf(arg1: ::libc::c_int, arg2: *mut ::libc::c_char,
                            arg3: *const ::libc::c_char, ...)
     -> *mut ::libc::c_char;
    pub fn sqlite3_malloc(arg1: ::libc::c_int) -> *mut ::libc::c_void;
    pub fn sqlite3_realloc(arg1: *mut ::libc::c_void, arg2: ::libc::c_int)
     -> *mut ::libc::c_void;
    pub fn sqlite3_free(arg1: *mut ::libc::c_void);
    pub fn sqlite3_memory_used() -> sqlite3_int64;
    pub fn sqlite3_memory_highwater(resetFlag: ::libc::c_int)
     -> sqlite3_int64;
    pub fn sqlite3_randomness(N: ::libc::c_int, P: *mut ::libc::c_void);
    pub fn sqlite3_set_authorizer(arg1: *mut sqlite3,
                                  xAuth:
                                      ::std::option::Option<extern "C" fn
                                                                (arg1:
                                                                     *mut sqlite3,
                                                                 xAuth:
                                                                     ::std::option::Option<extern "C" fn
                                                                                               (arg1:
                                                                                                    *mut ::libc::c_void,
                                                                                                arg2:
                                                                                                    ::libc::c_int,
                                                                                                arg3:
                                                                                                    *const ::libc::c_char,
                                                                                                arg4:
                                                                                                    *const ::libc::c_char,
                                                                                                arg5:
                                                                                                    *const ::libc::c_char,
                                                                                                arg6:
                                                                                                    *const ::libc::c_char)
                                                                                               ->
                                                                                                   ::libc::c_int>,
                                                                 pUserData:
                                                                     *mut ::libc::c_void)
                                                                ->
                                                                    ::libc::c_int>,
                                  pUserData: *mut ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_trace(arg1: *mut sqlite3,
                         xTrace:
                             ::std::option::Option<extern "C" fn
                                                       (arg1: *mut sqlite3,
                                                        xTrace:
                                                            ::std::option::Option<extern "C" fn
                                                                                      (arg1:
                                                                                           *mut ::libc::c_void,
                                                                                       arg2:
                                                                                           *const ::libc::c_char)>,
                                                        arg2:
                                                            *mut ::libc::c_void)>,
                         arg2: *mut ::libc::c_void) -> *mut ::libc::c_void;
    pub fn sqlite3_profile(arg1: *mut sqlite3,
                           xProfile:
                               ::std::option::Option<extern "C" fn
                                                         (arg1: *mut sqlite3,
                                                          xProfile:
                                                              ::std::option::Option<extern "C" fn
                                                                                        (arg1:
                                                                                             *mut ::libc::c_void,
                                                                                         arg2:
                                                                                             *const ::libc::c_char,
                                                                                         arg3:
                                                                                             sqlite3_uint64)>,
                                                          arg2:
                                                              *mut ::libc::c_void)>,
                           arg2: *mut ::libc::c_void) -> *mut ::libc::c_void;
    pub fn sqlite3_progress_handler(arg1: *mut sqlite3, arg2: ::libc::c_int,
                                    arg3:
                                        ::std::option::Option<extern "C" fn
                                                                  (arg1:
                                                                       *mut sqlite3,
                                                                   arg2:
                                                                       ::libc::c_int,
                                                                   arg3:
                                                                       ::std::option::Option<extern "C" fn
                                                                                                 (arg1:
                                                                                                      *mut ::libc::c_void)
                                                                                                 ->
                                                                                                     ::libc::c_int>,
                                                                   arg4:
                                                                       *mut ::libc::c_void)
                                                                  ->
                                                                      ::libc::c_int>,
                                    arg4: *mut ::libc::c_void);
    pub fn sqlite3_open(filename: *const ::libc::c_char,
                        ppDb: *mut *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_open16(filename: *const ::libc::c_void,
                          ppDb: *mut *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_open_v2(filename: *const ::libc::c_char,
                           ppDb: *mut *mut sqlite3, flags: ::libc::c_int,
                           zVfs: *const ::libc::c_char) -> ::libc::c_int;
    pub fn sqlite3_uri_parameter(zFilename: *const ::libc::c_char,
                                 zParam: *const ::libc::c_char)
     -> *const ::libc::c_char;
    pub fn sqlite3_uri_boolean(zFile: *const ::libc::c_char,
                               zParam: *const ::libc::c_char,
                               bDefault: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_uri_int64(arg1: *const ::libc::c_char,
                             arg2: *const ::libc::c_char, arg3: sqlite3_int64)
     -> sqlite3_int64;
    pub fn sqlite3_errcode(db: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_extended_errcode(db: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_errmsg(arg1: *mut sqlite3) -> *const ::libc::c_char;
    pub fn sqlite3_errmsg16(arg1: *mut sqlite3) -> *const ::libc::c_void;
    pub fn sqlite3_limit(arg1: *mut sqlite3, id: ::libc::c_int,
                         newVal: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_prepare(db: *mut sqlite3, zSql: *const ::libc::c_char,
                           nByte: ::libc::c_int,
                           ppStmt: *mut *mut sqlite3_stmt,
                           pzTail: *mut *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_prepare_v2(db: *mut sqlite3, zSql: *const ::libc::c_char,
                              nByte: ::libc::c_int,
                              ppStmt: *mut *mut sqlite3_stmt,
                              pzTail: *mut *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_prepare16(db: *mut sqlite3, zSql: *const ::libc::c_void,
                             nByte: ::libc::c_int,
                             ppStmt: *mut *mut sqlite3_stmt,
                             pzTail: *mut *const ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_prepare16_v2(db: *mut sqlite3, zSql: *const ::libc::c_void,
                                nByte: ::libc::c_int,
                                ppStmt: *mut *mut sqlite3_stmt,
                                pzTail: *mut *const ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_sql(pStmt: *mut sqlite3_stmt) -> *const ::libc::c_char;
    pub fn sqlite3_stmt_readonly(pStmt: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_stmt_busy(arg1: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_bind_blob(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                             arg3: *const ::libc::c_void, n: ::libc::c_int,
                             arg4:
                             ::std::option::Option<extern "C" fn
                             (arg1:
                              *mut ::libc::c_void)>)
        -> ::libc::c_int;
    pub fn sqlite3_bind_double(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                               arg3: ::libc::c_double) -> ::libc::c_int;
    pub fn sqlite3_bind_int(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                            arg3: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_bind_int64(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                              arg3: sqlite3_int64) -> ::libc::c_int;
    pub fn sqlite3_bind_null(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_bind_text(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                             arg3: *const ::libc::c_char, n: ::libc::c_int,
                             arg4:
                             ::std::option::Option<extern "C" fn
                             (arg1:
                              *mut ::libc::c_void)>)
        -> ::libc::c_int;
    pub fn sqlite3_bind_text16(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                               arg3: *const ::libc::c_void,
                               arg4: ::libc::c_int,
                               arg5:
                               ::std::option::Option<extern "C" fn
                               (arg1:
                                *mut ::libc::c_void)>)
        -> ::libc::c_int;
    pub fn sqlite3_bind_value(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                              arg3: *const sqlite3_value) -> ::libc::c_int;
    pub fn sqlite3_bind_zeroblob(arg1: *mut sqlite3_stmt, arg2: ::libc::c_int,
                                 n: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_bind_parameter_count(arg1: *mut sqlite3_stmt)
     -> ::libc::c_int;
    pub fn sqlite3_bind_parameter_name(arg1: *mut sqlite3_stmt,
                                       arg2: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_bind_parameter_index(arg1: *mut sqlite3_stmt,
                                        zName: *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_clear_bindings(arg1: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_column_count(pStmt: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_column_name(arg1: *mut sqlite3_stmt, N: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_column_name16(arg1: *mut sqlite3_stmt, N: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_database_name(arg1: *mut sqlite3_stmt,
                                        arg2: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_column_database_name16(arg1: *mut sqlite3_stmt,
                                          arg2: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_table_name(arg1: *mut sqlite3_stmt,
                                     arg2: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_column_table_name16(arg1: *mut sqlite3_stmt,
                                       arg2: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_origin_name(arg1: *mut sqlite3_stmt,
                                      arg2: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_column_origin_name16(arg1: *mut sqlite3_stmt,
                                        arg2: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_decltype(arg1: *mut sqlite3_stmt,
                                   arg2: ::libc::c_int)
     -> *const ::libc::c_char;
    pub fn sqlite3_column_decltype16(arg1: *mut sqlite3_stmt,
                                     arg2: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_step(arg1: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_data_count(pStmt: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_column_blob(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_bytes(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_column_bytes16(arg1: *mut sqlite3_stmt,
                                  iCol: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_column_double(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> ::libc::c_double;
    pub fn sqlite3_column_int(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_column_int64(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> sqlite3_int64;
    pub fn sqlite3_column_text(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> *const ::libc::c_uchar;
    pub fn sqlite3_column_text16(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> *const ::libc::c_void;
    pub fn sqlite3_column_type(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_column_value(arg1: *mut sqlite3_stmt, iCol: ::libc::c_int)
     -> *mut sqlite3_value;
    pub fn sqlite3_finalize(pStmt: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_reset(pStmt: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_create_function(db: *mut sqlite3,
                                   zFunctionName: *const ::libc::c_char,
                                   nArg: ::libc::c_int,
                                   eTextRep: ::libc::c_int,
                                   pApp: *mut ::libc::c_void,
                                   xFunc:
                                       ::std::option::Option<extern "C" fn
                                                                 (db:
                                                                      *mut sqlite3,
                                                                  zFunctionName:
                                                                      *const ::libc::c_char,
                                                                  nArg:
                                                                      ::libc::c_int,
                                                                  eTextRep:
                                                                      ::libc::c_int,
                                                                  pApp:
                                                                      *mut ::libc::c_void,
                                                                  xFunc:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xStep:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xFinal:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context)>)>,
                                   xStep:
                                       ::std::option::Option<extern "C" fn
                                                                 (db:
                                                                      *mut sqlite3,
                                                                  zFunctionName:
                                                                      *const ::libc::c_char,
                                                                  nArg:
                                                                      ::libc::c_int,
                                                                  eTextRep:
                                                                      ::libc::c_int,
                                                                  pApp:
                                                                      *mut ::libc::c_void,
                                                                  xFunc:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xStep:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xFinal:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context)>)>,
                                   xFinal:
                                       ::std::option::Option<extern "C" fn
                                                                 (db:
                                                                      *mut sqlite3,
                                                                  zFunctionName:
                                                                      *const ::libc::c_char,
                                                                  nArg:
                                                                      ::libc::c_int,
                                                                  eTextRep:
                                                                      ::libc::c_int,
                                                                  pApp:
                                                                      *mut ::libc::c_void,
                                                                  xFunc:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xStep:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context,
                                                                                                 arg2:
                                                                                                     ::libc::c_int,
                                                                                                 arg3:
                                                                                                     *mut *mut sqlite3_value)>,
                                                                  xFinal:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut sqlite3_context)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_create_function16(db: *mut sqlite3,
                                     zFunctionName: *const ::libc::c_void,
                                     nArg: ::libc::c_int,
                                     eTextRep: ::libc::c_int,
                                     pApp: *mut ::libc::c_void,
                                     xFunc:
                                         ::std::option::Option<extern "C" fn
                                                                   (db:
                                                                        *mut sqlite3,
                                                                    zFunctionName:
                                                                        *const ::libc::c_void,
                                                                    nArg:
                                                                        ::libc::c_int,
                                                                    eTextRep:
                                                                        ::libc::c_int,
                                                                    pApp:
                                                                        *mut ::libc::c_void,
                                                                    xFunc:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xStep:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xFinal:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context)>)>,
                                     xStep:
                                         ::std::option::Option<extern "C" fn
                                                                   (db:
                                                                        *mut sqlite3,
                                                                    zFunctionName:
                                                                        *const ::libc::c_void,
                                                                    nArg:
                                                                        ::libc::c_int,
                                                                    eTextRep:
                                                                        ::libc::c_int,
                                                                    pApp:
                                                                        *mut ::libc::c_void,
                                                                    xFunc:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xStep:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xFinal:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context)>)>,
                                     xFinal:
                                         ::std::option::Option<extern "C" fn
                                                                   (db:
                                                                        *mut sqlite3,
                                                                    zFunctionName:
                                                                        *const ::libc::c_void,
                                                                    nArg:
                                                                        ::libc::c_int,
                                                                    eTextRep:
                                                                        ::libc::c_int,
                                                                    pApp:
                                                                        *mut ::libc::c_void,
                                                                    xFunc:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xStep:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context,
                                                                                                   arg2:
                                                                                                       ::libc::c_int,
                                                                                                   arg3:
                                                                                                       *mut *mut sqlite3_value)>,
                                                                    xFinal:
                                                                        ::std::option::Option<extern "C" fn
                                                                                                  (arg1:
                                                                                                       *mut sqlite3_context)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_create_function_v2(db: *mut sqlite3,
                                      zFunctionName: *const ::libc::c_char,
                                      nArg: ::libc::c_int,
                                      eTextRep: ::libc::c_int,
                                      pApp: *mut ::libc::c_void,
                                      xFunc:
                                          ::std::option::Option<extern "C" fn
                                                                    (db:
                                                                         *mut sqlite3,
                                                                     zFunctionName:
                                                                         *const ::libc::c_char,
                                                                     nArg:
                                                                         ::libc::c_int,
                                                                     eTextRep:
                                                                         ::libc::c_int,
                                                                     pApp:
                                                                         *mut ::libc::c_void,
                                                                     xFunc:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xStep:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xFinal:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context)>,
                                                                     xDestroy:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void)>)>,
                                      xStep:
                                          ::std::option::Option<extern "C" fn
                                                                    (db:
                                                                         *mut sqlite3,
                                                                     zFunctionName:
                                                                         *const ::libc::c_char,
                                                                     nArg:
                                                                         ::libc::c_int,
                                                                     eTextRep:
                                                                         ::libc::c_int,
                                                                     pApp:
                                                                         *mut ::libc::c_void,
                                                                     xFunc:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xStep:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xFinal:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context)>,
                                                                     xDestroy:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void)>)>,
                                      xFinal:
                                          ::std::option::Option<extern "C" fn
                                                                    (db:
                                                                         *mut sqlite3,
                                                                     zFunctionName:
                                                                         *const ::libc::c_char,
                                                                     nArg:
                                                                         ::libc::c_int,
                                                                     eTextRep:
                                                                         ::libc::c_int,
                                                                     pApp:
                                                                         *mut ::libc::c_void,
                                                                     xFunc:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xStep:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xFinal:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context)>,
                                                                     xDestroy:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void)>)>,
                                      xDestroy:
                                          ::std::option::Option<extern "C" fn
                                                                    (db:
                                                                         *mut sqlite3,
                                                                     zFunctionName:
                                                                         *const ::libc::c_char,
                                                                     nArg:
                                                                         ::libc::c_int,
                                                                     eTextRep:
                                                                         ::libc::c_int,
                                                                     pApp:
                                                                         *mut ::libc::c_void,
                                                                     xFunc:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xStep:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *mut *mut sqlite3_value)>,
                                                                     xFinal:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut sqlite3_context)>,
                                                                     xDestroy:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_aggregate_count(arg1: *mut sqlite3_context)
     -> ::libc::c_int;
    pub fn sqlite3_expired(arg1: *mut sqlite3_stmt) -> ::libc::c_int;
    pub fn sqlite3_transfer_bindings(arg1: *mut sqlite3_stmt,
                                     arg2: *mut sqlite3_stmt)
     -> ::libc::c_int;
    pub fn sqlite3_global_recover() -> ::libc::c_int;
    pub fn sqlite3_thread_cleanup();
    pub fn sqlite3_memory_alarm(arg1:
                                    ::std::option::Option<extern "C" fn
                                                              (arg1:
                                                                   ::std::option::Option<extern "C" fn
                                                                                             (arg1:
                                                                                                  *mut ::libc::c_void,
                                                                                              arg2:
                                                                                                  sqlite3_int64,
                                                                                              arg3:
                                                                                                  ::libc::c_int)>,
                                                               arg2:
                                                                   *mut ::libc::c_void,
                                                               arg3:
                                                                   sqlite3_int64)>,
                                arg2: *mut ::libc::c_void,
                                arg3: sqlite3_int64) -> ::libc::c_int;
    pub fn sqlite3_value_blob(arg1: *mut sqlite3_value)
     -> *const ::libc::c_void;
    pub fn sqlite3_value_bytes(arg1: *mut sqlite3_value) -> ::libc::c_int;
    pub fn sqlite3_value_bytes16(arg1: *mut sqlite3_value) -> ::libc::c_int;
    pub fn sqlite3_value_double(arg1: *mut sqlite3_value) -> ::libc::c_double;
    pub fn sqlite3_value_int(arg1: *mut sqlite3_value) -> ::libc::c_int;
    pub fn sqlite3_value_int64(arg1: *mut sqlite3_value) -> sqlite3_int64;
    pub fn sqlite3_value_text(arg1: *mut sqlite3_value)
     -> *const ::libc::c_uchar;
    pub fn sqlite3_value_text16(arg1: *mut sqlite3_value)
     -> *const ::libc::c_void;
    pub fn sqlite3_value_text16le(arg1: *mut sqlite3_value)
     -> *const ::libc::c_void;
    pub fn sqlite3_value_text16be(arg1: *mut sqlite3_value)
     -> *const ::libc::c_void;
    pub fn sqlite3_value_type(arg1: *mut sqlite3_value) -> ::libc::c_int;
    pub fn sqlite3_value_numeric_type(arg1: *mut sqlite3_value)
     -> ::libc::c_int;
    pub fn sqlite3_aggregate_context(arg1: *mut sqlite3_context,
                                     nBytes: ::libc::c_int)
     -> *mut ::libc::c_void;
    pub fn sqlite3_user_data(arg1: *mut sqlite3_context)
     -> *mut ::libc::c_void;
    pub fn sqlite3_context_db_handle(arg1: *mut sqlite3_context)
     -> *mut sqlite3;
    pub fn sqlite3_get_auxdata(arg1: *mut sqlite3_context, N: ::libc::c_int)
     -> *mut ::libc::c_void;
    pub fn sqlite3_set_auxdata(arg1: *mut sqlite3_context, N: ::libc::c_int,
                               arg2: *mut ::libc::c_void,
                               arg3:
                                   ::std::option::Option<extern "C" fn
                                                             (arg1:
                                                                  *mut sqlite3_context,
                                                              N:
                                                                  ::libc::c_int,
                                                              arg2:
                                                                  *mut ::libc::c_void,
                                                              arg3:
                                                                  ::std::option::Option<extern "C" fn
                                                                                            (arg1:
                                                                                                 *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_blob(arg1: *mut sqlite3_context,
                               arg2: *const ::libc::c_void,
                               arg3: ::libc::c_int,
                               arg4:
                                   ::std::option::Option<extern "C" fn
                                                             (arg1:
                                                                  *mut sqlite3_context,
                                                              arg2:
                                                                  *const ::libc::c_void,
                                                              arg3:
                                                                  ::libc::c_int,
                                                              arg4:
                                                                  ::std::option::Option<extern "C" fn
                                                                                            (arg1:
                                                                                                 *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_double(arg1: *mut sqlite3_context,
                                 arg2: ::libc::c_double);
    pub fn sqlite3_result_error(arg1: *mut sqlite3_context,
                                arg2: *const ::libc::c_char,
                                arg3: ::libc::c_int);
    pub fn sqlite3_result_error16(arg1: *mut sqlite3_context,
                                  arg2: *const ::libc::c_void,
                                  arg3: ::libc::c_int);
    pub fn sqlite3_result_error_toobig(arg1: *mut sqlite3_context);
    pub fn sqlite3_result_error_nomem(arg1: *mut sqlite3_context);
    pub fn sqlite3_result_error_code(arg1: *mut sqlite3_context,
                                     arg2: ::libc::c_int);
    pub fn sqlite3_result_int(arg1: *mut sqlite3_context,
                              arg2: ::libc::c_int);
    pub fn sqlite3_result_int64(arg1: *mut sqlite3_context,
                                arg2: sqlite3_int64);
    pub fn sqlite3_result_null(arg1: *mut sqlite3_context);
    pub fn sqlite3_result_text(arg1: *mut sqlite3_context,
                               arg2: *const ::libc::c_char,
                               arg3: ::libc::c_int,
                               arg4:
                                   ::std::option::Option<extern "C" fn
                                                             (arg1:
                                                                  *mut sqlite3_context,
                                                              arg2:
                                                                  *const ::libc::c_char,
                                                              arg3:
                                                                  ::libc::c_int,
                                                              arg4:
                                                                  ::std::option::Option<extern "C" fn
                                                                                            (arg1:
                                                                                                 *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_text16(arg1: *mut sqlite3_context,
                                 arg2: *const ::libc::c_void,
                                 arg3: ::libc::c_int,
                                 arg4:
                                     ::std::option::Option<extern "C" fn
                                                               (arg1:
                                                                    *mut sqlite3_context,
                                                                arg2:
                                                                    *const ::libc::c_void,
                                                                arg3:
                                                                    ::libc::c_int,
                                                                arg4:
                                                                    ::std::option::Option<extern "C" fn
                                                                                              (arg1:
                                                                                                   *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_text16le(arg1: *mut sqlite3_context,
                                   arg2: *const ::libc::c_void,
                                   arg3: ::libc::c_int,
                                   arg4:
                                       ::std::option::Option<extern "C" fn
                                                                 (arg1:
                                                                      *mut sqlite3_context,
                                                                  arg2:
                                                                      *const ::libc::c_void,
                                                                  arg3:
                                                                      ::libc::c_int,
                                                                  arg4:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_text16be(arg1: *mut sqlite3_context,
                                   arg2: *const ::libc::c_void,
                                   arg3: ::libc::c_int,
                                   arg4:
                                       ::std::option::Option<extern "C" fn
                                                                 (arg1:
                                                                      *mut sqlite3_context,
                                                                  arg2:
                                                                      *const ::libc::c_void,
                                                                  arg3:
                                                                      ::libc::c_int,
                                                                  arg4:
                                                                      ::std::option::Option<extern "C" fn
                                                                                                (arg1:
                                                                                                     *mut ::libc::c_void)>)>);
    pub fn sqlite3_result_value(arg1: *mut sqlite3_context,
                                arg2: *mut sqlite3_value);
    pub fn sqlite3_result_zeroblob(arg1: *mut sqlite3_context,
                                   n: ::libc::c_int);
    pub fn sqlite3_create_collation(arg1: *mut sqlite3,
                                    zName: *const ::libc::c_char,
                                    eTextRep: ::libc::c_int,
                                    pArg: *mut ::libc::c_void,
                                    xCompare:
                                        ::std::option::Option<extern "C" fn
                                                                  (arg1:
                                                                       *mut sqlite3,
                                                                   zName:
                                                                       *const ::libc::c_char,
                                                                   eTextRep:
                                                                       ::libc::c_int,
                                                                   pArg:
                                                                       *mut ::libc::c_void,
                                                                   xCompare:
                                                                       ::std::option::Option<extern "C" fn
                                                                                                 (arg1:
                                                                                                      *mut ::libc::c_void,
                                                                                                  arg2:
                                                                                                      ::libc::c_int,
                                                                                                  arg3:
                                                                                                      *const ::libc::c_void,
                                                                                                  arg4:
                                                                                                      ::libc::c_int,
                                                                                                  arg5:
                                                                                                      *const ::libc::c_void)
                                                                                                 ->
                                                                                                     ::libc::c_int>)
                                                                  ->
                                                                      ::libc::c_int>)
     -> ::libc::c_int;
    pub fn sqlite3_create_collation_v2(arg1: *mut sqlite3,
                                       zName: *const ::libc::c_char,
                                       eTextRep: ::libc::c_int,
                                       pArg: *mut ::libc::c_void,
                                       xCompare:
                                           ::std::option::Option<extern "C" fn
                                                                     (arg1:
                                                                          *mut sqlite3,
                                                                      zName:
                                                                          *const ::libc::c_char,
                                                                      eTextRep:
                                                                          ::libc::c_int,
                                                                      pArg:
                                                                          *mut ::libc::c_void,
                                                                      xCompare:
                                                                          ::std::option::Option<extern "C" fn
                                                                                                    (arg1:
                                                                                                         *mut ::libc::c_void,
                                                                                                     arg2:
                                                                                                         ::libc::c_int,
                                                                                                     arg3:
                                                                                                         *const ::libc::c_void,
                                                                                                     arg4:
                                                                                                         ::libc::c_int,
                                                                                                     arg5:
                                                                                                         *const ::libc::c_void)
                                                                                                    ->
                                                                                                        ::libc::c_int>,
                                                                      xDestroy:
                                                                          ::std::option::Option<extern "C" fn
                                                                                                    (arg1:
                                                                                                         *mut ::libc::c_void)>)
                                                                     ->
                                                                         ::libc::c_int>,
                                       xDestroy:
                                           ::std::option::Option<extern "C" fn
                                                                     (arg1:
                                                                          *mut sqlite3,
                                                                      zName:
                                                                          *const ::libc::c_char,
                                                                      eTextRep:
                                                                          ::libc::c_int,
                                                                      pArg:
                                                                          *mut ::libc::c_void,
                                                                      xCompare:
                                                                          ::std::option::Option<extern "C" fn
                                                                                                    (arg1:
                                                                                                         *mut ::libc::c_void,
                                                                                                     arg2:
                                                                                                         ::libc::c_int,
                                                                                                     arg3:
                                                                                                         *const ::libc::c_void,
                                                                                                     arg4:
                                                                                                         ::libc::c_int,
                                                                                                     arg5:
                                                                                                         *const ::libc::c_void)
                                                                                                    ->
                                                                                                        ::libc::c_int>,
                                                                      xDestroy:
                                                                          ::std::option::Option<extern "C" fn
                                                                                                    (arg1:
                                                                                                         *mut ::libc::c_void)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_create_collation16(arg1: *mut sqlite3,
                                      zName: *const ::libc::c_void,
                                      eTextRep: ::libc::c_int,
                                      pArg: *mut ::libc::c_void,
                                      xCompare:
                                          ::std::option::Option<extern "C" fn
                                                                    (arg1:
                                                                         *mut sqlite3,
                                                                     zName:
                                                                         *const ::libc::c_void,
                                                                     eTextRep:
                                                                         ::libc::c_int,
                                                                     pArg:
                                                                         *mut ::libc::c_void,
                                                                     xCompare:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void,
                                                                                                    arg2:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *const ::libc::c_void,
                                                                                                    arg4:
                                                                                                        ::libc::c_int,
                                                                                                    arg5:
                                                                                                        *const ::libc::c_void)
                                                                                                   ->
                                                                                                       ::libc::c_int>)
                                                                    ->
                                                                        ::libc::c_int>)
     -> ::libc::c_int;
    pub fn sqlite3_collation_needed(arg1: *mut sqlite3,
                                    arg2: *mut ::libc::c_void,
                                    arg3:
                                        ::std::option::Option<extern "C" fn
                                                                  (arg1:
                                                                       *mut sqlite3,
                                                                   arg2:
                                                                       *mut ::libc::c_void,
                                                                   arg3:
                                                                       ::std::option::Option<extern "C" fn
                                                                                                 (arg1:
                                                                                                      *mut ::libc::c_void,
                                                                                                  arg2:
                                                                                                      *mut sqlite3,
                                                                                                  eTextRep:
                                                                                                      ::libc::c_int,
                                                                                                  arg3:
                                                                                                      *const ::libc::c_char)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_collation_needed16(arg1: *mut sqlite3,
                                      arg2: *mut ::libc::c_void,
                                      arg3:
                                          ::std::option::Option<extern "C" fn
                                                                    (arg1:
                                                                         *mut sqlite3,
                                                                     arg2:
                                                                         *mut ::libc::c_void,
                                                                     arg3:
                                                                         ::std::option::Option<extern "C" fn
                                                                                                   (arg1:
                                                                                                        *mut ::libc::c_void,
                                                                                                    arg2:
                                                                                                        *mut sqlite3,
                                                                                                    eTextRep:
                                                                                                        ::libc::c_int,
                                                                                                    arg3:
                                                                                                        *const ::libc::c_void)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_sleep(arg1: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_get_autocommit(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_db_handle(arg1: *mut sqlite3_stmt) -> *mut sqlite3;
    pub fn sqlite3_db_filename(db: *mut sqlite3,
                               zDbName: *const ::libc::c_char)
     -> *const ::libc::c_char;
    pub fn sqlite3_db_readonly(db: *mut sqlite3,
                               zDbName: *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_next_stmt(pDb: *mut sqlite3, pStmt: *mut sqlite3_stmt)
     -> *mut sqlite3_stmt;
    pub fn sqlite3_commit_hook(arg1: *mut sqlite3,
                               arg2:
                                   ::std::option::Option<extern "C" fn
                                                             (arg1:
                                                                  *mut sqlite3,
                                                              arg2:
                                                                  ::std::option::Option<extern "C" fn
                                                                                            (arg1:
                                                                                                 *mut ::libc::c_void)
                                                                                            ->
                                                                                                ::libc::c_int>,
                                                              arg3:
                                                                  *mut ::libc::c_void)
                                                             ->
                                                                 ::libc::c_int>,
                               arg3: *mut ::libc::c_void)
     -> *mut ::libc::c_void;
    pub fn sqlite3_rollback_hook(arg1: *mut sqlite3,
                                 arg2:
                                     ::std::option::Option<extern "C" fn
                                                               (arg1:
                                                                    *mut sqlite3,
                                                                arg2:
                                                                    ::std::option::Option<extern "C" fn
                                                                                              (arg1:
                                                                                                   *mut ::libc::c_void)>,
                                                                arg3:
                                                                    *mut ::libc::c_void)>,
                                 arg3: *mut ::libc::c_void)
     -> *mut ::libc::c_void;
    pub fn sqlite3_update_hook(arg1: *mut sqlite3,
                               arg2:
                                   ::std::option::Option<extern "C" fn
                                                             (arg1:
                                                                  *mut sqlite3,
                                                              arg2:
                                                                  ::std::option::Option<extern "C" fn
                                                                                            (arg1:
                                                                                                 *mut ::libc::c_void,
                                                                                             arg2:
                                                                                                 ::libc::c_int,
                                                                                             arg3:
                                                                                                 *const ::libc::c_char,
                                                                                             arg4:
                                                                                                 *const ::libc::c_char,
                                                                                             arg5:
                                                                                                 sqlite3_int64)>,
                                                              arg3:
                                                                  *mut ::libc::c_void)>,
                               arg3: *mut ::libc::c_void)
     -> *mut ::libc::c_void;
    pub fn sqlite3_enable_shared_cache(arg1: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_release_memory(arg1: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_db_release_memory(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_soft_heap_limit64(N: sqlite3_int64) -> sqlite3_int64;
    pub fn sqlite3_soft_heap_limit(N: ::libc::c_int);
    pub fn sqlite3_table_column_metadata(db: *mut sqlite3,
                                         zDbName: *const ::libc::c_char,
                                         zTableName: *const ::libc::c_char,
                                         zColumnName: *const ::libc::c_char,
                                         pzDataType:
                                             *mut *const ::libc::c_char,
                                         pzCollSeq:
                                             *mut *const ::libc::c_char,
                                         pNotNull: *mut ::libc::c_int,
                                         pPrimaryKey: *mut ::libc::c_int,
                                         pAutoinc: *mut ::libc::c_int)
     -> ::libc::c_int;
    #[cfg(feature = "load_extension")]
    pub fn sqlite3_load_extension(db: *mut sqlite3,
                                  zFile: *const ::libc::c_char,
                                  zProc: *const ::libc::c_char,
                                  pzErrMsg: *mut *mut ::libc::c_char)
     -> ::libc::c_int;
    #[cfg(feature = "load_extension")]
    pub fn sqlite3_enable_load_extension(db: *mut sqlite3,
                                         onoff: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_auto_extension(xEntryPoint:
                                      ::std::option::Option<extern "C" fn
                                                                (xEntryPoint:
                                                                     ::std::option::Option<extern "C" fn
                                                                                               ()>)>)
     -> ::libc::c_int;
    pub fn sqlite3_reset_auto_extension();
    pub fn sqlite3_create_module(db: *mut sqlite3,
                                 zName: *const ::libc::c_char,
                                 p: *const sqlite3_module,
                                 pClientData: *mut ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_create_module_v2(db: *mut sqlite3,
                                    zName: *const ::libc::c_char,
                                    p: *const sqlite3_module,
                                    pClientData: *mut ::libc::c_void,
                                    xDestroy:
                                        ::std::option::Option<extern "C" fn
                                                                  (db:
                                                                       *mut sqlite3,
                                                                   zName:
                                                                       *const ::libc::c_char,
                                                                   p:
                                                                       *const sqlite3_module,
                                                                   pClientData:
                                                                       *mut ::libc::c_void,
                                                                   xDestroy:
                                                                       ::std::option::Option<extern "C" fn
                                                                                                 (arg1:
                                                                                                      *mut ::libc::c_void)>)>)
     -> ::libc::c_int;
    pub fn sqlite3_declare_vtab(arg1: *mut sqlite3,
                                zSQL: *const ::libc::c_char) -> ::libc::c_int;
    pub fn sqlite3_overload_function(arg1: *mut sqlite3,
                                     zFuncName: *const ::libc::c_char,
                                     nArg: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_blob_open(arg1: *mut sqlite3, zDb: *const ::libc::c_char,
                             zTable: *const ::libc::c_char,
                             zColumn: *const ::libc::c_char,
                             iRow: sqlite3_int64, flags: ::libc::c_int,
                             ppBlob: *mut *mut sqlite3_blob) -> ::libc::c_int;
    pub fn sqlite3_blob_reopen(arg1: *mut sqlite3_blob, arg2: sqlite3_int64)
     -> ::libc::c_int;
    pub fn sqlite3_blob_close(arg1: *mut sqlite3_blob) -> ::libc::c_int;
    pub fn sqlite3_blob_bytes(arg1: *mut sqlite3_blob) -> ::libc::c_int;
    pub fn sqlite3_blob_read(arg1: *mut sqlite3_blob, Z: *mut ::libc::c_void,
                             N: ::libc::c_int, iOffset: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_blob_write(arg1: *mut sqlite3_blob,
                              z: *const ::libc::c_void, n: ::libc::c_int,
                              iOffset: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_vfs_find(zVfsName: *const ::libc::c_char)
     -> *mut sqlite3_vfs;
    pub fn sqlite3_vfs_register(arg1: *mut sqlite3_vfs,
                                makeDflt: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_vfs_unregister(arg1: *mut sqlite3_vfs) -> ::libc::c_int;
    pub fn sqlite3_mutex_alloc(arg1: ::libc::c_int) -> *mut sqlite3_mutex;
    pub fn sqlite3_mutex_free(arg1: *mut sqlite3_mutex);
    pub fn sqlite3_mutex_enter(arg1: *mut sqlite3_mutex);
    pub fn sqlite3_mutex_try(arg1: *mut sqlite3_mutex) -> ::libc::c_int;
    pub fn sqlite3_mutex_leave(arg1: *mut sqlite3_mutex);
    pub fn sqlite3_mutex_held(arg1: *mut sqlite3_mutex) -> ::libc::c_int;
    pub fn sqlite3_mutex_notheld(arg1: *mut sqlite3_mutex) -> ::libc::c_int;
    pub fn sqlite3_db_mutex(arg1: *mut sqlite3) -> *mut sqlite3_mutex;
    pub fn sqlite3_file_control(arg1: *mut sqlite3,
                                zDbName: *const ::libc::c_char,
                                op: ::libc::c_int, arg2: *mut ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_test_control(op: ::libc::c_int, ...) -> ::libc::c_int;
    pub fn sqlite3_status(op: ::libc::c_int, pCurrent: *mut ::libc::c_int,
                          pHighwater: *mut ::libc::c_int,
                          resetFlag: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_db_status(arg1: *mut sqlite3, op: ::libc::c_int,
                             pCur: *mut ::libc::c_int,
                             pHiwtr: *mut ::libc::c_int,
                             resetFlg: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_stmt_status(arg1: *mut sqlite3_stmt, op: ::libc::c_int,
                               resetFlg: ::libc::c_int) -> ::libc::c_int;
    pub fn sqlite3_backup_init(pDest: *mut sqlite3,
                               zDestName: *const ::libc::c_char,
                               pSource: *mut sqlite3,
                               zSourceName: *const ::libc::c_char)
     -> *mut sqlite3_backup;
    pub fn sqlite3_backup_step(p: *mut sqlite3_backup, nPage: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_backup_finish(p: *mut sqlite3_backup) -> ::libc::c_int;
    pub fn sqlite3_backup_remaining(p: *mut sqlite3_backup) -> ::libc::c_int;
    pub fn sqlite3_backup_pagecount(p: *mut sqlite3_backup) -> ::libc::c_int;
    pub fn sqlite3_unlock_notify(pBlocked: *mut sqlite3,
                                 xNotify:
                                     ::std::option::Option<extern "C" fn
                                                               (pBlocked:
                                                                    *mut sqlite3,
                                                                xNotify:
                                                                    ::std::option::Option<extern "C" fn
                                                                                              (apArg:
                                                                                                   *mut *mut ::libc::c_void,
                                                                                               nArg:
                                                                                                   ::libc::c_int)>,
                                                                pNotifyArg:
                                                                    *mut ::libc::c_void)>,
                                 pNotifyArg: *mut ::libc::c_void)
     -> ::libc::c_int;
    pub fn sqlite3_stricmp(arg1: *const ::libc::c_char,
                           arg2: *const ::libc::c_char) -> ::libc::c_int;
    pub fn sqlite3_strnicmp(arg1: *const ::libc::c_char,
                            arg2: *const ::libc::c_char, arg3: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_log(iErrCode: ::libc::c_int,
                       zFormat: *const ::libc::c_char, ...);
    pub fn sqlite3_wal_hook(arg1: *mut sqlite3,
                            arg2:
                                ::std::option::Option<extern "C" fn
                                                          (arg1: *mut sqlite3,
                                                           arg2:
                                                               ::std::option::Option<extern "C" fn
                                                                                         (arg1:
                                                                                              *mut ::libc::c_void,
                                                                                          arg2:
                                                                                              *mut sqlite3,
                                                                                          arg3:
                                                                                              *const ::libc::c_char,
                                                                                          arg4:
                                                                                              ::libc::c_int)
                                                                                         ->
                                                                                             ::libc::c_int>,
                                                           arg3:
                                                               *mut ::libc::c_void)
                                                          -> ::libc::c_int>,
                            arg3: *mut ::libc::c_void) -> *mut ::libc::c_void;
    pub fn sqlite3_wal_autocheckpoint(db: *mut sqlite3, N: ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_wal_checkpoint(db: *mut sqlite3,
                                  zDb: *const ::libc::c_char)
     -> ::libc::c_int;
    pub fn sqlite3_wal_checkpoint_v2(db: *mut sqlite3,
                                     zDb: *const ::libc::c_char,
                                     eMode: ::libc::c_int,
                                     pnLog: *mut ::libc::c_int,
                                     pnCkpt: *mut ::libc::c_int)
     -> ::libc::c_int;
    pub fn sqlite3_vtab_config(arg1: *mut sqlite3, op: ::libc::c_int, ...)
     -> ::libc::c_int;
    pub fn sqlite3_vtab_on_conflict(arg1: *mut sqlite3) -> ::libc::c_int;
    pub fn sqlite3_rtree_geometry_callback(db: *mut sqlite3,
                                           zGeom: *const ::libc::c_char,
                                           xGeom:
                                               ::std::option::Option<extern "C" fn
                                                                         (db:
                                                                              *mut sqlite3,
                                                                          zGeom:
                                                                              *const ::libc::c_char,
                                                                          xGeom:
                                                                              ::std::option::Option<extern "C" fn
                                                                                                        (arg1:
                                                                                                             *mut sqlite3_rtree_geometry,
                                                                                                         n:
                                                                                                             ::libc::c_int,
                                                                                                         a:
                                                                                                             *mut ::libc::c_double,
                                                                                                         pRes:
                                                                                                             *mut ::libc::c_int)
                                                                                                        ->
                                                                                                            ::libc::c_int>,
                                                                          pContext:
                                                                              *mut ::libc::c_void)
                                                                         ->
                                                                             ::libc::c_int>,
                                           pContext: *mut ::libc::c_void)
     -> ::libc::c_int;
}
