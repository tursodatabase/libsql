/*
** 2007 May 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** 
** This file contains code used for testing the SQLite system.
** None of the code in this file goes into a deliverable build.
** 
** The focus of this file is providing the TCL testing layer
** access to compile-time constants.
**
** $Id: test_config.c,v 1.7 2007/08/13 15:18:28 drh Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

/*
** This routine sets entries in the global ::sqlite_options() array variable
** according to the compile-time configuration of the database.  Test
** procedures use this to determine when tests should be omitted.
*/
static void set_options(Tcl_Interp *interp){
#ifdef SQLITE_32BIT_ROWID
  Tcl_SetVar2(interp, "sqlite_options", "rowid32", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "rowid32", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_CASE_SENSITIVE_LIKE
  Tcl_SetVar2(interp, "sqlite_options","casesensitivelike","1",TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options","casesensitivelike","0",TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_DEBUG
  Tcl_SetVar2(interp, "sqlite_options", "debug", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "debug", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_DISABLE_DIRSYNC
  Tcl_SetVar2(interp, "sqlite_options", "dirsync", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "dirsync", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_DISABLE_LFS
  Tcl_SetVar2(interp, "sqlite_options", "lfs", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "lfs", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_ALTERTABLE
  Tcl_SetVar2(interp, "sqlite_options", "altertable", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "altertable", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_ANALYZE
  Tcl_SetVar2(interp, "sqlite_options", "analyze", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "analyze", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_ATTACH
  Tcl_SetVar2(interp, "sqlite_options", "attach", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "attach", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_AUTHORIZATION
  Tcl_SetVar2(interp, "sqlite_options", "auth", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "auth", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_AUTOINCREMENT
  Tcl_SetVar2(interp, "sqlite_options", "autoinc", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "autoinc", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_AUTOVACUUM
  Tcl_SetVar2(interp, "sqlite_options", "autovacuum", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "autovacuum", "1", TCL_GLOBAL_ONLY);
#endif /* SQLITE_OMIT_AUTOVACUUM */
#if !defined(SQLITE_DEFAULT_AUTOVACUUM) || SQLITE_DEFAULT_AUTOVACUUM==0
  Tcl_SetVar2(interp,"sqlite_options","default_autovacuum","0",TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp,"sqlite_options","default_autovacuum","1",TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_BETWEEN_OPTIMIZATION
  Tcl_SetVar2(interp, "sqlite_options", "between_opt", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "between_opt", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_BLOB_LITERAL
  Tcl_SetVar2(interp, "sqlite_options", "bloblit", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "bloblit", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_CAST
  Tcl_SetVar2(interp, "sqlite_options", "cast", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "cast", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_CHECK
  Tcl_SetVar2(interp, "sqlite_options", "check", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "check", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_COLUMN_METADATA
  Tcl_SetVar2(interp, "sqlite_options", "columnmetadata", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "columnmetadata", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_COMPLETE
  Tcl_SetVar2(interp, "sqlite_options", "complete", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "complete", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_COMPOUND_SELECT
  Tcl_SetVar2(interp, "sqlite_options", "compound", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "compound", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_CONFLICT_CLAUSE
  Tcl_SetVar2(interp, "sqlite_options", "conflict", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "conflict", "1", TCL_GLOBAL_ONLY);
#endif

#if OS_UNIX
  Tcl_SetVar2(interp, "sqlite_options", "crashtest", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "crashtest", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_DATETIME_FUNCS
  Tcl_SetVar2(interp, "sqlite_options", "datetime", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "datetime", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_DISKIO
  Tcl_SetVar2(interp, "sqlite_options", "diskio", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "diskio", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_EXPLAIN
  Tcl_SetVar2(interp, "sqlite_options", "explain", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "explain", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_FLOATING_POINT
  Tcl_SetVar2(interp, "sqlite_options", "floatingpoint", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "floatingpoint", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_FOREIGN_KEY
  Tcl_SetVar2(interp, "sqlite_options", "foreignkey", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "foreignkey", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_FTS1
  Tcl_SetVar2(interp, "sqlite_options", "fts1", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "fts1", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_FTS2
  Tcl_SetVar2(interp, "sqlite_options", "fts2", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "fts2", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_GLOBALRECOVER
  Tcl_SetVar2(interp, "sqlite_options", "globalrecover", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "globalrecover", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_ICU
  Tcl_SetVar2(interp, "sqlite_options", "icu", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "icu", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_INCRBLOB
  Tcl_SetVar2(interp, "sqlite_options", "incrblob", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "incrblob", "1", TCL_GLOBAL_ONLY);
#endif /* SQLITE_OMIT_AUTOVACUUM */

#ifdef SQLITE_OMIT_INTEGRITY_CHECK
  Tcl_SetVar2(interp, "sqlite_options", "integrityck", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "integrityck", "1", TCL_GLOBAL_ONLY);
#endif

#if defined(SQLITE_DEFAULT_FILE_FORMAT) && SQLITE_DEFAULT_FILE_FORMAT==1
  Tcl_SetVar2(interp, "sqlite_options", "legacyformat", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "legacyformat", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_LIKE_OPTIMIZATION
  Tcl_SetVar2(interp, "sqlite_options", "like_opt", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "like_opt", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_LOAD_EXTENSION
  Tcl_SetVar2(interp, "sqlite_options", "load_ext", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "load_ext", "1", TCL_GLOBAL_ONLY);
#endif

Tcl_SetVar2(interp, "sqlite_options", "long_double",
              sizeof(LONGDOUBLE_TYPE)>sizeof(double) ? "1" : "0",
              TCL_GLOBAL_ONLY);

#ifdef SQLITE_OMIT_MEMORYDB
  Tcl_SetVar2(interp, "sqlite_options", "memorydb", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "memorydb", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  Tcl_SetVar2(interp, "sqlite_options", "memorymanage", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "memorymanage", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_OR_OPTIMIZATION
  Tcl_SetVar2(interp, "sqlite_options", "or_opt", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "or_opt", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_PAGER_PRAGMAS
  Tcl_SetVar2(interp, "sqlite_options", "pager_pragmas", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "pager_pragmas", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_PARSER
  Tcl_SetVar2(interp, "sqlite_options", "parser", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "parser", "1", TCL_GLOBAL_ONLY);
#endif

#if defined(SQLITE_OMIT_PRAGMA) || defined(SQLITE_OMIT_FLAG_PRAGMAS)
  Tcl_SetVar2(interp, "sqlite_options", "pragma", "0", TCL_GLOBAL_ONLY);
  Tcl_SetVar2(interp, "sqlite_options", "integrityck", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "pragma", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_PROGRESS_CALLBACK
  Tcl_SetVar2(interp, "sqlite_options", "progress", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "progress", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_ENABLE_REDEF_IO
  Tcl_SetVar2(interp, "sqlite_options", "redefio", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "redefio", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_REINDEX
  Tcl_SetVar2(interp, "sqlite_options", "reindex", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "reindex", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_SCHEMA_PRAGMAS
  Tcl_SetVar2(interp, "sqlite_options", "schema_pragmas", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "schema_pragmas", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS
  Tcl_SetVar2(interp, "sqlite_options", "schema_version", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "schema_version", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_SHARED_CACHE
  Tcl_SetVar2(interp, "sqlite_options", "shared_cache", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "shared_cache", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_SUBQUERY
  Tcl_SetVar2(interp, "sqlite_options", "subquery", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "subquery", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_TCL_VARIABLE
  Tcl_SetVar2(interp, "sqlite_options", "tclvar", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "tclvar", "1", TCL_GLOBAL_ONLY);
#endif

#if defined(THREADSAFE) && THREADSAFE
  Tcl_SetVar2(interp, "sqlite_options", "threadsafe", "1", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "threadsafe", "0", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_TRACE
  Tcl_SetVar2(interp, "sqlite_options", "trace", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "trace", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_TRIGGER
  Tcl_SetVar2(interp, "sqlite_options", "trigger", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "trigger", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_TEMPDB
  Tcl_SetVar2(interp, "sqlite_options", "tempdb", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "tempdb", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_UTF16
  Tcl_SetVar2(interp, "sqlite_options", "utf16", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "utf16", "1", TCL_GLOBAL_ONLY);
#endif

#if defined(SQLITE_OMIT_VACUUM) || defined(SQLITE_OMIT_ATTACH)
  Tcl_SetVar2(interp, "sqlite_options", "vacuum", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "vacuum", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_VIEW
  Tcl_SetVar2(interp, "sqlite_options", "view", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "view", "1", TCL_GLOBAL_ONLY);
#endif

#ifdef SQLITE_OMIT_VIRTUALTABLE
  Tcl_SetVar2(interp, "sqlite_options", "vtab", "0", TCL_GLOBAL_ONLY);
#else
  Tcl_SetVar2(interp, "sqlite_options", "vtab", "1", TCL_GLOBAL_ONLY);
#endif

  {
    static int sqlite_max_length = SQLITE_MAX_LENGTH;
    Tcl_LinkVar(interp, "SQLITE_MAX_LENGTH",
           (char*)&sqlite_max_length, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_column = SQLITE_MAX_COLUMN;
    Tcl_LinkVar(interp, "SQLITE_MAX_COLUMN",
           (char*)&sqlite_max_column, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_sql_length = SQLITE_MAX_SQL_LENGTH;
    Tcl_LinkVar(interp, "SQLITE_MAX_SQL_LENGTH",
           (char*)&sqlite_max_sql_length, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_expr_depth = SQLITE_MAX_EXPR_DEPTH;
    Tcl_LinkVar(interp, "SQLITE_MAX_EXPR_DEPTH",
           (char*)&sqlite_max_expr_depth, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_compound_select = SQLITE_MAX_COMPOUND_SELECT;
    Tcl_LinkVar(interp, "SQLITE_MAX_COMPOUND_SELECT",
           (char*)&sqlite_max_compound_select, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_vdbe_op = SQLITE_MAX_VDBE_OP;
    Tcl_LinkVar(interp, "SQLITE_MAX_VDBE_OP",
           (char*)&sqlite_max_vdbe_op, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_function_arg = SQLITE_MAX_FUNCTION_ARG;
    Tcl_LinkVar(interp, "SQLITE_MAX_FUNCTION_ARG",
           (char*)&sqlite_max_function_arg, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_default_temp_cache_size = SQLITE_DEFAULT_TEMP_CACHE_SIZE;
    Tcl_LinkVar(interp, "SQLITE_DEFAULT_TEMP_CACHE_SIZE",
           (char*)&sqlite_default_temp_cache_size,
           TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_default_cache_size = SQLITE_DEFAULT_CACHE_SIZE;
    Tcl_LinkVar(interp, "SQLITE_DEFAULT_CACHE_SIZE",
           (char*)&sqlite_default_cache_size, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_variable_number = SQLITE_MAX_VARIABLE_NUMBER;
    Tcl_LinkVar(interp, "SQLITE_MAX_VARIABLE_NUMBER",
           (char*)&sqlite_max_variable_number, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_default_page_size = SQLITE_DEFAULT_PAGE_SIZE;
    Tcl_LinkVar(interp, "SQLITE_DEFAULT_PAGE_SIZE",
           (char*)&sqlite_default_page_size, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_page_size = SQLITE_MAX_PAGE_SIZE;
    Tcl_LinkVar(interp, "SQLITE_MAX_PAGE_SIZE",
           (char*)&sqlite_max_page_size, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_page_count = SQLITE_MAX_PAGE_COUNT;
    Tcl_LinkVar(interp, "SQLITE_MAX_PAGE_COUNT",
           (char*)&sqlite_max_page_count, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int temp_store = TEMP_STORE;
    Tcl_LinkVar(interp, "TEMP_STORE",
           (char*)&temp_store, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_default_file_format = SQLITE_DEFAULT_FILE_FORMAT;
    Tcl_LinkVar(interp, "SQLITE_DEFAULT_FILE_FORMAT",
           (char*)&sqlite_default_file_format, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_like_pattern = SQLITE_MAX_LIKE_PATTERN_LENGTH;
    Tcl_LinkVar(interp, "SQLITE_MAX_LIKE_PATTERN_LENGTH",
           (char*)&sqlite_max_like_pattern, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
  {
    static int sqlite_max_attached = SQLITE_MAX_ATTACHED;
    Tcl_LinkVar(interp, "SQLITE_MAX_ATTACHED",
           (char*)&sqlite_max_attached, TCL_LINK_INT|TCL_LINK_READ_ONLY);
  }
}


/*
** Register commands with the TCL interpreter.
*/
int Sqliteconfig_Init(Tcl_Interp *interp){
  set_options(interp);
  return TCL_OK;
}
