/*
** 2006 June 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to dynamically load extensions into
** the SQLite library.
*/
#ifndef SQLITE_OMIT_LOAD_EXTENSION

#define SQLITE_CORE 1  /* Disable the API redefinition in sqlite3ext.h */
#include "sqlite3ext.h"
#include "sqliteInt.h"
#include <string.h>
#include <ctype.h>

#ifndef SQLITE_ENABLE_COLUMN_METADATA
# define sqlite3_column_database_name   0
# define sqlite3_column_database_name16 0
# define sqlite3_column_table_name      0
# define sqlite3_column_table_name16    0
# define sqlite3_column_origin_name     0
# define sqlite3_column_origin_name16   0
# define sqlite3_table_column_metadata  0
#endif

const sqlite3_api_routines sqlite3_api = {
  sqlite3_aggregate_context,
  sqlite3_aggregate_count,
  sqlite3_bind_blob,
  sqlite3_bind_double,
  sqlite3_bind_int,
  sqlite3_bind_int64,
  sqlite3_bind_null,
  sqlite3_bind_parameter_count,
  sqlite3_bind_parameter_index,
  sqlite3_bind_parameter_name,
  sqlite3_bind_text,
  sqlite3_bind_text16,
  sqlite3_busy_handler,
  sqlite3_busy_timeout,
  sqlite3_changes,
  sqlite3_close,
  sqlite3_collation_needed,
  sqlite3_collation_needed16,
  sqlite3_column_blob,
  sqlite3_column_bytes,
  sqlite3_column_bytes16,
  sqlite3_column_count,
  sqlite3_column_database_name,
  sqlite3_column_database_name16,
  sqlite3_column_decltype,
  sqlite3_column_decltype16,
  sqlite3_column_double,
  sqlite3_column_int,
  sqlite3_column_int64,
  sqlite3_column_name,
  sqlite3_column_name16,
  sqlite3_column_origin_name,
  sqlite3_column_origin_name16,
  sqlite3_column_table_name,
  sqlite3_column_table_name16,
  sqlite3_column_text,
  sqlite3_column_text16,
  sqlite3_column_type,
  sqlite3_commit_hook,
  sqlite3_complete,
  sqlite3_complete16,
  sqlite3_create_collation,
  sqlite3_create_collation16,
  sqlite3_create_function,
  sqlite3_create_function16,
  sqlite3_data_count,
  sqlite3_db_handle,
  sqlite3_enable_shared_cache,
  sqlite3_errcode,
  sqlite3_errmsg,
  sqlite3_errmsg16,
  sqlite3_exec,
  sqlite3_expired,
  sqlite3_finalize,
  sqlite3_free,
  sqlite3_free_table,
  sqlite3_get_autocommit,
  sqlite3_get_auxdata,
  sqlite3_get_table,
  sqlite3_global_recover,
  sqlite3_interrupt,
  sqlite3_last_insert_rowid,
  sqlite3_libversion,
  sqlite3_libversion_number,
  sqlite3_mprintf,
  sqlite3_open,
  sqlite3_open16,
  sqlite3_prepare,
  sqlite3_prepare16,
  sqlite3_profile,
  sqlite3_progress_handler,
  sqlite3_reset,
  sqlite3_result_blob,
  sqlite3_result_double,
  sqlite3_result_error,
  sqlite3_result_error16,
  sqlite3_result_int,
  sqlite3_result_int64,
  sqlite3_result_null,
  sqlite3_result_text,
  sqlite3_result_text16,
  sqlite3_result_text16be,
  sqlite3_result_text16le,
  sqlite3_result_value,
  sqlite3_rollback_hook,
  sqlite3_set_authorizer,
  sqlite3_set_auxdata,
  sqlite3_snprintf,
  sqlite3_step,
  sqlite3_table_column_metadata,
  sqlite3_thread_cleanup,
  sqlite3_total_changes,
  sqlite3_trace,
  sqlite3_transfer_bindings,
  sqlite3_update_hook,
  sqlite3_user_data,
  sqlite3_value_blob,
  sqlite3_value_bytes,
  sqlite3_value_bytes16,
  sqlite3_value_double,
  sqlite3_value_int,
  sqlite3_value_int64,
  sqlite3_value_numeric_type,
  sqlite3_value_text,
  sqlite3_value_text16,
  sqlite3_value_text16be,
  sqlite3_value_text16le,
  sqlite3_value_type,
  sqlite3_vmprintf,
};


 #if defined(_WIN32) || defined(WIN32) || defined(__MINGW32__) || defined(__BORLANDC__)
# include <windows.h>
# define SQLITE_LIBRARY_TYPE     HANDLE
# define SQLITE_OPEN_LIBRARY(A)  LoadLibrary(A)
# define SQLITE_FIND_SYMBOL(A,B) GetProcAddress(A,B)
# define SQLITE_CLOSE_LIBRARY(A) FreeLibrary(A)
#endif /* windows */

/*
** Non-windows implementation of shared-library loaders
*/
#if defined(HAVE_DLOPEN) && !defined(SQLITE_LIBRARY_TYPE)
# include <dlfcn.h>
# define SQLITE_LIBRARY_TYPE     void*
# define SQLITE_OPEN_LIBRARY(A)  dlopen(A, RTLD_NOW | RTLD_GLOBAL)
# define SQLITE_FIND_SYMBOL(A,B) dlsym(A,B)
# define SQLITE_CLOSE_LIBRARY(A) dlclose(A)
#endif

/*
** Attempt to load an SQLite extension library contained in the file
** zFile.  The entry point is zProc.  zProc may be 0 in which case the
** name of the entry point is derived from the filename.
**
** Return SQLITE_OK on success and SQLITE_ERROR if something goes wrong.
**
** If an error occurs and pzErrMsg is not 0, then fill *pzErrMsg with 
** error message text.  The calling function should free this memory
** by calling sqlite3_free().
**
** The entry point name is derived from the filename according to the
** following steps:
**
**    * Convert the name to lower case
**    * Remove the path prefix from the name
**    * Remove the first "." and all following characters from the name
**    * If the name begins with "lib" remove the first 3 characters
**    * Remove all characters that are not US-ASCII alphanumerics
**      or underscores
**    * Remove any leading digits and underscores from the name
**    * Append "_init" to the name
**
** So, for example, if the input filename is "/home/drh/libtest1.52.so"
** then the entry point would be computed as "test1_init".
**
** The derived entry point name is limited to a reasonable number of
** characters (currently 200).
*/
int sqlite3_load_extension(
  sqlite3 *db,          /* Load the extension into this database connection */
  const char *zFile,    /* Name of the shared library containing extension */
  const char *zProc,    /* Entry point.  Derived from zFile if 0 */
  char **pzErrMsg       /* Put error message here if not 0 */
){
#ifdef SQLITE_LIBRARY_TYPE
  SQLITE_LIBRARY_TYPE handle;
  int (*xInit)(sqlite3*,char**,const sqlite3_api_routines*);
  char *zErrmsg = 0;
  SQLITE_LIBRARY_TYPE *aHandle;

  db->nExtension++;
  aHandle = sqliteMalloc(sizeof(handle)*db->nExtension);
  if( aHandle==0 ){
    return SQLITE_NOMEM;
  }
  if( db->nExtension>0 ){
    memcpy(aHandle, db->aExtension, sizeof(handle)*(db->nExtension-1));
  }
  sqliteFree(db->aExtension);
  db->aExtension = aHandle;
  if( zProc==0 ){
    int i, j, n;
    char *z;
    char zBuf[200];
    n = strlen(zFile);
    for(i=n-1; i>0 && zFile[i-1]!='/'; i--){}
    for(j=i; zFile[j] && zFile[j]!='.'; j++){}
    if( j-i > sizeof(zBuf)-10 ) j = i + sizeof(zBuf) - 10;
    memcpy(zBuf, &zFile[i], j - i);
    zBuf[j - i] = 0;
    z = zBuf;
    for(i=j=0; z[i]; i++){
      int c = z[i];
      if( (c & 0x80)!=0 || (!isalnum(c) && c!='_') ) continue;
      z[j++] = tolower(c);
    }
    z[j] = 0;
    if( strncmp(z, "lib", 3)==0 ){
      z += 3;
    }
    while( z[0] && !isalpha(z[0]) ){
      z++;
    }
    strcat(z, "_init");
    zProc = z;
  }

  handle = SQLITE_OPEN_LIBRARY(zFile);
  if( handle==0 ){
    if( pzErrMsg ){
      *pzErrMsg = sqlite3_mprintf("unable to open shared library [%s]", zFile);
    }
    return SQLITE_ERROR;
  }
  xInit = (int(*)(sqlite3*,char**,const sqlite3_api_routines*))
                   SQLITE_FIND_SYMBOL(handle, zProc);
  if( xInit==0 ){
    if( pzErrMsg ){
       *pzErrMsg = sqlite3_mprintf("no entry point [%s] in shared library [%s]",
                                   zProc, zFile);
    }
    SQLITE_CLOSE_LIBRARY(handle);
    return SQLITE_ERROR;
  }else if( xInit(db, &zErrmsg, &sqlite3_api) ){
    if( pzErrMsg ){
      *pzErrMsg = sqlite3_mprintf("error during initialization: %s", zErrmsg);
    }
    sqlite3_free(zErrmsg);
    SQLITE_CLOSE_LIBRARY(handle);
    return SQLITE_ERROR;
  }
  ((SQLITE_LIBRARY_TYPE*)db->aExtension)[db->nExtension-1] = handle;
  return SQLITE_OK;
#else
  if( pzErrMsg ){
    *pzErrMsg = sqlite3_mprintf(
                     "shared library loading not enabled for this build");
  }
  return SQLITE_ERROR;
#endif
}

/*
** Call this routine when the database connection is closing in order
** to clean up loaded extensions
*/
void sqlite3CloseExtensions(sqlite3 *db){
#ifdef SQLITE_LIBRARY_TYPE
  int i;
  for(i=0; i<db->nExtension; i++){
    SQLITE_CLOSE_LIBRARY(((SQLITE_LIBRARY_TYPE*)db->aExtension)[i]);
  }
  sqliteFree(db->aExtension);
#endif
}

#endif /* SQLITE_OMIT_LOAD_EXTENSION */
