/*
** 2016-05-28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains the implementation of an SQLite virtual table for
** reading CSV files.
**
** Usage:
**
**    .load ./csv
**    CREATE VIRTUAL TABLE temp.csv USING csv(filename=FILENAME);
**    SELECT * FROM csv;
**
** The columns are named "c1", "c2", "c3", ... by default.  But the
** application can define its own CREATE TABLE statement as an additional
** parameter.  For example:
**
**    CREATE VIRTUAL TABLE temp.csv2 USING csv(
**       filename = "../http.log",
**       schema = "CREATE TABLE x(date,ipaddr,url,referrer,userAgent)"
**    );
*/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define CSV_NOINLINE  __attribute__((noinline))
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define CSV_NOINLINE  __declspec(noinline)
#else
#  define CSV_NOINLINE
#endif


/* Max size of the error message in a CsvReader */
#define CSV_MXERR 200

/* A context object used when read a CSV file. */
typedef struct CsvReader CsvReader;
struct CsvReader {
  FILE *in;              /* Read the CSV text from this input stream */
  char *z;               /* Accumulated text for a field */
  int n;                 /* Number of bytes in z */
  int nAlloc;            /* Space allocated for z[] */
  int nLine;             /* Current line number */
  int cTerm;             /* Character that terminated the most recent field */
  char zErr[CSV_MXERR];  /* Error message */
};

/* Initialize a CsvReader object */
static void csv_reader_init(CsvReader *p){
  memset(p, 0, sizeof(*p));
}

/* Close and reset a CsvReader object */
static void csv_reader_reset(CsvReader *p){
  if( p->in ) fclose(p->in);
  sqlite3_free(p->z);
  csv_reader_init(p);
}

/* Report an error on a CsvReader */
static void csv_errmsg(CsvReader *p, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(CSV_MXERR, p->zErr, zFormat, ap);
  va_end(ap);
}

/* Open the file associated with a CsvReader
** Return the number of errors.
*/
static int csv_reader_open(CsvReader *p, const char *zFilename){
  p->in = fopen(zFilename, "rb");
  if( p->in==0 ){
    csv_errmsg(p, "cannot open '%s' for reading", zFilename);
    return 1;
  }
  return 0;
}

/* Increase the size of p->z and append character c to the end. 
** Return 0 on success and non-zero if there is an OOM error */
static CSV_NOINLINE int csv_resize_and_append(CsvReader *p, char c){
  char *zNew;
  int nNew = p->nAlloc*2 + 100;
  zNew = sqlite3_realloc64(p->z, nNew);
  if( zNew ){
    p->z = zNew;
    p->nAlloc = nNew;
    p->z[p->n++] = c;
    return 0;
  }else{
    csv_errmsg(p, "out of memory");
    return 1;
  }
}

/* Append a single character to the CsvReader.z[] array.
** Return 0 on success and non-zero if there is an OOM error */
static int csv_append(CsvReader *p, char c){
  if( p->n>=p->nAlloc-1 ) return csv_resize_and_append(p, c);
  p->z[p->n++] = c;
  return 0;
}

/* Read a single field of CSV text.  Compatible with rfc4180 and extended
** with the option of having a separator other than ",".
**
**   +  Input comes from p->in.
**   +  Store results in p->z of length p->n.  Space to hold p->z comes
**      from sqlite3_malloc64().
**   +  Keep track of the line number in p->nLine.
**   +  Store the character that terminates the field in p->cTerm.  Store
**      EOF on end-of-file.
**
** Return "" at EOF.  Return 0 on an OOM error.
*/
static char *csv_read_one_field(CsvReader *p){
  int c;
  p->n = 0;
  c = fgetc(p->in);
  if( c==EOF ){
    p->cTerm = EOF;
    return "";
  }
  if( c=='"' ){
    int pc, ppc;
    int startLine = p->nLine;
    int cQuote = c;
    pc = ppc = 0;
    while( 1 ){
      c = fgetc(p->in);
      if( c=='\n' ) p->nLine++;
      if( c==cQuote ){
        if( pc==cQuote ){
          pc = 0;
          continue;
        }
      }
      if( (c==',' && pc==cQuote)
       || (c=='\n' && pc==cQuote)
       || (c=='\n' && pc=='\r' && ppc==cQuote)
       || (c==EOF && pc==cQuote)
      ){
        do{ p->n--; }while( p->z[p->n]!=cQuote );
        p->cTerm = c;
        break;
      }
      if( pc==cQuote && c!='\r' ){
        csv_errmsg(p, "line %d: unescaped %c character", p->nLine, cQuote);
        break;
      }
      if( c==EOF ){
        csv_errmsg(p, "line %d: unterminated %c-quoted field\n",
                   startLine, cQuote);
        p->cTerm = c;
        break;
      }
      if( csv_append(p, (char)c) ) return 0;
      ppc = pc;
      pc = c;
    }
  }else{
    while( c!=EOF && c!=',' && c!='\n' ){
      if( csv_append(p, (char)c) ) return 0;
      c = fgetc(p->in);
    }
    if( c=='\n' ){
      p->nLine++;
      if( p->n>0 && p->z[p->n-1]=='\r' ) p->n--;
    }
    p->cTerm = c;
  }
  if( p->z ) p->z[p->n] = 0;
  return p->z;
}


/* Forward references to the various virtual table methods implemented
** in this file. */
static int csvtabCreate(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int csvtabConnect(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int csvtabBestIndex(sqlite3_vtab*,sqlite3_index_info*);
static int csvtabDisconnect(sqlite3_vtab*);
static int csvtabOpen(sqlite3_vtab*, sqlite3_vtab_cursor**);
static int csvtabClose(sqlite3_vtab_cursor*);
static int csvtabFilter(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv);
static int csvtabNext(sqlite3_vtab_cursor*);
static int csvtabEof(sqlite3_vtab_cursor*);
static int csvtabColumn(sqlite3_vtab_cursor*,sqlite3_context*,int);
static int csvtabRowid(sqlite3_vtab_cursor*,sqlite3_int64*);

/* An instance of the CSV virtual table */
typedef struct CsvTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  char *zFilename;                /* Name of the CSV file */
  long iStart;                    /* Offset to start of data in zFilename */
  int nCol;                       /* Number of columns in the CSV file */
} CsvTable;

/* A cursor for the CSV virtual table */
typedef struct CsvCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  CsvReader rdr;                  /* The CsvReader object */
  char **azVal;                   /* Value of the current row */
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} CsvCursor;

/* Transfer error message text from a reader into a CsvTable */
static void csv_xfer_error(CsvTable *pTab, CsvReader *pRdr){
  sqlite3_free(pTab->base.zErrMsg);
  pTab->base.zErrMsg = sqlite3_mprintf("%s", pRdr->zErr);
}

/*
** This method is the destructor fo a CsvTable object.
*/
static int csvtabDisconnect(sqlite3_vtab *pVtab){
  CsvTable *p = (CsvTable*)pVtab;
  sqlite3_free(p->zFilename);
  sqlite3_free(p);
  return SQLITE_OK;
}

/* Skip leading whitespace.  Return a pointer to the first non-whitespace
** character, or to the zero terminator if the string has only whitespace */
static const char *csv_skip_whitespace(const char *z){
  while( isspace((unsigned char)z[0]) ) z++;
  return z;
}

/* Remove trailing whitespace from the end of string z[] */
static void csv_trim_whitespace(char *z){
  size_t n = strlen(z);
  while( n>0 && isspace((unsigned char)z[n]) ) n--;
  z[n] = 0;
}

/* Dequote the string */
static void csv_dequote(char *z){
  int i, j;
  char cQuote = z[0];
  size_t n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}

/* Check to see if the string is of the form:  "TAG = VALUE" with optional
** whitespace before and around tokens.  If it is, return a pointer to the
** first character of VALUE.  If it is not, return NULL.
*/
static const char *csv_parameter(const char *zTag, int nTag, const char *z){
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z+nTag);
  if( z[0]!='=' ) return 0;
  return csv_skip_whitespace(z+1);
}

/* Return 0 if the argument is false and 1 if it is true.  Return -1 if
** we cannot really tell.
*/
static int csv_boolean(const char *z){
  if( sqlite3_stricmp("yes",z)==0
   || sqlite3_stricmp("on",z)==0
   || sqlite3_stricmp("true",z)==0
   || (z[0]=='1' && z[0]==0)
  ){
    return 1;
  }
  if( sqlite3_stricmp("no",z)==0
   || sqlite3_stricmp("off",z)==0
   || sqlite3_stricmp("false",z)==0
   || (z[0]=='0' && z[1]==0)
  ){
    return 0;
  }
  return -1;
}


/*
** Parameters:
**    filename=FILENAME          Required
**    schema=SCHEMA              Optional
**    header=YES|NO              First row of CSV defines the names of
**                               columns if "yes".  Default "no".
**
** If header=no and not columns are listed, then the columns are named
** "c0", "c1", "c2", and so forth.
*/
static int csvtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  CsvTable *pNew = 0;
  int bHeader = -1;
  int rc = SQLITE_OK;
  int i;
  char *zFilename = 0;
  char *zSchema = 0;
  CsvReader sRdr;

  memset(&sRdr, 0, sizeof(sRdr));
  for(i=3; i<argc; i++){
    const char *z = argv[i];
    const char *zValue;
    if( (zValue = csv_parameter("filename",8,z))!=0 ){
      if( zFilename ){
        csv_errmsg(&sRdr, "more than one 'filename' parameter");
        goto csvtab_connect_error;
      }
      zFilename = sqlite3_mprintf("%s", zValue);
      if( zFilename==0 ) goto csvtab_connect_oom;
      csv_trim_whitespace(zFilename);
      csv_dequote(zFilename);
    }else
    if( (zValue = csv_parameter("schema",6,z))!=0 ){
      if( zSchema ){
        csv_errmsg(&sRdr, "more than one 'schema' parameter");
        goto csvtab_connect_error;
      }
      zSchema = sqlite3_mprintf("%s", zValue);
      if( zSchema==0 ) goto csvtab_connect_oom;
      csv_trim_whitespace(zSchema);
      csv_dequote(zSchema);
    }else
    if( (zValue = csv_parameter("header",6,z))!=0 ){
      int x;
      if( bHeader>=0 ){
        csv_errmsg(&sRdr, "more than one 'header' parameter");
        goto csvtab_connect_error;
      }
      x = csv_boolean(zValue);
      if( x==1 ){
        bHeader = 1;
      }else if( x==0 ){
        bHeader = 0;
      }else{
        csv_errmsg(&sRdr, "unrecognized argument to 'header': %s", zValue);
        goto csvtab_connect_error;
      }
    }else
    {
      csv_errmsg(&sRdr, "unrecognized parameter '%s'", z);
      goto csvtab_connect_error;
    }
  }
  if( zFilename==0 ){
    csv_errmsg(&sRdr, "missing 'filename' parameter");
    goto csvtab_connect_error;
  }
  if( csv_reader_open(&sRdr, zFilename) ){
    goto csvtab_connect_error;
  }
  pNew = sqlite3_malloc( sizeof(*pNew) );
  *ppVtab = (sqlite3_vtab*)pNew;
  if( pNew==0 ) goto csvtab_connect_oom;
  memset(pNew, 0, sizeof(*pNew));
  do{
    const char *z = csv_read_one_field(&sRdr);
    if( z==0 ) goto csvtab_connect_oom;
    pNew->nCol++;
  }while( sRdr.cTerm==',' );
  pNew->zFilename = zFilename;
  zFilename = 0;
  pNew->iStart = bHeader==1 ? ftell(sRdr.in) : 0;
  csv_reader_reset(&sRdr);
  if( zSchema==0 ){
    char *zSep = "";
    zSchema = sqlite3_mprintf("CREATE TABLE x(");
    if( zSchema==0 ) goto csvtab_connect_oom;
    for(i=0; i<pNew->nCol; i++){
      zSchema = sqlite3_mprintf("%z%sc%d TEXT",zSchema, zSep, i);
      zSep = ",";
    }
    zSchema = sqlite3_mprintf("%z);", zSchema);
  }
  rc = sqlite3_declare_vtab(db, zSchema);
  if( rc ) goto csvtab_connect_error;
  sqlite3_free(zSchema);
  return SQLITE_OK;

csvtab_connect_oom:
  rc = SQLITE_NOMEM;
  csv_errmsg(&sRdr, "out of memory");

csvtab_connect_error:
  if( pNew ) csvtabDisconnect(&pNew->base);
  sqlite3_free(zFilename);
  sqlite3_free(zSchema);
  if( sRdr.zErr[0] ){
    sqlite3_free(*pzErr);
    *pzErr = sqlite3_mprintf("%s", sRdr.zErr);
  }
  csv_reader_reset(&sRdr);
  return rc;
}

/*
** Reset the current row content held by a CsvCursor.
*/
static void csvtabCursorRowReset(CsvCursor *pCur){
  CsvTable *pTab = (CsvTable*)pCur->base.pVtab;
  int i;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_free(pCur->azVal[i]);
    pCur->azVal[i] = 0;
  }
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int csvtabCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
 return csvtabConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a CsvCursor.
*/
static int csvtabClose(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  csvtabCursorRowReset(pCur);
  csv_reader_reset(&pCur->rdr);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new CsvTable cursor object.
*/
static int csvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  CsvTable *pTab = (CsvTable*)p;
  CsvCursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) * sizeof(char*)*pTab->nCol );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur) + sizeof(char*)*pTab->nCol );
  pCur->azVal = (char**)&pCur[1];
  *ppCursor = &pCur->base;
  if( csv_reader_open(&pCur->rdr, pTab->zFilename) ){
    csv_xfer_error(pTab, &pCur->rdr);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}


/*
** Advance a CsvCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int csvtabNext(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  CsvTable *pTab = (CsvTable*)cur->pVtab;
  int i = 0;
  char *z;
  csvtabCursorRowReset(pCur);
  do{
    z = csv_read_one_field(&pCur->rdr);
    if( z==0 ){
      csv_xfer_error(pTab, &pCur->rdr);
      break;
    }
    z = sqlite3_mprintf("%s", z);
    if( z==0 ){
      csv_errmsg(&pCur->rdr, "out of memory");
      csv_xfer_error(pTab, &pCur->rdr);
      break;
    }
    if( i<pTab->nCol ){
      pCur->azVal[i++] = z;
    }
  }while( z!=0 && pCur->rdr.cTerm==',' );
  if( z==0 || pCur->rdr.cTerm==EOF ){
    pCur->iRowid = -1;
  }else{
    pCur->iRowid++;
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the CsvCursor
** is currently pointing.
*/
static int csvtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  CsvCursor *pCur = (CsvCursor*)cur;
  CsvTable *pTab = (CsvTable*)cur->pVtab;
  if( i>=0 && i<pTab->nCol && pCur->azVal[i]!=0 ){
    sqlite3_result_text(ctx, pCur->azVal[i], -1, SQLITE_STATIC);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int csvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  CsvCursor *pCur = (CsvCursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int csvtabEof(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  return pCur->iRowid<0;
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int csvtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  CsvCursor *pCur = (CsvCursor*)pVtabCursor;
  CsvTable *pTab = (CsvTable*)pVtabCursor->pVtab;
  pCur->iRowid = 0;
  fseek(pCur->rdr.in, pTab->iStart, SEEK_SET);
  return csvtabNext(pVtabCursor);
}

/*
** Only a forwards full table scan is supported.  xBestIndex is a no-op.
*/
static int csvtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}


static sqlite3_module CsvModule = {
  0,                       /* iVersion */
  csvtabCreate,            /* xCreate */
  csvtabConnect,           /* xConnect */
  csvtabBestIndex,         /* xBestIndex */
  csvtabDisconnect,        /* xDisconnect */
  csvtabDisconnect,        /* xDestroy */
  csvtabOpen,              /* xOpen - open a cursor */
  csvtabClose,             /* xClose - close a cursor */
  csvtabFilter,            /* xFilter - configure scan constraints */
  csvtabNext,              /* xNext - advance a cursor */
  csvtabEof,               /* xEof - check for end of scan */
  csvtabColumn,            /* xColumn - read data */
  csvtabRowid,             /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

#ifdef _WIN32
__declspec(dllexport)
#endif
/* 
** This routine is called when the extension is loaded.  The new
** CSV virtual table module is registered with the calling database
** connection.
*/
int sqlite3_csv_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return sqlite3_create_module(db, "csv", &CsvModule, 0);
}
