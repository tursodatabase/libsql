/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the COPY command.
**
** $Id: copy.c,v 1.11 2004/05/10 10:34:35 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** The COPY command is for compatibility with PostgreSQL and specificially
** for the ability to read the output of pg_dump.  The format is as
** follows:
**
**    COPY table FROM file [USING DELIMITERS string]
**
** "table" is an existing table name.  We will read lines of code from
** file to fill this table with data.  File might be "stdin".  The optional
** delimiter string identifies the field separators.  The default is a tab.
*/
void sqlite3Copy(
  Parse *pParse,       /* The parser context */
  SrcList *pTableName, /* The name of the table into which we will insert */
  Token *pFilename,    /* The file from which to obtain information */
  Token *pDelimiter,   /* Use this as the field delimiter */
  int onError          /* What to do if a constraint fails */
){
  Table *pTab;
  int i;
  Vdbe *v;
  int addr, end;
  char *zFile = 0;
  const char *zDb;
  sqlite *db = pParse->db;


  if( sqlite3_malloc_failed  ) goto copy_cleanup;
  assert( pTableName->nSrc==1 );
  pTab = sqlite3SrcListLookup(pParse, pTableName);
  if( pTab==0 || sqlite3IsReadOnly(pParse, pTab, 0) ) goto copy_cleanup;
  zFile = sqliteStrNDup(pFilename->z, pFilename->n);
  sqlite3Dequote(zFile);
  assert( pTab->iDb<db->nDb );
  zDb = db->aDb[pTab->iDb].zName;
  if( sqlite3AuthCheck(pParse, SQLITE_INSERT, pTab->zName, 0, zDb)
      || sqlite3AuthCheck(pParse, SQLITE_COPY, pTab->zName, zFile, zDb) ){
    goto copy_cleanup;
  }
  v = sqlite3GetVdbe(pParse);
  if( v ){
    sqlite3BeginWriteOperation(pParse, 1, pTab->iDb);
    addr = sqlite3VdbeOp3(v, OP_FileOpen, 0, 0, pFilename->z, pFilename->n);
    sqlite3VdbeDequoteP3(v, addr);
    sqlite3OpenTableAndIndices(pParse, pTab, 0);
    if( db->flags & SQLITE_CountRows ){
      sqlite3VdbeAddOp(v, OP_Integer, 0, 0);  /* Initialize the row count */
    }
    end = sqlite3VdbeMakeLabel(v);
    addr = sqlite3VdbeAddOp(v, OP_FileRead, pTab->nCol, end);
    if( pDelimiter ){
      sqlite3VdbeChangeP3(v, addr, pDelimiter->z, pDelimiter->n);
      sqlite3VdbeDequoteP3(v, addr);
    }else{
      sqlite3VdbeChangeP3(v, addr, "\t", 1);
    }
    if( pTab->iPKey>=0 ){
      sqlite3VdbeAddOp(v, OP_FileColumn, pTab->iPKey, 0);
      sqlite3VdbeAddOp(v, OP_MustBeInt, 0, 0);
    }else{
      sqlite3VdbeAddOp(v, OP_NewRecno, 0, 0);
    }
    for(i=0; i<pTab->nCol; i++){
      if( i==pTab->iPKey ){
        /* The integer primary key column is filled with NULL since its
        ** value is always pulled from the record number */
        sqlite3VdbeAddOp(v, OP_String, 0, 0);
      }else{
        sqlite3VdbeAddOp(v, OP_FileColumn, i, 0);
      }
    }
    sqlite3GenerateConstraintChecks(pParse, pTab, 0, 0, pTab->iPKey>=0, 
                                   0, onError, addr);
    sqlite3CompleteInsertion(pParse, pTab, 0, 0, 0, 0, -1);
    if( (db->flags & SQLITE_CountRows)!=0 ){
      sqlite3VdbeAddOp(v, OP_AddImm, 1, 0);  /* Increment row count */
    }
    sqlite3VdbeAddOp(v, OP_Goto, 0, addr);
    sqlite3VdbeResolveLabel(v, end);
    sqlite3VdbeAddOp(v, OP_Noop, 0, 0);
    sqlite3EndWriteOperation(pParse);
    if( db->flags & SQLITE_CountRows ){
      sqlite3VdbeAddOp(v, OP_ColumnName, 0, 1);
      sqlite3VdbeChangeP3(v, -1, "rows inserted", P3_STATIC);
      sqlite3VdbeAddOp(v, OP_Callback, 1, 0);
    }
  }
  
copy_cleanup:
  sqlite3SrcListDelete(pTableName);
  sqliteFree(zFile);
  return;
}



