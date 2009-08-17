/*
** 2005 July 8
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code associated with the ANALYZE command.
**
** @(#) $Id: analyze.c,v 1.52 2009/04/16 17:45:48 drh Exp $
*/
#ifndef SQLITE_OMIT_ANALYZE
#include "sqliteInt.h"

/*
** This routine generates code that opens the sqlite_stat1 table for
** writing with cursor iStatCur. The sqlite_stat2 table is opened
** for writing using cursor (iStatCur+1).
**
** If the sqlite_stat1 tables does not previously exist, it is created.
** If it does previously exist, all entires associated with table zWhere
** are removed.  If zWhere==0 then all entries are removed.
*/
static void openStatTable(
  Parse *pParse,          /* Parsing context */
  int iDb,                /* The database we are looking in */
  int iStatCur,           /* Open the sqlite_stat1 table on this cursor */
  const char *zWhere      /* Delete entries associated with this table */
){
  const char *aName[] = { "sqlite_stat1", "sqlite_stat2" };
  const char *aCols[] = { "tbl,idx,stat", "tbl,idx," SQLITE_INDEX_SAMPLE_COLS };
  int aRoot[] = {0, 0};
  int aCreateTbl[] = {0, 0};

  int i;
  sqlite3 *db = pParse->db;
  Db *pDb;
  Vdbe *v = sqlite3GetVdbe(pParse);
  if( v==0 ) return;
  assert( sqlite3BtreeHoldsAllMutexes(db) );
  assert( sqlite3VdbeDb(v)==db );
  pDb = &db->aDb[iDb];

  for(i=0; i<ArraySize(aName); i++){
    Table *pStat;
    if( (pStat = sqlite3FindTable(db, aName[i], pDb->zName))==0 ){
      /* The sqlite_stat[12] table does not exist. Create it. Note that a 
      ** side-effect of the CREATE TABLE statement is to leave the rootpage 
      ** of the new table in register pParse->regRoot. This is important 
      ** because the OpenWrite opcode below will be needing it. */
      sqlite3NestedParse(pParse,
          "CREATE TABLE %Q.%s(%s)", pDb->zName, aName[i], aCols[i]
      );
      aRoot[i] = pParse->regRoot;
      aCreateTbl[i] = 1;
    }else{
      /* The table already exists. If zWhere is not NULL, delete all entries 
      ** associated with the table zWhere. If zWhere is NULL, delete the
      ** entire contents of the table. */
      aRoot[i] = pStat->tnum;
      sqlite3TableLock(pParse, iDb, aRoot[i], 1, aName[i]);
      if( zWhere ){
        sqlite3NestedParse(pParse,
           "DELETE FROM %Q.%s WHERE tbl=%Q", pDb->zName, aName[i], zWhere
        );
      }else{
        /* The sqlite_stat[12] table already exists.  Delete all rows. */
        sqlite3VdbeAddOp2(v, OP_Clear, aRoot[i], iDb);
      }
    }
  }

  /* Open the sqlite_stat[12] tables for writing. */
  for(i=0; i<ArraySize(aName); i++){
    sqlite3VdbeAddOp3(v, OP_OpenWrite, iStatCur+i, aRoot[i], iDb);
    sqlite3VdbeChangeP4(v, -1, (char *)3, P4_INT32);
    sqlite3VdbeChangeP5(v, aCreateTbl[i]);
  }
}

/*
** Generate code to do an analysis of all indices associated with
** a single table.
*/
static void analyzeOneTable(
  Parse *pParse,   /* Parser context */
  Table *pTab,     /* Table whose indices are to be analyzed */
  int iStatCur,    /* Index of VdbeCursor that writes the sqlite_stat1 table */
  int iMem         /* Available memory locations begin here */
){
  Index *pIdx;     /* An index to being analyzed */
  int iIdxCur;     /* Index of VdbeCursor for index being analyzed */
  int nCol;        /* Number of columns in the index */
  Vdbe *v;         /* The virtual machine being built up */
  int i;           /* Loop counter */
  int topOfLoop;   /* The top of the loop */
  int endOfLoop;   /* The end of the loop */
  int addr;        /* The address of an instruction */
  int iDb;         /* Index of database containing pTab */

  v = sqlite3GetVdbe(pParse);
  if( v==0 || NEVER(pTab==0) || pTab->pIndex==0 ){
    /* Do no analysis for tables that have no indices */
    return;
  }
  assert( sqlite3BtreeHoldsAllMutexes(pParse->db) );
  iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
  assert( iDb>=0 );
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( sqlite3AuthCheck(pParse, SQLITE_ANALYZE, pTab->zName, 0,
      pParse->db->aDb[iDb].zName ) ){
    return;
  }
#endif

  /* Establish a read-lock on the table at the shared-cache level. */
  sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);

  iMem += 3;
  iIdxCur = pParse->nTab++;
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    KeyInfo *pKey = sqlite3IndexKeyinfo(pParse, pIdx);
    int regFields;    /* Register block for building records */
    int regRec;       /* Register holding completed record */
    int regTemp;      /* Temporary use register */
    int regCol;       /* Content of a column from the table being analyzed */
    int regRowid;     /* Rowid for the inserted record */
    int regF2;
    int regStat2;

    /* Open a cursor to the index to be analyzed
    */
    assert( iDb==sqlite3SchemaToIndex(pParse->db, pIdx->pSchema) );
    nCol = pIdx->nColumn;
    sqlite3VdbeAddOp4(v, OP_OpenRead, iIdxCur, pIdx->tnum, iDb,
        (char *)pKey, P4_KEYINFO_HANDOFF);
    VdbeComment((v, "%s", pIdx->zName));
    regStat2 = iMem+nCol*2+1;
    regFields = regStat2+2+SQLITE_INDEX_SAMPLES;
    regTemp = regRowid = regCol = regFields+3;
    regRec = regCol+1;
    if( regRec>pParse->nMem ){
      pParse->nMem = regRec;
    }

    /* Fill in the register with the total number of rows. */
    if( pTab->pIndex==pIdx ){
      sqlite3VdbeAddOp2(v, OP_Count, iIdxCur, iMem-3);
    }
    sqlite3VdbeAddOp2(v, OP_Integer, 0, iMem-2);
    sqlite3VdbeAddOp2(v, OP_Integer, 1, iMem-1);

    /* Memory cells are used as follows. All memory cell addresses are
    ** offset by iMem. That is, cell 0 below is actually cell iMem, cell
    ** 1 is cell 1+iMem, etc.
    **
    **    0:               The total number of rows in the table.
    **
    **    1..nCol:         Number of distinct entries in index considering the
    **                     left-most N columns, where N is the same as the 
    **                     memory cell number.
    **
    **    nCol+1..2*nCol:  Previous value of indexed columns, from left to
    **                     right.
    **
    **    2*nCol+1..2*nCol+10: 10 evenly spaced samples.
    **
    ** Cells iMem through iMem+nCol are initialized to 0.  The others
    ** are initialized to NULL.
    */
    for(i=0; i<=nCol; i++){
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iMem+i);
    }
    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp2(v, OP_Null, 0, iMem+nCol+i+1);
    }

    /* Start the analysis loop. This loop runs through all the entries inof
    ** the index b-tree.  */
    endOfLoop = sqlite3VdbeMakeLabel(v);
    sqlite3VdbeAddOp2(v, OP_Rewind, iIdxCur, endOfLoop);
    topOfLoop = sqlite3VdbeCurrentAddr(v);
    sqlite3VdbeAddOp2(v, OP_AddImm, iMem, 1);

    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, i, regCol);
      if( i==0 ){
        sqlite3VdbeAddOp3(v, OP_Sample, iMem-3, regCol, regStat2+2);
      }
      sqlite3VdbeAddOp3(v, OP_Ne, regCol, 0, iMem+nCol+i+1);
      /**** TODO:  add collating sequence *****/
      sqlite3VdbeChangeP5(v, SQLITE_JUMPIFNULL);
    }
    sqlite3VdbeAddOp2(v, OP_Goto, 0, endOfLoop);
    for(i=0; i<nCol; i++){
      sqlite3VdbeJumpHere(v, topOfLoop + 1 + 2*(i + 1));
      sqlite3VdbeAddOp2(v, OP_AddImm, iMem+i+1, 1);
      sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, i, iMem+nCol+i+1);
    }

    /* End of the analysis loop. */
    sqlite3VdbeResolveLabel(v, endOfLoop);
    sqlite3VdbeAddOp2(v, OP_Next, iIdxCur, topOfLoop);
    sqlite3VdbeAddOp1(v, OP_Close, iIdxCur);

    /* Store the results in sqlite_stat1.
    **
    ** The result is a single row of the sqlite_stat1 table.  The first
    ** two columns are the names of the table and index.  The third column
    ** is a string composed of a list of integer statistics about the
    ** index.  The first integer in the list is the total number of entries
    ** in the index.  There is one additional integer in the list for each
    ** column of the table.  This additional integer is a guess of how many
    ** rows of the table the index will select.  If D is the count of distinct
    ** values and K is the total number of rows, then the integer is computed
    ** as:
    **
    **        I = (K+D-1)/D
    **
    ** If K==0 then no entry is made into the sqlite_stat1 table.  
    ** If K>0 then it is always the case the D>0 so division by zero
    ** is never possible.
    */
    addr = sqlite3VdbeAddOp1(v, OP_IfNot, iMem);
    sqlite3VdbeAddOp4(v, OP_String8, 0, regFields, 0, pTab->zName, 0);
    sqlite3VdbeAddOp4(v, OP_String8, 0, regFields+1, 0, pIdx->zName, 0);
    regF2 = regFields+2;
    sqlite3VdbeAddOp2(v, OP_SCopy, iMem, regF2);
    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp4(v, OP_String8, 0, regTemp, 0, " ", 0);
      sqlite3VdbeAddOp3(v, OP_Concat, regTemp, regF2, regF2);
      sqlite3VdbeAddOp3(v, OP_Add, iMem, iMem+i+1, regTemp);
      sqlite3VdbeAddOp2(v, OP_AddImm, regTemp, -1);
      sqlite3VdbeAddOp3(v, OP_Divide, iMem+i+1, regTemp, regTemp);
      sqlite3VdbeAddOp1(v, OP_ToInt, regTemp);
      sqlite3VdbeAddOp3(v, OP_Concat, regTemp, regF2, regF2);
    }
    sqlite3VdbeAddOp4(v, OP_MakeRecord, regFields, 3, regRec, "aaa", 0);
    sqlite3VdbeAddOp2(v, OP_NewRowid, iStatCur, regRowid);
    sqlite3VdbeAddOp3(v, OP_Insert, iStatCur, regRec, regRowid);
    sqlite3VdbeChangeP5(v, OPFLAG_APPEND);

    /* Store the results in sqlite_stat2. */
    sqlite3VdbeAddOp4(v, OP_String8, 0, regStat2, 0, pTab->zName, 0);
    sqlite3VdbeAddOp4(v, OP_String8, 0, regStat2+1, 0, pIdx->zName, 0);
    sqlite3VdbeAddOp4(v, OP_MakeRecord, regStat2, SQLITE_INDEX_SAMPLES+2,
	regRec, "aabbbbbbbbbb", 0
    );
    sqlite3VdbeAddOp2(v, OP_NewRowid, iStatCur+1, regRowid);
    sqlite3VdbeAddOp3(v, OP_Insert, iStatCur+1, regRec, regRowid);

    sqlite3VdbeJumpHere(v, addr);
  }
}

/*
** Generate code that will cause the most recent index analysis to
** be laoded into internal hash tables where is can be used.
*/
static void loadAnalysis(Parse *pParse, int iDb){
  Vdbe *v = sqlite3GetVdbe(pParse);
  if( v ){
    sqlite3VdbeAddOp1(v, OP_LoadAnalysis, iDb);
  }
}

/*
** Generate code that will do an analysis of an entire database
*/
static void analyzeDatabase(Parse *pParse, int iDb){
  sqlite3 *db = pParse->db;
  Schema *pSchema = db->aDb[iDb].pSchema;    /* Schema of database iDb */
  HashElem *k;
  int iStatCur;
  int iMem;

  sqlite3BeginWriteOperation(pParse, 0, iDb);
  iStatCur = pParse->nTab;
  pParse->nTab += 2;
  openStatTable(pParse, iDb, iStatCur, 0);
  iMem = pParse->nMem+1;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    analyzeOneTable(pParse, pTab, iStatCur, iMem);
  }
  loadAnalysis(pParse, iDb);
}

/*
** Generate code that will do an analysis of a single table in
** a database.
*/
static void analyzeTable(Parse *pParse, Table *pTab){
  int iDb;
  int iStatCur;

  assert( pTab!=0 );
  assert( sqlite3BtreeHoldsAllMutexes(pParse->db) );
  iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
  sqlite3BeginWriteOperation(pParse, 0, iDb);
  iStatCur = pParse->nTab;
  pParse->nTab += 2;
  openStatTable(pParse, iDb, iStatCur, pTab->zName);
  analyzeOneTable(pParse, pTab, iStatCur, pParse->nMem+1);
  loadAnalysis(pParse, iDb);
}

/*
** Generate code for the ANALYZE command.  The parser calls this routine
** when it recognizes an ANALYZE command.
**
**        ANALYZE                            -- 1
**        ANALYZE  <database>                -- 2
**        ANALYZE  ?<database>.?<tablename>  -- 3
**
** Form 1 causes all indices in all attached databases to be analyzed.
** Form 2 analyzes all indices the single database named.
** Form 3 analyzes all indices associated with the named table.
*/
void sqlite3Analyze(Parse *pParse, Token *pName1, Token *pName2){
  sqlite3 *db = pParse->db;
  int iDb;
  int i;
  char *z, *zDb;
  Table *pTab;
  Token *pTableName;

  /* Read the database schema. If an error occurs, leave an error message
  ** and code in pParse and return NULL. */
  assert( sqlite3BtreeHoldsAllMutexes(pParse->db) );
  if( SQLITE_OK!=sqlite3ReadSchema(pParse) ){
    return;
  }

  assert( pName2!=0 || pName1==0 );
  if( pName1==0 ){
    /* Form 1:  Analyze everything */
    for(i=0; i<db->nDb; i++){
      if( i==1 ) continue;  /* Do not analyze the TEMP database */
      analyzeDatabase(pParse, i);
    }
  }else if( pName2->n==0 ){
    /* Form 2:  Analyze the database or table named */
    iDb = sqlite3FindDb(db, pName1);
    if( iDb>=0 ){
      analyzeDatabase(pParse, iDb);
    }else{
      z = sqlite3NameFromToken(db, pName1);
      if( z ){
        pTab = sqlite3LocateTable(pParse, 0, z, 0);
        sqlite3DbFree(db, z);
        if( pTab ){
          analyzeTable(pParse, pTab);
        }
      }
    }
  }else{
    /* Form 3: Analyze the fully qualified table name */
    iDb = sqlite3TwoPartName(pParse, pName1, pName2, &pTableName);
    if( iDb>=0 ){
      zDb = db->aDb[iDb].zName;
      z = sqlite3NameFromToken(db, pTableName);
      if( z ){
        pTab = sqlite3LocateTable(pParse, 0, z, zDb);
        sqlite3DbFree(db, z);
        if( pTab ){
          analyzeTable(pParse, pTab);
        }
      }
    }   
  }
}

/*
** Used to pass information from the analyzer reader through to the
** callback routine.
*/
typedef struct analysisInfo analysisInfo;
struct analysisInfo {
  sqlite3 *db;
  const char *zDatabase;
};

/*
** This callback is invoked once for each index when reading the
** sqlite_stat1 table.  
**
**     argv[0] = name of the index
**     argv[1] = results of analysis - on integer for each column
*/
static int analysisLoader(void *pData, int argc, char **argv, char **NotUsed){
  analysisInfo *pInfo = (analysisInfo*)pData;
  Index *pIndex;
  int i, c;
  unsigned int v;
  const char *z;

  assert( argc==2 );
  UNUSED_PARAMETER2(NotUsed, argc);

  if( argv==0 || argv[0]==0 || argv[1]==0 ){
    return 0;
  }
  pIndex = sqlite3FindIndex(pInfo->db, argv[0], pInfo->zDatabase);
  if( pIndex==0 ){
    return 0;
  }
  z = argv[1];
  for(i=0; *z && i<=pIndex->nColumn; i++){
    v = 0;
    while( (c=z[0])>='0' && c<='9' ){
      v = v*10 + c - '0';
      z++;
    }
    pIndex->aiRowEst[i] = v;
    if( *z==' ' ) z++;
  }
  return 0;
}

/*
** Load the content of the sqlite_stat1 and sqlite_stat2 tables into the 
** index hash tables.
*/
int sqlite3AnalysisLoad(sqlite3 *db, int iDb){
  analysisInfo sInfo;
  HashElem *i;
  char *zSql;
  int rc;

  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pBt!=0 );
  assert( sqlite3BtreeHoldsMutex(db->aDb[iDb].pBt) );

  /* Clear any prior statistics */
  for(i=sqliteHashFirst(&db->aDb[iDb].pSchema->idxHash);i;i=sqliteHashNext(i)){
    Index *pIdx = sqliteHashData(i);
    sqlite3DefaultRowEst(pIdx);
  }

  /* Check to make sure the sqlite_stat1 table existss */
  sInfo.db = db;
  sInfo.zDatabase = db->aDb[iDb].zName;
  if( sqlite3FindTable(db, "sqlite_stat1", sInfo.zDatabase)==0 ){
     return SQLITE_ERROR;
  }

  /* Load new statistics out of the sqlite_stat1 table */
  zSql = sqlite3MPrintf(db, "SELECT idx, stat FROM %Q.sqlite_stat1",
                        sInfo.zDatabase);
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    (void)sqlite3SafetyOff(db);
    rc = sqlite3_exec(db, zSql, analysisLoader, &sInfo, 0);
    (void)sqlite3SafetyOn(db);
    sqlite3DbFree(db, zSql);
  }

  /* Load the statistics from the sqlite_stat2 table */
  if( rc==SQLITE_OK ){
    zSql = sqlite3MPrintf(db, 
	"SELECT idx," SQLITE_INDEX_SAMPLE_COLS " FROM %Q.sqlite_stat2",
        sInfo.zDatabase
    );
    if( zSql ){
      sqlite3_stmt *pStmt = 0;
      (void)sqlite3SafetyOff(db);
      rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
      if( rc==SQLITE_OK ){
	while( SQLITE_ROW==sqlite3_step(pStmt) ){
	  char *zIndex = (char *)sqlite3_column_text(pStmt, 0);
	  Index *pIdx;
          pIdx = sqlite3FindIndex(db, zIndex, sInfo.zDatabase);
	  if( pIdx ){
	    char *pSpace;
	    IndexSample *pSample;
	    int iCol;
	    int nAlloc = SQLITE_INDEX_SAMPLES * sizeof(IndexSample);
	    for(iCol=1; iCol<=SQLITE_INDEX_SAMPLES; iCol++){
	      int eType = sqlite3_column_type(pStmt, iCol);
	      if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
	        nAlloc += sqlite3_column_bytes(pStmt, iCol);
	      }
	    }
	    pSample = sqlite3DbMallocRaw(db, nAlloc);
	    if( !pSample ){
	      rc = SQLITE_NOMEM;
	      break;
	    }
	    sqlite3DbFree(db, pIdx->aSample);
	    pIdx->aSample = pSample;
	    pSpace = (char *)&pSample[SQLITE_INDEX_SAMPLES];
	    for(iCol=1; iCol<=SQLITE_INDEX_SAMPLES; iCol++){
	      int eType = sqlite3_column_type(pStmt, iCol);
	      pSample[iCol-1].eType = eType;
	      switch( eType ){
                case SQLITE_BLOB:
                case SQLITE_TEXT: {
                  const char *z = (const char *)(
		      (eType==SQLITE_BLOB) ?
                      sqlite3_column_blob(pStmt, iCol):
                      sqlite3_column_text(pStmt, iCol)
		  );
                  int n = sqlite3_column_bytes(pStmt, iCol);
		  if( n>24 ){
		    n = 24;
		  }
		  pSample[iCol-1].nByte = n;
		  pSample[iCol-1].u.z = pSpace;
		  memcpy(pSpace, z, n);
		  pSpace += n;
		  break;
                }
                case SQLITE_INTEGER:
                case SQLITE_FLOAT:
		  pSample[iCol-1].u.r = sqlite3_column_double(pStmt, iCol);
		  break;
                case SQLITE_NULL:
		  break;
	      }
	    }
	  }
	}
	if( rc==SQLITE_NOMEM ){
	  sqlite3_finalize(pStmt);
	}else{
	  rc = sqlite3_finalize(pStmt);
	}
      }
      (void)sqlite3SafetyOn(db);
      sqlite3DbFree(db, zSql);
    }else{
      rc = SQLITE_NOMEM;
    }
  }

  if( rc==SQLITE_NOMEM ) db->mallocFailed = 1;
  return rc;
}


#endif /* SQLITE_OMIT_ANALYZE */
