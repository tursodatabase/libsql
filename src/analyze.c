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
** The ANALYZE command gather statistics about the content of tables
** and indices.  These statistics are made available to the query planner
** to help it make better decisions about how to perform queries.
**
** The following system tables are or have been supported:
**
**    CREATE TABLE sqlite_stat1(tbl, idx, stat);
**    CREATE TABLE sqlite_stat2(tbl, idx, sampleno, sample);
**    CREATE TABLE sqlite_stat3(tbl, idx, nEq, nLt, nDLt, sample);
**
** Additional tables might be added in future releases of SQLite.
** The sqlite_stat2 table is not created or used unless the SQLite version
** is between 3.6.18 and 3.7.8, inclusive, and unless SQLite is compiled
** with SQLITE_ENABLE_STAT2.  The sqlite_stat2 table is deprecated.
** The sqlite_stat2 table is superseded by sqlite_stat3, which is only
** created and used by SQLite versions 3.7.9 and later and with
** SQLITE_ENABLE_STAT3 defined.  The fucntionality of sqlite_stat3
** is a superset of sqlite_stat2.  
**
** Format of sqlite_stat1:
**
** There is normally one row per index, with the index identified by the
** name in the idx column.  The tbl column is the name of the table to
** which the index belongs.  In each such row, the stat column will be
** a string consisting of a list of integers.  The first integer in this
** list is the number of rows in the index and in the table.  The second
** integer is the average number of rows in the index that have the same
** value in the first column of the index.  The third integer is the average
** number of rows in the index that have the same value for the first two
** columns.  The N-th integer (for N>1) is the average number of rows in 
** the index which have the same value for the first N-1 columns.  For
** a K-column index, there will be K+1 integers in the stat column.  If
** the index is unique, then the last integer will be 1.
**
** The list of integers in the stat column can optionally be followed
** by the keyword "unordered".  The "unordered" keyword, if it is present,
** must be separated from the last integer by a single space.  If the
** "unordered" keyword is present, then the query planner assumes that
** the index is unordered and will not use the index for a range query.
** 
** If the sqlite_stat1.idx column is NULL, then the sqlite_stat1.stat
** column contains a single integer which is the (estimated) number of
** rows in the table identified by sqlite_stat1.tbl.
**
** Format of sqlite_stat2:
**
** The sqlite_stat2 is only created and is only used if SQLite is compiled
** with SQLITE_ENABLE_STAT2 and if the SQLite version number is between
** 3.6.18 and 3.7.8.  The "stat2" table contains additional information
** about the distribution of keys within an index.  The index is identified by
** the "idx" column and the "tbl" column is the name of the table to which
** the index belongs.  There are usually 10 rows in the sqlite_stat2
** table for each index.
**
** The sqlite_stat2 entries for an index that have sampleno between 0 and 9
** inclusive are samples of the left-most key value in the index taken at
** evenly spaced points along the index.  Let the number of samples be S
** (10 in the standard build) and let C be the number of rows in the index.
** Then the sampled rows are given by:
**
**     rownumber = (i*C*2 + C)/(S*2)
**
** For i between 0 and S-1.  Conceptually, the index space is divided into
** S uniform buckets and the samples are the middle row from each bucket.
**
** The format for sqlite_stat2 is recorded here for legacy reference.  This
** version of SQLite does not support sqlite_stat2.  It neither reads nor
** writes the sqlite_stat2 table.  This version of SQLite only supports
** sqlite_stat3.
**
** Format for sqlite_stat3:
**
** The sqlite_stat3 is an enhancement to sqlite_stat2.  A new name is
** used to avoid compatibility problems.  
**
** The format of the sqlite_stat3 table is similar to the format of
** the sqlite_stat2 table.  There are multiple entries for each index.
** The idx column names the index and the tbl column is the table of the
** index.  If the idx and tbl columns are the same, then the sample is
** of the INTEGER PRIMARY KEY.  The sample column is a value taken from
** the left-most column of the index.  The nEq column is the approximate
** number of entires in the index whose left-most column exactly matches
** the sample.  nLt is the approximate number of entires whose left-most
** column is less than the sample.  The nDLt column is the approximate
** number of distinct left-most entries in the index that are less than
** the sample.
**
** Future versions of SQLite might change to store a string containing
** multiple integers values in the nDLt column of sqlite_stat3.  The first
** integer will be the number of prior index entires that are distinct in
** the left-most column.  The second integer will be the number of prior index
** entries that are distinct in the first two columns.  The third integer
** will be the number of prior index entries that are distinct in the first
** three columns.  And so forth.  With that extension, the nDLt field is
** similar in function to the sqlite_stat1.stat field.
**
** There can be an arbitrary number of sqlite_stat3 entries per index.
** The ANALYZE command will typically generate sqlite_stat3 tables
** that contain between 10 and 40 samples which are distributed across
** the key space, though not uniformly, and which include samples with
** largest possible nEq values.
*/
#ifndef SQLITE_OMIT_ANALYZE
#include "sqliteInt.h"

/*
** This routine generates code that opens the sqlite_stat1 table for
** writing with cursor iStatCur. If the library was built with the
** SQLITE_ENABLE_STAT4 macro defined, then the sqlite_stat4 table is
** opened for writing using cursor (iStatCur+1)
**
** If the sqlite_stat1 tables does not previously exist, it is created.
** Similarly, if the sqlite_stat4 table does not exist and the library
** is compiled with SQLITE_ENABLE_STAT4 defined, it is created. 
**
** Argument zWhere may be a pointer to a buffer containing a table name,
** or it may be a NULL pointer. If it is not NULL, then all entries in
** the sqlite_stat1 and (if applicable) sqlite_stat4 tables associated
** with the named table are deleted. If zWhere==0, then code is generated
** to delete all stat table entries.
*/
static void openStatTable(
  Parse *pParse,          /* Parsing context */
  int iDb,                /* The database we are looking in */
  int iStatCur,           /* Open the sqlite_stat1 table on this cursor */
  const char *zWhere,     /* Delete entries for this table or index */
  const char *zWhereType  /* Either "tbl" or "idx" */
){
  static const struct {
    const char *zName;
    const char *zCols;
  } aTable[] = {
    { "sqlite_stat1", "tbl,idx,stat" },
#ifdef SQLITE_ENABLE_STAT4
    { "sqlite_stat4", "tbl,idx,neq,nlt,ndlt,sample" },
#endif
  };

  int aRoot[] = {0, 0};
  u8 aCreateTbl[] = {0, 0};

  int i;
  sqlite3 *db = pParse->db;
  Db *pDb;
  Vdbe *v = sqlite3GetVdbe(pParse);
  if( v==0 ) return;
  assert( sqlite3BtreeHoldsAllMutexes(db) );
  assert( sqlite3VdbeDb(v)==db );
  pDb = &db->aDb[iDb];

  /* Create new statistic tables if they do not exist, or clear them
  ** if they do already exist.
  */
  for(i=0; i<ArraySize(aTable); i++){
    const char *zTab = aTable[i].zName;
    Table *pStat;
    if( (pStat = sqlite3FindTable(db, zTab, pDb->zName))==0 ){
      /* The sqlite_stat[12] table does not exist. Create it. Note that a 
      ** side-effect of the CREATE TABLE statement is to leave the rootpage 
      ** of the new table in register pParse->regRoot. This is important 
      ** because the OpenWrite opcode below will be needing it. */
      sqlite3NestedParse(pParse,
          "CREATE TABLE %Q.%s(%s)", pDb->zName, zTab, aTable[i].zCols
      );
      aRoot[i] = pParse->regRoot;
      aCreateTbl[i] = OPFLAG_P2ISREG;
    }else{
      /* The table already exists. If zWhere is not NULL, delete all entries 
      ** associated with the table zWhere. If zWhere is NULL, delete the
      ** entire contents of the table. */
      aRoot[i] = pStat->tnum;
      sqlite3TableLock(pParse, iDb, aRoot[i], 1, zTab);
      if( zWhere ){
        sqlite3NestedParse(pParse,
           "DELETE FROM %Q.%s WHERE %s=%Q", pDb->zName, zTab, zWhereType, zWhere
        );
      }else{
        /* The sqlite_stat[12] table already exists.  Delete all rows. */
        sqlite3VdbeAddOp2(v, OP_Clear, aRoot[i], iDb);
      }
    }
  }

  /* Open the sqlite_stat[14] tables for writing. */
  for(i=0; i<ArraySize(aTable); i++){
    sqlite3VdbeAddOp3(v, OP_OpenWrite, iStatCur+i, aRoot[i], iDb);
    sqlite3VdbeChangeP4(v, -1, (char *)3, P4_INT32);
    sqlite3VdbeChangeP5(v, aCreateTbl[i]);
  }
}

/*
** Recommended number of samples for sqlite_stat4
*/
#ifndef SQLITE_STAT4_SAMPLES
# define SQLITE_STAT4_SAMPLES 24
#endif

/*
** Three SQL functions - stat4_init(), stat4_push(), and stat4_pop() -
** share an instance of the following structure to hold their state
** information.
*/
typedef struct Stat4Accum Stat4Accum;
struct Stat4Accum {
  tRowcnt nRow;             /* Number of rows in the entire table */
  tRowcnt nPSample;         /* How often to do a periodic sample */
  int iMin;                 /* Index of entry with minimum nSumEq and hash */
  int mxSample;             /* Maximum number of samples to accumulate */
  int nSample;              /* Current number of samples */
  int nCol;                 /* Number of columns in the index */
  u32 iPrn;                 /* Pseudo-random number used for sampling */
  struct Stat4Sample {
    i64 iRowid;                /* Rowid in main table of the key */
    tRowcnt nSumEq;            /* Sum of anEq[] values */
    tRowcnt *anEq;             /* sqlite_stat4.nEq */
    tRowcnt *anLt;             /* sqlite_stat4.nLt */
    tRowcnt *anDLt;            /* sqlite_stat4.nDLt */
    u8 isPSample;              /* True if a periodic sample */
    u32 iHash;                 /* Tiebreaker hash */
  } *a;                     /* An array of samples */
};

#ifdef SQLITE_ENABLE_STAT4
/*
** Implementation of the stat4_init(C,N,S) SQL function. The three parameters
** are the number of rows in the table or index (C), the number of columns
** in the index (N) and the number of samples to accumulate (S).
**
** This routine allocates the Stat4Accum object in heap memory. The return 
** value is a pointer to the the Stat4Accum object encoded as a blob (i.e. 
** the size of the blob is sizeof(void*) bytes). 
*/
static void stat4Init(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Stat4Accum *p;
  u8 *pSpace;                     /* Allocated space not yet assigned */
  tRowcnt nRow;                   /* Number of rows in table (C) */
  int mxSample;                   /* Maximum number of samples collected */
  int nCol;                       /* Number of columns in index being sampled */
  int n;                          /* Bytes of space to allocate */
  int i;                          /* Used to iterate through p->aSample[] */

  /* Decode the three function arguments */
  UNUSED_PARAMETER(argc);
  nRow = (tRowcnt)sqlite3_value_int64(argv[0]);
  nCol = sqlite3_value_int(argv[1]);
  mxSample = sqlite3_value_int(argv[2]);
  assert( nCol>0 );

  /* Allocate the space required for the Stat4Accum object */
  n = sizeof(*p) + (sizeof(p->a[0]) + 3*sizeof(tRowcnt)*nCol)*mxSample;
  p = sqlite3MallocZero( n );
  if( p==0 ){
    sqlite3_result_error_nomem(context);
    return;
  }

  /* Populate the new Stat4Accum object */
  p->nRow = nRow;
  p->nCol = nCol;
  p->mxSample = mxSample;
  p->nPSample = p->nRow/(mxSample/3+1) + 1;
  sqlite3_randomness(sizeof(p->iPrn), &p->iPrn);
  p->a = (struct Stat4Sample*)&p[1];
  pSpace = (u8*)(&p->a[mxSample]);
  for(i=0; i<mxSample; i++){
    p->a[i].anEq = (tRowcnt *)pSpace; pSpace += (sizeof(tRowcnt) * nCol);
    p->a[i].anLt = (tRowcnt *)pSpace; pSpace += (sizeof(tRowcnt) * nCol);
    p->a[i].anDLt = (tRowcnt *)pSpace; pSpace += (sizeof(tRowcnt) * nCol);
  }
  assert( (pSpace - (u8*)p)==n );

  /* Return a pointer to the allocated object to the caller */
  sqlite3_result_blob(context, p, sizeof(p), sqlite3_free);
}
static const FuncDef stat4InitFuncdef = {
  3,                /* nArg */
  SQLITE_UTF8,      /* iPrefEnc */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat4Init,        /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat4_init",     /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};


/*
** Implementation of the stat4_push SQL function. The arguments describe a
** single key instance. This routine makes the decision about whether or 
** not to retain this key for the sqlite_stat4 table.
** 
** The calling convention is:
**
**     stat4_push(P, rowid, ...nEq args..., ...nLt args..., ...nDLt args...)
**
** where each instance of the "...nXX args..." is replaced by an array of
** nCol arguments, where nCol is the number of columns in the index being
** sampled (if the index being sampled is "CREATE INDEX i ON t(a, b)", a 
** total of 8 arguments are passed when this function is invoked).
**
** The return value is always NULL.
*/
static void stat4Push(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Stat4Accum *p = (Stat4Accum*)sqlite3_value_blob(argv[0]);
  i64 rowid = sqlite3_value_int64(argv[1]);
  i64 nSumEq = 0;                 /* Sum of all nEq parameters */
  struct Stat4Sample *pSample;
  u32 h;
  int iMin = p->iMin;
  int i;
  u8 isPSample = 0;
  u8 doInsert = 0;

  sqlite3_value **aEq = &argv[2];
  sqlite3_value **aLt = &argv[2+p->nCol];
  sqlite3_value **aDLt = &argv[2+p->nCol+p->nCol];

  i64 nEq = sqlite3_value_int64(aEq[p->nCol-1]);
  i64 nLt = sqlite3_value_int64(aLt[p->nCol-1]);

  UNUSED_PARAMETER(context);
  UNUSED_PARAMETER(argc);

  assert( p->nCol>0 );
  assert( argc==(2 + 3*p->nCol) );

  /* Set nSumEq to the sum of all nEq parameters. */
  for(i=0; i<p->nCol; i++){
    nSumEq += sqlite3_value_int64(aEq[i]);
  }
  if( nSumEq==0 ) return;

  /* Figure out if this sample will be used. Set isPSample to true if this
  ** is a periodic sample, or false if it is being captured because of a
  ** large nSumEq value. If the sample will not be used, return early.  */
  h = p->iPrn = p->iPrn*1103515245 + 12345;
  if( (nLt/p->nPSample)!=((nEq+nLt)/p->nPSample) ){
    doInsert = isPSample = 1;
  }else if( (p->nSample<p->mxSample)
         || (nSumEq>p->a[iMin].nSumEq)
         || (nSumEq==p->a[iMin].nSumEq && h>p->a[iMin].iHash) 
  ){
    doInsert = 1;
  }
  if( !doInsert ) return;

  /* Fill in the new Stat4Sample object. */
  if( p->nSample==p->mxSample ){
    struct Stat4Sample *pMin = &p->a[iMin];
    tRowcnt *anEq = pMin->anEq;
    tRowcnt *anDLt = pMin->anDLt;
    tRowcnt *anLt = pMin->anLt;
    assert( p->nSample - iMin - 1 >= 0 );
    memmove(pMin, &pMin[1], sizeof(p->a[0])*(p->nSample-iMin-1));
    pSample = &p->a[p->nSample-1];
    pSample->anEq = anEq;
    pSample->anDLt = anDLt;
    pSample->anLt = anLt;
  }else{
    pSample = &p->a[p->nSample++];
  }
  pSample->iRowid = rowid;
  pSample->iHash = h;
  pSample->isPSample = isPSample;
  pSample->nSumEq = nSumEq;
  for(i=0; i<p->nCol; i++){
    pSample->anEq[i] = sqlite3_value_int64(aEq[i]);
    pSample->anLt[i] = sqlite3_value_int64(aLt[i]);
    pSample->anDLt[i] = sqlite3_value_int64(aDLt[i])-1;
    assert( sqlite3_value_int64(aDLt[i])>0 );
  } 

  /* Find the new minimum */
  if( p->nSample==p->mxSample ){
    u32 iHash = 0;                /* Hash corresponding to iMin/nSumEq entry */
    i64 nMinEq = LARGEST_INT64;   /* Smallest nSumEq seen so far */
    assert( iMin = -1 );

    for(i=0; i<p->mxSample; i++){
      if( p->a[i].isPSample ) continue;
      if( (p->a[i].nSumEq<nMinEq)
       || (p->a[i].nSumEq==nMinEq && p->a[i].iHash<iHash)
      ){
        iMin = i;
        nMinEq = p->a[i].nSumEq;
        iHash = p->a[i].iHash;
      }
    }
    assert( iMin>=0 );
    p->iMin = iMin;
  }
}
static const FuncDef stat4PushFuncdef = {
  -1,               /* nArg */
  SQLITE_UTF8,      /* iPrefEnc */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat4Push,        /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat4_push",     /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};

/*
** Implementation of the stat3_get(P,N,...) SQL function.  This routine is
** used to query the results.  Content is returned for the Nth sqlite_stat3
** row where N is between 0 and S-1 and S is the number of samples.  The
** value returned depends on the number of arguments.
**
**   argc==2    result:  rowid
**   argc==3    result:  nEq
**   argc==4    result:  nLt
**   argc==5    result:  nDLt
*/
static void stat4Get(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Stat4Accum *p = (Stat4Accum*)sqlite3_value_blob(argv[0]);
  int n = sqlite3_value_int(argv[1]);

  assert( p!=0 );
  if( n<p->nSample ){
    tRowcnt *aCnt = 0;
    char *zRet;
 
    switch( argc ){
      case 2:  
        sqlite3_result_int64(context, p->a[n].iRowid);
        return;
      case 3:  aCnt = p->a[n].anEq; break;
      case 4:  aCnt = p->a[n].anLt; break;
      default: aCnt = p->a[n].anDLt; break;
    }

    zRet = sqlite3MallocZero(p->nCol * 25);
    if( zRet==0 ){
      sqlite3_result_error_nomem(context);
    }else{
      int i;
      char *z = zRet;
      for(i=0; i<p->nCol; i++){
        sqlite3_snprintf(24, z, "%lld ", aCnt[i]);
        z += sqlite3Strlen30(z);
      }
      assert( z[0]=='\0' && z>zRet );
      z[-1] = '\0';
      sqlite3_result_text(context, zRet, -1, sqlite3_free);
    }
  }
}
static const FuncDef stat4GetFuncdef = {
  -1,               /* nArg */
  SQLITE_UTF8,      /* iPrefEnc */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat4Get,         /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat4_get",     /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};
#endif /* SQLITE_ENABLE_STAT4 */




/*
** Generate code to do an analysis of all indices associated with
** a single table.
*/
static void analyzeOneTable(
  Parse *pParse,   /* Parser context */
  Table *pTab,     /* Table whose indices are to be analyzed */
  Index *pOnlyIdx, /* If not NULL, only analyze this one index */
  int iStatCur,    /* Index of VdbeCursor that writes the sqlite_stat1 table */
  int iMem         /* Available memory locations begin here */
){
  sqlite3 *db = pParse->db;    /* Database handle */
  Index *pIdx;                 /* An index to being analyzed */
  int iIdxCur;                 /* Cursor open on index being analyzed */
  Vdbe *v;                     /* The virtual machine being built up */
  int i;                       /* Loop counter */
  int topOfLoop;               /* The top of the loop */
  int endOfLoop;               /* The end of the loop */
  int jZeroRows = -1;          /* Jump from here if number of rows is zero */
  int iDb;                     /* Index of database containing pTab */
  u8 needTableCnt = 1;         /* True to count the table */
  int regTabname = iMem++;     /* Register containing table name */
  int regIdxname = iMem++;     /* Register containing index name */
  int regStat1 = iMem++;       /* The stat column of sqlite_stat1 */
#ifdef SQLITE_ENABLE_STAT4
  int regNumEq = regStat1;     /* Number of instances.  Same as regStat1 */
  int regNumLt = iMem++;       /* Number of keys less than regSample */
  int regNumDLt = iMem++;      /* Number of distinct keys less than regSample */
  int regSample = iMem++;      /* The next sample value */
  int regRowid = regSample;    /* Rowid of a sample */
  int regAccum = iMem++;       /* Register to hold Stat4Accum object */
  int regLoop = iMem++;        /* Loop counter */
  int regCount = iMem++;       /* Number of rows in the table or index */
  int regTemp1 = iMem++;       /* Intermediate register */
  int regTemp2 = iMem++;       /* Intermediate register */
  int once = 1;                /* One-time initialization */
  int shortJump = 0;           /* Instruction address */
  int iTabCur = pParse->nTab++; /* Table cursor */
#endif
  int regCol = iMem++;         /* Content of a column in analyzed table */
  int regRec = iMem++;         /* Register holding completed record */
  int regTemp = iMem++;        /* Temporary use register */
  int regNewRowid = iMem++;    /* Rowid for the inserted record */

  int regStat4 = iMem++;       /* Register to hold Stat4Accum object */

  v = sqlite3GetVdbe(pParse);
  if( v==0 || NEVER(pTab==0) ){
    return;
  }
  if( pTab->tnum==0 ){
    /* Do not gather statistics on views or virtual tables */
    return;
  }
  if( sqlite3_strnicmp(pTab->zName, "sqlite_", 7)==0 ){
    /* Do not gather statistics on system tables */
    return;
  }
  assert( sqlite3BtreeHoldsAllMutexes(db) );
  iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
  assert( iDb>=0 );
  assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( sqlite3AuthCheck(pParse, SQLITE_ANALYZE, pTab->zName, 0,
      db->aDb[iDb].zName ) ){
    return;
  }
#endif

  /* Establish a read-lock on the table at the shared-cache level. 
  ** Also open a read-only cursor on the table.  */
  sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);
  iTabCur = pParse->nTab++;
  sqlite3OpenTable(pParse, iTabCur, iDb, pTab, OP_OpenRead);
  sqlite3VdbeAddOp4(v, OP_String8, 0, regTabname, 0, pTab->zName, 0);

  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    int nCol;                     /* Number of columns indexed by pIdx */
    KeyInfo *pKey;                /* KeyInfo structure for pIdx */
    int addrIfNot = 0;            /* address of OP_IfNot */
    int *aChngAddr;               /* Array of jump instruction addresses */

    int regRowid;                 /* Register for rowid of current row */
    int regPrev;                  /* First in array of previous values */
    int regDLte;                  /* First in array of nDlt registers */
    int regLt;                    /* First in array of nLt registers */
    int regEq;                    /* First in array of nEq registers */
    int regCnt;                   /* Number of index entries */
    int regEof;                   /* True once cursors are all at EOF */
    int endOfScan;                /* Label to jump to once scan is finished */

    if( pOnlyIdx && pOnlyIdx!=pIdx ) continue;
    if( pIdx->pPartIdxWhere==0 ) needTableCnt = 0;
    VdbeNoopComment((v, "Begin analysis of %s", pIdx->zName));
    nCol = pIdx->nColumn;
    aChngAddr = sqlite3DbMallocRaw(db, sizeof(int)*nCol);
    if( aChngAddr==0 ) continue;
    pKey = sqlite3IndexKeyinfo(pParse, pIdx);

    /* Populate the register containing the index name. */
    sqlite3VdbeAddOp4(v, OP_String8, 0, regIdxname, 0, pIdx->zName, 0);

    /*
    ** The following pseudo-code demonstrates the way the VM scans an index 
    ** to call stat4_push() and collect the values for the sqlite_stat1 
    ** entry. The code below is for an index with 2 columns. The actual
    ** VM code generated may be for any number of columns.
    **
    ** One cursor is opened for each column in the index (nCol). All cursors 
    ** scan concurrently the index from start to end. All variables used in
    ** the pseudo-code are initialized to zero.
    **
    **   Rewind csr(0)
    **   Rewind csr(1)
    ** 
    **  next_0:
    **   regPrev(0) = csr(0)[0]
    **   regDLte(0) += 1
    **   regLt(0) += regEq(0)
    **   regEq(0) = 0
    **   do {
    **     regEq(0) += 1
    **     Next csr(0)
    **   }while ( csr(0)[0] == regPrev(0) )
    ** 
    **  next_1:
    **   regPrev(1) = csr(1)[1]
    **   regDLte(1) += 1
    **   regLt(1) += regEq(1)
    **   regEq(1) = 0
    **   regRowid = csr(1)[rowid]        // innermost cursor only
    **   do {
    **     regEq(1) += 1
    **     regCnt += 1                   // innermost cursor only
    **     Next csr(1)
    **   }while ( csr(1)[0..1] == regPrev(0..1) )
    ** 
    **   stat4_push(regRowid, regEq, regLt, regDLte);
    ** 
    **   if( eof( csr(1) ) ) goto endOfScan
    **   if( csr(1)[0] != regPrev(0) ) goto next_0
    **   goto next_1
    **
    **  endOfScan:
    **   // done!
    **
    ** The last two lines above modify the contents of the regDLte array
    ** so that each element contains the number of distinct key prefixes
    ** of the corresponding length. As required to calculate the contents
    ** of the sqlite_stat1 entry.
    **
    ** Currently, the last memory cell allocated (that with the largest 
    ** integer identifier) is regStat4. Immediately following regStat4
    ** we allocate the following:
    **
    **     regRowid -    1 register
    **     regEq -    nCol registers
    **     regLt -    nCol registers
    **     regDLte -  nCol registers
    **     regCnt -      1 register
    **     regPrev -  nCol registers
    **     regEof -      1 register
    **
    ** The regRowid, regEq, regLt and regDLte registers must be positioned in 
    ** that order immediately following regStat4 so that they can be passed
    ** to the stat4_push() function.
    **
    ** All of the above are initialized to contain integer value 0.
    */
    regRowid = regStat4+1;        /* Rowid argument */
    regEq = regRowid+1;           /* First in array of nEq value registers */
    regLt = regEq+nCol;           /* First in array of nLt value registers */
    regDLte = regLt+nCol;         /* First in array of nDLt value registers */
    regCnt = regDLte+nCol;        /* Row counter */
    regPrev = regCnt+1;           /* First in array of prev. value registers */
    regEof = regPrev+nCol;        /* True once last row read from index */
    if( regEof+1>pParse->nMem ){
      pParse->nMem = regPrev+nCol;
    }

    /* Open a read-only cursor for each column of the index. */
    assert( iDb==sqlite3SchemaToIndex(db, pIdx->pSchema) );
    iIdxCur = pParse->nTab++;
    pParse->nTab += (nCol-1);
    for(i=0; i<nCol; i++){
      int iMode = (i==0 ? P4_KEYINFO_HANDOFF : P4_KEYINFO);
      sqlite3VdbeAddOp3(v, OP_OpenRead, iIdxCur+i, pIdx->tnum, iDb);
      sqlite3VdbeChangeP4(v, -1, (char*)pKey, iMode); 
      VdbeComment((v, "%s", pIdx->zName));
    }

#ifdef SQLITE_ENABLE_STAT4
    /* Invoke the stat4_init() function. The arguments are:
    ** 
    **     * the number of rows in the index,
    **     * the number of columns in the index,
    **     * the recommended number of samples for the stat4 table.
    */
    sqlite3VdbeAddOp2(v, OP_Count, iIdxCur, regStat4+1);
    sqlite3VdbeAddOp2(v, OP_Integer, nCol, regStat4+2);
    sqlite3VdbeAddOp2(v, OP_Integer, SQLITE_STAT4_SAMPLES, regStat4+3);
    sqlite3VdbeAddOp3(v, OP_Function, 0, regStat4+1, regStat4);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4InitFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 3);
#endif /* SQLITE_ENABLE_STAT4 */

    /* Initialize all the memory registers allocated above to 0. */
    for(i=regRowid; i<=regEof; i++){
      sqlite3VdbeAddOp2(v, OP_Integer, 0, i);
    }

    /* Rewind all cursors open on the index. If the table is entry, this
    ** will cause control to jump to address endOfScan immediately.  */
    endOfScan = sqlite3VdbeMakeLabel(v);
    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp2(v, OP_Rewind, iIdxCur+i, endOfScan);
    }

    for(i=0; i<nCol; i++){
      char *pColl = (char*)sqlite3LocateCollSeq(pParse, pIdx->azColl[i]);
      int iCsr = iIdxCur+i;
      int iDo;
      int iNe;                    /* Jump here to exit do{...}while loop */
      int j;
      int bInner = (i==(nCol-1)); /* True for innermost cursor */

      /* Implementation of the following pseudo-code:
      **
      **   regPrev(i) = csr(i)[i]
      **   regDLte(i) += 1
      **   regLt(i) += regEq(i)
      **   regEq(i) = 0
      **   regRowid = csr(i)[rowid]        // innermost cursor only
      */
      aChngAddr[i] = sqlite3VdbeAddOp3(v, OP_Column, iCsr, i, regPrev+i);
      VdbeComment((v, "regPrev(%d) = csr(%d)(%d)", i, i, i));
      sqlite3VdbeAddOp2(v, OP_AddImm, regDLte+i, 1);
      VdbeComment((v, "regDLte(%d) += 1", i));
      sqlite3VdbeAddOp3(v, OP_Add, regEq+i, regLt+i, regLt+i);
      VdbeComment((v, "regLt(%d) += regEq(%d)", i, i));
      sqlite3VdbeAddOp2(v, OP_Integer, 0, regEq+i);
      VdbeComment((v, "regEq(%d) = 0", i));
      if( bInner ) sqlite3VdbeAddOp2(v, OP_IdxRowid, iCsr, regRowid);

      /* This bit:
      **
      **   do {
      **     regEq(i) += 1
      **     regCnt += 1                   // innermost cursor only
      **     Next csr(i)
      **     if( Eof csr(i) ){
      **       regEof = 1                  // innermost cursor only
      **       break
      **     }
      **   }while ( csr(i)[0..i] == regPrev(0..i) )
      */
      iDo = sqlite3VdbeAddOp2(v, OP_AddImm, regEq+i, 1);
      VdbeComment((v, "regEq(%d) += 1", i));
      if( bInner ){
        sqlite3VdbeAddOp2(v, OP_AddImm, regCnt, 1);
        VdbeComment((v, "regCnt += 1"));
      }
      sqlite3VdbeAddOp2(v, OP_Next, iCsr, sqlite3VdbeCurrentAddr(v)+2+bInner);
      if( bInner ) sqlite3VdbeAddOp2(v, OP_Integer, 1, regEof);
      iNe = sqlite3VdbeMakeLabel(v);
      sqlite3VdbeAddOp2(v, OP_Goto, 0, iNe);
      for(j=0; j<=i; j++){
        sqlite3VdbeAddOp3(v, OP_Column, iCsr, j, regCol);
        sqlite3VdbeAddOp4(v, OP_Ne, regCol, iNe, regPrev+j, pColl, P4_COLLSEQ);
        sqlite3VdbeChangeP5(v, SQLITE_NULLEQ);
        VdbeComment((v, "if( regPrev(%d) != csr(%d)(%d) )", j, i, j));
      }
      sqlite3VdbeAddOp2(v, OP_Goto, 0, iDo);
      sqlite3VdbeResolveLabel(v, iNe);
    }

    /* Invoke stat4_push() */
    sqlite3VdbeAddOp3(v, OP_Function, 1, regStat4, regTemp2);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4PushFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 2 + 3*nCol);

    sqlite3VdbeAddOp2(v, OP_If, regEof, endOfScan);
    for(i=0; i<nCol-1; i++){
      char *pColl = (char*)sqlite3LocateCollSeq(pParse, pIdx->azColl[i]);
      sqlite3VdbeAddOp3(v, OP_Column, iIdxCur+nCol-1, i, regCol);
      sqlite3VdbeAddOp3(v, OP_Ne, regCol, aChngAddr[i], regPrev+i);
      sqlite3VdbeChangeP4(v, -1, pColl, P4_COLLSEQ);
      sqlite3VdbeChangeP5(v, SQLITE_NULLEQ);
    }
    sqlite3VdbeAddOp2(v, OP_Goto, 0, aChngAddr[nCol-1]);
    sqlite3DbFree(db, aChngAddr);

    sqlite3VdbeResolveLabel(v, endOfScan);

    /* Close all the cursors */
    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp1(v, OP_Close, iIdxCur+i);
      VdbeComment((v, "close index cursor %d", i));
    }

#ifdef SQLITE_ENABLE_STAT4
    /* Add rows to the sqlite_stat4 table */
    regLoop = regStat4+1;
    sqlite3VdbeAddOp2(v, OP_Integer, -1, regLoop);
    shortJump = sqlite3VdbeAddOp2(v, OP_AddImm, regLoop, 1);
    sqlite3VdbeAddOp3(v, OP_Function, 0, regStat4, regTemp1);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4GetFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 2);
    sqlite3VdbeAddOp1(v, OP_IsNull, regTemp1);

    sqlite3VdbeAddOp3(v, OP_NotExists, iTabCur, shortJump, regTemp1);
    for(i=0; i<nCol; i++){
      int iCol = pIdx->aiColumn[i];
      sqlite3ExprCodeGetColumnOfTable(v, pTab, iTabCur, iCol, regPrev+i);
    }
    sqlite3VdbeAddOp3(v, OP_MakeRecord, regPrev, nCol, regSample);
    sqlite3VdbeChangeP4(v, -1, pIdx->zColAff, 0);

    sqlite3VdbeAddOp3(v, OP_Function, 1, regStat4, regNumEq);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4GetFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 3);

    sqlite3VdbeAddOp3(v, OP_Function, 1, regStat4, regNumLt);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4GetFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 4);

    sqlite3VdbeAddOp3(v, OP_Function, 1, regStat4, regNumDLt);
    sqlite3VdbeChangeP4(v, -1, (char*)&stat4GetFuncdef, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, 5);

    sqlite3VdbeAddOp4(v, OP_MakeRecord, regTabname, 6, regRec, "bbbbbb", 0);
    sqlite3VdbeAddOp2(v, OP_NewRowid, iStatCur+1, regNewRowid);
    sqlite3VdbeAddOp3(v, OP_Insert, iStatCur+1, regRec, regNewRowid);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, shortJump);
    sqlite3VdbeJumpHere(v, shortJump+2);
#endif        

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
    sqlite3VdbeAddOp2(v, OP_SCopy, regCnt, regStat1);
    jZeroRows = sqlite3VdbeAddOp1(v, OP_IfNot, regCnt);
    for(i=0; i<nCol; i++){
      sqlite3VdbeAddOp4(v, OP_String8, 0, regTemp, 0, " ", 0);
      sqlite3VdbeAddOp3(v, OP_Concat, regTemp, regStat1, regStat1);
      sqlite3VdbeAddOp3(v, OP_Add, regCnt, regDLte+i, regTemp);
      sqlite3VdbeAddOp2(v, OP_AddImm, regTemp, -1);
      sqlite3VdbeAddOp3(v, OP_Divide, regDLte+i, regTemp, regTemp);
      sqlite3VdbeAddOp1(v, OP_ToInt, regTemp);
      sqlite3VdbeAddOp3(v, OP_Concat, regTemp, regStat1, regStat1);
    }
    if( pIdx->pPartIdxWhere!=0 ) sqlite3VdbeJumpHere(v, jZeroRows);
    sqlite3VdbeAddOp4(v, OP_MakeRecord, regTabname, 3, regRec, "aaa", 0);
    sqlite3VdbeAddOp2(v, OP_NewRowid, iStatCur, regNewRowid);
    sqlite3VdbeAddOp3(v, OP_Insert, iStatCur, regRec, regNewRowid);
    sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
    if( pIdx->pPartIdxWhere==0 ) sqlite3VdbeJumpHere(v, jZeroRows);
  }

  /* Create a single sqlite_stat1 entry containing NULL as the index
  ** name and the row count as the content.
  */
  if( pOnlyIdx==0 && needTableCnt ){
    VdbeComment((v, "%s", pTab->zName));
    sqlite3VdbeAddOp2(v, OP_Count, iTabCur, regStat1);
    jZeroRows = sqlite3VdbeAddOp1(v, OP_IfNot, regStat1);
    sqlite3VdbeAddOp2(v, OP_Null, 0, regIdxname);
    sqlite3VdbeAddOp4(v, OP_MakeRecord, regTabname, 3, regRec, "aaa", 0);
    sqlite3VdbeAddOp2(v, OP_NewRowid, iStatCur, regNewRowid);
    sqlite3VdbeAddOp3(v, OP_Insert, iStatCur, regRec, regNewRowid);
    sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
    sqlite3VdbeJumpHere(v, jZeroRows);
  }

  sqlite3VdbeAddOp1(v, OP_Close, iTabCur);

  /* TODO: Not sure about this... */
  if( pParse->nMem<regRec ) pParse->nMem = regRec;
}


/*
** Generate code that will cause the most recent index analysis to
** be loaded into internal hash tables where is can be used.
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
  pParse->nTab += 3;
  openStatTable(pParse, iDb, iStatCur, 0, 0);
  iMem = pParse->nMem+1;
  assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    analyzeOneTable(pParse, pTab, 0, iStatCur, iMem);
  }
  loadAnalysis(pParse, iDb);
}

/*
** Generate code that will do an analysis of a single table in
** a database.  If pOnlyIdx is not NULL then it is a single index
** in pTab that should be analyzed.
*/
static void analyzeTable(Parse *pParse, Table *pTab, Index *pOnlyIdx){
  int iDb;
  int iStatCur;

  assert( pTab!=0 );
  assert( sqlite3BtreeHoldsAllMutexes(pParse->db) );
  iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
  sqlite3BeginWriteOperation(pParse, 0, iDb);
  iStatCur = pParse->nTab;
  pParse->nTab += 3;
  if( pOnlyIdx ){
    openStatTable(pParse, iDb, iStatCur, pOnlyIdx->zName, "idx");
  }else{
    openStatTable(pParse, iDb, iStatCur, pTab->zName, "tbl");
  }
  analyzeOneTable(pParse, pTab, pOnlyIdx, iStatCur, pParse->nMem+1);
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
  Index *pIdx;
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
        if( (pIdx = sqlite3FindIndex(db, z, 0))!=0 ){
          analyzeTable(pParse, pIdx->pTable, pIdx);
        }else if( (pTab = sqlite3LocateTable(pParse, 0, z, 0))!=0 ){
          analyzeTable(pParse, pTab, 0);
        }
        sqlite3DbFree(db, z);
      }
    }
  }else{
    /* Form 3: Analyze the fully qualified table name */
    iDb = sqlite3TwoPartName(pParse, pName1, pName2, &pTableName);
    if( iDb>=0 ){
      zDb = db->aDb[iDb].zName;
      z = sqlite3NameFromToken(db, pTableName);
      if( z ){
        if( (pIdx = sqlite3FindIndex(db, z, zDb))!=0 ){
          analyzeTable(pParse, pIdx->pTable, pIdx);
        }else if( (pTab = sqlite3LocateTable(pParse, 0, z, zDb))!=0 ){
          analyzeTable(pParse, pTab, 0);
        }
        sqlite3DbFree(db, z);
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
** The first argument points to a nul-terminated string containing a
** list of space separated integers. Read the first nOut of these into
** the array aOut[].
*/
static void decodeIntArray(
  char *zIntArray, 
  int nOut, 
  tRowcnt *aOut, 
  int *pbUnordered
){
  char *z = zIntArray;
  int c;
  int i;
  tRowcnt v;

  assert( pbUnordered==0 || *pbUnordered==0 );

  for(i=0; *z && i<nOut; i++){
    v = 0;
    while( (c=z[0])>='0' && c<='9' ){
      v = v*10 + c - '0';
      z++;
    }
    aOut[i] = v;
    if( *z==' ' ) z++;
  }
  if( pbUnordered && strcmp(z, "unordered")==0 ){
    *pbUnordered = 1;
  }
}

/*
** This callback is invoked once for each index when reading the
** sqlite_stat1 table.  
**
**     argv[0] = name of the table
**     argv[1] = name of the index (might be NULL)
**     argv[2] = results of analysis - on integer for each column
**
** Entries for which argv[1]==NULL simply record the number of rows in
** the table.
*/
static int analysisLoader(void *pData, int argc, char **argv, char **NotUsed){
  analysisInfo *pInfo = (analysisInfo*)pData;
  Index *pIndex;
  Table *pTable;
  const char *z;

  assert( argc==3 );
  UNUSED_PARAMETER2(NotUsed, argc);

  if( argv==0 || argv[0]==0 || argv[2]==0 ){
    return 0;
  }
  pTable = sqlite3FindTable(pInfo->db, argv[0], pInfo->zDatabase);
  if( pTable==0 ){
    return 0;
  }
  if( argv[1] ){
    pIndex = sqlite3FindIndex(pInfo->db, argv[1], pInfo->zDatabase);
  }else{
    pIndex = 0;
  }
  z = argv[2];

  if( pIndex ){
    int bUnordered = 0;
    decodeIntArray((char*)z, pIndex->nColumn+1, pIndex->aiRowEst, &bUnordered);
    if( pIndex->pPartIdxWhere==0 ) pTable->nRowEst = pIndex->aiRowEst[0];
    pIndex->bUnordered = bUnordered;
  }else{
    decodeIntArray((char*)z, 1, &pTable->nRowEst, 0);
  }

  return 0;
}

/*
** If the Index.aSample variable is not NULL, delete the aSample[] array
** and its contents.
*/
void sqlite3DeleteIndexSamples(sqlite3 *db, Index *pIdx){
#ifdef SQLITE_ENABLE_STAT4
  if( pIdx->aSample ){
    int j;
    for(j=0; j<pIdx->nSample; j++){
      IndexSample *p = &pIdx->aSample[j];
      sqlite3DbFree(db, p->p);
    }
    sqlite3DbFree(db, pIdx->aSample);
  }
  if( db && db->pnBytesFreed==0 ){
    pIdx->nSample = 0;
    pIdx->aSample = 0;
  }
#else
  UNUSED_PARAMETER(db);
  UNUSED_PARAMETER(pIdx);
#endif
}

#ifdef SQLITE_ENABLE_STAT4
/*
** Load content from the sqlite_stat4 table into the Index.aSample[]
** arrays of all indices.
*/
static int loadStat4(sqlite3 *db, const char *zDb){
  int rc;                       /* Result codes from subroutines */
  sqlite3_stmt *pStmt = 0;      /* An SQL statement being run */
  char *zSql;                   /* Text of the SQL statement */
  Index *pPrevIdx = 0;          /* Previous index in the loop */
  int idx = 0;                  /* slot in pIdx->aSample[] for next sample */
  IndexSample *pSample;         /* A slot in pIdx->aSample[] */

  assert( db->lookaside.bEnabled==0 );
  if( !sqlite3FindTable(db, "sqlite_stat4", zDb) ){
    return SQLITE_OK;
  }

  zSql = sqlite3MPrintf(db, 
      "SELECT idx,count(*) FROM %Q.sqlite_stat4"
      " GROUP BY idx", zDb);
  if( !zSql ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  sqlite3DbFree(db, zSql);
  if( rc ) return rc;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    char *zIndex;   /* Index name */
    Index *pIdx;    /* Pointer to the index object */
    int nSample;    /* Number of samples */
    int nByte;      /* Bytes of space required */
    int i;          /* Bytes of space required */
    tRowcnt *pSpace;

    zIndex = (char *)sqlite3_column_text(pStmt, 0);
    if( zIndex==0 ) continue;
    nSample = sqlite3_column_int(pStmt, 1);
    pIdx = sqlite3FindIndex(db, zIndex, zDb);
    if( pIdx==0 ) continue;
    assert( pIdx->nSample==0 );
    pIdx->nSample = nSample;
    nByte = sizeof(IndexSample) * nSample;
    nByte += sizeof(tRowcnt) * pIdx->nColumn * 3 * nSample;

    pIdx->aSample = sqlite3DbMallocZero(db, nByte);
    pIdx->avgEq = pIdx->aiRowEst[1];
    if( pIdx->aSample==0 ){
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    pSpace = (tRowcnt*)&pIdx->aSample[nSample];
    for(i=0; i<pIdx->nSample; i++){
      pIdx->aSample[i].anEq = pSpace; pSpace += pIdx->nColumn;
      pIdx->aSample[i].anLt = pSpace; pSpace += pIdx->nColumn;
      pIdx->aSample[i].anDLt = pSpace; pSpace += pIdx->nColumn;
    }
    assert( ((u8*)pSpace)-nByte==(u8*)(pIdx->aSample) );
  }
  rc = sqlite3_finalize(pStmt);
  if( rc ) return rc;

  zSql = sqlite3MPrintf(db, 
      "SELECT idx,neq,nlt,ndlt,sample FROM %Q.sqlite_stat4", zDb);
  if( !zSql ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  sqlite3DbFree(db, zSql);
  if( rc ) return rc;

  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    char *zIndex;   /* Index name */
    Index *pIdx;    /* Pointer to the index object */
    int i;          /* Loop counter */
    tRowcnt sumEq;  /* Sum of the nEq values */
    int nCol;       /* Number of columns in index */

    zIndex = (char *)sqlite3_column_text(pStmt, 0);
    if( zIndex==0 ) continue;
    pIdx = sqlite3FindIndex(db, zIndex, zDb);
    if( pIdx==0 ) continue;
    if( pIdx==pPrevIdx ){
      idx++;
    }else{
      pPrevIdx = pIdx;
      idx = 0;
    }
    assert( idx<pIdx->nSample );
    pSample = &pIdx->aSample[idx];

    nCol = pIdx->nColumn;
    decodeIntArray((char*)sqlite3_column_text(pStmt,1), nCol, pSample->anEq, 0);
    decodeIntArray((char*)sqlite3_column_text(pStmt,2), nCol, pSample->anLt, 0);
    decodeIntArray((char*)sqlite3_column_text(pStmt,3), nCol, pSample->anDLt,0);

    if( idx==pIdx->nSample-1 ){
      if( pSample->anDLt[0]>0 ){
        for(i=0, sumEq=0; i<=idx-1; i++) sumEq += pIdx->aSample[i].anEq[0];
        pIdx->avgEq = (pSample->anLt[0] - sumEq)/pSample->anDLt[0];
      }
      if( pIdx->avgEq<=0 ) pIdx->avgEq = 1;
    }

    pSample->n = sqlite3_column_bytes(pStmt, 4);
    pSample->p = sqlite3DbMallocZero(db, pSample->n);
    if( pSample->p==0 ){
      sqlite3_finalize(pStmt);
      return SQLITE_NOMEM;
    }
    memcpy(pSample->p, sqlite3_column_blob(pStmt, 4), pSample->n);

  }
  return sqlite3_finalize(pStmt);
}
#endif /* SQLITE_ENABLE_STAT4 */

/*
** Load the content of the sqlite_stat1 and sqlite_stat4 tables. The
** contents of sqlite_stat1 are used to populate the Index.aiRowEst[]
** arrays. The contents of sqlite_stat4 are used to populate the
** Index.aSample[] arrays.
**
** If the sqlite_stat1 table is not present in the database, SQLITE_ERROR
** is returned. In this case, even if SQLITE_ENABLE_STAT4 was defined 
** during compilation and the sqlite_stat4 table is present, no data is 
** read from it.
**
** If SQLITE_ENABLE_STAT4 was defined during compilation and the 
** sqlite_stat4 table is not present in the database, SQLITE_ERROR is
** returned. However, in this case, data is read from the sqlite_stat1
** table (if it is present) before returning.
**
** If an OOM error occurs, this function always sets db->mallocFailed.
** This means if the caller does not care about other errors, the return
** code may be ignored.
*/
int sqlite3AnalysisLoad(sqlite3 *db, int iDb){
  analysisInfo sInfo;
  HashElem *i;
  char *zSql;
  int rc;

  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pBt!=0 );

  /* Clear any prior statistics */
  assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
  for(i=sqliteHashFirst(&db->aDb[iDb].pSchema->idxHash);i;i=sqliteHashNext(i)){
    Index *pIdx = sqliteHashData(i);
    sqlite3DefaultRowEst(pIdx);
#ifdef SQLITE_ENABLE_STAT4
    sqlite3DeleteIndexSamples(db, pIdx);
    pIdx->aSample = 0;
#endif
  }

  /* Check to make sure the sqlite_stat1 table exists */
  sInfo.db = db;
  sInfo.zDatabase = db->aDb[iDb].zName;
  if( sqlite3FindTable(db, "sqlite_stat1", sInfo.zDatabase)==0 ){
    return SQLITE_ERROR;
  }

  /* Load new statistics out of the sqlite_stat1 table */
  zSql = sqlite3MPrintf(db, 
      "SELECT tbl,idx,stat FROM %Q.sqlite_stat1", sInfo.zDatabase);
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_exec(db, zSql, analysisLoader, &sInfo, 0);
    sqlite3DbFree(db, zSql);
  }


  /* Load the statistics from the sqlite_stat4 table. */
#ifdef SQLITE_ENABLE_STAT4
  if( rc==SQLITE_OK ){
    int lookasideEnabled = db->lookaside.bEnabled;
    db->lookaside.bEnabled = 0;
    rc = loadStat4(db, sInfo.zDatabase);
    db->lookaside.bEnabled = lookasideEnabled;
  }
#endif

  if( rc==SQLITE_NOMEM ){
    db->mallocFailed = 1;
  }
  return rc;
}


#endif /* SQLITE_OMIT_ANALYZE */
