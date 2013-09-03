/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This module contains C code that generates VDBE code used to process
** the WHERE clause of SQL statements.  This module is responsible for
** generating the code that loops through a table looking for applicable
** rows.  Indices are selected and used to speed the search when doing
** so is applicable.  Because this module is responsible for selecting
** indices, you might also think of this module as the "query optimizer".
*/
#include "sqliteInt.h"


/*
** Trace output macros
*/
#if defined(SQLITE_TEST) || defined(SQLITE_DEBUG)
/***/ int sqlite3WhereTrace = 0;
#endif
#if defined(SQLITE_DEBUG) \
    && (defined(SQLITE_TEST) || defined(SQLITE_ENABLE_WHERETRACE))
# define WHERETRACE(K,X)  if(sqlite3WhereTrace&(K)) sqlite3DebugPrintf X
# define WHERETRACE_ENABLED 1
#else
# define WHERETRACE(K,X)
#endif

/* Forward references
*/
typedef struct WhereClause WhereClause;
typedef struct WhereMaskSet WhereMaskSet;
typedef struct WhereOrInfo WhereOrInfo;
typedef struct WhereAndInfo WhereAndInfo;
typedef struct WhereLevel WhereLevel;
typedef struct WhereLoop WhereLoop;
typedef struct WherePath WherePath;
typedef struct WhereTerm WhereTerm;
typedef struct WhereLoopBuilder WhereLoopBuilder;
typedef struct WhereScan WhereScan;
typedef struct WhereOrCost WhereOrCost;
typedef struct WhereOrSet WhereOrSet;

/*
** Cost X is tracked as 10*log2(X) stored in a 16-bit integer.  The
** maximum cost for ordinary tables is 64*(2**63) which becomes 6900.
** (Virtual tables can return a larger cost, but let's assume they do not.)
** So all costs can be stored in a 16-bit unsigned integer without risk
** of overflow.
**
** Costs are estimates, so no effort is made to compute 10*log2(X) exactly.
** Instead, a close estimate is used.  Any value of X<=1 is stored as 0.
** X=2 is 10.  X=3 is 16.  X=1000 is 99. etc.
**
** The tool/wherecosttest.c source file implements a command-line program
** that will convert WhereCosts to integers, convert integers to WhereCosts
** and do addition and multiplication on WhereCost values.  The wherecosttest
** command-line program is a useful utility to have around when working with
** this module.
*/
typedef unsigned short int WhereCost;

/*
** This object contains information needed to implement a single nested
** loop in WHERE clause.
**
** Contrast this object with WhereLoop.  This object describes the
** implementation of the loop.  WhereLoop describes the algorithm.
** This object contains a pointer to the WhereLoop algorithm as one of
** its elements.
**
** The WhereInfo object contains a single instance of this object for
** each term in the FROM clause (which is to say, for each of the
** nested loops as implemented).  The order of WhereLevel objects determines
** the loop nested order, with WhereInfo.a[0] being the outer loop and
** WhereInfo.a[WhereInfo.nLevel-1] being the inner loop.
*/
struct WhereLevel {
  int iLeftJoin;        /* Memory cell used to implement LEFT OUTER JOIN */
  int iTabCur;          /* The VDBE cursor used to access the table */
  int iIdxCur;          /* The VDBE cursor used to access pIdx */
  int addrBrk;          /* Jump here to break out of the loop */
  int addrNxt;          /* Jump here to start the next IN combination */
  int addrCont;         /* Jump here to continue with the next loop cycle */
  int addrFirst;        /* First instruction of interior of the loop */
  int addrBody;         /* Beginning of the body of this loop */
  u8 iFrom;             /* Which entry in the FROM clause */
  u8 op, p5;            /* Opcode and P5 of the opcode that ends the loop */
  int p1, p2;           /* Operands of the opcode used to ends the loop */
  union {               /* Information that depends on pWLoop->wsFlags */
    struct {
      int nIn;              /* Number of entries in aInLoop[] */
      struct InLoop {
        int iCur;              /* The VDBE cursor used by this IN operator */
        int addrInTop;         /* Top of the IN loop */
        u8 eEndLoopOp;         /* IN Loop terminator. OP_Next or OP_Prev */
      } *aInLoop;           /* Information about each nested IN operator */
    } in;                 /* Used when pWLoop->wsFlags&WHERE_IN_ABLE */
    Index *pCovidx;       /* Possible covering index for WHERE_MULTI_OR */
  } u;
  struct WhereLoop *pWLoop;  /* The selected WhereLoop object */
};

/*
** Each instance of this object represents an algorithm for evaluating one
** term of a join.  Every term of the FROM clause will have at least
** one corresponding WhereLoop object (unless INDEXED BY constraints
** prevent a query solution - which is an error) and many terms of the
** FROM clause will have multiple WhereLoop objects, each describing a
** potential way of implementing that FROM-clause term, together with
** dependencies and cost estimates for using the chosen algorithm.
**
** Query planning consists of building up a collection of these WhereLoop
** objects, then computing a particular sequence of WhereLoop objects, with
** one WhereLoop object per FROM clause term, that satisfy all dependencies
** and that minimize the overall cost.
*/
struct WhereLoop {
  Bitmask prereq;       /* Bitmask of other loops that must run first */
  Bitmask maskSelf;     /* Bitmask identifying table iTab */
#ifdef SQLITE_DEBUG
  char cId;             /* Symbolic ID of this loop for debugging use */
#endif
  u8 iTab;              /* Position in FROM clause of table for this loop */
  u8 iSortIdx;          /* Sorting index number.  0==None */
  WhereCost rSetup;     /* One-time setup cost (ex: create transient index) */
  WhereCost rRun;       /* Cost of running each loop */
  WhereCost nOut;       /* Estimated number of output rows */
  union {
    struct {               /* Information for internal btree tables */
      int nEq;               /* Number of equality constraints */
      Index *pIndex;         /* Index used, or NULL */
    } btree;
    struct {               /* Information for virtual tables */
      int idxNum;            /* Index number */
      u8 needFree;           /* True if sqlite3_free(idxStr) is needed */
      u8 isOrdered;          /* True if satisfies ORDER BY */
      u16 omitMask;          /* Terms that may be omitted */
      char *idxStr;          /* Index identifier string */
    } vtab;
  } u;
  u32 wsFlags;          /* WHERE_* flags describing the plan */
  u16 nLTerm;           /* Number of entries in aLTerm[] */
  /**** whereLoopXfer() copies fields above ***********************/
# define WHERE_LOOP_XFER_SZ offsetof(WhereLoop,nLSlot)
  u16 nLSlot;           /* Number of slots allocated for aLTerm[] */
  WhereTerm **aLTerm;   /* WhereTerms used */
  WhereLoop *pNextLoop; /* Next WhereLoop object in the WhereClause */
  WhereTerm *aLTermSpace[4];  /* Initial aLTerm[] space */
};

/* This object holds the prerequisites and the cost of running a
** subquery on one operand of an OR operator in the WHERE clause.
** See WhereOrSet for additional information 
*/
struct WhereOrCost {
  Bitmask prereq;     /* Prerequisites */
  WhereCost rRun;     /* Cost of running this subquery */
  WhereCost nOut;     /* Number of outputs for this subquery */
};

/* The WhereOrSet object holds a set of possible WhereOrCosts that
** correspond to the subquery(s) of OR-clause processing.  Only the
** best N_OR_COST elements are retained.
*/
#define N_OR_COST 3
struct WhereOrSet {
  u16 n;                      /* Number of valid a[] entries */
  WhereOrCost a[N_OR_COST];   /* Set of best costs */
};


/* Forward declaration of methods */
static int whereLoopResize(sqlite3*, WhereLoop*, int);

/*
** Each instance of this object holds a sequence of WhereLoop objects
** that implement some or all of a query plan.
**
** Think of each WhereLoop object as a node in a graph with arcs
** showing dependences and costs for travelling between nodes.  (That is
** not a completely accurate description because WhereLoop costs are a
** vector, not a scalar, and because dependences are many-to-one, not
** one-to-one as are graph nodes.  But it is a useful visualization aid.)
** Then a WherePath object is a path through the graph that visits some
** or all of the WhereLoop objects once.
**
** The "solver" works by creating the N best WherePath objects of length
** 1.  Then using those as a basis to compute the N best WherePath objects
** of length 2.  And so forth until the length of WherePaths equals the
** number of nodes in the FROM clause.  The best (lowest cost) WherePath
** at the end is the choosen query plan.
*/
struct WherePath {
  Bitmask maskLoop;     /* Bitmask of all WhereLoop objects in this path */
  Bitmask revLoop;      /* aLoop[]s that should be reversed for ORDER BY */
  WhereCost nRow;       /* Estimated number of rows generated by this path */
  WhereCost rCost;      /* Total cost of this path */
  u8 isOrdered;         /* True if this path satisfies ORDER BY */
  u8 isOrderedValid;    /* True if the isOrdered field is valid */
  WhereLoop **aLoop;    /* Array of WhereLoop objects implementing this path */
};

/*
** The query generator uses an array of instances of this structure to
** help it analyze the subexpressions of the WHERE clause.  Each WHERE
** clause subexpression is separated from the others by AND operators,
** usually, or sometimes subexpressions separated by OR.
**
** All WhereTerms are collected into a single WhereClause structure.  
** The following identity holds:
**
**        WhereTerm.pWC->a[WhereTerm.idx] == WhereTerm
**
** When a term is of the form:
**
**              X <op> <expr>
**
** where X is a column name and <op> is one of certain operators,
** then WhereTerm.leftCursor and WhereTerm.u.leftColumn record the
** cursor number and column number for X.  WhereTerm.eOperator records
** the <op> using a bitmask encoding defined by WO_xxx below.  The
** use of a bitmask encoding for the operator allows us to search
** quickly for terms that match any of several different operators.
**
** A WhereTerm might also be two or more subterms connected by OR:
**
**         (t1.X <op> <expr>) OR (t1.Y <op> <expr>) OR ....
**
** In this second case, wtFlag has the TERM_ORINFO bit set and eOperator==WO_OR
** and the WhereTerm.u.pOrInfo field points to auxiliary information that
** is collected about the OR clause.
**
** If a term in the WHERE clause does not match either of the two previous
** categories, then eOperator==0.  The WhereTerm.pExpr field is still set
** to the original subexpression content and wtFlags is set up appropriately
** but no other fields in the WhereTerm object are meaningful.
**
** When eOperator!=0, prereqRight and prereqAll record sets of cursor numbers,
** but they do so indirectly.  A single WhereMaskSet structure translates
** cursor number into bits and the translated bit is stored in the prereq
** fields.  The translation is used in order to maximize the number of
** bits that will fit in a Bitmask.  The VDBE cursor numbers might be
** spread out over the non-negative integers.  For example, the cursor
** numbers might be 3, 8, 9, 10, 20, 23, 41, and 45.  The WhereMaskSet
** translates these sparse cursor numbers into consecutive integers
** beginning with 0 in order to make the best possible use of the available
** bits in the Bitmask.  So, in the example above, the cursor numbers
** would be mapped into integers 0 through 7.
**
** The number of terms in a join is limited by the number of bits
** in prereqRight and prereqAll.  The default is 64 bits, hence SQLite
** is only able to process joins with 64 or fewer tables.
*/
struct WhereTerm {
  Expr *pExpr;            /* Pointer to the subexpression that is this term */
  int iParent;            /* Disable pWC->a[iParent] when this term disabled */
  int leftCursor;         /* Cursor number of X in "X <op> <expr>" */
  union {
    int leftColumn;         /* Column number of X in "X <op> <expr>" */
    WhereOrInfo *pOrInfo;   /* Extra information if (eOperator & WO_OR)!=0 */
    WhereAndInfo *pAndInfo; /* Extra information if (eOperator& WO_AND)!=0 */
  } u;
  u16 eOperator;          /* A WO_xx value describing <op> */
  u8 wtFlags;             /* TERM_xxx bit flags.  See below */
  u8 nChild;              /* Number of children that must disable us */
  WhereClause *pWC;       /* The clause this term is part of */
  Bitmask prereqRight;    /* Bitmask of tables used by pExpr->pRight */
  Bitmask prereqAll;      /* Bitmask of tables referenced by pExpr */
};

/*
** Allowed values of WhereTerm.wtFlags
*/
#define TERM_DYNAMIC    0x01   /* Need to call sqlite3ExprDelete(db, pExpr) */
#define TERM_VIRTUAL    0x02   /* Added by the optimizer.  Do not code */
#define TERM_CODED      0x04   /* This term is already coded */
#define TERM_COPIED     0x08   /* Has a child */
#define TERM_ORINFO     0x10   /* Need to free the WhereTerm.u.pOrInfo object */
#define TERM_ANDINFO    0x20   /* Need to free the WhereTerm.u.pAndInfo obj */
#define TERM_OR_OK      0x40   /* Used during OR-clause processing */
#ifdef SQLITE_ENABLE_STAT3
#  define TERM_VNULL    0x80   /* Manufactured x>NULL or x<=NULL term */
#else
#  define TERM_VNULL    0x00   /* Disabled if not using stat3 */
#endif

/*
** An instance of the WhereScan object is used as an iterator for locating
** terms in the WHERE clause that are useful to the query planner.
*/
struct WhereScan {
  WhereClause *pOrigWC;      /* Original, innermost WhereClause */
  WhereClause *pWC;          /* WhereClause currently being scanned */
  char *zCollName;           /* Required collating sequence, if not NULL */
  char idxaff;               /* Must match this affinity, if zCollName!=NULL */
  unsigned char nEquiv;      /* Number of entries in aEquiv[] */
  unsigned char iEquiv;      /* Next unused slot in aEquiv[] */
  u32 opMask;                /* Acceptable operators */
  int k;                     /* Resume scanning at this->pWC->a[this->k] */
  int aEquiv[22];            /* Cursor,Column pairs for equivalence classes */
};

/*
** An instance of the following structure holds all information about a
** WHERE clause.  Mostly this is a container for one or more WhereTerms.
**
** Explanation of pOuter:  For a WHERE clause of the form
**
**           a AND ((b AND c) OR (d AND e)) AND f
**
** There are separate WhereClause objects for the whole clause and for
** the subclauses "(b AND c)" and "(d AND e)".  The pOuter field of the
** subclauses points to the WhereClause object for the whole clause.
*/
struct WhereClause {
  WhereInfo *pWInfo;       /* WHERE clause processing context */
  WhereClause *pOuter;     /* Outer conjunction */
  u8 op;                   /* Split operator.  TK_AND or TK_OR */
  int nTerm;               /* Number of terms */
  int nSlot;               /* Number of entries in a[] */
  WhereTerm *a;            /* Each a[] describes a term of the WHERE cluase */
#if defined(SQLITE_SMALL_STACK)
  WhereTerm aStatic[1];    /* Initial static space for a[] */
#else
  WhereTerm aStatic[8];    /* Initial static space for a[] */
#endif
};

/*
** A WhereTerm with eOperator==WO_OR has its u.pOrInfo pointer set to
** a dynamically allocated instance of the following structure.
*/
struct WhereOrInfo {
  WhereClause wc;          /* Decomposition into subterms */
  Bitmask indexable;       /* Bitmask of all indexable tables in the clause */
};

/*
** A WhereTerm with eOperator==WO_AND has its u.pAndInfo pointer set to
** a dynamically allocated instance of the following structure.
*/
struct WhereAndInfo {
  WhereClause wc;          /* The subexpression broken out */
};

/*
** An instance of the following structure keeps track of a mapping
** between VDBE cursor numbers and bits of the bitmasks in WhereTerm.
**
** The VDBE cursor numbers are small integers contained in 
** SrcList_item.iCursor and Expr.iTable fields.  For any given WHERE 
** clause, the cursor numbers might not begin with 0 and they might
** contain gaps in the numbering sequence.  But we want to make maximum
** use of the bits in our bitmasks.  This structure provides a mapping
** from the sparse cursor numbers into consecutive integers beginning
** with 0.
**
** If WhereMaskSet.ix[A]==B it means that The A-th bit of a Bitmask
** corresponds VDBE cursor number B.  The A-th bit of a bitmask is 1<<A.
**
** For example, if the WHERE clause expression used these VDBE
** cursors:  4, 5, 8, 29, 57, 73.  Then the  WhereMaskSet structure
** would map those cursor numbers into bits 0 through 5.
**
** Note that the mapping is not necessarily ordered.  In the example
** above, the mapping might go like this:  4->3, 5->1, 8->2, 29->0,
** 57->5, 73->4.  Or one of 719 other combinations might be used. It
** does not really matter.  What is important is that sparse cursor
** numbers all get mapped into bit numbers that begin with 0 and contain
** no gaps.
*/
struct WhereMaskSet {
  int n;                        /* Number of assigned cursor values */
  int ix[BMS];                  /* Cursor assigned to each bit */
};

/*
** This object is a convenience wrapper holding all information needed
** to construct WhereLoop objects for a particular query.
*/
struct WhereLoopBuilder {
  WhereInfo *pWInfo;        /* Information about this WHERE */
  WhereClause *pWC;         /* WHERE clause terms */
  ExprList *pOrderBy;       /* ORDER BY clause */
  WhereLoop *pNew;          /* Template WhereLoop */
  WhereOrSet *pOrSet;       /* Record best loops here, if not NULL */
};

/*
** The WHERE clause processing routine has two halves.  The
** first part does the start of the WHERE loop and the second
** half does the tail of the WHERE loop.  An instance of
** this structure is returned by the first half and passed
** into the second half to give some continuity.
**
** An instance of this object holds the complete state of the query
** planner.
*/
struct WhereInfo {
  Parse *pParse;            /* Parsing and code generating context */
  SrcList *pTabList;        /* List of tables in the join */
  ExprList *pOrderBy;       /* The ORDER BY clause or NULL */
  ExprList *pResultSet;     /* Result set. DISTINCT operates on these */
  WhereLoop *pLoops;        /* List of all WhereLoop objects */
  Bitmask revMask;          /* Mask of ORDER BY terms that need reversing */
  WhereCost nRowOut;        /* Estimated number of output rows */
  u16 wctrlFlags;           /* Flags originally passed to sqlite3WhereBegin() */
  u8 bOBSat;                /* ORDER BY satisfied by indices */
  u8 okOnePass;             /* Ok to use one-pass algorithm for UPDATE/DELETE */
  u8 untestedTerms;         /* Not all WHERE terms resolved by outer loop */
  u8 eDistinct;             /* One of the WHERE_DISTINCT_* values below */
  u8 nLevel;                /* Number of nested loop */
  int iTop;                 /* The very beginning of the WHERE loop */
  int iContinue;            /* Jump here to continue with next record */
  int iBreak;               /* Jump here to break out of the loop */
  int savedNQueryLoop;      /* pParse->nQueryLoop outside the WHERE loop */
  WhereMaskSet sMaskSet;    /* Map cursor numbers to bitmasks */
  WhereClause sWC;          /* Decomposition of the WHERE clause */
  WhereLevel a[1];          /* Information about each nest loop in WHERE */
};

/*
** Bitmasks for the operators on WhereTerm objects.  These are all
** operators that are of interest to the query planner.  An
** OR-ed combination of these values can be used when searching for
** particular WhereTerms within a WhereClause.
*/
#define WO_IN     0x001
#define WO_EQ     0x002
#define WO_LT     (WO_EQ<<(TK_LT-TK_EQ))
#define WO_LE     (WO_EQ<<(TK_LE-TK_EQ))
#define WO_GT     (WO_EQ<<(TK_GT-TK_EQ))
#define WO_GE     (WO_EQ<<(TK_GE-TK_EQ))
#define WO_MATCH  0x040
#define WO_ISNULL 0x080
#define WO_OR     0x100       /* Two or more OR-connected terms */
#define WO_AND    0x200       /* Two or more AND-connected terms */
#define WO_EQUIV  0x400       /* Of the form A==B, both columns */
#define WO_NOOP   0x800       /* This term does not restrict search space */

#define WO_ALL    0xfff       /* Mask of all possible WO_* values */
#define WO_SINGLE 0x0ff       /* Mask of all non-compound WO_* values */

/*
** These are definitions of bits in the WhereLoop.wsFlags field.
** The particular combination of bits in each WhereLoop help to
** determine the algorithm that WhereLoop represents.
*/
#define WHERE_COLUMN_EQ    0x00000001  /* x=EXPR */
#define WHERE_COLUMN_RANGE 0x00000002  /* x<EXPR and/or x>EXPR */
#define WHERE_COLUMN_IN    0x00000004  /* x IN (...) */
#define WHERE_COLUMN_NULL  0x00000008  /* x IS NULL */
#define WHERE_CONSTRAINT   0x0000000f  /* Any of the WHERE_COLUMN_xxx values */
#define WHERE_TOP_LIMIT    0x00000010  /* x<EXPR or x<=EXPR constraint */
#define WHERE_BTM_LIMIT    0x00000020  /* x>EXPR or x>=EXPR constraint */
#define WHERE_BOTH_LIMIT   0x00000030  /* Both x>EXPR and x<EXPR */
#define WHERE_IDX_ONLY     0x00000040  /* Use index only - omit table */
#define WHERE_IPK          0x00000100  /* x is the INTEGER PRIMARY KEY */
#define WHERE_INDEXED      0x00000200  /* WhereLoop.u.btree.pIndex is valid */
#define WHERE_VIRTUALTABLE 0x00000400  /* WhereLoop.u.vtab is valid */
#define WHERE_IN_ABLE      0x00000800  /* Able to support an IN operator */
#define WHERE_ONEROW       0x00001000  /* Selects no more than one row */
#define WHERE_MULTI_OR     0x00002000  /* OR using multiple indices */
#define WHERE_AUTO_INDEX   0x00004000  /* Uses an ephemeral index */


/* Convert a WhereCost value (10 times log2(X)) into its integer value X.
** A rough approximation is used.  The value returned is not exact.
*/
static u64 whereCostToInt(WhereCost x){
  u64 n;
  if( x<10 ) return 1;
  n = x%10;
  x /= 10;
  if( n>=5 ) n -= 2;
  else if( n>=1 ) n -= 1;
  if( x>=3 ) return (n+8)<<(x-3);
  return (n+8)>>(3-x);
}

/*
** Return the estimated number of output rows from a WHERE clause
*/
u64 sqlite3WhereOutputRowCount(WhereInfo *pWInfo){
  return whereCostToInt(pWInfo->nRowOut);
}

/*
** Return one of the WHERE_DISTINCT_xxxxx values to indicate how this
** WHERE clause returns outputs for DISTINCT processing.
*/
int sqlite3WhereIsDistinct(WhereInfo *pWInfo){
  return pWInfo->eDistinct;
}

/*
** Return TRUE if the WHERE clause returns rows in ORDER BY order.
** Return FALSE if the output needs to be sorted.
*/
int sqlite3WhereIsOrdered(WhereInfo *pWInfo){
  return pWInfo->bOBSat!=0;
}

/*
** Return the VDBE address or label to jump to in order to continue
** immediately with the next row of a WHERE clause.
*/
int sqlite3WhereContinueLabel(WhereInfo *pWInfo){
  return pWInfo->iContinue;
}

/*
** Return the VDBE address or label to jump to in order to break
** out of a WHERE loop.
*/
int sqlite3WhereBreakLabel(WhereInfo *pWInfo){
  return pWInfo->iBreak;
}

/*
** Return TRUE if an UPDATE or DELETE statement can operate directly on
** the rowids returned by a WHERE clause.  Return FALSE if doing an
** UPDATE or DELETE might change subsequent WHERE clause results.
*/
int sqlite3WhereOkOnePass(WhereInfo *pWInfo){
  return pWInfo->okOnePass;
}

/*
** Move the content of pSrc into pDest
*/
static void whereOrMove(WhereOrSet *pDest, WhereOrSet *pSrc){
  pDest->n = pSrc->n;
  memcpy(pDest->a, pSrc->a, pDest->n*sizeof(pDest->a[0]));
}

/*
** Try to insert a new prerequisite/cost entry into the WhereOrSet pSet.
**
** The new entry might overwrite an existing entry, or it might be
** appended, or it might be discarded.  Do whatever is the right thing
** so that pSet keeps the N_OR_COST best entries seen so far.
*/
static int whereOrInsert(
  WhereOrSet *pSet,      /* The WhereOrSet to be updated */
  Bitmask prereq,        /* Prerequisites of the new entry */
  WhereCost rRun,        /* Run-cost of the new entry */
  WhereCost nOut         /* Number of outputs for the new entry */
){
  u16 i;
  WhereOrCost *p;
  for(i=pSet->n, p=pSet->a; i>0; i--, p++){
    if( rRun<=p->rRun && (prereq & p->prereq)==prereq ){
      goto whereOrInsert_done;
    }
    if( p->rRun<=rRun && (p->prereq & prereq)==p->prereq ){
      return 0;
    }
  }
  if( pSet->n<N_OR_COST ){
    p = &pSet->a[pSet->n++];
    p->nOut = nOut;
  }else{
    p = pSet->a;
    for(i=1; i<pSet->n; i++){
      if( p->rRun>pSet->a[i].rRun ) p = pSet->a + i;
    }
    if( p->rRun<=rRun ) return 0;
  }
whereOrInsert_done:
  p->prereq = prereq;
  p->rRun = rRun;
  if( p->nOut>nOut ) p->nOut = nOut;
  return 1;
}

/*
** Initialize a preallocated WhereClause structure.
*/
static void whereClauseInit(
  WhereClause *pWC,        /* The WhereClause to be initialized */
  WhereInfo *pWInfo        /* The WHERE processing context */
){
  pWC->pWInfo = pWInfo;
  pWC->pOuter = 0;
  pWC->nTerm = 0;
  pWC->nSlot = ArraySize(pWC->aStatic);
  pWC->a = pWC->aStatic;
}

/* Forward reference */
static void whereClauseClear(WhereClause*);

/*
** Deallocate all memory associated with a WhereOrInfo object.
*/
static void whereOrInfoDelete(sqlite3 *db, WhereOrInfo *p){
  whereClauseClear(&p->wc);
  sqlite3DbFree(db, p);
}

/*
** Deallocate all memory associated with a WhereAndInfo object.
*/
static void whereAndInfoDelete(sqlite3 *db, WhereAndInfo *p){
  whereClauseClear(&p->wc);
  sqlite3DbFree(db, p);
}

/*
** Deallocate a WhereClause structure.  The WhereClause structure
** itself is not freed.  This routine is the inverse of whereClauseInit().
*/
static void whereClauseClear(WhereClause *pWC){
  int i;
  WhereTerm *a;
  sqlite3 *db = pWC->pWInfo->pParse->db;
  for(i=pWC->nTerm-1, a=pWC->a; i>=0; i--, a++){
    if( a->wtFlags & TERM_DYNAMIC ){
      sqlite3ExprDelete(db, a->pExpr);
    }
    if( a->wtFlags & TERM_ORINFO ){
      whereOrInfoDelete(db, a->u.pOrInfo);
    }else if( a->wtFlags & TERM_ANDINFO ){
      whereAndInfoDelete(db, a->u.pAndInfo);
    }
  }
  if( pWC->a!=pWC->aStatic ){
    sqlite3DbFree(db, pWC->a);
  }
}

/*
** Add a single new WhereTerm entry to the WhereClause object pWC.
** The new WhereTerm object is constructed from Expr p and with wtFlags.
** The index in pWC->a[] of the new WhereTerm is returned on success.
** 0 is returned if the new WhereTerm could not be added due to a memory
** allocation error.  The memory allocation failure will be recorded in
** the db->mallocFailed flag so that higher-level functions can detect it.
**
** This routine will increase the size of the pWC->a[] array as necessary.
**
** If the wtFlags argument includes TERM_DYNAMIC, then responsibility
** for freeing the expression p is assumed by the WhereClause object pWC.
** This is true even if this routine fails to allocate a new WhereTerm.
**
** WARNING:  This routine might reallocate the space used to store
** WhereTerms.  All pointers to WhereTerms should be invalidated after
** calling this routine.  Such pointers may be reinitialized by referencing
** the pWC->a[] array.
*/
static int whereClauseInsert(WhereClause *pWC, Expr *p, u8 wtFlags){
  WhereTerm *pTerm;
  int idx;
  testcase( wtFlags & TERM_VIRTUAL );
  if( pWC->nTerm>=pWC->nSlot ){
    WhereTerm *pOld = pWC->a;
    sqlite3 *db = pWC->pWInfo->pParse->db;
    pWC->a = sqlite3DbMallocRaw(db, sizeof(pWC->a[0])*pWC->nSlot*2 );
    if( pWC->a==0 ){
      if( wtFlags & TERM_DYNAMIC ){
        sqlite3ExprDelete(db, p);
      }
      pWC->a = pOld;
      return 0;
    }
    memcpy(pWC->a, pOld, sizeof(pWC->a[0])*pWC->nTerm);
    if( pOld!=pWC->aStatic ){
      sqlite3DbFree(db, pOld);
    }
    pWC->nSlot = sqlite3DbMallocSize(db, pWC->a)/sizeof(pWC->a[0]);
  }
  pTerm = &pWC->a[idx = pWC->nTerm++];
  pTerm->pExpr = sqlite3ExprSkipCollate(p);
  pTerm->wtFlags = wtFlags;
  pTerm->pWC = pWC;
  pTerm->iParent = -1;
  return idx;
}

/*
** This routine identifies subexpressions in the WHERE clause where
** each subexpression is separated by the AND operator or some other
** operator specified in the op parameter.  The WhereClause structure
** is filled with pointers to subexpressions.  For example:
**
**    WHERE  a=='hello' AND coalesce(b,11)<10 AND (c+12!=d OR c==22)
**           \________/     \_______________/     \________________/
**            slot[0]            slot[1]               slot[2]
**
** The original WHERE clause in pExpr is unaltered.  All this routine
** does is make slot[] entries point to substructure within pExpr.
**
** In the previous sentence and in the diagram, "slot[]" refers to
** the WhereClause.a[] array.  The slot[] array grows as needed to contain
** all terms of the WHERE clause.
*/
static void whereSplit(WhereClause *pWC, Expr *pExpr, u8 op){
  pWC->op = op;
  if( pExpr==0 ) return;
  if( pExpr->op!=op ){
    whereClauseInsert(pWC, pExpr, 0);
  }else{
    whereSplit(pWC, pExpr->pLeft, op);
    whereSplit(pWC, pExpr->pRight, op);
  }
}

/*
** Initialize a WhereMaskSet object
*/
#define initMaskSet(P)  (P)->n=0

/*
** Return the bitmask for the given cursor number.  Return 0 if
** iCursor is not in the set.
*/
static Bitmask getMask(WhereMaskSet *pMaskSet, int iCursor){
  int i;
  assert( pMaskSet->n<=(int)sizeof(Bitmask)*8 );
  for(i=0; i<pMaskSet->n; i++){
    if( pMaskSet->ix[i]==iCursor ){
      return MASKBIT(i);
    }
  }
  return 0;
}

/*
** Create a new mask for cursor iCursor.
**
** There is one cursor per table in the FROM clause.  The number of
** tables in the FROM clause is limited by a test early in the
** sqlite3WhereBegin() routine.  So we know that the pMaskSet->ix[]
** array will never overflow.
*/
static void createMask(WhereMaskSet *pMaskSet, int iCursor){
  assert( pMaskSet->n < ArraySize(pMaskSet->ix) );
  pMaskSet->ix[pMaskSet->n++] = iCursor;
}

/*
** These routines walk (recursively) an expression tree and generate
** a bitmask indicating which tables are used in that expression
** tree.
*/
static Bitmask exprListTableUsage(WhereMaskSet*, ExprList*);
static Bitmask exprSelectTableUsage(WhereMaskSet*, Select*);
static Bitmask exprTableUsage(WhereMaskSet *pMaskSet, Expr *p){
  Bitmask mask = 0;
  if( p==0 ) return 0;
  if( p->op==TK_COLUMN ){
    mask = getMask(pMaskSet, p->iTable);
    return mask;
  }
  mask = exprTableUsage(pMaskSet, p->pRight);
  mask |= exprTableUsage(pMaskSet, p->pLeft);
  if( ExprHasProperty(p, EP_xIsSelect) ){
    mask |= exprSelectTableUsage(pMaskSet, p->x.pSelect);
  }else{
    mask |= exprListTableUsage(pMaskSet, p->x.pList);
  }
  return mask;
}
static Bitmask exprListTableUsage(WhereMaskSet *pMaskSet, ExprList *pList){
  int i;
  Bitmask mask = 0;
  if( pList ){
    for(i=0; i<pList->nExpr; i++){
      mask |= exprTableUsage(pMaskSet, pList->a[i].pExpr);
    }
  }
  return mask;
}
static Bitmask exprSelectTableUsage(WhereMaskSet *pMaskSet, Select *pS){
  Bitmask mask = 0;
  while( pS ){
    SrcList *pSrc = pS->pSrc;
    mask |= exprListTableUsage(pMaskSet, pS->pEList);
    mask |= exprListTableUsage(pMaskSet, pS->pGroupBy);
    mask |= exprListTableUsage(pMaskSet, pS->pOrderBy);
    mask |= exprTableUsage(pMaskSet, pS->pWhere);
    mask |= exprTableUsage(pMaskSet, pS->pHaving);
    if( ALWAYS(pSrc!=0) ){
      int i;
      for(i=0; i<pSrc->nSrc; i++){
        mask |= exprSelectTableUsage(pMaskSet, pSrc->a[i].pSelect);
        mask |= exprTableUsage(pMaskSet, pSrc->a[i].pOn);
      }
    }
    pS = pS->pPrior;
  }
  return mask;
}

/*
** Return TRUE if the given operator is one of the operators that is
** allowed for an indexable WHERE clause term.  The allowed operators are
** "=", "<", ">", "<=", ">=", "IN", and "IS NULL"
*/
static int allowedOp(int op){
  assert( TK_GT>TK_EQ && TK_GT<TK_GE );
  assert( TK_LT>TK_EQ && TK_LT<TK_GE );
  assert( TK_LE>TK_EQ && TK_LE<TK_GE );
  assert( TK_GE==TK_EQ+4 );
  return op==TK_IN || (op>=TK_EQ && op<=TK_GE) || op==TK_ISNULL;
}

/*
** Swap two objects of type TYPE.
*/
#define SWAP(TYPE,A,B) {TYPE t=A; A=B; B=t;}

/*
** Commute a comparison operator.  Expressions of the form "X op Y"
** are converted into "Y op X".
**
** If left/right precedence rules come into play when determining the
** collating sequence, then COLLATE operators are adjusted to ensure
** that the collating sequence does not change.  For example:
** "Y collate NOCASE op X" becomes "X op Y" because any collation sequence on
** the left hand side of a comparison overrides any collation sequence 
** attached to the right. For the same reason the EP_Collate flag
** is not commuted.
*/
static void exprCommute(Parse *pParse, Expr *pExpr){
  u16 expRight = (pExpr->pRight->flags & EP_Collate);
  u16 expLeft = (pExpr->pLeft->flags & EP_Collate);
  assert( allowedOp(pExpr->op) && pExpr->op!=TK_IN );
  if( expRight==expLeft ){
    /* Either X and Y both have COLLATE operator or neither do */
    if( expRight ){
      /* Both X and Y have COLLATE operators.  Make sure X is always
      ** used by clearing the EP_Collate flag from Y. */
      pExpr->pRight->flags &= ~EP_Collate;
    }else if( sqlite3ExprCollSeq(pParse, pExpr->pLeft)!=0 ){
      /* Neither X nor Y have COLLATE operators, but X has a non-default
      ** collating sequence.  So add the EP_Collate marker on X to cause
      ** it to be searched first. */
      pExpr->pLeft->flags |= EP_Collate;
    }
  }
  SWAP(Expr*,pExpr->pRight,pExpr->pLeft);
  if( pExpr->op>=TK_GT ){
    assert( TK_LT==TK_GT+2 );
    assert( TK_GE==TK_LE+2 );
    assert( TK_GT>TK_EQ );
    assert( TK_GT<TK_LE );
    assert( pExpr->op>=TK_GT && pExpr->op<=TK_GE );
    pExpr->op = ((pExpr->op-TK_GT)^2)+TK_GT;
  }
}

/*
** Translate from TK_xx operator to WO_xx bitmask.
*/
static u16 operatorMask(int op){
  u16 c;
  assert( allowedOp(op) );
  if( op==TK_IN ){
    c = WO_IN;
  }else if( op==TK_ISNULL ){
    c = WO_ISNULL;
  }else{
    assert( (WO_EQ<<(op-TK_EQ)) < 0x7fff );
    c = (u16)(WO_EQ<<(op-TK_EQ));
  }
  assert( op!=TK_ISNULL || c==WO_ISNULL );
  assert( op!=TK_IN || c==WO_IN );
  assert( op!=TK_EQ || c==WO_EQ );
  assert( op!=TK_LT || c==WO_LT );
  assert( op!=TK_LE || c==WO_LE );
  assert( op!=TK_GT || c==WO_GT );
  assert( op!=TK_GE || c==WO_GE );
  return c;
}

/*
** Advance to the next WhereTerm that matches according to the criteria
** established when the pScan object was initialized by whereScanInit().
** Return NULL if there are no more matching WhereTerms.
*/
static WhereTerm *whereScanNext(WhereScan *pScan){
  int iCur;            /* The cursor on the LHS of the term */
  int iColumn;         /* The column on the LHS of the term.  -1 for IPK */
  Expr *pX;            /* An expression being tested */
  WhereClause *pWC;    /* Shorthand for pScan->pWC */
  WhereTerm *pTerm;    /* The term being tested */
  int k = pScan->k;    /* Where to start scanning */

  while( pScan->iEquiv<=pScan->nEquiv ){
    iCur = pScan->aEquiv[pScan->iEquiv-2];
    iColumn = pScan->aEquiv[pScan->iEquiv-1];
    while( (pWC = pScan->pWC)!=0 ){
      for(pTerm=pWC->a+k; k<pWC->nTerm; k++, pTerm++){
        if( pTerm->leftCursor==iCur && pTerm->u.leftColumn==iColumn ){
          if( (pTerm->eOperator & WO_EQUIV)!=0
           && pScan->nEquiv<ArraySize(pScan->aEquiv)
          ){
            int j;
            pX = sqlite3ExprSkipCollate(pTerm->pExpr->pRight);
            assert( pX->op==TK_COLUMN );
            for(j=0; j<pScan->nEquiv; j+=2){
              if( pScan->aEquiv[j]==pX->iTable
               && pScan->aEquiv[j+1]==pX->iColumn ){
                  break;
              }
            }
            if( j==pScan->nEquiv ){
              pScan->aEquiv[j] = pX->iTable;
              pScan->aEquiv[j+1] = pX->iColumn;
              pScan->nEquiv += 2;
            }
          }
          if( (pTerm->eOperator & pScan->opMask)!=0 ){
            /* Verify the affinity and collating sequence match */
            if( pScan->zCollName && (pTerm->eOperator & WO_ISNULL)==0 ){
              CollSeq *pColl;
              Parse *pParse = pWC->pWInfo->pParse;
              pX = pTerm->pExpr;
              if( !sqlite3IndexAffinityOk(pX, pScan->idxaff) ){
                continue;
              }
              assert(pX->pLeft);
              pColl = sqlite3BinaryCompareCollSeq(pParse,
                                                  pX->pLeft, pX->pRight);
              if( pColl==0 ) pColl = pParse->db->pDfltColl;
              if( sqlite3StrICmp(pColl->zName, pScan->zCollName) ){
                continue;
              }
            }
            if( (pTerm->eOperator & WO_EQ)!=0
             && (pX = pTerm->pExpr->pRight)->op==TK_COLUMN
             && pX->iTable==pScan->aEquiv[0]
             && pX->iColumn==pScan->aEquiv[1]
            ){
              continue;
            }
            pScan->k = k+1;
            return pTerm;
          }
        }
      }
      pScan->pWC = pScan->pWC->pOuter;
      k = 0;
    }
    pScan->pWC = pScan->pOrigWC;
    k = 0;
    pScan->iEquiv += 2;
  }
  return 0;
}

/*
** Initialize a WHERE clause scanner object.  Return a pointer to the
** first match.  Return NULL if there are no matches.
**
** The scanner will be searching the WHERE clause pWC.  It will look
** for terms of the form "X <op> <expr>" where X is column iColumn of table
** iCur.  The <op> must be one of the operators described by opMask.
**
** If the search is for X and the WHERE clause contains terms of the
** form X=Y then this routine might also return terms of the form
** "Y <op> <expr>".  The number of levels of transitivity is limited,
** but is enough to handle most commonly occurring SQL statements.
**
** If X is not the INTEGER PRIMARY KEY then X must be compatible with
** index pIdx.
*/
static WhereTerm *whereScanInit(
  WhereScan *pScan,       /* The WhereScan object being initialized */
  WhereClause *pWC,       /* The WHERE clause to be scanned */
  int iCur,               /* Cursor to scan for */
  int iColumn,            /* Column to scan for */
  u32 opMask,             /* Operator(s) to scan for */
  Index *pIdx             /* Must be compatible with this index */
){
  int j;

  /* memset(pScan, 0, sizeof(*pScan)); */
  pScan->pOrigWC = pWC;
  pScan->pWC = pWC;
  if( pIdx && iColumn>=0 ){
    pScan->idxaff = pIdx->pTable->aCol[iColumn].affinity;
    for(j=0; pIdx->aiColumn[j]!=iColumn; j++){
      if( NEVER(j>=pIdx->nColumn) ) return 0;
    }
    pScan->zCollName = pIdx->azColl[j];
  }else{
    pScan->idxaff = 0;
    pScan->zCollName = 0;
  }
  pScan->opMask = opMask;
  pScan->k = 0;
  pScan->aEquiv[0] = iCur;
  pScan->aEquiv[1] = iColumn;
  pScan->nEquiv = 2;
  pScan->iEquiv = 2;
  return whereScanNext(pScan);
}

/*
** Search for a term in the WHERE clause that is of the form "X <op> <expr>"
** where X is a reference to the iColumn of table iCur and <op> is one of
** the WO_xx operator codes specified by the op parameter.
** Return a pointer to the term.  Return 0 if not found.
**
** The term returned might by Y=<expr> if there is another constraint in
** the WHERE clause that specifies that X=Y.  Any such constraints will be
** identified by the WO_EQUIV bit in the pTerm->eOperator field.  The
** aEquiv[] array holds X and all its equivalents, with each SQL variable
** taking up two slots in aEquiv[].  The first slot is for the cursor number
** and the second is for the column number.  There are 22 slots in aEquiv[]
** so that means we can look for X plus up to 10 other equivalent values.
** Hence a search for X will return <expr> if X=A1 and A1=A2 and A2=A3
** and ... and A9=A10 and A10=<expr>.
**
** If there are multiple terms in the WHERE clause of the form "X <op> <expr>"
** then try for the one with no dependencies on <expr> - in other words where
** <expr> is a constant expression of some kind.  Only return entries of
** the form "X <op> Y" where Y is a column in another table if no terms of
** the form "X <op> <const-expr>" exist.   If no terms with a constant RHS
** exist, try to return a term that does not use WO_EQUIV.
*/
static WhereTerm *findTerm(
  WhereClause *pWC,     /* The WHERE clause to be searched */
  int iCur,             /* Cursor number of LHS */
  int iColumn,          /* Column number of LHS */
  Bitmask notReady,     /* RHS must not overlap with this mask */
  u32 op,               /* Mask of WO_xx values describing operator */
  Index *pIdx           /* Must be compatible with this index, if not NULL */
){
  WhereTerm *pResult = 0;
  WhereTerm *p;
  WhereScan scan;

  p = whereScanInit(&scan, pWC, iCur, iColumn, op, pIdx);
  while( p ){
    if( (p->prereqRight & notReady)==0 ){
      if( p->prereqRight==0 && (p->eOperator&WO_EQ)!=0 ){
        return p;
      }
      if( pResult==0 ) pResult = p;
    }
    p = whereScanNext(&scan);
  }
  return pResult;
}

/* Forward reference */
static void exprAnalyze(SrcList*, WhereClause*, int);

/*
** Call exprAnalyze on all terms in a WHERE clause.  
*/
static void exprAnalyzeAll(
  SrcList *pTabList,       /* the FROM clause */
  WhereClause *pWC         /* the WHERE clause to be analyzed */
){
  int i;
  for(i=pWC->nTerm-1; i>=0; i--){
    exprAnalyze(pTabList, pWC, i);
  }
}

#ifndef SQLITE_OMIT_LIKE_OPTIMIZATION
/*
** Check to see if the given expression is a LIKE or GLOB operator that
** can be optimized using inequality constraints.  Return TRUE if it is
** so and false if not.
**
** In order for the operator to be optimizible, the RHS must be a string
** literal that does not begin with a wildcard.  
*/
static int isLikeOrGlob(
  Parse *pParse,    /* Parsing and code generating context */
  Expr *pExpr,      /* Test this expression */
  Expr **ppPrefix,  /* Pointer to TK_STRING expression with pattern prefix */
  int *pisComplete, /* True if the only wildcard is % in the last character */
  int *pnoCase      /* True if uppercase is equivalent to lowercase */
){
  const char *z = 0;         /* String on RHS of LIKE operator */
  Expr *pRight, *pLeft;      /* Right and left size of LIKE operator */
  ExprList *pList;           /* List of operands to the LIKE operator */
  int c;                     /* One character in z[] */
  int cnt;                   /* Number of non-wildcard prefix characters */
  char wc[3];                /* Wildcard characters */
  sqlite3 *db = pParse->db;  /* Database connection */
  sqlite3_value *pVal = 0;
  int op;                    /* Opcode of pRight */

  if( !sqlite3IsLikeFunction(db, pExpr, pnoCase, wc) ){
    return 0;
  }
#ifdef SQLITE_EBCDIC
  if( *pnoCase ) return 0;
#endif
  pList = pExpr->x.pList;
  pLeft = pList->a[1].pExpr;
  if( pLeft->op!=TK_COLUMN 
   || sqlite3ExprAffinity(pLeft)!=SQLITE_AFF_TEXT 
   || IsVirtual(pLeft->pTab)
  ){
    /* IMP: R-02065-49465 The left-hand side of the LIKE or GLOB operator must
    ** be the name of an indexed column with TEXT affinity. */
    return 0;
  }
  assert( pLeft->iColumn!=(-1) ); /* Because IPK never has AFF_TEXT */

  pRight = pList->a[0].pExpr;
  op = pRight->op;
  if( op==TK_REGISTER ){
    op = pRight->op2;
  }
  if( op==TK_VARIABLE ){
    Vdbe *pReprepare = pParse->pReprepare;
    int iCol = pRight->iColumn;
    pVal = sqlite3VdbeGetBoundValue(pReprepare, iCol, SQLITE_AFF_NONE);
    if( pVal && sqlite3_value_type(pVal)==SQLITE_TEXT ){
      z = (char *)sqlite3_value_text(pVal);
    }
    sqlite3VdbeSetVarmask(pParse->pVdbe, iCol);
    assert( pRight->op==TK_VARIABLE || pRight->op==TK_REGISTER );
  }else if( op==TK_STRING ){
    z = pRight->u.zToken;
  }
  if( z ){
    cnt = 0;
    while( (c=z[cnt])!=0 && c!=wc[0] && c!=wc[1] && c!=wc[2] ){
      cnt++;
    }
    if( cnt!=0 && 255!=(u8)z[cnt-1] ){
      Expr *pPrefix;
      *pisComplete = c==wc[0] && z[cnt+1]==0;
      pPrefix = sqlite3Expr(db, TK_STRING, z);
      if( pPrefix ) pPrefix->u.zToken[cnt] = 0;
      *ppPrefix = pPrefix;
      if( op==TK_VARIABLE ){
        Vdbe *v = pParse->pVdbe;
        sqlite3VdbeSetVarmask(v, pRight->iColumn);
        if( *pisComplete && pRight->u.zToken[1] ){
          /* If the rhs of the LIKE expression is a variable, and the current
          ** value of the variable means there is no need to invoke the LIKE
          ** function, then no OP_Variable will be added to the program.
          ** This causes problems for the sqlite3_bind_parameter_name()
          ** API. To workaround them, add a dummy OP_Variable here.
          */ 
          int r1 = sqlite3GetTempReg(pParse);
          sqlite3ExprCodeTarget(pParse, pRight, r1);
          sqlite3VdbeChangeP3(v, sqlite3VdbeCurrentAddr(v)-1, 0);
          sqlite3ReleaseTempReg(pParse, r1);
        }
      }
    }else{
      z = 0;
    }
  }

  sqlite3ValueFree(pVal);
  return (z!=0);
}
#endif /* SQLITE_OMIT_LIKE_OPTIMIZATION */


#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Check to see if the given expression is of the form
**
**         column MATCH expr
**
** If it is then return TRUE.  If not, return FALSE.
*/
static int isMatchOfColumn(
  Expr *pExpr      /* Test this expression */
){
  ExprList *pList;

  if( pExpr->op!=TK_FUNCTION ){
    return 0;
  }
  if( sqlite3StrICmp(pExpr->u.zToken,"match")!=0 ){
    return 0;
  }
  pList = pExpr->x.pList;
  if( pList->nExpr!=2 ){
    return 0;
  }
  if( pList->a[1].pExpr->op != TK_COLUMN ){
    return 0;
  }
  return 1;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** If the pBase expression originated in the ON or USING clause of
** a join, then transfer the appropriate markings over to derived.
*/
static void transferJoinMarkings(Expr *pDerived, Expr *pBase){
  if( pDerived ){
    pDerived->flags |= pBase->flags & EP_FromJoin;
    pDerived->iRightJoinTable = pBase->iRightJoinTable;
  }
}

#if !defined(SQLITE_OMIT_OR_OPTIMIZATION) && !defined(SQLITE_OMIT_SUBQUERY)
/*
** Analyze a term that consists of two or more OR-connected
** subterms.  So in:
**
**     ... WHERE  (a=5) AND (b=7 OR c=9 OR d=13) AND (d=13)
**                          ^^^^^^^^^^^^^^^^^^^^
**
** This routine analyzes terms such as the middle term in the above example.
** A WhereOrTerm object is computed and attached to the term under
** analysis, regardless of the outcome of the analysis.  Hence:
**
**     WhereTerm.wtFlags   |=  TERM_ORINFO
**     WhereTerm.u.pOrInfo  =  a dynamically allocated WhereOrTerm object
**
** The term being analyzed must have two or more of OR-connected subterms.
** A single subterm might be a set of AND-connected sub-subterms.
** Examples of terms under analysis:
**
**     (A)     t1.x=t2.y OR t1.x=t2.z OR t1.y=15 OR t1.z=t3.a+5
**     (B)     x=expr1 OR expr2=x OR x=expr3
**     (C)     t1.x=t2.y OR (t1.x=t2.z AND t1.y=15)
**     (D)     x=expr1 OR (y>11 AND y<22 AND z LIKE '*hello*')
**     (E)     (p.a=1 AND q.b=2 AND r.c=3) OR (p.x=4 AND q.y=5 AND r.z=6)
**
** CASE 1:
**
** If all subterms are of the form T.C=expr for some single column of C and
** a single table T (as shown in example B above) then create a new virtual
** term that is an equivalent IN expression.  In other words, if the term
** being analyzed is:
**
**      x = expr1  OR  expr2 = x  OR  x = expr3
**
** then create a new virtual term like this:
**
**      x IN (expr1,expr2,expr3)
**
** CASE 2:
**
** If all subterms are indexable by a single table T, then set
**
**     WhereTerm.eOperator              =  WO_OR
**     WhereTerm.u.pOrInfo->indexable  |=  the cursor number for table T
**
** A subterm is "indexable" if it is of the form
** "T.C <op> <expr>" where C is any column of table T and 
** <op> is one of "=", "<", "<=", ">", ">=", "IS NULL", or "IN".
** A subterm is also indexable if it is an AND of two or more
** subsubterms at least one of which is indexable.  Indexable AND 
** subterms have their eOperator set to WO_AND and they have
** u.pAndInfo set to a dynamically allocated WhereAndTerm object.
**
** From another point of view, "indexable" means that the subterm could
** potentially be used with an index if an appropriate index exists.
** This analysis does not consider whether or not the index exists; that
** is decided elsewhere.  This analysis only looks at whether subterms
** appropriate for indexing exist.
**
** All examples A through E above satisfy case 2.  But if a term
** also statisfies case 1 (such as B) we know that the optimizer will
** always prefer case 1, so in that case we pretend that case 2 is not
** satisfied.
**
** It might be the case that multiple tables are indexable.  For example,
** (E) above is indexable on tables P, Q, and R.
**
** Terms that satisfy case 2 are candidates for lookup by using
** separate indices to find rowids for each subterm and composing
** the union of all rowids using a RowSet object.  This is similar
** to "bitmap indices" in other database engines.
**
** OTHERWISE:
**
** If neither case 1 nor case 2 apply, then leave the eOperator set to
** zero.  This term is not useful for search.
*/
static void exprAnalyzeOrTerm(
  SrcList *pSrc,            /* the FROM clause */
  WhereClause *pWC,         /* the complete WHERE clause */
  int idxTerm               /* Index of the OR-term to be analyzed */
){
  WhereInfo *pWInfo = pWC->pWInfo;        /* WHERE clause processing context */
  Parse *pParse = pWInfo->pParse;         /* Parser context */
  sqlite3 *db = pParse->db;               /* Database connection */
  WhereTerm *pTerm = &pWC->a[idxTerm];    /* The term to be analyzed */
  Expr *pExpr = pTerm->pExpr;             /* The expression of the term */
  int i;                                  /* Loop counters */
  WhereClause *pOrWc;       /* Breakup of pTerm into subterms */
  WhereTerm *pOrTerm;       /* A Sub-term within the pOrWc */
  WhereOrInfo *pOrInfo;     /* Additional information associated with pTerm */
  Bitmask chngToIN;         /* Tables that might satisfy case 1 */
  Bitmask indexable;        /* Tables that are indexable, satisfying case 2 */

  /*
  ** Break the OR clause into its separate subterms.  The subterms are
  ** stored in a WhereClause structure containing within the WhereOrInfo
  ** object that is attached to the original OR clause term.
  */
  assert( (pTerm->wtFlags & (TERM_DYNAMIC|TERM_ORINFO|TERM_ANDINFO))==0 );
  assert( pExpr->op==TK_OR );
  pTerm->u.pOrInfo = pOrInfo = sqlite3DbMallocZero(db, sizeof(*pOrInfo));
  if( pOrInfo==0 ) return;
  pTerm->wtFlags |= TERM_ORINFO;
  pOrWc = &pOrInfo->wc;
  whereClauseInit(pOrWc, pWInfo);
  whereSplit(pOrWc, pExpr, TK_OR);
  exprAnalyzeAll(pSrc, pOrWc);
  if( db->mallocFailed ) return;
  assert( pOrWc->nTerm>=2 );

  /*
  ** Compute the set of tables that might satisfy cases 1 or 2.
  */
  indexable = ~(Bitmask)0;
  chngToIN = ~(Bitmask)0;
  for(i=pOrWc->nTerm-1, pOrTerm=pOrWc->a; i>=0 && indexable; i--, pOrTerm++){
    if( (pOrTerm->eOperator & WO_SINGLE)==0 ){
      WhereAndInfo *pAndInfo;
      assert( (pOrTerm->wtFlags & (TERM_ANDINFO|TERM_ORINFO))==0 );
      chngToIN = 0;
      pAndInfo = sqlite3DbMallocRaw(db, sizeof(*pAndInfo));
      if( pAndInfo ){
        WhereClause *pAndWC;
        WhereTerm *pAndTerm;
        int j;
        Bitmask b = 0;
        pOrTerm->u.pAndInfo = pAndInfo;
        pOrTerm->wtFlags |= TERM_ANDINFO;
        pOrTerm->eOperator = WO_AND;
        pAndWC = &pAndInfo->wc;
        whereClauseInit(pAndWC, pWC->pWInfo);
        whereSplit(pAndWC, pOrTerm->pExpr, TK_AND);
        exprAnalyzeAll(pSrc, pAndWC);
        pAndWC->pOuter = pWC;
        testcase( db->mallocFailed );
        if( !db->mallocFailed ){
          for(j=0, pAndTerm=pAndWC->a; j<pAndWC->nTerm; j++, pAndTerm++){
            assert( pAndTerm->pExpr );
            if( allowedOp(pAndTerm->pExpr->op) ){
              b |= getMask(&pWInfo->sMaskSet, pAndTerm->leftCursor);
            }
          }
        }
        indexable &= b;
      }
    }else if( pOrTerm->wtFlags & TERM_COPIED ){
      /* Skip this term for now.  We revisit it when we process the
      ** corresponding TERM_VIRTUAL term */
    }else{
      Bitmask b;
      b = getMask(&pWInfo->sMaskSet, pOrTerm->leftCursor);
      if( pOrTerm->wtFlags & TERM_VIRTUAL ){
        WhereTerm *pOther = &pOrWc->a[pOrTerm->iParent];
        b |= getMask(&pWInfo->sMaskSet, pOther->leftCursor);
      }
      indexable &= b;
      if( (pOrTerm->eOperator & WO_EQ)==0 ){
        chngToIN = 0;
      }else{
        chngToIN &= b;
      }
    }
  }

  /*
  ** Record the set of tables that satisfy case 2.  The set might be
  ** empty.
  */
  pOrInfo->indexable = indexable;
  pTerm->eOperator = indexable==0 ? 0 : WO_OR;

  /*
  ** chngToIN holds a set of tables that *might* satisfy case 1.  But
  ** we have to do some additional checking to see if case 1 really
  ** is satisfied.
  **
  ** chngToIN will hold either 0, 1, or 2 bits.  The 0-bit case means
  ** that there is no possibility of transforming the OR clause into an
  ** IN operator because one or more terms in the OR clause contain
  ** something other than == on a column in the single table.  The 1-bit
  ** case means that every term of the OR clause is of the form
  ** "table.column=expr" for some single table.  The one bit that is set
  ** will correspond to the common table.  We still need to check to make
  ** sure the same column is used on all terms.  The 2-bit case is when
  ** the all terms are of the form "table1.column=table2.column".  It
  ** might be possible to form an IN operator with either table1.column
  ** or table2.column as the LHS if either is common to every term of
  ** the OR clause.
  **
  ** Note that terms of the form "table.column1=table.column2" (the
  ** same table on both sizes of the ==) cannot be optimized.
  */
  if( chngToIN ){
    int okToChngToIN = 0;     /* True if the conversion to IN is valid */
    int iColumn = -1;         /* Column index on lhs of IN operator */
    int iCursor = -1;         /* Table cursor common to all terms */
    int j = 0;                /* Loop counter */

    /* Search for a table and column that appears on one side or the
    ** other of the == operator in every subterm.  That table and column
    ** will be recorded in iCursor and iColumn.  There might not be any
    ** such table and column.  Set okToChngToIN if an appropriate table
    ** and column is found but leave okToChngToIN false if not found.
    */
    for(j=0; j<2 && !okToChngToIN; j++){
      pOrTerm = pOrWc->a;
      for(i=pOrWc->nTerm-1; i>=0; i--, pOrTerm++){
        assert( pOrTerm->eOperator & WO_EQ );
        pOrTerm->wtFlags &= ~TERM_OR_OK;
        if( pOrTerm->leftCursor==iCursor ){
          /* This is the 2-bit case and we are on the second iteration and
          ** current term is from the first iteration.  So skip this term. */
          assert( j==1 );
          continue;
        }
        if( (chngToIN & getMask(&pWInfo->sMaskSet, pOrTerm->leftCursor))==0 ){
          /* This term must be of the form t1.a==t2.b where t2 is in the
          ** chngToIN set but t1 is not.  This term will be either preceeded
          ** or follwed by an inverted copy (t2.b==t1.a).  Skip this term 
          ** and use its inversion. */
          testcase( pOrTerm->wtFlags & TERM_COPIED );
          testcase( pOrTerm->wtFlags & TERM_VIRTUAL );
          assert( pOrTerm->wtFlags & (TERM_COPIED|TERM_VIRTUAL) );
          continue;
        }
        iColumn = pOrTerm->u.leftColumn;
        iCursor = pOrTerm->leftCursor;
        break;
      }
      if( i<0 ){
        /* No candidate table+column was found.  This can only occur
        ** on the second iteration */
        assert( j==1 );
        assert( IsPowerOfTwo(chngToIN) );
        assert( chngToIN==getMask(&pWInfo->sMaskSet, iCursor) );
        break;
      }
      testcase( j==1 );

      /* We have found a candidate table and column.  Check to see if that
      ** table and column is common to every term in the OR clause */
      okToChngToIN = 1;
      for(; i>=0 && okToChngToIN; i--, pOrTerm++){
        assert( pOrTerm->eOperator & WO_EQ );
        if( pOrTerm->leftCursor!=iCursor ){
          pOrTerm->wtFlags &= ~TERM_OR_OK;
        }else if( pOrTerm->u.leftColumn!=iColumn ){
          okToChngToIN = 0;
        }else{
          int affLeft, affRight;
          /* If the right-hand side is also a column, then the affinities
          ** of both right and left sides must be such that no type
          ** conversions are required on the right.  (Ticket #2249)
          */
          affRight = sqlite3ExprAffinity(pOrTerm->pExpr->pRight);
          affLeft = sqlite3ExprAffinity(pOrTerm->pExpr->pLeft);
          if( affRight!=0 && affRight!=affLeft ){
            okToChngToIN = 0;
          }else{
            pOrTerm->wtFlags |= TERM_OR_OK;
          }
        }
      }
    }

    /* At this point, okToChngToIN is true if original pTerm satisfies
    ** case 1.  In that case, construct a new virtual term that is 
    ** pTerm converted into an IN operator.
    */
    if( okToChngToIN ){
      Expr *pDup;            /* A transient duplicate expression */
      ExprList *pList = 0;   /* The RHS of the IN operator */
      Expr *pLeft = 0;       /* The LHS of the IN operator */
      Expr *pNew;            /* The complete IN operator */

      for(i=pOrWc->nTerm-1, pOrTerm=pOrWc->a; i>=0; i--, pOrTerm++){
        if( (pOrTerm->wtFlags & TERM_OR_OK)==0 ) continue;
        assert( pOrTerm->eOperator & WO_EQ );
        assert( pOrTerm->leftCursor==iCursor );
        assert( pOrTerm->u.leftColumn==iColumn );
        pDup = sqlite3ExprDup(db, pOrTerm->pExpr->pRight, 0);
        pList = sqlite3ExprListAppend(pWInfo->pParse, pList, pDup);
        pLeft = pOrTerm->pExpr->pLeft;
      }
      assert( pLeft!=0 );
      pDup = sqlite3ExprDup(db, pLeft, 0);
      pNew = sqlite3PExpr(pParse, TK_IN, pDup, 0, 0);
      if( pNew ){
        int idxNew;
        transferJoinMarkings(pNew, pExpr);
        assert( !ExprHasProperty(pNew, EP_xIsSelect) );
        pNew->x.pList = pList;
        idxNew = whereClauseInsert(pWC, pNew, TERM_VIRTUAL|TERM_DYNAMIC);
        testcase( idxNew==0 );
        exprAnalyze(pSrc, pWC, idxNew);
        pTerm = &pWC->a[idxTerm];
        pWC->a[idxNew].iParent = idxTerm;
        pTerm->nChild = 1;
      }else{
        sqlite3ExprListDelete(db, pList);
      }
      pTerm->eOperator = WO_NOOP;  /* case 1 trumps case 2 */
    }
  }
}
#endif /* !SQLITE_OMIT_OR_OPTIMIZATION && !SQLITE_OMIT_SUBQUERY */

/*
** The input to this routine is an WhereTerm structure with only the
** "pExpr" field filled in.  The job of this routine is to analyze the
** subexpression and populate all the other fields of the WhereTerm
** structure.
**
** If the expression is of the form "<expr> <op> X" it gets commuted
** to the standard form of "X <op> <expr>".
**
** If the expression is of the form "X <op> Y" where both X and Y are
** columns, then the original expression is unchanged and a new virtual
** term of the form "Y <op> X" is added to the WHERE clause and
** analyzed separately.  The original term is marked with TERM_COPIED
** and the new term is marked with TERM_DYNAMIC (because it's pExpr
** needs to be freed with the WhereClause) and TERM_VIRTUAL (because it
** is a commuted copy of a prior term.)  The original term has nChild=1
** and the copy has idxParent set to the index of the original term.
*/
static void exprAnalyze(
  SrcList *pSrc,            /* the FROM clause */
  WhereClause *pWC,         /* the WHERE clause */
  int idxTerm               /* Index of the term to be analyzed */
){
  WhereInfo *pWInfo = pWC->pWInfo; /* WHERE clause processing context */
  WhereTerm *pTerm;                /* The term to be analyzed */
  WhereMaskSet *pMaskSet;          /* Set of table index masks */
  Expr *pExpr;                     /* The expression to be analyzed */
  Bitmask prereqLeft;              /* Prerequesites of the pExpr->pLeft */
  Bitmask prereqAll;               /* Prerequesites of pExpr */
  Bitmask extraRight = 0;          /* Extra dependencies on LEFT JOIN */
  Expr *pStr1 = 0;                 /* RHS of LIKE/GLOB operator */
  int isComplete = 0;              /* RHS of LIKE/GLOB ends with wildcard */
  int noCase = 0;                  /* LIKE/GLOB distinguishes case */
  int op;                          /* Top-level operator.  pExpr->op */
  Parse *pParse = pWInfo->pParse;  /* Parsing context */
  sqlite3 *db = pParse->db;        /* Database connection */

  if( db->mallocFailed ){
    return;
  }
  pTerm = &pWC->a[idxTerm];
  pMaskSet = &pWInfo->sMaskSet;
  pExpr = pTerm->pExpr;
  assert( pExpr->op!=TK_AS && pExpr->op!=TK_COLLATE );
  prereqLeft = exprTableUsage(pMaskSet, pExpr->pLeft);
  op = pExpr->op;
  if( op==TK_IN ){
    assert( pExpr->pRight==0 );
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      pTerm->prereqRight = exprSelectTableUsage(pMaskSet, pExpr->x.pSelect);
    }else{
      pTerm->prereqRight = exprListTableUsage(pMaskSet, pExpr->x.pList);
    }
  }else if( op==TK_ISNULL ){
    pTerm->prereqRight = 0;
  }else{
    pTerm->prereqRight = exprTableUsage(pMaskSet, pExpr->pRight);
  }
  prereqAll = exprTableUsage(pMaskSet, pExpr);
  if( ExprHasProperty(pExpr, EP_FromJoin) ){
    Bitmask x = getMask(pMaskSet, pExpr->iRightJoinTable);
    prereqAll |= x;
    extraRight = x-1;  /* ON clause terms may not be used with an index
                       ** on left table of a LEFT JOIN.  Ticket #3015 */
  }
  pTerm->prereqAll = prereqAll;
  pTerm->leftCursor = -1;
  pTerm->iParent = -1;
  pTerm->eOperator = 0;
  if( allowedOp(op) ){
    Expr *pLeft = sqlite3ExprSkipCollate(pExpr->pLeft);
    Expr *pRight = sqlite3ExprSkipCollate(pExpr->pRight);
    u16 opMask = (pTerm->prereqRight & prereqLeft)==0 ? WO_ALL : WO_EQUIV;
    if( pLeft->op==TK_COLUMN ){
      pTerm->leftCursor = pLeft->iTable;
      pTerm->u.leftColumn = pLeft->iColumn;
      pTerm->eOperator = operatorMask(op) & opMask;
    }
    if( pRight && pRight->op==TK_COLUMN ){
      WhereTerm *pNew;
      Expr *pDup;
      u16 eExtraOp = 0;        /* Extra bits for pNew->eOperator */
      if( pTerm->leftCursor>=0 ){
        int idxNew;
        pDup = sqlite3ExprDup(db, pExpr, 0);
        if( db->mallocFailed ){
          sqlite3ExprDelete(db, pDup);
          return;
        }
        idxNew = whereClauseInsert(pWC, pDup, TERM_VIRTUAL|TERM_DYNAMIC);
        if( idxNew==0 ) return;
        pNew = &pWC->a[idxNew];
        pNew->iParent = idxTerm;
        pTerm = &pWC->a[idxTerm];
        pTerm->nChild = 1;
        pTerm->wtFlags |= TERM_COPIED;
        if( pExpr->op==TK_EQ
         && !ExprHasProperty(pExpr, EP_FromJoin)
         && OptimizationEnabled(db, SQLITE_Transitive)
        ){
          pTerm->eOperator |= WO_EQUIV;
          eExtraOp = WO_EQUIV;
        }
      }else{
        pDup = pExpr;
        pNew = pTerm;
      }
      exprCommute(pParse, pDup);
      pLeft = sqlite3ExprSkipCollate(pDup->pLeft);
      pNew->leftCursor = pLeft->iTable;
      pNew->u.leftColumn = pLeft->iColumn;
      testcase( (prereqLeft | extraRight) != prereqLeft );
      pNew->prereqRight = prereqLeft | extraRight;
      pNew->prereqAll = prereqAll;
      pNew->eOperator = (operatorMask(pDup->op) + eExtraOp) & opMask;
    }
  }

#ifndef SQLITE_OMIT_BETWEEN_OPTIMIZATION
  /* If a term is the BETWEEN operator, create two new virtual terms
  ** that define the range that the BETWEEN implements.  For example:
  **
  **      a BETWEEN b AND c
  **
  ** is converted into:
  **
  **      (a BETWEEN b AND c) AND (a>=b) AND (a<=c)
  **
  ** The two new terms are added onto the end of the WhereClause object.
  ** The new terms are "dynamic" and are children of the original BETWEEN
  ** term.  That means that if the BETWEEN term is coded, the children are
  ** skipped.  Or, if the children are satisfied by an index, the original
  ** BETWEEN term is skipped.
  */
  else if( pExpr->op==TK_BETWEEN && pWC->op==TK_AND ){
    ExprList *pList = pExpr->x.pList;
    int i;
    static const u8 ops[] = {TK_GE, TK_LE};
    assert( pList!=0 );
    assert( pList->nExpr==2 );
    for(i=0; i<2; i++){
      Expr *pNewExpr;
      int idxNew;
      pNewExpr = sqlite3PExpr(pParse, ops[i], 
                             sqlite3ExprDup(db, pExpr->pLeft, 0),
                             sqlite3ExprDup(db, pList->a[i].pExpr, 0), 0);
      transferJoinMarkings(pNewExpr, pExpr);
      idxNew = whereClauseInsert(pWC, pNewExpr, TERM_VIRTUAL|TERM_DYNAMIC);
      testcase( idxNew==0 );
      exprAnalyze(pSrc, pWC, idxNew);
      pTerm = &pWC->a[idxTerm];
      pWC->a[idxNew].iParent = idxTerm;
    }
    pTerm->nChild = 2;
  }
#endif /* SQLITE_OMIT_BETWEEN_OPTIMIZATION */

#if !defined(SQLITE_OMIT_OR_OPTIMIZATION) && !defined(SQLITE_OMIT_SUBQUERY)
  /* Analyze a term that is composed of two or more subterms connected by
  ** an OR operator.
  */
  else if( pExpr->op==TK_OR ){
    assert( pWC->op==TK_AND );
    exprAnalyzeOrTerm(pSrc, pWC, idxTerm);
    pTerm = &pWC->a[idxTerm];
  }
#endif /* SQLITE_OMIT_OR_OPTIMIZATION */

#ifndef SQLITE_OMIT_LIKE_OPTIMIZATION
  /* Add constraints to reduce the search space on a LIKE or GLOB
  ** operator.
  **
  ** A like pattern of the form "x LIKE 'abc%'" is changed into constraints
  **
  **          x>='abc' AND x<'abd' AND x LIKE 'abc%'
  **
  ** The last character of the prefix "abc" is incremented to form the
  ** termination condition "abd".
  */
  if( pWC->op==TK_AND 
   && isLikeOrGlob(pParse, pExpr, &pStr1, &isComplete, &noCase)
  ){
    Expr *pLeft;       /* LHS of LIKE/GLOB operator */
    Expr *pStr2;       /* Copy of pStr1 - RHS of LIKE/GLOB operator */
    Expr *pNewExpr1;
    Expr *pNewExpr2;
    int idxNew1;
    int idxNew2;
    Token sCollSeqName;  /* Name of collating sequence */

    pLeft = pExpr->x.pList->a[1].pExpr;
    pStr2 = sqlite3ExprDup(db, pStr1, 0);
    if( !db->mallocFailed ){
      u8 c, *pC;       /* Last character before the first wildcard */
      pC = (u8*)&pStr2->u.zToken[sqlite3Strlen30(pStr2->u.zToken)-1];
      c = *pC;
      if( noCase ){
        /* The point is to increment the last character before the first
        ** wildcard.  But if we increment '@', that will push it into the
        ** alphabetic range where case conversions will mess up the 
        ** inequality.  To avoid this, make sure to also run the full
        ** LIKE on all candidate expressions by clearing the isComplete flag
        */
        if( c=='A'-1 ) isComplete = 0;
        c = sqlite3UpperToLower[c];
      }
      *pC = c + 1;
    }
    sCollSeqName.z = noCase ? "NOCASE" : "BINARY";
    sCollSeqName.n = 6;
    pNewExpr1 = sqlite3ExprDup(db, pLeft, 0);
    pNewExpr1 = sqlite3PExpr(pParse, TK_GE, 
           sqlite3ExprAddCollateToken(pParse,pNewExpr1,&sCollSeqName),
           pStr1, 0);
    transferJoinMarkings(pNewExpr1, pExpr);
    idxNew1 = whereClauseInsert(pWC, pNewExpr1, TERM_VIRTUAL|TERM_DYNAMIC);
    testcase( idxNew1==0 );
    exprAnalyze(pSrc, pWC, idxNew1);
    pNewExpr2 = sqlite3ExprDup(db, pLeft, 0);
    pNewExpr2 = sqlite3PExpr(pParse, TK_LT,
           sqlite3ExprAddCollateToken(pParse,pNewExpr2,&sCollSeqName),
           pStr2, 0);
    transferJoinMarkings(pNewExpr2, pExpr);
    idxNew2 = whereClauseInsert(pWC, pNewExpr2, TERM_VIRTUAL|TERM_DYNAMIC);
    testcase( idxNew2==0 );
    exprAnalyze(pSrc, pWC, idxNew2);
    pTerm = &pWC->a[idxTerm];
    if( isComplete ){
      pWC->a[idxNew1].iParent = idxTerm;
      pWC->a[idxNew2].iParent = idxTerm;
      pTerm->nChild = 2;
    }
  }
#endif /* SQLITE_OMIT_LIKE_OPTIMIZATION */

#ifndef SQLITE_OMIT_VIRTUALTABLE
  /* Add a WO_MATCH auxiliary term to the constraint set if the
  ** current expression is of the form:  column MATCH expr.
  ** This information is used by the xBestIndex methods of
  ** virtual tables.  The native query optimizer does not attempt
  ** to do anything with MATCH functions.
  */
  if( isMatchOfColumn(pExpr) ){
    int idxNew;
    Expr *pRight, *pLeft;
    WhereTerm *pNewTerm;
    Bitmask prereqColumn, prereqExpr;

    pRight = pExpr->x.pList->a[0].pExpr;
    pLeft = pExpr->x.pList->a[1].pExpr;
    prereqExpr = exprTableUsage(pMaskSet, pRight);
    prereqColumn = exprTableUsage(pMaskSet, pLeft);
    if( (prereqExpr & prereqColumn)==0 ){
      Expr *pNewExpr;
      pNewExpr = sqlite3PExpr(pParse, TK_MATCH, 
                              0, sqlite3ExprDup(db, pRight, 0), 0);
      idxNew = whereClauseInsert(pWC, pNewExpr, TERM_VIRTUAL|TERM_DYNAMIC);
      testcase( idxNew==0 );
      pNewTerm = &pWC->a[idxNew];
      pNewTerm->prereqRight = prereqExpr;
      pNewTerm->leftCursor = pLeft->iTable;
      pNewTerm->u.leftColumn = pLeft->iColumn;
      pNewTerm->eOperator = WO_MATCH;
      pNewTerm->iParent = idxTerm;
      pTerm = &pWC->a[idxTerm];
      pTerm->nChild = 1;
      pTerm->wtFlags |= TERM_COPIED;
      pNewTerm->prereqAll = pTerm->prereqAll;
    }
  }
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef SQLITE_ENABLE_STAT3
  /* When sqlite_stat3 histogram data is available an operator of the
  ** form "x IS NOT NULL" can sometimes be evaluated more efficiently
  ** as "x>NULL" if x is not an INTEGER PRIMARY KEY.  So construct a
  ** virtual term of that form.
  **
  ** Note that the virtual term must be tagged with TERM_VNULL.  This
  ** TERM_VNULL tag will suppress the not-null check at the beginning
  ** of the loop.  Without the TERM_VNULL flag, the not-null check at
  ** the start of the loop will prevent any results from being returned.
  */
  if( pExpr->op==TK_NOTNULL
   && pExpr->pLeft->op==TK_COLUMN
   && pExpr->pLeft->iColumn>=0
   && OptimizationEnabled(db, SQLITE_Stat3)
  ){
    Expr *pNewExpr;
    Expr *pLeft = pExpr->pLeft;
    int idxNew;
    WhereTerm *pNewTerm;

    pNewExpr = sqlite3PExpr(pParse, TK_GT,
                            sqlite3ExprDup(db, pLeft, 0),
                            sqlite3PExpr(pParse, TK_NULL, 0, 0, 0), 0);

    idxNew = whereClauseInsert(pWC, pNewExpr,
                              TERM_VIRTUAL|TERM_DYNAMIC|TERM_VNULL);
    if( idxNew ){
      pNewTerm = &pWC->a[idxNew];
      pNewTerm->prereqRight = 0;
      pNewTerm->leftCursor = pLeft->iTable;
      pNewTerm->u.leftColumn = pLeft->iColumn;
      pNewTerm->eOperator = WO_GT;
      pNewTerm->iParent = idxTerm;
      pTerm = &pWC->a[idxTerm];
      pTerm->nChild = 1;
      pTerm->wtFlags |= TERM_COPIED;
      pNewTerm->prereqAll = pTerm->prereqAll;
    }
  }
#endif /* SQLITE_ENABLE_STAT */

  /* Prevent ON clause terms of a LEFT JOIN from being used to drive
  ** an index for tables to the left of the join.
  */
  pTerm->prereqRight |= extraRight;
}

/*
** This function searches pList for a entry that matches the iCol-th column
** of index pIdx.
**
** If such an expression is found, its index in pList->a[] is returned. If
** no expression is found, -1 is returned.
*/
static int findIndexCol(
  Parse *pParse,                  /* Parse context */
  ExprList *pList,                /* Expression list to search */
  int iBase,                      /* Cursor for table associated with pIdx */
  Index *pIdx,                    /* Index to match column of */
  int iCol                        /* Column of index to match */
){
  int i;
  const char *zColl = pIdx->azColl[iCol];

  for(i=0; i<pList->nExpr; i++){
    Expr *p = sqlite3ExprSkipCollate(pList->a[i].pExpr);
    if( p->op==TK_COLUMN
     && p->iColumn==pIdx->aiColumn[iCol]
     && p->iTable==iBase
    ){
      CollSeq *pColl = sqlite3ExprCollSeq(pParse, pList->a[i].pExpr);
      if( ALWAYS(pColl) && 0==sqlite3StrICmp(pColl->zName, zColl) ){
        return i;
      }
    }
  }

  return -1;
}

/*
** Return true if the DISTINCT expression-list passed as the third argument
** is redundant.
**
** A DISTINCT list is redundant if the database contains some subset of
** columns that are unique and non-null.
*/
static int isDistinctRedundant(
  Parse *pParse,            /* Parsing context */
  SrcList *pTabList,        /* The FROM clause */
  WhereClause *pWC,         /* The WHERE clause */
  ExprList *pDistinct       /* The result set that needs to be DISTINCT */
){
  Table *pTab;
  Index *pIdx;
  int i;                          
  int iBase;

  /* If there is more than one table or sub-select in the FROM clause of
  ** this query, then it will not be possible to show that the DISTINCT 
  ** clause is redundant. */
  if( pTabList->nSrc!=1 ) return 0;
  iBase = pTabList->a[0].iCursor;
  pTab = pTabList->a[0].pTab;

  /* If any of the expressions is an IPK column on table iBase, then return 
  ** true. Note: The (p->iTable==iBase) part of this test may be false if the
  ** current SELECT is a correlated sub-query.
  */
  for(i=0; i<pDistinct->nExpr; i++){
    Expr *p = sqlite3ExprSkipCollate(pDistinct->a[i].pExpr);
    if( p->op==TK_COLUMN && p->iTable==iBase && p->iColumn<0 ) return 1;
  }

  /* Loop through all indices on the table, checking each to see if it makes
  ** the DISTINCT qualifier redundant. It does so if:
  **
  **   1. The index is itself UNIQUE, and
  **
  **   2. All of the columns in the index are either part of the pDistinct
  **      list, or else the WHERE clause contains a term of the form "col=X",
  **      where X is a constant value. The collation sequences of the
  **      comparison and select-list expressions must match those of the index.
  **
  **   3. All of those index columns for which the WHERE clause does not
  **      contain a "col=X" term are subject to a NOT NULL constraint.
  */
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    if( pIdx->onError==OE_None ) continue;
    for(i=0; i<pIdx->nColumn; i++){
      int iCol = pIdx->aiColumn[i];
      if( 0==findTerm(pWC, iBase, iCol, ~(Bitmask)0, WO_EQ, pIdx) ){
        int iIdxCol = findIndexCol(pParse, pDistinct, iBase, pIdx, i);
        if( iIdxCol<0 || pTab->aCol[pIdx->aiColumn[i]].notNull==0 ){
          break;
        }
      }
    }
    if( i==pIdx->nColumn ){
      /* This index implies that the DISTINCT qualifier is redundant. */
      return 1;
    }
  }

  return 0;
}

/* 
** Find (an approximate) sum of two WhereCosts.  This computation is
** not a simple "+" operator because WhereCost is stored as a logarithmic
** value.
** 
*/
static WhereCost whereCostAdd(WhereCost a, WhereCost b){
  static const unsigned char x[] = {
     10, 10,                         /* 0,1 */
      9, 9,                          /* 2,3 */
      8, 8,                          /* 4,5 */
      7, 7, 7,                       /* 6,7,8 */
      6, 6, 6,                       /* 9,10,11 */
      5, 5, 5,                       /* 12-14 */
      4, 4, 4, 4,                    /* 15-18 */
      3, 3, 3, 3, 3, 3,              /* 19-24 */
      2, 2, 2, 2, 2, 2, 2,           /* 25-31 */
  };
  if( a>=b ){
    if( a>b+49 ) return a;
    if( a>b+31 ) return a+1;
    return a+x[a-b];
  }else{
    if( b>a+49 ) return b;
    if( b>a+31 ) return b+1;
    return b+x[b-a];
  }
}

/*
** Convert an integer into a WhereCost.  In other words, compute a
** good approximatation for 10*log2(x).
*/
static WhereCost whereCost(tRowcnt x){
  static WhereCost a[] = { 0, 2, 3, 5, 6, 7, 8, 9 };
  WhereCost y = 40;
  if( x<8 ){
    if( x<2 ) return 0;
    while( x<8 ){  y -= 10; x <<= 1; }
  }else{
    while( x>255 ){ y += 40; x >>= 4; }
    while( x>15 ){  y += 10; x >>= 1; }
  }
  return a[x&7] + y - 10;
}

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Convert a double (as received from xBestIndex of a virtual table)
** into a WhereCost.  In other words, compute an approximation for
** 10*log2(x).
*/
static WhereCost whereCostFromDouble(double x){
  u64 a;
  WhereCost e;
  assert( sizeof(x)==8 && sizeof(a)==8 );
  if( x<=1 ) return 0;
  if( x<=2000000000 ) return whereCost((tRowcnt)x);
  memcpy(&a, &x, 8);
  e = (a>>52) - 1022;
  return e*10;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** Estimate the logarithm of the input value to base 2.
*/
static WhereCost estLog(WhereCost N){
  WhereCost x = whereCost(N);
  return x>33 ? x - 33 : 0;
}

/*
** Two routines for printing the content of an sqlite3_index_info
** structure.  Used for testing and debugging only.  If neither
** SQLITE_TEST or SQLITE_DEBUG are defined, then these routines
** are no-ops.
*/
#if !defined(SQLITE_OMIT_VIRTUALTABLE) && defined(WHERETRACE_ENABLED)
static void TRACE_IDX_INPUTS(sqlite3_index_info *p){
  int i;
  if( !sqlite3WhereTrace ) return;
  for(i=0; i<p->nConstraint; i++){
    sqlite3DebugPrintf("  constraint[%d]: col=%d termid=%d op=%d usabled=%d\n",
       i,
       p->aConstraint[i].iColumn,
       p->aConstraint[i].iTermOffset,
       p->aConstraint[i].op,
       p->aConstraint[i].usable);
  }
  for(i=0; i<p->nOrderBy; i++){
    sqlite3DebugPrintf("  orderby[%d]: col=%d desc=%d\n",
       i,
       p->aOrderBy[i].iColumn,
       p->aOrderBy[i].desc);
  }
}
static void TRACE_IDX_OUTPUTS(sqlite3_index_info *p){
  int i;
  if( !sqlite3WhereTrace ) return;
  for(i=0; i<p->nConstraint; i++){
    sqlite3DebugPrintf("  usage[%d]: argvIdx=%d omit=%d\n",
       i,
       p->aConstraintUsage[i].argvIndex,
       p->aConstraintUsage[i].omit);
  }
  sqlite3DebugPrintf("  idxNum=%d\n", p->idxNum);
  sqlite3DebugPrintf("  idxStr=%s\n", p->idxStr);
  sqlite3DebugPrintf("  orderByConsumed=%d\n", p->orderByConsumed);
  sqlite3DebugPrintf("  estimatedCost=%g\n", p->estimatedCost);
}
#else
#define TRACE_IDX_INPUTS(A)
#define TRACE_IDX_OUTPUTS(A)
#endif

#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
/*
** Return TRUE if the WHERE clause term pTerm is of a form where it
** could be used with an index to access pSrc, assuming an appropriate
** index existed.
*/
static int termCanDriveIndex(
  WhereTerm *pTerm,              /* WHERE clause term to check */
  struct SrcList_item *pSrc,     /* Table we are trying to access */
  Bitmask notReady               /* Tables in outer loops of the join */
){
  char aff;
  if( pTerm->leftCursor!=pSrc->iCursor ) return 0;
  if( (pTerm->eOperator & WO_EQ)==0 ) return 0;
  if( (pTerm->prereqRight & notReady)!=0 ) return 0;
  if( pTerm->u.leftColumn<0 ) return 0;
  aff = pSrc->pTab->aCol[pTerm->u.leftColumn].affinity;
  if( !sqlite3IndexAffinityOk(pTerm->pExpr, aff) ) return 0;
  return 1;
}
#endif


#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
/*
** Generate code to construct the Index object for an automatic index
** and to set up the WhereLevel object pLevel so that the code generator
** makes use of the automatic index.
*/
static void constructAutomaticIndex(
  Parse *pParse,              /* The parsing context */
  WhereClause *pWC,           /* The WHERE clause */
  struct SrcList_item *pSrc,  /* The FROM clause term to get the next index */
  Bitmask notReady,           /* Mask of cursors that are not available */
  WhereLevel *pLevel          /* Write new index here */
){
  int nColumn;                /* Number of columns in the constructed index */
  WhereTerm *pTerm;           /* A single term of the WHERE clause */
  WhereTerm *pWCEnd;          /* End of pWC->a[] */
  int nByte;                  /* Byte of memory needed for pIdx */
  Index *pIdx;                /* Object describing the transient index */
  Vdbe *v;                    /* Prepared statement under construction */
  int addrInit;               /* Address of the initialization bypass jump */
  Table *pTable;              /* The table being indexed */
  KeyInfo *pKeyinfo;          /* Key information for the index */   
  int addrTop;                /* Top of the index fill loop */
  int regRecord;              /* Register holding an index record */
  int n;                      /* Column counter */
  int i;                      /* Loop counter */
  int mxBitCol;               /* Maximum column in pSrc->colUsed */
  CollSeq *pColl;             /* Collating sequence to on a column */
  WhereLoop *pLoop;           /* The Loop object */
  Bitmask idxCols;            /* Bitmap of columns used for indexing */
  Bitmask extraCols;          /* Bitmap of additional columns */
  u8 sentWarning = 0;         /* True if a warnning has been issued */

  /* Generate code to skip over the creation and initialization of the
  ** transient index on 2nd and subsequent iterations of the loop. */
  v = pParse->pVdbe;
  assert( v!=0 );
  addrInit = sqlite3CodeOnce(pParse);

  /* Count the number of columns that will be added to the index
  ** and used to match WHERE clause constraints */
  nColumn = 0;
  pTable = pSrc->pTab;
  pWCEnd = &pWC->a[pWC->nTerm];
  pLoop = pLevel->pWLoop;
  idxCols = 0;
  for(pTerm=pWC->a; pTerm<pWCEnd; pTerm++){
    if( termCanDriveIndex(pTerm, pSrc, notReady) ){
      int iCol = pTerm->u.leftColumn;
      Bitmask cMask = iCol>=BMS ? MASKBIT(BMS-1) : MASKBIT(iCol);
      testcase( iCol==BMS );
      testcase( iCol==BMS-1 );
      if( !sentWarning ){
        sqlite3_log(SQLITE_WARNING_AUTOINDEX,
            "automatic index on %s(%s)", pTable->zName,
            pTable->aCol[iCol].zName);
        sentWarning = 1;
      }
      if( (idxCols & cMask)==0 ){
        if( whereLoopResize(pParse->db, pLoop, nColumn+1) ) return;
        pLoop->aLTerm[nColumn++] = pTerm;
        idxCols |= cMask;
      }
    }
  }
  assert( nColumn>0 );
  pLoop->u.btree.nEq = pLoop->nLTerm = nColumn;
  pLoop->wsFlags = WHERE_COLUMN_EQ | WHERE_IDX_ONLY | WHERE_INDEXED
                     | WHERE_AUTO_INDEX;

  /* Count the number of additional columns needed to create a
  ** covering index.  A "covering index" is an index that contains all
  ** columns that are needed by the query.  With a covering index, the
  ** original table never needs to be accessed.  Automatic indices must
  ** be a covering index because the index will not be updated if the
  ** original table changes and the index and table cannot both be used
  ** if they go out of sync.
  */
  extraCols = pSrc->colUsed & (~idxCols | MASKBIT(BMS-1));
  mxBitCol = (pTable->nCol >= BMS-1) ? BMS-1 : pTable->nCol;
  testcase( pTable->nCol==BMS-1 );
  testcase( pTable->nCol==BMS-2 );
  for(i=0; i<mxBitCol; i++){
    if( extraCols & MASKBIT(i) ) nColumn++;
  }
  if( pSrc->colUsed & MASKBIT(BMS-1) ){
    nColumn += pTable->nCol - BMS + 1;
  }
  pLoop->wsFlags |= WHERE_COLUMN_EQ | WHERE_IDX_ONLY;

  /* Construct the Index object to describe this index */
  nByte = sizeof(Index);
  nByte += nColumn*sizeof(int);     /* Index.aiColumn */
  nByte += nColumn*sizeof(char*);   /* Index.azColl */
  nByte += nColumn;                 /* Index.aSortOrder */
  pIdx = sqlite3DbMallocZero(pParse->db, nByte);
  if( pIdx==0 ) return;
  pLoop->u.btree.pIndex = pIdx;
  pIdx->azColl = (char**)&pIdx[1];
  pIdx->aiColumn = (int*)&pIdx->azColl[nColumn];
  pIdx->aSortOrder = (u8*)&pIdx->aiColumn[nColumn];
  pIdx->zName = "auto-index";
  pIdx->nColumn = nColumn;
  pIdx->pTable = pTable;
  n = 0;
  idxCols = 0;
  for(pTerm=pWC->a; pTerm<pWCEnd; pTerm++){
    if( termCanDriveIndex(pTerm, pSrc, notReady) ){
      int iCol = pTerm->u.leftColumn;
      Bitmask cMask = iCol>=BMS ? MASKBIT(BMS-1) : MASKBIT(iCol);
      testcase( iCol==BMS-1 );
      testcase( iCol==BMS );
      if( (idxCols & cMask)==0 ){
        Expr *pX = pTerm->pExpr;
        idxCols |= cMask;
        pIdx->aiColumn[n] = pTerm->u.leftColumn;
        pColl = sqlite3BinaryCompareCollSeq(pParse, pX->pLeft, pX->pRight);
        pIdx->azColl[n] = ALWAYS(pColl) ? pColl->zName : "BINARY";
        n++;
      }
    }
  }
  assert( (u32)n==pLoop->u.btree.nEq );

  /* Add additional columns needed to make the automatic index into
  ** a covering index */
  for(i=0; i<mxBitCol; i++){
    if( extraCols & MASKBIT(i) ){
      pIdx->aiColumn[n] = i;
      pIdx->azColl[n] = "BINARY";
      n++;
    }
  }
  if( pSrc->colUsed & MASKBIT(BMS-1) ){
    for(i=BMS-1; i<pTable->nCol; i++){
      pIdx->aiColumn[n] = i;
      pIdx->azColl[n] = "BINARY";
      n++;
    }
  }
  assert( n==nColumn );

  /* Create the automatic index */
  pKeyinfo = sqlite3IndexKeyinfo(pParse, pIdx);
  assert( pLevel->iIdxCur>=0 );
  pLevel->iIdxCur = pParse->nTab++;
  sqlite3VdbeAddOp4(v, OP_OpenAutoindex, pLevel->iIdxCur, nColumn+1, 0,
                    (char*)pKeyinfo, P4_KEYINFO_HANDOFF);
  VdbeComment((v, "for %s", pTable->zName));

  /* Fill the automatic index with content */
  addrTop = sqlite3VdbeAddOp1(v, OP_Rewind, pLevel->iTabCur);
  regRecord = sqlite3GetTempReg(pParse);
  sqlite3GenerateIndexKey(pParse, pIdx, pLevel->iTabCur, regRecord, 1, 0);
  sqlite3VdbeAddOp2(v, OP_IdxInsert, pLevel->iIdxCur, regRecord);
  sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
  sqlite3VdbeAddOp2(v, OP_Next, pLevel->iTabCur, addrTop+1);
  sqlite3VdbeChangeP5(v, SQLITE_STMTSTATUS_AUTOINDEX);
  sqlite3VdbeJumpHere(v, addrTop);
  sqlite3ReleaseTempReg(pParse, regRecord);
  
  /* Jump here when skipping the initialization */
  sqlite3VdbeJumpHere(v, addrInit);
}
#endif /* SQLITE_OMIT_AUTOMATIC_INDEX */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Allocate and populate an sqlite3_index_info structure. It is the 
** responsibility of the caller to eventually release the structure
** by passing the pointer returned by this function to sqlite3_free().
*/
static sqlite3_index_info *allocateIndexInfo(
  Parse *pParse,
  WhereClause *pWC,
  struct SrcList_item *pSrc,
  ExprList *pOrderBy
){
  int i, j;
  int nTerm;
  struct sqlite3_index_constraint *pIdxCons;
  struct sqlite3_index_orderby *pIdxOrderBy;
  struct sqlite3_index_constraint_usage *pUsage;
  WhereTerm *pTerm;
  int nOrderBy;
  sqlite3_index_info *pIdxInfo;

  /* Count the number of possible WHERE clause constraints referring
  ** to this virtual table */
  for(i=nTerm=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    if( pTerm->leftCursor != pSrc->iCursor ) continue;
    assert( IsPowerOfTwo(pTerm->eOperator & ~WO_EQUIV) );
    testcase( pTerm->eOperator & WO_IN );
    testcase( pTerm->eOperator & WO_ISNULL );
    if( pTerm->eOperator & (WO_ISNULL) ) continue;
    if( pTerm->wtFlags & TERM_VNULL ) continue;
    nTerm++;
  }

  /* If the ORDER BY clause contains only columns in the current 
  ** virtual table then allocate space for the aOrderBy part of
  ** the sqlite3_index_info structure.
  */
  nOrderBy = 0;
  if( pOrderBy ){
    int n = pOrderBy->nExpr;
    for(i=0; i<n; i++){
      Expr *pExpr = pOrderBy->a[i].pExpr;
      if( pExpr->op!=TK_COLUMN || pExpr->iTable!=pSrc->iCursor ) break;
    }
    if( i==n){
      nOrderBy = n;
    }
  }

  /* Allocate the sqlite3_index_info structure
  */
  pIdxInfo = sqlite3DbMallocZero(pParse->db, sizeof(*pIdxInfo)
                           + (sizeof(*pIdxCons) + sizeof(*pUsage))*nTerm
                           + sizeof(*pIdxOrderBy)*nOrderBy );
  if( pIdxInfo==0 ){
    sqlite3ErrorMsg(pParse, "out of memory");
    return 0;
  }

  /* Initialize the structure.  The sqlite3_index_info structure contains
  ** many fields that are declared "const" to prevent xBestIndex from
  ** changing them.  We have to do some funky casting in order to
  ** initialize those fields.
  */
  pIdxCons = (struct sqlite3_index_constraint*)&pIdxInfo[1];
  pIdxOrderBy = (struct sqlite3_index_orderby*)&pIdxCons[nTerm];
  pUsage = (struct sqlite3_index_constraint_usage*)&pIdxOrderBy[nOrderBy];
  *(int*)&pIdxInfo->nConstraint = nTerm;
  *(int*)&pIdxInfo->nOrderBy = nOrderBy;
  *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint = pIdxCons;
  *(struct sqlite3_index_orderby**)&pIdxInfo->aOrderBy = pIdxOrderBy;
  *(struct sqlite3_index_constraint_usage**)&pIdxInfo->aConstraintUsage =
                                                                   pUsage;

  for(i=j=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    u8 op;
    if( pTerm->leftCursor != pSrc->iCursor ) continue;
    assert( IsPowerOfTwo(pTerm->eOperator & ~WO_EQUIV) );
    testcase( pTerm->eOperator & WO_IN );
    testcase( pTerm->eOperator & WO_ISNULL );
    if( pTerm->eOperator & (WO_ISNULL) ) continue;
    if( pTerm->wtFlags & TERM_VNULL ) continue;
    pIdxCons[j].iColumn = pTerm->u.leftColumn;
    pIdxCons[j].iTermOffset = i;
    op = (u8)pTerm->eOperator & WO_ALL;
    if( op==WO_IN ) op = WO_EQ;
    pIdxCons[j].op = op;
    /* The direct assignment in the previous line is possible only because
    ** the WO_ and SQLITE_INDEX_CONSTRAINT_ codes are identical.  The
    ** following asserts verify this fact. */
    assert( WO_EQ==SQLITE_INDEX_CONSTRAINT_EQ );
    assert( WO_LT==SQLITE_INDEX_CONSTRAINT_LT );
    assert( WO_LE==SQLITE_INDEX_CONSTRAINT_LE );
    assert( WO_GT==SQLITE_INDEX_CONSTRAINT_GT );
    assert( WO_GE==SQLITE_INDEX_CONSTRAINT_GE );
    assert( WO_MATCH==SQLITE_INDEX_CONSTRAINT_MATCH );
    assert( pTerm->eOperator & (WO_IN|WO_EQ|WO_LT|WO_LE|WO_GT|WO_GE|WO_MATCH) );
    j++;
  }
  for(i=0; i<nOrderBy; i++){
    Expr *pExpr = pOrderBy->a[i].pExpr;
    pIdxOrderBy[i].iColumn = pExpr->iColumn;
    pIdxOrderBy[i].desc = pOrderBy->a[i].sortOrder;
  }

  return pIdxInfo;
}

/*
** The table object reference passed as the second argument to this function
** must represent a virtual table. This function invokes the xBestIndex()
** method of the virtual table with the sqlite3_index_info object that
** comes in as the 3rd argument to this function.
**
** If an error occurs, pParse is populated with an error message and a
** non-zero value is returned. Otherwise, 0 is returned and the output
** part of the sqlite3_index_info structure is left populated.
**
** Whether or not an error is returned, it is the responsibility of the
** caller to eventually free p->idxStr if p->needToFreeIdxStr indicates
** that this is required.
*/
static int vtabBestIndex(Parse *pParse, Table *pTab, sqlite3_index_info *p){
  sqlite3_vtab *pVtab = sqlite3GetVTable(pParse->db, pTab)->pVtab;
  int i;
  int rc;

  TRACE_IDX_INPUTS(p);
  rc = pVtab->pModule->xBestIndex(pVtab, p);
  TRACE_IDX_OUTPUTS(p);

  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_NOMEM ){
      pParse->db->mallocFailed = 1;
    }else if( !pVtab->zErrMsg ){
      sqlite3ErrorMsg(pParse, "%s", sqlite3ErrStr(rc));
    }else{
      sqlite3ErrorMsg(pParse, "%s", pVtab->zErrMsg);
    }
  }
  sqlite3_free(pVtab->zErrMsg);
  pVtab->zErrMsg = 0;

  for(i=0; i<p->nConstraint; i++){
    if( !p->aConstraint[i].usable && p->aConstraintUsage[i].argvIndex>0 ){
      sqlite3ErrorMsg(pParse, 
          "table %s: xBestIndex returned an invalid plan", pTab->zName);
    }
  }

  return pParse->nErr;
}
#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */


#ifdef SQLITE_ENABLE_STAT3
/*
** Estimate the location of a particular key among all keys in an
** index.  Store the results in aStat as follows:
**
**    aStat[0]      Est. number of rows less than pVal
**    aStat[1]      Est. number of rows equal to pVal
**
** Return SQLITE_OK on success.
*/
static int whereKeyStats(
  Parse *pParse,              /* Database connection */
  Index *pIdx,                /* Index to consider domain of */
  sqlite3_value *pVal,        /* Value to consider */
  int roundUp,                /* Round up if true.  Round down if false */
  tRowcnt *aStat              /* OUT: stats written here */
){
  tRowcnt n;
  IndexSample *aSample;
  int i, eType;
  int isEq = 0;
  i64 v;
  double r, rS;

  assert( roundUp==0 || roundUp==1 );
  assert( pIdx->nSample>0 );
  if( pVal==0 ) return SQLITE_ERROR;
  n = pIdx->aiRowEst[0];
  aSample = pIdx->aSample;
  eType = sqlite3_value_type(pVal);

  if( eType==SQLITE_INTEGER ){
    v = sqlite3_value_int64(pVal);
    r = (i64)v;
    for(i=0; i<pIdx->nSample; i++){
      if( aSample[i].eType==SQLITE_NULL ) continue;
      if( aSample[i].eType>=SQLITE_TEXT ) break;
      if( aSample[i].eType==SQLITE_INTEGER ){
        if( aSample[i].u.i>=v ){
          isEq = aSample[i].u.i==v;
          break;
        }
      }else{
        assert( aSample[i].eType==SQLITE_FLOAT );
        if( aSample[i].u.r>=r ){
          isEq = aSample[i].u.r==r;
          break;
        }
      }
    }
  }else if( eType==SQLITE_FLOAT ){
    r = sqlite3_value_double(pVal);
    for(i=0; i<pIdx->nSample; i++){
      if( aSample[i].eType==SQLITE_NULL ) continue;
      if( aSample[i].eType>=SQLITE_TEXT ) break;
      if( aSample[i].eType==SQLITE_FLOAT ){
        rS = aSample[i].u.r;
      }else{
        rS = aSample[i].u.i;
      }
      if( rS>=r ){
        isEq = rS==r;
        break;
      }
    }
  }else if( eType==SQLITE_NULL ){
    i = 0;
    if( aSample[0].eType==SQLITE_NULL ) isEq = 1;
  }else{
    assert( eType==SQLITE_TEXT || eType==SQLITE_BLOB );
    for(i=0; i<pIdx->nSample; i++){
      if( aSample[i].eType==SQLITE_TEXT || aSample[i].eType==SQLITE_BLOB ){
        break;
      }
    }
    if( i<pIdx->nSample ){      
      sqlite3 *db = pParse->db;
      CollSeq *pColl;
      const u8 *z;
      if( eType==SQLITE_BLOB ){
        z = (const u8 *)sqlite3_value_blob(pVal);
        pColl = db->pDfltColl;
        assert( pColl->enc==SQLITE_UTF8 );
      }else{
        pColl = sqlite3GetCollSeq(pParse, SQLITE_UTF8, 0, *pIdx->azColl);
        /* If the collating sequence was unavailable, we should have failed
        ** long ago and never reached this point.  But we'll check just to
        ** be doubly sure. */
        if( NEVER(pColl==0) ) return SQLITE_ERROR;
        z = (const u8 *)sqlite3ValueText(pVal, pColl->enc);
        if( !z ){
          return SQLITE_NOMEM;
        }
        assert( z && pColl && pColl->xCmp );
      }
      n = sqlite3ValueBytes(pVal, pColl->enc);
  
      for(; i<pIdx->nSample; i++){
        int c;
        int eSampletype = aSample[i].eType;
        if( eSampletype<eType ) continue;
        if( eSampletype!=eType ) break;
#ifndef SQLITE_OMIT_UTF16
        if( pColl->enc!=SQLITE_UTF8 ){
          int nSample;
          char *zSample = sqlite3Utf8to16(
              db, pColl->enc, aSample[i].u.z, aSample[i].nByte, &nSample
          );
          if( !zSample ){
            assert( db->mallocFailed );
            return SQLITE_NOMEM;
          }
          c = pColl->xCmp(pColl->pUser, nSample, zSample, n, z);
          sqlite3DbFree(db, zSample);
        }else
#endif
        {
          c = pColl->xCmp(pColl->pUser, aSample[i].nByte, aSample[i].u.z, n, z);
        }
        if( c>=0 ){
          if( c==0 ) isEq = 1;
          break;
        }
      }
    }
  }

  /* At this point, aSample[i] is the first sample that is greater than
  ** or equal to pVal.  Or if i==pIdx->nSample, then all samples are less
  ** than pVal.  If aSample[i]==pVal, then isEq==1.
  */
  if( isEq ){
    assert( i<pIdx->nSample );
    aStat[0] = aSample[i].nLt;
    aStat[1] = aSample[i].nEq;
  }else{
    tRowcnt iLower, iUpper, iGap;
    if( i==0 ){
      iLower = 0;
      iUpper = aSample[0].nLt;
    }else{
      iUpper = i>=pIdx->nSample ? n : aSample[i].nLt;
      iLower = aSample[i-1].nEq + aSample[i-1].nLt;
    }
    aStat[1] = pIdx->avgEq;
    if( iLower>=iUpper ){
      iGap = 0;
    }else{
      iGap = iUpper - iLower;
    }
    if( roundUp ){
      iGap = (iGap*2)/3;
    }else{
      iGap = iGap/3;
    }
    aStat[0] = iLower + iGap;
  }
  return SQLITE_OK;
}
#endif /* SQLITE_ENABLE_STAT3 */

/*
** If expression pExpr represents a literal value, set *pp to point to
** an sqlite3_value structure containing the same value, with affinity
** aff applied to it, before returning. It is the responsibility of the 
** caller to eventually release this structure by passing it to 
** sqlite3ValueFree().
**
** If the current parse is a recompile (sqlite3Reprepare()) and pExpr
** is an SQL variable that currently has a non-NULL value bound to it,
** create an sqlite3_value structure containing this value, again with
** affinity aff applied to it, instead.
**
** If neither of the above apply, set *pp to NULL.
**
** If an error occurs, return an error code. Otherwise, SQLITE_OK.
*/
#ifdef SQLITE_ENABLE_STAT3
static int valueFromExpr(
  Parse *pParse, 
  Expr *pExpr, 
  u8 aff, 
  sqlite3_value **pp
){
  if( pExpr->op==TK_VARIABLE
   || (pExpr->op==TK_REGISTER && pExpr->op2==TK_VARIABLE)
  ){
    int iVar = pExpr->iColumn;
    sqlite3VdbeSetVarmask(pParse->pVdbe, iVar);
    *pp = sqlite3VdbeGetBoundValue(pParse->pReprepare, iVar, aff);
    return SQLITE_OK;
  }
  return sqlite3ValueFromExpr(pParse->db, pExpr, SQLITE_UTF8, aff, pp);
}
#endif

/*
** This function is used to estimate the number of rows that will be visited
** by scanning an index for a range of values. The range may have an upper
** bound, a lower bound, or both. The WHERE clause terms that set the upper
** and lower bounds are represented by pLower and pUpper respectively. For
** example, assuming that index p is on t1(a):
**
**   ... FROM t1 WHERE a > ? AND a < ? ...
**                    |_____|   |_____|
**                       |         |
**                     pLower    pUpper
**
** If either of the upper or lower bound is not present, then NULL is passed in
** place of the corresponding WhereTerm.
**
** The nEq parameter is passed the index of the index column subject to the
** range constraint. Or, equivalently, the number of equality constraints
** optimized by the proposed index scan. For example, assuming index p is
** on t1(a, b), and the SQL query is:
**
**   ... FROM t1 WHERE a = ? AND b > ? AND b < ? ...
**
** then nEq should be passed the value 1 (as the range restricted column,
** b, is the second left-most column of the index). Or, if the query is:
**
**   ... FROM t1 WHERE a > ? AND a < ? ...
**
** then nEq should be passed 0.
**
** The returned value is an integer divisor to reduce the estimated
** search space.  A return value of 1 means that range constraints are
** no help at all.  A return value of 2 means range constraints are
** expected to reduce the search space by half.  And so forth...
**
** In the absence of sqlite_stat3 ANALYZE data, each range inequality
** reduces the search space by a factor of 4.  Hence a single constraint (x>?)
** results in a return of 4 and a range constraint (x>? AND x<?) results
** in a return of 16.
*/
static int whereRangeScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  Index *p,            /* The index containing the range-compared column; "x" */
  int nEq,             /* index into p->aCol[] of the range-compared column */
  WhereTerm *pLower,   /* Lower bound on the range. ex: "x>123" Might be NULL */
  WhereTerm *pUpper,   /* Upper bound on the range. ex: "x<455" Might be NULL */
  WhereCost *pRangeDiv /* OUT: Reduce search space by this divisor */
){
  int rc = SQLITE_OK;

#ifdef SQLITE_ENABLE_STAT3

  if( nEq==0 && p->nSample && OptimizationEnabled(pParse->db, SQLITE_Stat3) ){
    sqlite3_value *pRangeVal;
    tRowcnt iLower = 0;
    tRowcnt iUpper = p->aiRowEst[0];
    tRowcnt a[2];
    u8 aff = p->pTable->aCol[p->aiColumn[0]].affinity;

    if( pLower ){
      Expr *pExpr = pLower->pExpr->pRight;
      rc = valueFromExpr(pParse, pExpr, aff, &pRangeVal);
      assert( (pLower->eOperator & (WO_GT|WO_GE))!=0 );
      if( rc==SQLITE_OK
       && whereKeyStats(pParse, p, pRangeVal, 0, a)==SQLITE_OK
      ){
        iLower = a[0];
        if( (pLower->eOperator & WO_GT)!=0 ) iLower += a[1];
      }
      sqlite3ValueFree(pRangeVal);
    }
    if( rc==SQLITE_OK && pUpper ){
      Expr *pExpr = pUpper->pExpr->pRight;
      rc = valueFromExpr(pParse, pExpr, aff, &pRangeVal);
      assert( (pUpper->eOperator & (WO_LT|WO_LE))!=0 );
      if( rc==SQLITE_OK
       && whereKeyStats(pParse, p, pRangeVal, 1, a)==SQLITE_OK
      ){
        iUpper = a[0];
        if( (pUpper->eOperator & WO_LE)!=0 ) iUpper += a[1];
      }
      sqlite3ValueFree(pRangeVal);
    }
    if( rc==SQLITE_OK ){
      WhereCost iBase = whereCost(p->aiRowEst[0]);
      if( iUpper>iLower ){
        iBase -= whereCost(iUpper - iLower);
      }
      *pRangeDiv = iBase;
      WHERETRACE(0x100, ("range scan regions: %u..%u  div=%d\n",
                         (u32)iLower, (u32)iUpper, *pRangeDiv));
      return SQLITE_OK;
    }
  }
#else
  UNUSED_PARAMETER(pParse);
  UNUSED_PARAMETER(p);
  UNUSED_PARAMETER(nEq);
#endif
  assert( pLower || pUpper );
  *pRangeDiv = 0;
  /* TUNING:  Each inequality constraint reduces the search space 4-fold.
  ** A BETWEEN operator, therefore, reduces the search space 16-fold */
  if( pLower && (pLower->wtFlags & TERM_VNULL)==0 ){
    *pRangeDiv += 20;  assert( 20==whereCost(4) );
  }
  if( pUpper ){
    *pRangeDiv += 20;  assert( 20==whereCost(4) );
  }
  return rc;
}

#ifdef SQLITE_ENABLE_STAT3
/*
** Estimate the number of rows that will be returned based on
** an equality constraint x=VALUE and where that VALUE occurs in
** the histogram data.  This only works when x is the left-most
** column of an index and sqlite_stat3 histogram data is available
** for that index.  When pExpr==NULL that means the constraint is
** "x IS NULL" instead of "x=VALUE".
**
** Write the estimated row count into *pnRow and return SQLITE_OK. 
** If unable to make an estimate, leave *pnRow unchanged and return
** non-zero.
**
** This routine can fail if it is unable to load a collating sequence
** required for string comparison, or if unable to allocate memory
** for a UTF conversion required for comparison.  The error is stored
** in the pParse structure.
*/
static int whereEqualScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  Index *p,            /* The index whose left-most column is pTerm */
  Expr *pExpr,         /* Expression for VALUE in the x=VALUE constraint */
  tRowcnt *pnRow       /* Write the revised row estimate here */
){
  sqlite3_value *pRhs = 0;  /* VALUE on right-hand side of pTerm */
  u8 aff;                   /* Column affinity */
  int rc;                   /* Subfunction return code */
  tRowcnt a[2];             /* Statistics */

  assert( p->aSample!=0 );
  assert( p->nSample>0 );
  aff = p->pTable->aCol[p->aiColumn[0]].affinity;
  if( pExpr ){
    rc = valueFromExpr(pParse, pExpr, aff, &pRhs);
    if( rc ) goto whereEqualScanEst_cancel;
  }else{
    pRhs = sqlite3ValueNew(pParse->db);
  }
  if( pRhs==0 ) return SQLITE_NOTFOUND;
  rc = whereKeyStats(pParse, p, pRhs, 0, a);
  if( rc==SQLITE_OK ){
    WHERETRACE(0x100,("equality scan regions: %d\n", (int)a[1]));
    *pnRow = a[1];
  }
whereEqualScanEst_cancel:
  sqlite3ValueFree(pRhs);
  return rc;
}
#endif /* defined(SQLITE_ENABLE_STAT3) */

#ifdef SQLITE_ENABLE_STAT3
/*
** Estimate the number of rows that will be returned based on
** an IN constraint where the right-hand side of the IN operator
** is a list of values.  Example:
**
**        WHERE x IN (1,2,3,4)
**
** Write the estimated row count into *pnRow and return SQLITE_OK. 
** If unable to make an estimate, leave *pnRow unchanged and return
** non-zero.
**
** This routine can fail if it is unable to load a collating sequence
** required for string comparison, or if unable to allocate memory
** for a UTF conversion required for comparison.  The error is stored
** in the pParse structure.
*/
static int whereInScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  Index *p,            /* The index whose left-most column is pTerm */
  ExprList *pList,     /* The value list on the RHS of "x IN (v1,v2,v3,...)" */
  tRowcnt *pnRow       /* Write the revised row estimate here */
){
  int rc = SQLITE_OK;     /* Subfunction return code */
  tRowcnt nEst;           /* Number of rows for a single term */
  tRowcnt nRowEst = 0;    /* New estimate of the number of rows */
  int i;                  /* Loop counter */

  assert( p->aSample!=0 );
  for(i=0; rc==SQLITE_OK && i<pList->nExpr; i++){
    nEst = p->aiRowEst[0];
    rc = whereEqualScanEst(pParse, p, pList->a[i].pExpr, &nEst);
    nRowEst += nEst;
  }
  if( rc==SQLITE_OK ){
    if( nRowEst > p->aiRowEst[0] ) nRowEst = p->aiRowEst[0];
    *pnRow = nRowEst;
    WHERETRACE(0x100,("IN row estimate: est=%g\n", nRowEst));
  }
  return rc;
}
#endif /* defined(SQLITE_ENABLE_STAT3) */

/*
** Disable a term in the WHERE clause.  Except, do not disable the term
** if it controls a LEFT OUTER JOIN and it did not originate in the ON
** or USING clause of that join.
**
** Consider the term t2.z='ok' in the following queries:
**
**   (1)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x WHERE t2.z='ok'
**   (2)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x AND t2.z='ok'
**   (3)  SELECT * FROM t1, t2 WHERE t1.a=t2.x AND t2.z='ok'
**
** The t2.z='ok' is disabled in the in (2) because it originates
** in the ON clause.  The term is disabled in (3) because it is not part
** of a LEFT OUTER JOIN.  In (1), the term is not disabled.
**
** Disabling a term causes that term to not be tested in the inner loop
** of the join.  Disabling is an optimization.  When terms are satisfied
** by indices, we disable them to prevent redundant tests in the inner
** loop.  We would get the correct results if nothing were ever disabled,
** but joins might run a little slower.  The trick is to disable as much
** as we can without disabling too much.  If we disabled in (1), we'd get
** the wrong answer.  See ticket #813.
*/
static void disableTerm(WhereLevel *pLevel, WhereTerm *pTerm){
  if( pTerm
      && (pTerm->wtFlags & TERM_CODED)==0
      && (pLevel->iLeftJoin==0 || ExprHasProperty(pTerm->pExpr, EP_FromJoin))
  ){
    pTerm->wtFlags |= TERM_CODED;
    if( pTerm->iParent>=0 ){
      WhereTerm *pOther = &pTerm->pWC->a[pTerm->iParent];
      if( (--pOther->nChild)==0 ){
        disableTerm(pLevel, pOther);
      }
    }
  }
}

/*
** Code an OP_Affinity opcode to apply the column affinity string zAff
** to the n registers starting at base. 
**
** As an optimization, SQLITE_AFF_NONE entries (which are no-ops) at the
** beginning and end of zAff are ignored.  If all entries in zAff are
** SQLITE_AFF_NONE, then no code gets generated.
**
** This routine makes its own copy of zAff so that the caller is free
** to modify zAff after this routine returns.
*/
static void codeApplyAffinity(Parse *pParse, int base, int n, char *zAff){
  Vdbe *v = pParse->pVdbe;
  if( zAff==0 ){
    assert( pParse->db->mallocFailed );
    return;
  }
  assert( v!=0 );

  /* Adjust base and n to skip over SQLITE_AFF_NONE entries at the beginning
  ** and end of the affinity string.
  */
  while( n>0 && zAff[0]==SQLITE_AFF_NONE ){
    n--;
    base++;
    zAff++;
  }
  while( n>1 && zAff[n-1]==SQLITE_AFF_NONE ){
    n--;
  }

  /* Code the OP_Affinity opcode if there is anything left to do. */
  if( n>0 ){
    sqlite3VdbeAddOp2(v, OP_Affinity, base, n);
    sqlite3VdbeChangeP4(v, -1, zAff, n);
    sqlite3ExprCacheAffinityChange(pParse, base, n);
  }
}


/*
** Generate code for a single equality term of the WHERE clause.  An equality
** term can be either X=expr or X IN (...).   pTerm is the term to be 
** coded.
**
** The current value for the constraint is left in register iReg.
**
** For a constraint of the form X=expr, the expression is evaluated and its
** result is left on the stack.  For constraints of the form X IN (...)
** this routine sets up a loop that will iterate over all values of X.
*/
static int codeEqualityTerm(
  Parse *pParse,      /* The parsing context */
  WhereTerm *pTerm,   /* The term of the WHERE clause to be coded */
  WhereLevel *pLevel, /* The level of the FROM clause we are working on */
  int iEq,            /* Index of the equality term within this level */
  int bRev,           /* True for reverse-order IN operations */
  int iTarget         /* Attempt to leave results in this register */
){
  Expr *pX = pTerm->pExpr;
  Vdbe *v = pParse->pVdbe;
  int iReg;                  /* Register holding results */

  assert( iTarget>0 );
  if( pX->op==TK_EQ ){
    iReg = sqlite3ExprCodeTarget(pParse, pX->pRight, iTarget);
  }else if( pX->op==TK_ISNULL ){
    iReg = iTarget;
    sqlite3VdbeAddOp2(v, OP_Null, 0, iReg);
#ifndef SQLITE_OMIT_SUBQUERY
  }else{
    int eType;
    int iTab;
    struct InLoop *pIn;
    WhereLoop *pLoop = pLevel->pWLoop;

    if( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0
      && pLoop->u.btree.pIndex!=0
      && pLoop->u.btree.pIndex->aSortOrder[iEq]
    ){
      testcase( iEq==0 );
      testcase( bRev );
      bRev = !bRev;
    }
    assert( pX->op==TK_IN );
    iReg = iTarget;
    eType = sqlite3FindInIndex(pParse, pX, 0);
    if( eType==IN_INDEX_INDEX_DESC ){
      testcase( bRev );
      bRev = !bRev;
    }
    iTab = pX->iTable;
    sqlite3VdbeAddOp2(v, bRev ? OP_Last : OP_Rewind, iTab, 0);
    assert( (pLoop->wsFlags & WHERE_MULTI_OR)==0 );
    pLoop->wsFlags |= WHERE_IN_ABLE;
    if( pLevel->u.in.nIn==0 ){
      pLevel->addrNxt = sqlite3VdbeMakeLabel(v);
    }
    pLevel->u.in.nIn++;
    pLevel->u.in.aInLoop =
       sqlite3DbReallocOrFree(pParse->db, pLevel->u.in.aInLoop,
                              sizeof(pLevel->u.in.aInLoop[0])*pLevel->u.in.nIn);
    pIn = pLevel->u.in.aInLoop;
    if( pIn ){
      pIn += pLevel->u.in.nIn - 1;
      pIn->iCur = iTab;
      if( eType==IN_INDEX_ROWID ){
        pIn->addrInTop = sqlite3VdbeAddOp2(v, OP_Rowid, iTab, iReg);
      }else{
        pIn->addrInTop = sqlite3VdbeAddOp3(v, OP_Column, iTab, 0, iReg);
      }
      pIn->eEndLoopOp = bRev ? OP_Prev : OP_Next;
      sqlite3VdbeAddOp1(v, OP_IsNull, iReg);
    }else{
      pLevel->u.in.nIn = 0;
    }
#endif
  }
  disableTerm(pLevel, pTerm);
  return iReg;
}

/*
** Generate code that will evaluate all == and IN constraints for an
** index.
**
** For example, consider table t1(a,b,c,d,e,f) with index i1(a,b,c).
** Suppose the WHERE clause is this:  a==5 AND b IN (1,2,3) AND c>5 AND c<10
** The index has as many as three equality constraints, but in this
** example, the third "c" value is an inequality.  So only two 
** constraints are coded.  This routine will generate code to evaluate
** a==5 and b IN (1,2,3).  The current values for a and b will be stored
** in consecutive registers and the index of the first register is returned.
**
** In the example above nEq==2.  But this subroutine works for any value
** of nEq including 0.  If nEq==0, this routine is nearly a no-op.
** The only thing it does is allocate the pLevel->iMem memory cell and
** compute the affinity string.
**
** This routine always allocates at least one memory cell and returns
** the index of that memory cell. The code that
** calls this routine will use that memory cell to store the termination
** key value of the loop.  If one or more IN operators appear, then
** this routine allocates an additional nEq memory cells for internal
** use.
**
** Before returning, *pzAff is set to point to a buffer containing a
** copy of the column affinity string of the index allocated using
** sqlite3DbMalloc(). Except, entries in the copy of the string associated
** with equality constraints that use NONE affinity are set to
** SQLITE_AFF_NONE. This is to deal with SQL such as the following:
**
**   CREATE TABLE t1(a TEXT PRIMARY KEY, b);
**   SELECT ... FROM t1 AS t2, t1 WHERE t1.a = t2.b;
**
** In the example above, the index on t1(a) has TEXT affinity. But since
** the right hand side of the equality constraint (t2.b) has NONE affinity,
** no conversion should be attempted before using a t2.b value as part of
** a key to search the index. Hence the first byte in the returned affinity
** string in this example would be set to SQLITE_AFF_NONE.
*/
static int codeAllEqualityTerms(
  Parse *pParse,        /* Parsing context */
  WhereLevel *pLevel,   /* Which nested loop of the FROM we are coding */
  int bRev,             /* Reverse the order of IN operators */
  int nExtraReg,        /* Number of extra registers to allocate */
  char **pzAff          /* OUT: Set to point to affinity string */
){
  int nEq;                      /* The number of == or IN constraints to code */
  Vdbe *v = pParse->pVdbe;      /* The vm under construction */
  Index *pIdx;                  /* The index being used for this loop */
  WhereTerm *pTerm;             /* A single constraint term */
  WhereLoop *pLoop;             /* The WhereLoop object */
  int j;                        /* Loop counter */
  int regBase;                  /* Base register */
  int nReg;                     /* Number of registers to allocate */
  char *zAff;                   /* Affinity string to return */

  /* This module is only called on query plans that use an index. */
  pLoop = pLevel->pWLoop;
  assert( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0 );
  nEq = pLoop->u.btree.nEq;
  pIdx = pLoop->u.btree.pIndex;
  assert( pIdx!=0 );

  /* Figure out how many memory cells we will need then allocate them.
  */
  regBase = pParse->nMem + 1;
  nReg = pLoop->u.btree.nEq + nExtraReg;
  pParse->nMem += nReg;

  zAff = sqlite3DbStrDup(pParse->db, sqlite3IndexAffinityStr(v, pIdx));
  if( !zAff ){
    pParse->db->mallocFailed = 1;
  }

  /* Evaluate the equality constraints
  */
  assert( zAff==0 || strlen(zAff)>=nEq );
  for(j=0; j<nEq; j++){
    int r1;
    pTerm = pLoop->aLTerm[j];
    assert( pTerm!=0 );
    /* The following true for indices with redundant columns. 
    ** Ex: CREATE INDEX i1 ON t1(a,b,a); SELECT * FROM t1 WHERE a=0 AND b=0; */
    testcase( (pTerm->wtFlags & TERM_CODED)!=0 );
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    r1 = codeEqualityTerm(pParse, pTerm, pLevel, j, bRev, regBase+j);
    if( r1!=regBase+j ){
      if( nReg==1 ){
        sqlite3ReleaseTempReg(pParse, regBase);
        regBase = r1;
      }else{
        sqlite3VdbeAddOp2(v, OP_SCopy, r1, regBase+j);
      }
    }
    testcase( pTerm->eOperator & WO_ISNULL );
    testcase( pTerm->eOperator & WO_IN );
    if( (pTerm->eOperator & (WO_ISNULL|WO_IN))==0 ){
      Expr *pRight = pTerm->pExpr->pRight;
      sqlite3ExprCodeIsNullJump(v, pRight, regBase+j, pLevel->addrBrk);
      if( zAff ){
        if( sqlite3CompareAffinity(pRight, zAff[j])==SQLITE_AFF_NONE ){
          zAff[j] = SQLITE_AFF_NONE;
        }
        if( sqlite3ExprNeedsNoAffinityChange(pRight, zAff[j]) ){
          zAff[j] = SQLITE_AFF_NONE;
        }
      }
    }
  }
  *pzAff = zAff;
  return regBase;
}

#ifndef SQLITE_OMIT_EXPLAIN
/*
** This routine is a helper for explainIndexRange() below
**
** pStr holds the text of an expression that we are building up one term
** at a time.  This routine adds a new term to the end of the expression.
** Terms are separated by AND so add the "AND" text for second and subsequent
** terms only.
*/
static void explainAppendTerm(
  StrAccum *pStr,             /* The text expression being built */
  int iTerm,                  /* Index of this term.  First is zero */
  const char *zColumn,        /* Name of the column */
  const char *zOp             /* Name of the operator */
){
  if( iTerm ) sqlite3StrAccumAppend(pStr, " AND ", 5);
  sqlite3StrAccumAppend(pStr, zColumn, -1);
  sqlite3StrAccumAppend(pStr, zOp, 1);
  sqlite3StrAccumAppend(pStr, "?", 1);
}

/*
** Argument pLevel describes a strategy for scanning table pTab. This 
** function returns a pointer to a string buffer containing a description
** of the subset of table rows scanned by the strategy in the form of an
** SQL expression. Or, if all rows are scanned, NULL is returned.
**
** For example, if the query:
**
**   SELECT * FROM t1 WHERE a=1 AND b>2;
**
** is run and there is an index on (a, b), then this function returns a
** string similar to:
**
**   "a=? AND b>?"
**
** The returned pointer points to memory obtained from sqlite3DbMalloc().
** It is the responsibility of the caller to free the buffer when it is
** no longer required.
*/
static char *explainIndexRange(sqlite3 *db, WhereLoop *pLoop, Table *pTab){
  Index *pIndex = pLoop->u.btree.pIndex;
  int nEq = pLoop->u.btree.nEq;
  int i, j;
  Column *aCol = pTab->aCol;
  int *aiColumn = pIndex->aiColumn;
  StrAccum txt;

  if( nEq==0 && (pLoop->wsFlags & (WHERE_BTM_LIMIT|WHERE_TOP_LIMIT))==0 ){
    return 0;
  }
  sqlite3StrAccumInit(&txt, 0, 0, SQLITE_MAX_LENGTH);
  txt.db = db;
  sqlite3StrAccumAppend(&txt, " (", 2);
  for(i=0; i<nEq; i++){
    char *z = (i==pIndex->nColumn ) ? "rowid" : aCol[aiColumn[i]].zName;
    explainAppendTerm(&txt, i, z, "=");
  }

  j = i;
  if( pLoop->wsFlags&WHERE_BTM_LIMIT ){
    char *z = (j==pIndex->nColumn ) ? "rowid" : aCol[aiColumn[j]].zName;
    explainAppendTerm(&txt, i++, z, ">");
  }
  if( pLoop->wsFlags&WHERE_TOP_LIMIT ){
    char *z = (j==pIndex->nColumn ) ? "rowid" : aCol[aiColumn[j]].zName;
    explainAppendTerm(&txt, i, z, "<");
  }
  sqlite3StrAccumAppend(&txt, ")", 1);
  return sqlite3StrAccumFinish(&txt);
}

/*
** This function is a no-op unless currently processing an EXPLAIN QUERY PLAN
** command. If the query being compiled is an EXPLAIN QUERY PLAN, a single
** record is added to the output to describe the table scan strategy in 
** pLevel.
*/
static void explainOneScan(
  Parse *pParse,                  /* Parse context */
  SrcList *pTabList,              /* Table list this loop refers to */
  WhereLevel *pLevel,             /* Scan to write OP_Explain opcode for */
  int iLevel,                     /* Value for "level" column of output */
  int iFrom,                      /* Value for "from" column of output */
  u16 wctrlFlags                  /* Flags passed to sqlite3WhereBegin() */
){
  if( pParse->explain==2 ){
    struct SrcList_item *pItem = &pTabList->a[pLevel->iFrom];
    Vdbe *v = pParse->pVdbe;      /* VM being constructed */
    sqlite3 *db = pParse->db;     /* Database handle */
    char *zMsg;                   /* Text to add to EQP output */
    int iId = pParse->iSelectId;  /* Select id (left-most output column) */
    int isSearch;                 /* True for a SEARCH. False for SCAN. */
    WhereLoop *pLoop;             /* The controlling WhereLoop object */
    u32 flags;                    /* Flags that describe this loop */

    pLoop = pLevel->pWLoop;
    flags = pLoop->wsFlags;
    if( (flags&WHERE_MULTI_OR) || (wctrlFlags&WHERE_ONETABLE_ONLY) ) return;

    isSearch = (flags&(WHERE_BTM_LIMIT|WHERE_TOP_LIMIT))!=0
            || ((flags&WHERE_VIRTUALTABLE)==0 && (pLoop->u.btree.nEq>0))
            || (wctrlFlags&(WHERE_ORDERBY_MIN|WHERE_ORDERBY_MAX));

    zMsg = sqlite3MPrintf(db, "%s", isSearch?"SEARCH":"SCAN");
    if( pItem->pSelect ){
      zMsg = sqlite3MAppendf(db, zMsg, "%s SUBQUERY %d", zMsg,pItem->iSelectId);
    }else{
      zMsg = sqlite3MAppendf(db, zMsg, "%s TABLE %s", zMsg, pItem->zName);
    }

    if( pItem->zAlias ){
      zMsg = sqlite3MAppendf(db, zMsg, "%s AS %s", zMsg, pItem->zAlias);
    }
    if( (flags & (WHERE_IPK|WHERE_VIRTUALTABLE))==0
     && ALWAYS(pLoop->u.btree.pIndex!=0)
    ){
      char *zWhere = explainIndexRange(db, pLoop, pItem->pTab);
      zMsg = sqlite3MAppendf(db, zMsg,
               ((flags & WHERE_AUTO_INDEX) ? 
                   "%s USING AUTOMATIC %sINDEX%.0s%s" :
                   "%s USING %sINDEX %s%s"), 
               zMsg, ((flags & WHERE_IDX_ONLY) ? "COVERING " : ""),
               pLoop->u.btree.pIndex->zName, zWhere);
      sqlite3DbFree(db, zWhere);
    }else if( (flags & WHERE_IPK)!=0 && (flags & WHERE_CONSTRAINT)!=0 ){
      zMsg = sqlite3MAppendf(db, zMsg, "%s USING INTEGER PRIMARY KEY", zMsg);

      if( flags&(WHERE_COLUMN_EQ|WHERE_COLUMN_IN) ){
        zMsg = sqlite3MAppendf(db, zMsg, "%s (rowid=?)", zMsg);
      }else if( (flags&WHERE_BOTH_LIMIT)==WHERE_BOTH_LIMIT ){
        zMsg = sqlite3MAppendf(db, zMsg, "%s (rowid>? AND rowid<?)", zMsg);
      }else if( flags&WHERE_BTM_LIMIT ){
        zMsg = sqlite3MAppendf(db, zMsg, "%s (rowid>?)", zMsg);
      }else if( ALWAYS(flags&WHERE_TOP_LIMIT) ){
        zMsg = sqlite3MAppendf(db, zMsg, "%s (rowid<?)", zMsg);
      }
    }
#ifndef SQLITE_OMIT_VIRTUALTABLE
    else if( (flags & WHERE_VIRTUALTABLE)!=0 ){
      zMsg = sqlite3MAppendf(db, zMsg, "%s VIRTUAL TABLE INDEX %d:%s", zMsg,
                  pLoop->u.vtab.idxNum, pLoop->u.vtab.idxStr);
    }
#endif
    zMsg = sqlite3MAppendf(db, zMsg, "%s", zMsg);
    sqlite3VdbeAddOp4(v, OP_Explain, iId, iLevel, iFrom, zMsg, P4_DYNAMIC);
  }
}
#else
# define explainOneScan(u,v,w,x,y,z)
#endif /* SQLITE_OMIT_EXPLAIN */


/*
** Generate code for the start of the iLevel-th loop in the WHERE clause
** implementation described by pWInfo.
*/
static Bitmask codeOneLoopStart(
  WhereInfo *pWInfo,   /* Complete information about the WHERE clause */
  int iLevel,          /* Which level of pWInfo->a[] should be coded */
  Bitmask notReady     /* Which tables are currently available */
){
  int j, k;            /* Loop counters */
  int iCur;            /* The VDBE cursor for the table */
  int addrNxt;         /* Where to jump to continue with the next IN case */
  int omitTable;       /* True if we use the index only */
  int bRev;            /* True if we need to scan in reverse order */
  WhereLevel *pLevel;  /* The where level to be coded */
  WhereLoop *pLoop;    /* The WhereLoop object being coded */
  WhereClause *pWC;    /* Decomposition of the entire WHERE clause */
  WhereTerm *pTerm;               /* A WHERE clause term */
  Parse *pParse;                  /* Parsing context */
  sqlite3 *db;                    /* Database connection */
  Vdbe *v;                        /* The prepared stmt under constructions */
  struct SrcList_item *pTabItem;  /* FROM clause term being coded */
  int addrBrk;                    /* Jump here to break out of the loop */
  int addrCont;                   /* Jump here to continue with next cycle */
  int iRowidReg = 0;        /* Rowid is stored in this register, if not zero */
  int iReleaseReg = 0;      /* Temp register to free before returning */
  Bitmask newNotReady;      /* Return value */

  pParse = pWInfo->pParse;
  v = pParse->pVdbe;
  pWC = &pWInfo->sWC;
  db = pParse->db;
  pLevel = &pWInfo->a[iLevel];
  pLoop = pLevel->pWLoop;
  pTabItem = &pWInfo->pTabList->a[pLevel->iFrom];
  iCur = pTabItem->iCursor;
  bRev = (pWInfo->revMask>>iLevel)&1;
  omitTable = (pLoop->wsFlags & WHERE_IDX_ONLY)!=0 
           && (pWInfo->wctrlFlags & WHERE_FORCE_TABLE)==0;
  VdbeNoopComment((v, "Begin Join Loop %d", iLevel));

  /* Create labels for the "break" and "continue" instructions
  ** for the current loop.  Jump to addrBrk to break out of a loop.
  ** Jump to cont to go immediately to the next iteration of the
  ** loop.
  **
  ** When there is an IN operator, we also have a "addrNxt" label that
  ** means to continue with the next IN value combination.  When
  ** there are no IN operators in the constraints, the "addrNxt" label
  ** is the same as "addrBrk".
  */
  addrBrk = pLevel->addrBrk = pLevel->addrNxt = sqlite3VdbeMakeLabel(v);
  addrCont = pLevel->addrCont = sqlite3VdbeMakeLabel(v);

  /* If this is the right table of a LEFT OUTER JOIN, allocate and
  ** initialize a memory cell that records if this table matches any
  ** row of the left table of the join.
  */
  if( pLevel->iFrom>0 && (pTabItem[0].jointype & JT_LEFT)!=0 ){
    pLevel->iLeftJoin = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_Integer, 0, pLevel->iLeftJoin);
    VdbeComment((v, "init LEFT JOIN no-match flag"));
  }

  /* Special case of a FROM clause subquery implemented as a co-routine */
  if( pTabItem->viaCoroutine ){
    int regYield = pTabItem->regReturn;
    sqlite3VdbeAddOp2(v, OP_Integer, pTabItem->addrFillSub-1, regYield);
    pLevel->p2 =  sqlite3VdbeAddOp1(v, OP_Yield, regYield);
    VdbeComment((v, "next row of co-routine %s", pTabItem->pTab->zName));
    sqlite3VdbeAddOp2(v, OP_If, regYield+1, addrBrk);
    pLevel->op = OP_Goto;
  }else

#ifndef SQLITE_OMIT_VIRTUALTABLE
  if(  (pLoop->wsFlags & WHERE_VIRTUALTABLE)!=0 ){
    /* Case 1:  The table is a virtual-table.  Use the VFilter and VNext
    **          to access the data.
    */
    int iReg;   /* P3 Value for OP_VFilter */
    int addrNotFound;
    int nConstraint = pLoop->nLTerm;

    sqlite3ExprCachePush(pParse);
    iReg = sqlite3GetTempRange(pParse, nConstraint+2);
    addrNotFound = pLevel->addrBrk;
    for(j=0; j<nConstraint; j++){
      int iTarget = iReg+j+2;
      pTerm = pLoop->aLTerm[j];
      if( pTerm==0 ) continue;
      if( pTerm->eOperator & WO_IN ){
        codeEqualityTerm(pParse, pTerm, pLevel, j, bRev, iTarget);
        addrNotFound = pLevel->addrNxt;
      }else{
        sqlite3ExprCode(pParse, pTerm->pExpr->pRight, iTarget);
      }
    }
    sqlite3VdbeAddOp2(v, OP_Integer, pLoop->u.vtab.idxNum, iReg);
    sqlite3VdbeAddOp2(v, OP_Integer, nConstraint, iReg+1);
    sqlite3VdbeAddOp4(v, OP_VFilter, iCur, addrNotFound, iReg,
                      pLoop->u.vtab.idxStr,
                      pLoop->u.vtab.needFree ? P4_MPRINTF : P4_STATIC);
    pLoop->u.vtab.needFree = 0;
    for(j=0; j<nConstraint && j<16; j++){
      if( (pLoop->u.vtab.omitMask>>j)&1 ){
        disableTerm(pLevel, pLoop->aLTerm[j]);
      }
    }
    pLevel->op = OP_VNext;
    pLevel->p1 = iCur;
    pLevel->p2 = sqlite3VdbeCurrentAddr(v);
    sqlite3ReleaseTempRange(pParse, iReg, nConstraint+2);
    sqlite3ExprCachePop(pParse, 1);
  }else
#endif /* SQLITE_OMIT_VIRTUALTABLE */

  if( (pLoop->wsFlags & WHERE_IPK)!=0
   && (pLoop->wsFlags & (WHERE_COLUMN_IN|WHERE_COLUMN_EQ))!=0
  ){
    /* Case 2:  We can directly reference a single row using an
    **          equality comparison against the ROWID field.  Or
    **          we reference multiple rows using a "rowid IN (...)"
    **          construct.
    */
    assert( pLoop->u.btree.nEq==1 );
    iReleaseReg = sqlite3GetTempReg(pParse);
    pTerm = pLoop->aLTerm[0];
    assert( pTerm!=0 );
    assert( pTerm->pExpr!=0 );
    assert( omitTable==0 );
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    iRowidReg = codeEqualityTerm(pParse, pTerm, pLevel, 0, bRev, iReleaseReg);
    addrNxt = pLevel->addrNxt;
    sqlite3VdbeAddOp2(v, OP_MustBeInt, iRowidReg, addrNxt);
    sqlite3VdbeAddOp3(v, OP_NotExists, iCur, addrNxt, iRowidReg);
    sqlite3ExprCacheAffinityChange(pParse, iRowidReg, 1);
    sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
    VdbeComment((v, "pk"));
    pLevel->op = OP_Noop;
  }else if( (pLoop->wsFlags & WHERE_IPK)!=0
         && (pLoop->wsFlags & WHERE_COLUMN_RANGE)!=0
  ){
    /* Case 3:  We have an inequality comparison against the ROWID field.
    */
    int testOp = OP_Noop;
    int start;
    int memEndValue = 0;
    WhereTerm *pStart, *pEnd;

    assert( omitTable==0 );
    j = 0;
    pStart = pEnd = 0;
    if( pLoop->wsFlags & WHERE_BTM_LIMIT ) pStart = pLoop->aLTerm[j++];
    if( pLoop->wsFlags & WHERE_TOP_LIMIT ) pEnd = pLoop->aLTerm[j++];
    assert( pStart!=0 || pEnd!=0 );
    if( bRev ){
      pTerm = pStart;
      pStart = pEnd;
      pEnd = pTerm;
    }
    if( pStart ){
      Expr *pX;             /* The expression that defines the start bound */
      int r1, rTemp;        /* Registers for holding the start boundary */

      /* The following constant maps TK_xx codes into corresponding 
      ** seek opcodes.  It depends on a particular ordering of TK_xx
      */
      const u8 aMoveOp[] = {
           /* TK_GT */  OP_SeekGt,
           /* TK_LE */  OP_SeekLe,
           /* TK_LT */  OP_SeekLt,
           /* TK_GE */  OP_SeekGe
      };
      assert( TK_LE==TK_GT+1 );      /* Make sure the ordering.. */
      assert( TK_LT==TK_GT+2 );      /*  ... of the TK_xx values... */
      assert( TK_GE==TK_GT+3 );      /*  ... is correcct. */

      assert( (pStart->wtFlags & TERM_VNULL)==0 );
      testcase( pStart->wtFlags & TERM_VIRTUAL );
      pX = pStart->pExpr;
      assert( pX!=0 );
      testcase( pStart->leftCursor!=iCur ); /* transitive constraints */
      r1 = sqlite3ExprCodeTemp(pParse, pX->pRight, &rTemp);
      sqlite3VdbeAddOp3(v, aMoveOp[pX->op-TK_GT], iCur, addrBrk, r1);
      VdbeComment((v, "pk"));
      sqlite3ExprCacheAffinityChange(pParse, r1, 1);
      sqlite3ReleaseTempReg(pParse, rTemp);
      disableTerm(pLevel, pStart);
    }else{
      sqlite3VdbeAddOp2(v, bRev ? OP_Last : OP_Rewind, iCur, addrBrk);
    }
    if( pEnd ){
      Expr *pX;
      pX = pEnd->pExpr;
      assert( pX!=0 );
      assert( (pEnd->wtFlags & TERM_VNULL)==0 );
      testcase( pEnd->leftCursor!=iCur ); /* Transitive constraints */
      testcase( pEnd->wtFlags & TERM_VIRTUAL );
      memEndValue = ++pParse->nMem;
      sqlite3ExprCode(pParse, pX->pRight, memEndValue);
      if( pX->op==TK_LT || pX->op==TK_GT ){
        testOp = bRev ? OP_Le : OP_Ge;
      }else{
        testOp = bRev ? OP_Lt : OP_Gt;
      }
      disableTerm(pLevel, pEnd);
    }
    start = sqlite3VdbeCurrentAddr(v);
    pLevel->op = bRev ? OP_Prev : OP_Next;
    pLevel->p1 = iCur;
    pLevel->p2 = start;
    assert( pLevel->p5==0 );
    if( testOp!=OP_Noop ){
      iRowidReg = iReleaseReg = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp2(v, OP_Rowid, iCur, iRowidReg);
      sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
      sqlite3VdbeAddOp3(v, testOp, memEndValue, addrBrk, iRowidReg);
      sqlite3VdbeChangeP5(v, SQLITE_AFF_NUMERIC | SQLITE_JUMPIFNULL);
    }
  }else if( pLoop->wsFlags & WHERE_INDEXED ){
    /* Case 4: A scan using an index.
    **
    **         The WHERE clause may contain zero or more equality 
    **         terms ("==" or "IN" operators) that refer to the N
    **         left-most columns of the index. It may also contain
    **         inequality constraints (>, <, >= or <=) on the indexed
    **         column that immediately follows the N equalities. Only 
    **         the right-most column can be an inequality - the rest must
    **         use the "==" and "IN" operators. For example, if the 
    **         index is on (x,y,z), then the following clauses are all 
    **         optimized:
    **
    **            x=5
    **            x=5 AND y=10
    **            x=5 AND y<10
    **            x=5 AND y>5 AND y<10
    **            x=5 AND y=5 AND z<=10
    **
    **         The z<10 term of the following cannot be used, only
    **         the x=5 term:
    **
    **            x=5 AND z<10
    **
    **         N may be zero if there are inequality constraints.
    **         If there are no inequality constraints, then N is at
    **         least one.
    **
    **         This case is also used when there are no WHERE clause
    **         constraints but an index is selected anyway, in order
    **         to force the output order to conform to an ORDER BY.
    */  
    static const u8 aStartOp[] = {
      0,
      0,
      OP_Rewind,           /* 2: (!start_constraints && startEq &&  !bRev) */
      OP_Last,             /* 3: (!start_constraints && startEq &&   bRev) */
      OP_SeekGt,           /* 4: (start_constraints  && !startEq && !bRev) */
      OP_SeekLt,           /* 5: (start_constraints  && !startEq &&  bRev) */
      OP_SeekGe,           /* 6: (start_constraints  &&  startEq && !bRev) */
      OP_SeekLe            /* 7: (start_constraints  &&  startEq &&  bRev) */
    };
    static const u8 aEndOp[] = {
      OP_Noop,             /* 0: (!end_constraints) */
      OP_IdxGE,            /* 1: (end_constraints && !bRev) */
      OP_IdxLT             /* 2: (end_constraints && bRev) */
    };
    int nEq = pLoop->u.btree.nEq;  /* Number of == or IN terms */
    int isMinQuery = 0;            /* If this is an optimized SELECT min(x).. */
    int regBase;                 /* Base register holding constraint values */
    int r1;                      /* Temp register */
    WhereTerm *pRangeStart = 0;  /* Inequality constraint at range start */
    WhereTerm *pRangeEnd = 0;    /* Inequality constraint at range end */
    int startEq;                 /* True if range start uses ==, >= or <= */
    int endEq;                   /* True if range end uses ==, >= or <= */
    int start_constraints;       /* Start of range is constrained */
    int nConstraint;             /* Number of constraint terms */
    Index *pIdx;                 /* The index we will be using */
    int iIdxCur;                 /* The VDBE cursor for the index */
    int nExtraReg = 0;           /* Number of extra registers needed */
    int op;                      /* Instruction opcode */
    char *zStartAff;             /* Affinity for start of range constraint */
    char *zEndAff;               /* Affinity for end of range constraint */

    pIdx = pLoop->u.btree.pIndex;
    iIdxCur = pLevel->iIdxCur;

    /* If this loop satisfies a sort order (pOrderBy) request that 
    ** was passed to this function to implement a "SELECT min(x) ..." 
    ** query, then the caller will only allow the loop to run for
    ** a single iteration. This means that the first row returned
    ** should not have a NULL value stored in 'x'. If column 'x' is
    ** the first one after the nEq equality constraints in the index,
    ** this requires some special handling.
    */
    if( (pWInfo->wctrlFlags&WHERE_ORDERBY_MIN)!=0
     && (pWInfo->bOBSat!=0)
     && (pIdx->nColumn>nEq)
    ){
      /* assert( pOrderBy->nExpr==1 ); */
      /* assert( pOrderBy->a[0].pExpr->iColumn==pIdx->aiColumn[nEq] ); */
      isMinQuery = 1;
      nExtraReg = 1;
    }

    /* Find any inequality constraint terms for the start and end 
    ** of the range. 
    */
    j = nEq;
    if( pLoop->wsFlags & WHERE_BTM_LIMIT ){
      pRangeStart = pLoop->aLTerm[j++];
      nExtraReg = 1;
    }
    if( pLoop->wsFlags & WHERE_TOP_LIMIT ){
      pRangeEnd = pLoop->aLTerm[j++];
      nExtraReg = 1;
    }

    /* Generate code to evaluate all constraint terms using == or IN
    ** and store the values of those terms in an array of registers
    ** starting at regBase.
    */
    regBase = codeAllEqualityTerms(pParse,pLevel,bRev,nExtraReg,&zStartAff);
    zEndAff = sqlite3DbStrDup(db, zStartAff);
    addrNxt = pLevel->addrNxt;

    /* If we are doing a reverse order scan on an ascending index, or
    ** a forward order scan on a descending index, interchange the 
    ** start and end terms (pRangeStart and pRangeEnd).
    */
    if( (nEq<pIdx->nColumn && bRev==(pIdx->aSortOrder[nEq]==SQLITE_SO_ASC))
     || (bRev && pIdx->nColumn==nEq)
    ){
      SWAP(WhereTerm *, pRangeEnd, pRangeStart);
    }

    testcase( pRangeStart && (pRangeStart->eOperator & WO_LE)!=0 );
    testcase( pRangeStart && (pRangeStart->eOperator & WO_GE)!=0 );
    testcase( pRangeEnd && (pRangeEnd->eOperator & WO_LE)!=0 );
    testcase( pRangeEnd && (pRangeEnd->eOperator & WO_GE)!=0 );
    startEq = !pRangeStart || pRangeStart->eOperator & (WO_LE|WO_GE);
    endEq =   !pRangeEnd || pRangeEnd->eOperator & (WO_LE|WO_GE);
    start_constraints = pRangeStart || nEq>0;

    /* Seek the index cursor to the start of the range. */
    nConstraint = nEq;
    if( pRangeStart ){
      Expr *pRight = pRangeStart->pExpr->pRight;
      sqlite3ExprCode(pParse, pRight, regBase+nEq);
      if( (pRangeStart->wtFlags & TERM_VNULL)==0 ){
        sqlite3ExprCodeIsNullJump(v, pRight, regBase+nEq, addrNxt);
      }
      if( zStartAff ){
        if( sqlite3CompareAffinity(pRight, zStartAff[nEq])==SQLITE_AFF_NONE){
          /* Since the comparison is to be performed with no conversions
          ** applied to the operands, set the affinity to apply to pRight to 
          ** SQLITE_AFF_NONE.  */
          zStartAff[nEq] = SQLITE_AFF_NONE;
        }
        if( sqlite3ExprNeedsNoAffinityChange(pRight, zStartAff[nEq]) ){
          zStartAff[nEq] = SQLITE_AFF_NONE;
        }
      }  
      nConstraint++;
      testcase( pRangeStart->wtFlags & TERM_VIRTUAL );
    }else if( isMinQuery ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, regBase+nEq);
      nConstraint++;
      startEq = 0;
      start_constraints = 1;
    }
    codeApplyAffinity(pParse, regBase, nConstraint, zStartAff);
    op = aStartOp[(start_constraints<<2) + (startEq<<1) + bRev];
    assert( op!=0 );
    testcase( op==OP_Rewind );
    testcase( op==OP_Last );
    testcase( op==OP_SeekGt );
    testcase( op==OP_SeekGe );
    testcase( op==OP_SeekLe );
    testcase( op==OP_SeekLt );
    sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase, nConstraint);

    /* Load the value for the inequality constraint at the end of the
    ** range (if any).
    */
    nConstraint = nEq;
    if( pRangeEnd ){
      Expr *pRight = pRangeEnd->pExpr->pRight;
      sqlite3ExprCacheRemove(pParse, regBase+nEq, 1);
      sqlite3ExprCode(pParse, pRight, regBase+nEq);
      if( (pRangeEnd->wtFlags & TERM_VNULL)==0 ){
        sqlite3ExprCodeIsNullJump(v, pRight, regBase+nEq, addrNxt);
      }
      if( zEndAff ){
        if( sqlite3CompareAffinity(pRight, zEndAff[nEq])==SQLITE_AFF_NONE){
          /* Since the comparison is to be performed with no conversions
          ** applied to the operands, set the affinity to apply to pRight to 
          ** SQLITE_AFF_NONE.  */
          zEndAff[nEq] = SQLITE_AFF_NONE;
        }
        if( sqlite3ExprNeedsNoAffinityChange(pRight, zEndAff[nEq]) ){
          zEndAff[nEq] = SQLITE_AFF_NONE;
        }
      }  
      codeApplyAffinity(pParse, regBase, nEq+1, zEndAff);
      nConstraint++;
      testcase( pRangeEnd->wtFlags & TERM_VIRTUAL );
    }
    sqlite3DbFree(db, zStartAff);
    sqlite3DbFree(db, zEndAff);

    /* Top of the loop body */
    pLevel->p2 = sqlite3VdbeCurrentAddr(v);

    /* Check if the index cursor is past the end of the range. */
    op = aEndOp[(pRangeEnd || nEq) * (1 + bRev)];
    testcase( op==OP_Noop );
    testcase( op==OP_IdxGE );
    testcase( op==OP_IdxLT );
    if( op!=OP_Noop ){
      sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase, nConstraint);
      sqlite3VdbeChangeP5(v, endEq!=bRev ?1:0);
    }

    /* If there are inequality constraints, check that the value
    ** of the table column that the inequality contrains is not NULL.
    ** If it is, jump to the next iteration of the loop.
    */
    r1 = sqlite3GetTempReg(pParse);
    testcase( pLoop->wsFlags & WHERE_BTM_LIMIT );
    testcase( pLoop->wsFlags & WHERE_TOP_LIMIT );
    if( (pLoop->wsFlags & (WHERE_BTM_LIMIT|WHERE_TOP_LIMIT))!=0 ){
      sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, nEq, r1);
      sqlite3VdbeAddOp2(v, OP_IsNull, r1, addrCont);
    }
    sqlite3ReleaseTempReg(pParse, r1);

    /* Seek the table cursor, if required */
    disableTerm(pLevel, pRangeStart);
    disableTerm(pLevel, pRangeEnd);
    if( !omitTable ){
      iRowidReg = iReleaseReg = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp2(v, OP_IdxRowid, iIdxCur, iRowidReg);
      sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
      sqlite3VdbeAddOp2(v, OP_Seek, iCur, iRowidReg);  /* Deferred seek */
    }

    /* Record the instruction used to terminate the loop. Disable 
    ** WHERE clause terms made redundant by the index range scan.
    */
    if( pLoop->wsFlags & WHERE_ONEROW ){
      pLevel->op = OP_Noop;
    }else if( bRev ){
      pLevel->op = OP_Prev;
    }else{
      pLevel->op = OP_Next;
    }
    pLevel->p1 = iIdxCur;
    if( (pLoop->wsFlags & WHERE_CONSTRAINT)==0 ){
      pLevel->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
    }else{
      assert( pLevel->p5==0 );
    }
  }else

#ifndef SQLITE_OMIT_OR_OPTIMIZATION
  if( pLoop->wsFlags & WHERE_MULTI_OR ){
    /* Case 5:  Two or more separately indexed terms connected by OR
    **
    ** Example:
    **
    **   CREATE TABLE t1(a,b,c,d);
    **   CREATE INDEX i1 ON t1(a);
    **   CREATE INDEX i2 ON t1(b);
    **   CREATE INDEX i3 ON t1(c);
    **
    **   SELECT * FROM t1 WHERE a=5 OR b=7 OR (c=11 AND d=13)
    **
    ** In the example, there are three indexed terms connected by OR.
    ** The top of the loop looks like this:
    **
    **          Null       1                # Zero the rowset in reg 1
    **
    ** Then, for each indexed term, the following. The arguments to
    ** RowSetTest are such that the rowid of the current row is inserted
    ** into the RowSet. If it is already present, control skips the
    ** Gosub opcode and jumps straight to the code generated by WhereEnd().
    **
    **        sqlite3WhereBegin(<term>)
    **          RowSetTest                  # Insert rowid into rowset
    **          Gosub      2 A
    **        sqlite3WhereEnd()
    **
    ** Following the above, code to terminate the loop. Label A, the target
    ** of the Gosub above, jumps to the instruction right after the Goto.
    **
    **          Null       1                # Zero the rowset in reg 1
    **          Goto       B                # The loop is finished.
    **
    **       A: <loop body>                 # Return data, whatever.
    **
    **          Return     2                # Jump back to the Gosub
    **
    **       B: <after the loop>
    **
    */
    WhereClause *pOrWc;    /* The OR-clause broken out into subterms */
    SrcList *pOrTab;       /* Shortened table list or OR-clause generation */
    Index *pCov = 0;             /* Potential covering index (or NULL) */
    int iCovCur = pParse->nTab++;  /* Cursor used for index scans (if any) */

    int regReturn = ++pParse->nMem;           /* Register used with OP_Gosub */
    int regRowset = 0;                        /* Register for RowSet object */
    int regRowid = 0;                         /* Register holding rowid */
    int iLoopBody = sqlite3VdbeMakeLabel(v);  /* Start of loop body */
    int iRetInit;                             /* Address of regReturn init */
    int untestedTerms = 0;             /* Some terms not completely tested */
    int ii;                            /* Loop counter */
    Expr *pAndExpr = 0;                /* An ".. AND (...)" expression */
   
    pTerm = pLoop->aLTerm[0];
    assert( pTerm!=0 );
    assert( pTerm->eOperator & WO_OR );
    assert( (pTerm->wtFlags & TERM_ORINFO)!=0 );
    pOrWc = &pTerm->u.pOrInfo->wc;
    pLevel->op = OP_Return;
    pLevel->p1 = regReturn;

    /* Set up a new SrcList in pOrTab containing the table being scanned
    ** by this loop in the a[0] slot and all notReady tables in a[1..] slots.
    ** This becomes the SrcList in the recursive call to sqlite3WhereBegin().
    */
    if( pWInfo->nLevel>1 ){
      int nNotReady;                 /* The number of notReady tables */
      struct SrcList_item *origSrc;     /* Original list of tables */
      nNotReady = pWInfo->nLevel - iLevel - 1;
      pOrTab = sqlite3StackAllocRaw(db,
                            sizeof(*pOrTab)+ nNotReady*sizeof(pOrTab->a[0]));
      if( pOrTab==0 ) return notReady;
      pOrTab->nAlloc = (u8)(nNotReady + 1);
      pOrTab->nSrc = pOrTab->nAlloc;
      memcpy(pOrTab->a, pTabItem, sizeof(*pTabItem));
      origSrc = pWInfo->pTabList->a;
      for(k=1; k<=nNotReady; k++){
        memcpy(&pOrTab->a[k], &origSrc[pLevel[k].iFrom], sizeof(pOrTab->a[k]));
      }
    }else{
      pOrTab = pWInfo->pTabList;
    }

    /* Initialize the rowset register to contain NULL. An SQL NULL is 
    ** equivalent to an empty rowset.
    **
    ** Also initialize regReturn to contain the address of the instruction 
    ** immediately following the OP_Return at the bottom of the loop. This
    ** is required in a few obscure LEFT JOIN cases where control jumps
    ** over the top of the loop into the body of it. In this case the 
    ** correct response for the end-of-loop code (the OP_Return) is to 
    ** fall through to the next instruction, just as an OP_Next does if
    ** called on an uninitialized cursor.
    */
    if( (pWInfo->wctrlFlags & WHERE_DUPLICATES_OK)==0 ){
      regRowset = ++pParse->nMem;
      regRowid = ++pParse->nMem;
      sqlite3VdbeAddOp2(v, OP_Null, 0, regRowset);
    }
    iRetInit = sqlite3VdbeAddOp2(v, OP_Integer, 0, regReturn);

    /* If the original WHERE clause is z of the form:  (x1 OR x2 OR ...) AND y
    ** Then for every term xN, evaluate as the subexpression: xN AND z
    ** That way, terms in y that are factored into the disjunction will
    ** be picked up by the recursive calls to sqlite3WhereBegin() below.
    **
    ** Actually, each subexpression is converted to "xN AND w" where w is
    ** the "interesting" terms of z - terms that did not originate in the
    ** ON or USING clause of a LEFT JOIN, and terms that are usable as 
    ** indices.
    **
    ** This optimization also only applies if the (x1 OR x2 OR ...) term
    ** is not contained in the ON clause of a LEFT JOIN.
    ** See ticket http://www.sqlite.org/src/info/f2369304e4
    */
    if( pWC->nTerm>1 ){
      int iTerm;
      for(iTerm=0; iTerm<pWC->nTerm; iTerm++){
        Expr *pExpr = pWC->a[iTerm].pExpr;
        if( &pWC->a[iTerm] == pTerm ) continue;
        if( ExprHasProperty(pExpr, EP_FromJoin) ) continue;
        if( pWC->a[iTerm].wtFlags & (TERM_ORINFO) ) continue;
        if( (pWC->a[iTerm].eOperator & WO_ALL)==0 ) continue;
        pExpr = sqlite3ExprDup(db, pExpr, 0);
        pAndExpr = sqlite3ExprAnd(db, pAndExpr, pExpr);
      }
      if( pAndExpr ){
        pAndExpr = sqlite3PExpr(pParse, TK_AND, 0, pAndExpr, 0);
      }
    }

    for(ii=0; ii<pOrWc->nTerm; ii++){
      WhereTerm *pOrTerm = &pOrWc->a[ii];
      if( pOrTerm->leftCursor==iCur || (pOrTerm->eOperator & WO_AND)!=0 ){
        WhereInfo *pSubWInfo;          /* Info for single OR-term scan */
        Expr *pOrExpr = pOrTerm->pExpr;
        if( pAndExpr && !ExprHasProperty(pOrExpr, EP_FromJoin) ){
          pAndExpr->pLeft = pOrExpr;
          pOrExpr = pAndExpr;
        }
        /* Loop through table entries that match term pOrTerm. */
        pSubWInfo = sqlite3WhereBegin(pParse, pOrTab, pOrExpr, 0, 0,
                        WHERE_OMIT_OPEN_CLOSE | WHERE_AND_ONLY |
                        WHERE_FORCE_TABLE | WHERE_ONETABLE_ONLY, iCovCur);
        assert( pSubWInfo || pParse->nErr || db->mallocFailed );
        if( pSubWInfo ){
          WhereLoop *pSubLoop;
          explainOneScan(
              pParse, pOrTab, &pSubWInfo->a[0], iLevel, pLevel->iFrom, 0
          );
          if( (pWInfo->wctrlFlags & WHERE_DUPLICATES_OK)==0 ){
            int iSet = ((ii==pOrWc->nTerm-1)?-1:ii);
            int r;
            r = sqlite3ExprCodeGetColumn(pParse, pTabItem->pTab, -1, iCur, 
                                         regRowid, 0);
            sqlite3VdbeAddOp4Int(v, OP_RowSetTest, regRowset,
                                 sqlite3VdbeCurrentAddr(v)+2, r, iSet);
          }
          sqlite3VdbeAddOp2(v, OP_Gosub, regReturn, iLoopBody);

          /* The pSubWInfo->untestedTerms flag means that this OR term
          ** contained one or more AND term from a notReady table.  The
          ** terms from the notReady table could not be tested and will
          ** need to be tested later.
          */
          if( pSubWInfo->untestedTerms ) untestedTerms = 1;

          /* If all of the OR-connected terms are optimized using the same
          ** index, and the index is opened using the same cursor number
          ** by each call to sqlite3WhereBegin() made by this loop, it may
          ** be possible to use that index as a covering index.
          **
          ** If the call to sqlite3WhereBegin() above resulted in a scan that
          ** uses an index, and this is either the first OR-connected term
          ** processed or the index is the same as that used by all previous
          ** terms, set pCov to the candidate covering index. Otherwise, set 
          ** pCov to NULL to indicate that no candidate covering index will 
          ** be available.
          */
          pSubLoop = pSubWInfo->a[0].pWLoop;
          assert( (pSubLoop->wsFlags & WHERE_AUTO_INDEX)==0 );
          if( (pSubLoop->wsFlags & WHERE_INDEXED)!=0
           && (ii==0 || pSubLoop->u.btree.pIndex==pCov)
          ){
            assert( pSubWInfo->a[0].iIdxCur==iCovCur );
            pCov = pSubLoop->u.btree.pIndex;
          }else{
            pCov = 0;
          }

          /* Finish the loop through table entries that match term pOrTerm. */
          sqlite3WhereEnd(pSubWInfo);
        }
      }
    }
    pLevel->u.pCovidx = pCov;
    if( pCov ) pLevel->iIdxCur = iCovCur;
    if( pAndExpr ){
      pAndExpr->pLeft = 0;
      sqlite3ExprDelete(db, pAndExpr);
    }
    sqlite3VdbeChangeP1(v, iRetInit, sqlite3VdbeCurrentAddr(v));
    sqlite3VdbeAddOp2(v, OP_Goto, 0, pLevel->addrBrk);
    sqlite3VdbeResolveLabel(v, iLoopBody);

    if( pWInfo->nLevel>1 ) sqlite3StackFree(db, pOrTab);
    if( !untestedTerms ) disableTerm(pLevel, pTerm);
  }else
#endif /* SQLITE_OMIT_OR_OPTIMIZATION */

  {
    /* Case 6:  There is no usable index.  We must do a complete
    **          scan of the entire table.
    */
    static const u8 aStep[] = { OP_Next, OP_Prev };
    static const u8 aStart[] = { OP_Rewind, OP_Last };
    assert( bRev==0 || bRev==1 );
    pLevel->op = aStep[bRev];
    pLevel->p1 = iCur;
    pLevel->p2 = 1 + sqlite3VdbeAddOp2(v, aStart[bRev], iCur, addrBrk);
    pLevel->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
  }
  newNotReady = notReady & ~getMask(&pWInfo->sMaskSet, iCur);

  /* Insert code to test every subexpression that can be completely
  ** computed using the current set of tables.
  */
  for(pTerm=pWC->a, j=pWC->nTerm; j>0; j--, pTerm++){
    Expr *pE;
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    testcase( pTerm->wtFlags & TERM_CODED );
    if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
    if( (pTerm->prereqAll & newNotReady)!=0 ){
      testcase( pWInfo->untestedTerms==0
               && (pWInfo->wctrlFlags & WHERE_ONETABLE_ONLY)!=0 );
      pWInfo->untestedTerms = 1;
      continue;
    }
    pE = pTerm->pExpr;
    assert( pE!=0 );
    if( pLevel->iLeftJoin && !ExprHasProperty(pE, EP_FromJoin) ){
      continue;
    }
    sqlite3ExprIfFalse(pParse, pE, addrCont, SQLITE_JUMPIFNULL);
    pTerm->wtFlags |= TERM_CODED;
  }

  /* Insert code to test for implied constraints based on transitivity
  ** of the "==" operator.
  **
  ** Example: If the WHERE clause contains "t1.a=t2.b" and "t2.b=123"
  ** and we are coding the t1 loop and the t2 loop has not yet coded,
  ** then we cannot use the "t1.a=t2.b" constraint, but we can code
  ** the implied "t1.a=123" constraint.
  */
  for(pTerm=pWC->a, j=pWC->nTerm; j>0; j--, pTerm++){
    Expr *pE, *pEAlt;
    WhereTerm *pAlt;
    if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
    if( pTerm->eOperator!=(WO_EQUIV|WO_EQ) ) continue;
    if( pTerm->leftCursor!=iCur ) continue;
    if( pLevel->iLeftJoin ) continue;
    pE = pTerm->pExpr;
    assert( !ExprHasProperty(pE, EP_FromJoin) );
    assert( (pTerm->prereqRight & newNotReady)!=0 );
    pAlt = findTerm(pWC, iCur, pTerm->u.leftColumn, notReady, WO_EQ|WO_IN, 0);
    if( pAlt==0 ) continue;
    if( pAlt->wtFlags & (TERM_CODED) ) continue;
    testcase( pAlt->eOperator & WO_EQ );
    testcase( pAlt->eOperator & WO_IN );
    VdbeNoopComment((v, "begin transitive constraint"));
    pEAlt = sqlite3StackAllocRaw(db, sizeof(*pEAlt));
    if( pEAlt ){
      *pEAlt = *pAlt->pExpr;
      pEAlt->pLeft = pE->pLeft;
      sqlite3ExprIfFalse(pParse, pEAlt, addrCont, SQLITE_JUMPIFNULL);
      sqlite3StackFree(db, pEAlt);
    }
  }

  /* For a LEFT OUTER JOIN, generate code that will record the fact that
  ** at least one row of the right table has matched the left table.  
  */
  if( pLevel->iLeftJoin ){
    pLevel->addrFirst = sqlite3VdbeCurrentAddr(v);
    sqlite3VdbeAddOp2(v, OP_Integer, 1, pLevel->iLeftJoin);
    VdbeComment((v, "record LEFT JOIN hit"));
    sqlite3ExprCacheClear(pParse);
    for(pTerm=pWC->a, j=0; j<pWC->nTerm; j++, pTerm++){
      testcase( pTerm->wtFlags & TERM_VIRTUAL );
      testcase( pTerm->wtFlags & TERM_CODED );
      if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
      if( (pTerm->prereqAll & newNotReady)!=0 ){
        assert( pWInfo->untestedTerms );
        continue;
      }
      assert( pTerm->pExpr );
      sqlite3ExprIfFalse(pParse, pTerm->pExpr, addrCont, SQLITE_JUMPIFNULL);
      pTerm->wtFlags |= TERM_CODED;
    }
  }
  sqlite3ReleaseTempReg(pParse, iReleaseReg);

  return newNotReady;
}

#ifdef WHERETRACE_ENABLED
/*
** Print a WhereLoop object for debugging purposes
*/
static void whereLoopPrint(WhereLoop *p, SrcList *pTabList){
  int nb = 1+(pTabList->nSrc+7)/8;
  struct SrcList_item *pItem = pTabList->a + p->iTab;
  Table *pTab = pItem->pTab;
  sqlite3DebugPrintf("%c%2d.%0*llx.%0*llx", p->cId,
                     p->iTab, nb, p->maskSelf, nb, p->prereq);
  sqlite3DebugPrintf(" %12s",
                     pItem->zAlias ? pItem->zAlias : pTab->zName);
  if( (p->wsFlags & WHERE_VIRTUALTABLE)==0 ){
    if( p->u.btree.pIndex ){
      const char *zName = p->u.btree.pIndex->zName;
      if( zName==0 ) zName = "ipk";
      if( strncmp(zName, "sqlite_autoindex_", 17)==0 ){
        int i = sqlite3Strlen30(zName) - 1;
        while( zName[i]!='_' ) i--;
        zName += i;
      }
      sqlite3DebugPrintf(".%-16s %2d", zName, p->u.btree.nEq);
    }else{
      sqlite3DebugPrintf("%20s","");
    }
  }else{
    char *z;
    if( p->u.vtab.idxStr ){
      z = sqlite3_mprintf("(%d,\"%s\",%x)",
                p->u.vtab.idxNum, p->u.vtab.idxStr, p->u.vtab.omitMask);
    }else{
      z = sqlite3_mprintf("(%d,%x)", p->u.vtab.idxNum, p->u.vtab.omitMask);
    }
    sqlite3DebugPrintf(" %-19s", z);
    sqlite3_free(z);
  }
  sqlite3DebugPrintf(" f %04x N %d", p->wsFlags, p->nLTerm);
  sqlite3DebugPrintf(" cost %d,%d,%d\n", p->rSetup, p->rRun, p->nOut);
}
#endif

/*
** Convert bulk memory into a valid WhereLoop that can be passed
** to whereLoopClear harmlessly.
*/
static void whereLoopInit(WhereLoop *p){
  p->aLTerm = p->aLTermSpace;
  p->nLTerm = 0;
  p->nLSlot = ArraySize(p->aLTermSpace);
  p->wsFlags = 0;
}

/*
** Clear the WhereLoop.u union.  Leave WhereLoop.pLTerm intact.
*/
static void whereLoopClearUnion(sqlite3 *db, WhereLoop *p){
  if( p->wsFlags & (WHERE_VIRTUALTABLE|WHERE_AUTO_INDEX) ){
    if( (p->wsFlags & WHERE_VIRTUALTABLE)!=0 && p->u.vtab.needFree ){
      sqlite3_free(p->u.vtab.idxStr);
      p->u.vtab.needFree = 0;
      p->u.vtab.idxStr = 0;
    }else if( (p->wsFlags & WHERE_AUTO_INDEX)!=0 && p->u.btree.pIndex!=0 ){
      sqlite3DbFree(db, p->u.btree.pIndex->zColAff);
      sqlite3DbFree(db, p->u.btree.pIndex);
      p->u.btree.pIndex = 0;
    }
  }
}

/*
** Deallocate internal memory used by a WhereLoop object
*/
static void whereLoopClear(sqlite3 *db, WhereLoop *p){
  if( p->aLTerm!=p->aLTermSpace ) sqlite3DbFree(db, p->aLTerm);
  whereLoopClearUnion(db, p);
  whereLoopInit(p);
}

/*
** Increase the memory allocation for pLoop->aLTerm[] to be at least n.
*/
static int whereLoopResize(sqlite3 *db, WhereLoop *p, int n){
  WhereTerm **paNew;
  if( p->nLSlot>=n ) return SQLITE_OK;
  n = (n+7)&~7;
  paNew = sqlite3DbMallocRaw(db, sizeof(p->aLTerm[0])*n);
  if( paNew==0 ) return SQLITE_NOMEM;
  memcpy(paNew, p->aLTerm, sizeof(p->aLTerm[0])*p->nLSlot);
  if( p->aLTerm!=p->aLTermSpace ) sqlite3DbFree(db, p->aLTerm);
  p->aLTerm = paNew;
  p->nLSlot = n;
  return SQLITE_OK;
}

/*
** Transfer content from the second pLoop into the first.
*/
static int whereLoopXfer(sqlite3 *db, WhereLoop *pTo, WhereLoop *pFrom){
  if( whereLoopResize(db, pTo, pFrom->nLTerm) ) return SQLITE_NOMEM;
  whereLoopClearUnion(db, pTo);
  memcpy(pTo, pFrom, WHERE_LOOP_XFER_SZ);
  memcpy(pTo->aLTerm, pFrom->aLTerm, pTo->nLTerm*sizeof(pTo->aLTerm[0]));
  if( pFrom->wsFlags & WHERE_VIRTUALTABLE ){
    pFrom->u.vtab.needFree = 0;
  }else if( (pFrom->wsFlags & WHERE_AUTO_INDEX)!=0 ){
    pFrom->u.btree.pIndex = 0;
  }
  return SQLITE_OK;
}

/*
** Delete a WhereLoop object
*/
static void whereLoopDelete(sqlite3 *db, WhereLoop *p){
  whereLoopClear(db, p);
  sqlite3DbFree(db, p);
}

/*
** Free a WhereInfo structure
*/
static void whereInfoFree(sqlite3 *db, WhereInfo *pWInfo){
  if( ALWAYS(pWInfo) ){
    whereClauseClear(&pWInfo->sWC);
    while( pWInfo->pLoops ){
      WhereLoop *p = pWInfo->pLoops;
      pWInfo->pLoops = p->pNextLoop;
      whereLoopDelete(db, p);
    }
    sqlite3DbFree(db, pWInfo);
  }
}

/*
** Insert or replace a WhereLoop entry using the template supplied.
**
** An existing WhereLoop entry might be overwritten if the new template
** is better and has fewer dependencies.  Or the template will be ignored
** and no insert will occur if an existing WhereLoop is faster and has
** fewer dependencies than the template.  Otherwise a new WhereLoop is
** added based on the template.
**
** If pBuilder->pOrSet is not NULL then we only care about only the
** prerequisites and rRun and nOut costs of the N best loops.  That
** information is gathered in the pBuilder->pOrSet object.  This special
** processing mode is used only for OR clause processing.
**
** When accumulating multiple loops (when pBuilder->pOrSet is NULL) we
** still might overwrite similar loops with the new template if the
** template is better.  Loops may be overwritten if the following 
** conditions are met:
**
**    (1)  They have the same iTab.
**    (2)  They have the same iSortIdx.
**    (3)  The template has same or fewer dependencies than the current loop
**    (4)  The template has the same or lower cost than the current loop
**    (5)  The template uses more terms of the same index but has no additional
**         dependencies          
*/
static int whereLoopInsert(WhereLoopBuilder *pBuilder, WhereLoop *pTemplate){
  WhereLoop **ppPrev, *p, *pNext = 0;
  WhereInfo *pWInfo = pBuilder->pWInfo;
  sqlite3 *db = pWInfo->pParse->db;

  /* If pBuilder->pOrSet is defined, then only keep track of the costs
  ** and prereqs.
  */
  if( pBuilder->pOrSet!=0 ){
#if WHERETRACE_ENABLED
    u16 n = pBuilder->pOrSet->n;
    int x =
#endif
    whereOrInsert(pBuilder->pOrSet, pTemplate->prereq, pTemplate->rRun,
                                    pTemplate->nOut);
#if WHERETRACE_ENABLED
    if( sqlite3WhereTrace & 0x8 ){
      sqlite3DebugPrintf(x?"   or-%d:  ":"   or-X:  ", n);
      whereLoopPrint(pTemplate, pWInfo->pTabList);
    }
#endif
    return SQLITE_OK;
  }

  /* Search for an existing WhereLoop to overwrite, or which takes
  ** priority over pTemplate.
  */
  for(ppPrev=&pWInfo->pLoops, p=*ppPrev; p; ppPrev=&p->pNextLoop, p=*ppPrev){
    if( p->iTab!=pTemplate->iTab || p->iSortIdx!=pTemplate->iSortIdx ){
      /* If either the iTab or iSortIdx values for two WhereLoop are different
      ** then those WhereLoops need to be considered separately.  Neither is
      ** a candidate to replace the other. */
      continue;
    }
    /* In the current implementation, the rSetup value is either zero
    ** or the cost of building an automatic index (NlogN) and the NlogN
    ** is the same for compatible WhereLoops. */
    assert( p->rSetup==0 || pTemplate->rSetup==0 
                 || p->rSetup==pTemplate->rSetup );

    /* whereLoopAddBtree() always generates and inserts the automatic index
    ** case first.  Hence compatible candidate WhereLoops never have a larger
    ** rSetup. Call this SETUP-INVARIANT */
    assert( p->rSetup>=pTemplate->rSetup );

    if( (p->prereq & pTemplate->prereq)==p->prereq
     && p->rSetup<=pTemplate->rSetup
     && p->rRun<=pTemplate->rRun
    ){
      /* This branch taken when p is equal or better than pTemplate in 
      ** all of (1) dependences (2) setup-cost, and (3) run-cost. */
      assert( p->rSetup==pTemplate->rSetup );
      if( p->nLTerm<pTemplate->nLTerm
       && (p->wsFlags & WHERE_INDEXED)!=0
       && (pTemplate->wsFlags & WHERE_INDEXED)!=0
       && p->u.btree.pIndex==pTemplate->u.btree.pIndex
       && p->prereq==pTemplate->prereq
      ){
        /* Overwrite an existing WhereLoop with an similar one that uses
        ** more terms of the index */
        pNext = p->pNextLoop;
        break;
      }else{
        /* pTemplate is not helpful.
        ** Return without changing or adding anything */
        goto whereLoopInsert_noop;
      }
    }
    if( (p->prereq & pTemplate->prereq)==pTemplate->prereq
     && p->rRun>=pTemplate->rRun
     && ALWAYS(p->rSetup>=pTemplate->rSetup) /* See SETUP-INVARIANT above */
    ){
      /* Overwrite an existing WhereLoop with a better one: one that is
      ** better at one of (1) dependences, (2) setup-cost, or (3) run-cost
      ** and is no worse in any of those categories. */
      pNext = p->pNextLoop;
      break;
    }
  }

  /* If we reach this point it means that either p[] should be overwritten
  ** with pTemplate[] if p[] exists, or if p==NULL then allocate a new
  ** WhereLoop and insert it.
  */
#if WHERETRACE_ENABLED
  if( sqlite3WhereTrace & 0x8 ){
    if( p!=0 ){
      sqlite3DebugPrintf("ins-del:  ");
      whereLoopPrint(p, pWInfo->pTabList);
    }
    sqlite3DebugPrintf("ins-new:  ");
    whereLoopPrint(pTemplate, pWInfo->pTabList);
  }
#endif
  if( p==0 ){
    p = sqlite3DbMallocRaw(db, sizeof(WhereLoop));
    if( p==0 ) return SQLITE_NOMEM;
    whereLoopInit(p);
  }
  whereLoopXfer(db, p, pTemplate);
  p->pNextLoop = pNext;
  *ppPrev = p;
  if( (p->wsFlags & WHERE_VIRTUALTABLE)==0 ){
    Index *pIndex = p->u.btree.pIndex;
    if( pIndex && pIndex->tnum==0 ){
      p->u.btree.pIndex = 0;
    }
  }
  return SQLITE_OK;

  /* Jump here if the insert is a no-op */
whereLoopInsert_noop:
#if WHERETRACE_ENABLED
  if( sqlite3WhereTrace & 0x8 ){
    sqlite3DebugPrintf("ins-noop: ");
    whereLoopPrint(pTemplate, pWInfo->pTabList);
  }
#endif
  return SQLITE_OK;  
}

/*
** We have so far matched pBuilder->pNew->u.btree.nEq terms of the index pIndex.
** Try to match one more.
**
** If pProbe->tnum==0, that means pIndex is a fake index used for the
** INTEGER PRIMARY KEY.
*/
static int whereLoopAddBtreeIndex(
  WhereLoopBuilder *pBuilder,     /* The WhereLoop factory */
  struct SrcList_item *pSrc,      /* FROM clause term being analyzed */
  Index *pProbe,                  /* An index on pSrc */
  WhereCost nInMul                /* log(Number of iterations due to IN) */
){
  WhereInfo *pWInfo = pBuilder->pWInfo;  /* WHERE analyse context */
  Parse *pParse = pWInfo->pParse;        /* Parsing context */
  sqlite3 *db = pParse->db;       /* Database connection malloc context */
  WhereLoop *pNew;                /* Template WhereLoop under construction */
  WhereTerm *pTerm;               /* A WhereTerm under consideration */
  int opMask;                     /* Valid operators for constraints */
  WhereScan scan;                 /* Iterator for WHERE terms */
  Bitmask saved_prereq;           /* Original value of pNew->prereq */
  u16 saved_nLTerm;               /* Original value of pNew->nLTerm */
  int saved_nEq;                  /* Original value of pNew->u.btree.nEq */
  u32 saved_wsFlags;              /* Original value of pNew->wsFlags */
  WhereCost saved_nOut;           /* Original value of pNew->nOut */
  int iCol;                       /* Index of the column in the table */
  int rc = SQLITE_OK;             /* Return code */
  WhereCost nRowEst;              /* Estimated index selectivity */
  WhereCost rLogSize;             /* Logarithm of table size */
  WhereTerm *pTop = 0, *pBtm = 0; /* Top and bottom range constraints */

  pNew = pBuilder->pNew;
  if( db->mallocFailed ) return SQLITE_NOMEM;

  assert( (pNew->wsFlags & WHERE_VIRTUALTABLE)==0 );
  assert( (pNew->wsFlags & WHERE_TOP_LIMIT)==0 );
  if( pNew->wsFlags & WHERE_BTM_LIMIT ){
    opMask = WO_LT|WO_LE;
  }else if( pProbe->tnum<=0 || (pSrc->jointype & JT_LEFT)!=0 ){
    opMask = WO_EQ|WO_IN|WO_GT|WO_GE|WO_LT|WO_LE;
  }else{
    opMask = WO_EQ|WO_IN|WO_ISNULL|WO_GT|WO_GE|WO_LT|WO_LE;
  }
  if( pProbe->bUnordered ) opMask &= ~(WO_GT|WO_GE|WO_LT|WO_LE);

  assert( pNew->u.btree.nEq<=pProbe->nColumn );
  if( pNew->u.btree.nEq < pProbe->nColumn ){
    iCol = pProbe->aiColumn[pNew->u.btree.nEq];
    nRowEst = whereCost(pProbe->aiRowEst[pNew->u.btree.nEq+1]);
    if( nRowEst==0 && pProbe->onError==OE_None ) nRowEst = 1;
  }else{
    iCol = -1;
    nRowEst = 0;
  }
  pTerm = whereScanInit(&scan, pBuilder->pWC, pSrc->iCursor, iCol,
                        opMask, pProbe);
  saved_nEq = pNew->u.btree.nEq;
  saved_nLTerm = pNew->nLTerm;
  saved_wsFlags = pNew->wsFlags;
  saved_prereq = pNew->prereq;
  saved_nOut = pNew->nOut;
  pNew->rSetup = 0;
  rLogSize = estLog(whereCost(pProbe->aiRowEst[0]));
  for(; rc==SQLITE_OK && pTerm!=0; pTerm = whereScanNext(&scan)){
    int nIn = 0;
    if( pTerm->prereqRight & pNew->maskSelf ) continue;
    if( (pTerm->eOperator==WO_ISNULL || (pTerm->wtFlags&TERM_VNULL)!=0)
     && (iCol<0 || pSrc->pTab->aCol[iCol].notNull)
    ){
      continue; /* ignore IS [NOT] NULL constraints on NOT NULL columns */
    }
    pNew->wsFlags = saved_wsFlags;
    pNew->u.btree.nEq = saved_nEq;
    pNew->nLTerm = saved_nLTerm;
    if( whereLoopResize(db, pNew, pNew->nLTerm+1) ) break; /* OOM */
    pNew->aLTerm[pNew->nLTerm++] = pTerm;
    pNew->prereq = (saved_prereq | pTerm->prereqRight) & ~pNew->maskSelf;
    pNew->rRun = rLogSize; /* Baseline cost is log2(N).  Adjustments below */
    if( pTerm->eOperator & WO_IN ){
      Expr *pExpr = pTerm->pExpr;
      pNew->wsFlags |= WHERE_COLUMN_IN;
      if( ExprHasProperty(pExpr, EP_xIsSelect) ){
        /* "x IN (SELECT ...)":  TUNING: the SELECT returns 25 rows */
        nIn = 46;  assert( 46==whereCost(25) );
      }else if( ALWAYS(pExpr->x.pList && pExpr->x.pList->nExpr) ){
        /* "x IN (value, value, ...)" */
        nIn = whereCost(pExpr->x.pList->nExpr);
      }
      pNew->rRun += nIn;
      pNew->u.btree.nEq++;
      pNew->nOut = nRowEst + nInMul + nIn;
    }else if( pTerm->eOperator & (WO_EQ) ){
      assert( (pNew->wsFlags & (WHERE_COLUMN_NULL|WHERE_COLUMN_IN))!=0
                  || nInMul==0 );
      pNew->wsFlags |= WHERE_COLUMN_EQ;
      if( iCol<0  
       || (pProbe->onError!=OE_None && nInMul==0
           && pNew->u.btree.nEq==pProbe->nColumn-1)
      ){
        assert( (pNew->wsFlags & WHERE_COLUMN_IN)==0 || iCol<0 );
        pNew->wsFlags |= WHERE_ONEROW;
      }
      pNew->u.btree.nEq++;
      pNew->nOut = nRowEst + nInMul;
    }else if( pTerm->eOperator & (WO_ISNULL) ){
      pNew->wsFlags |= WHERE_COLUMN_NULL;
      pNew->u.btree.nEq++;
      /* TUNING: IS NULL selects 2 rows */
      nIn = 10;  assert( 10==whereCost(2) );
      pNew->nOut = nRowEst + nInMul + nIn;
    }else if( pTerm->eOperator & (WO_GT|WO_GE) ){
      testcase( pTerm->eOperator & WO_GT );
      testcase( pTerm->eOperator & WO_GE );
      pNew->wsFlags |= WHERE_COLUMN_RANGE|WHERE_BTM_LIMIT;
      pBtm = pTerm;
      pTop = 0;
    }else{
      assert( pTerm->eOperator & (WO_LT|WO_LE) );
      testcase( pTerm->eOperator & WO_LT );
      testcase( pTerm->eOperator & WO_LE );
      pNew->wsFlags |= WHERE_COLUMN_RANGE|WHERE_TOP_LIMIT;
      pTop = pTerm;
      pBtm = (pNew->wsFlags & WHERE_BTM_LIMIT)!=0 ?
                     pNew->aLTerm[pNew->nLTerm-2] : 0;
    }
    if( pNew->wsFlags & WHERE_COLUMN_RANGE ){
      /* Adjust nOut and rRun for STAT3 range values */
      WhereCost rDiv;
      whereRangeScanEst(pParse, pProbe, pNew->u.btree.nEq,
                        pBtm, pTop, &rDiv);
      pNew->nOut = saved_nOut>rDiv+10 ? saved_nOut - rDiv : 10;
    }
#ifdef SQLITE_ENABLE_STAT3
    if( pNew->u.btree.nEq==1 && pProbe->nSample
     &&  OptimizationEnabled(db, SQLITE_Stat3) ){
      tRowcnt nOut = 0;
      if( (pTerm->eOperator & (WO_EQ|WO_ISNULL))!=0 ){
        testcase( pTerm->eOperator & WO_EQ );
        testcase( pTerm->eOperator & WO_ISNULL );
        rc = whereEqualScanEst(pParse, pProbe, pTerm->pExpr->pRight, &nOut);
      }else if( (pTerm->eOperator & WO_IN)
             &&  !ExprHasProperty(pTerm->pExpr, EP_xIsSelect)  ){
        rc = whereInScanEst(pParse, pProbe, pTerm->pExpr->x.pList, &nOut);
      }
      assert( nOut==0 || rc==SQLITE_OK );
      if( nOut ) pNew->nOut = whereCost(nOut);
    }
#endif
    if( (pNew->wsFlags & (WHERE_IDX_ONLY|WHERE_IPK))==0 ){
      /* Each row involves a step of the index, then a binary search of
      ** the main table */
      pNew->rRun =  whereCostAdd(pNew->rRun, rLogSize>27 ? rLogSize-17 : 10);
    }
    /* Step cost for each output row */
    pNew->rRun = whereCostAdd(pNew->rRun, pNew->nOut);
    /* TBD: Adjust nOut for additional constraints */
    rc = whereLoopInsert(pBuilder, pNew);
    if( (pNew->wsFlags & WHERE_TOP_LIMIT)==0
     && pNew->u.btree.nEq<(pProbe->nColumn + (pProbe->zName!=0))
    ){
      whereLoopAddBtreeIndex(pBuilder, pSrc, pProbe, nInMul+nIn);
    }
  }
  pNew->prereq = saved_prereq;
  pNew->u.btree.nEq = saved_nEq;
  pNew->wsFlags = saved_wsFlags;
  pNew->nOut = saved_nOut;
  pNew->nLTerm = saved_nLTerm;
  return rc;
}

/*
** Return True if it is possible that pIndex might be useful in
** implementing the ORDER BY clause in pBuilder.
**
** Return False if pBuilder does not contain an ORDER BY clause or
** if there is no way for pIndex to be useful in implementing that
** ORDER BY clause.
*/
static int indexMightHelpWithOrderBy(
  WhereLoopBuilder *pBuilder,
  Index *pIndex,
  int iCursor
){
  ExprList *pOB;
  int ii, jj;

  if( pIndex->bUnordered ) return 0;
  if( (pOB = pBuilder->pWInfo->pOrderBy)==0 ) return 0;
  for(ii=0; ii<pOB->nExpr; ii++){
    Expr *pExpr = sqlite3ExprSkipCollate(pOB->a[ii].pExpr);
    if( pExpr->op!=TK_COLUMN ) return 0;
    if( pExpr->iTable==iCursor ){
      for(jj=0; jj<pIndex->nColumn; jj++){
        if( pExpr->iColumn==pIndex->aiColumn[jj] ) return 1;
      }
    }
  }
  return 0;
}

/*
** Return a bitmask where 1s indicate that the corresponding column of
** the table is used by an index.  Only the first 63 columns are considered.
*/
static Bitmask columnsInIndex(Index *pIdx){
  Bitmask m = 0;
  int j;
  for(j=pIdx->nColumn-1; j>=0; j--){
    int x = pIdx->aiColumn[j];
    assert( x>=0 );
    testcase( x==BMS-1 );
    testcase( x==BMS-2 );
    if( x<BMS-1 ) m |= MASKBIT(x);
  }
  return m;
}

/* Check to see if a partial index with pPartIndexWhere can be used
** in the current query.  Return true if it can be and false if not.
*/
static int whereUsablePartialIndex(int iTab, WhereClause *pWC, Expr *pWhere){
  int i;
  WhereTerm *pTerm;
  for(i=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    if( sqlite3ExprImpliesExpr(pTerm->pExpr, pWhere, iTab) ) return 1;
  }
  return 0;
}

/*
** Add all WhereLoop objects for a single table of the join where the table
** is idenfied by pBuilder->pNew->iTab.  That table is guaranteed to be
** a b-tree table, not a virtual table.
*/
static int whereLoopAddBtree(
  WhereLoopBuilder *pBuilder, /* WHERE clause information */
  Bitmask mExtra              /* Extra prerequesites for using this table */
){
  WhereInfo *pWInfo;          /* WHERE analysis context */
  Index *pProbe;              /* An index we are evaluating */
  Index sPk;                  /* A fake index object for the primary key */
  tRowcnt aiRowEstPk[2];      /* The aiRowEst[] value for the sPk index */
  int aiColumnPk = -1;        /* The aColumn[] value for the sPk index */
  SrcList *pTabList;          /* The FROM clause */
  struct SrcList_item *pSrc;  /* The FROM clause btree term to add */
  WhereLoop *pNew;            /* Template WhereLoop object */
  int rc = SQLITE_OK;         /* Return code */
  int iSortIdx = 1;           /* Index number */
  int b;                      /* A boolean value */
  WhereCost rSize;            /* number of rows in the table */
  WhereCost rLogSize;         /* Logarithm of the number of rows in the table */
  WhereClause *pWC;           /* The parsed WHERE clause */
  
  pNew = pBuilder->pNew;
  pWInfo = pBuilder->pWInfo;
  pTabList = pWInfo->pTabList;
  pSrc = pTabList->a + pNew->iTab;
  pWC = pBuilder->pWC;
  assert( !IsVirtual(pSrc->pTab) );

  if( pSrc->pIndex ){
    /* An INDEXED BY clause specifies a particular index to use */
    pProbe = pSrc->pIndex;
  }else{
    /* There is no INDEXED BY clause.  Create a fake Index object in local
    ** variable sPk to represent the rowid primary key index.  Make this
    ** fake index the first in a chain of Index objects with all of the real
    ** indices to follow */
    Index *pFirst;                  /* First of real indices on the table */
    memset(&sPk, 0, sizeof(Index));
    sPk.nColumn = 1;
    sPk.aiColumn = &aiColumnPk;
    sPk.aiRowEst = aiRowEstPk;
    sPk.onError = OE_Replace;
    sPk.pTable = pSrc->pTab;
    aiRowEstPk[0] = pSrc->pTab->nRowEst;
    aiRowEstPk[1] = 1;
    pFirst = pSrc->pTab->pIndex;
    if( pSrc->notIndexed==0 ){
      /* The real indices of the table are only considered if the
      ** NOT INDEXED qualifier is omitted from the FROM clause */
      sPk.pNext = pFirst;
    }
    pProbe = &sPk;
  }
  rSize = whereCost(pSrc->pTab->nRowEst);
  rLogSize = estLog(rSize);

#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
  /* Automatic indexes */
  if( !pBuilder->pOrSet
   && (pWInfo->pParse->db->flags & SQLITE_AutoIndex)!=0
   && pSrc->pIndex==0
   && !pSrc->viaCoroutine
   && !pSrc->notIndexed
   && !pSrc->isCorrelated
  ){
    /* Generate auto-index WhereLoops */
    WhereTerm *pTerm;
    WhereTerm *pWCEnd = pWC->a + pWC->nTerm;
    for(pTerm=pWC->a; rc==SQLITE_OK && pTerm<pWCEnd; pTerm++){
      if( pTerm->prereqRight & pNew->maskSelf ) continue;
      if( termCanDriveIndex(pTerm, pSrc, 0) ){
        pNew->u.btree.nEq = 1;
        pNew->u.btree.pIndex = 0;
        pNew->nLTerm = 1;
        pNew->aLTerm[0] = pTerm;
        /* TUNING: One-time cost for computing the automatic index is
        ** approximately 7*N*log2(N) where N is the number of rows in
        ** the table being indexed. */
        pNew->rSetup = rLogSize + rSize + 28;  assert( 28==whereCost(7) );
        /* TUNING: Each index lookup yields 20 rows in the table.  This
        ** is more than the usual guess of 10 rows, since we have no way
        ** of knowning how selective the index will ultimately be.  It would
        ** not be unreasonable to make this value much larger. */
        pNew->nOut = 43;  assert( 43==whereCost(20) );
        pNew->rRun = whereCostAdd(rLogSize,pNew->nOut);
        pNew->wsFlags = WHERE_AUTO_INDEX;
        pNew->prereq = mExtra | pTerm->prereqRight;
        rc = whereLoopInsert(pBuilder, pNew);
      }
    }
  }
#endif /* SQLITE_OMIT_AUTOMATIC_INDEX */

  /* Loop over all indices
  */
  for(; rc==SQLITE_OK && pProbe; pProbe=pProbe->pNext, iSortIdx++){
    if( pProbe->pPartIdxWhere!=0
     && !whereUsablePartialIndex(pNew->iTab, pWC, pProbe->pPartIdxWhere) ){
      continue;  /* Partial index inappropriate for this query */
    }
    pNew->u.btree.nEq = 0;
    pNew->nLTerm = 0;
    pNew->iSortIdx = 0;
    pNew->rSetup = 0;
    pNew->prereq = mExtra;
    pNew->nOut = rSize;
    pNew->u.btree.pIndex = pProbe;
    b = indexMightHelpWithOrderBy(pBuilder, pProbe, pSrc->iCursor);
    /* The ONEPASS_DESIRED flags never occurs together with ORDER BY */
    assert( (pWInfo->wctrlFlags & WHERE_ONEPASS_DESIRED)==0 || b==0 );
    if( pProbe->tnum<=0 ){
      /* Integer primary key index */
      pNew->wsFlags = WHERE_IPK;

      /* Full table scan */
      pNew->iSortIdx = b ? iSortIdx : 0;
      /* TUNING: Cost of full table scan is 3*(N + log2(N)).
      **  +  The extra 3 factor is to encourage the use of indexed lookups
      **     over full scans.  A smaller constant 2 is used for covering
      **     index scans so that a covering index scan will be favored over
      **     a table scan. */
      pNew->rRun = whereCostAdd(rSize,rLogSize) + 16;
      rc = whereLoopInsert(pBuilder, pNew);
      if( rc ) break;
    }else{
      Bitmask m = pSrc->colUsed & ~columnsInIndex(pProbe);
      pNew->wsFlags = (m==0) ? (WHERE_IDX_ONLY|WHERE_INDEXED) : WHERE_INDEXED;

      /* Full scan via index */
      if( b
       || ( m==0
         && pProbe->bUnordered==0
         && (pWInfo->wctrlFlags & WHERE_ONEPASS_DESIRED)==0
         && sqlite3GlobalConfig.bUseCis
         && OptimizationEnabled(pWInfo->pParse->db, SQLITE_CoverIdxScan)
          )
      ){
        pNew->iSortIdx = b ? iSortIdx : 0;
        if( m==0 ){
          /* TUNING: Cost of a covering index scan is 2*(N + log2(N)).
          **  +  The extra 2 factor is to encourage the use of indexed lookups
          **     over index scans.  A table scan uses a factor of 3 so that
          **     index scans are favored over table scans.
          **  +  If this covering index might also help satisfy the ORDER BY
          **     clause, then the cost is fudged down slightly so that this
          **     index is favored above other indices that have no hope of
          **     helping with the ORDER BY. */
          pNew->rRun = 10 + whereCostAdd(rSize,rLogSize) - b;
        }else{
          assert( b!=0 ); 
          /* TUNING: Cost of scanning a non-covering index is (N+1)*log2(N)
          ** which we will simplify to just N*log2(N) */
          pNew->rRun = rSize + rLogSize;
        }
        rc = whereLoopInsert(pBuilder, pNew);
        if( rc ) break;
      }
    }
    rc = whereLoopAddBtreeIndex(pBuilder, pSrc, pProbe, 0);

    /* If there was an INDEXED BY clause, then only that one index is
    ** considered. */
    if( pSrc->pIndex ) break;
  }
  return rc;
}

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Add all WhereLoop objects for a table of the join identified by
** pBuilder->pNew->iTab.  That table is guaranteed to be a virtual table.
*/
static int whereLoopAddVirtual(
  WhereLoopBuilder *pBuilder   /* WHERE clause information */
){
  WhereInfo *pWInfo;           /* WHERE analysis context */
  Parse *pParse;               /* The parsing context */
  WhereClause *pWC;            /* The WHERE clause */
  struct SrcList_item *pSrc;   /* The FROM clause term to search */
  Table *pTab;
  sqlite3 *db;
  sqlite3_index_info *pIdxInfo;
  struct sqlite3_index_constraint *pIdxCons;
  struct sqlite3_index_constraint_usage *pUsage;
  WhereTerm *pTerm;
  int i, j;
  int iTerm, mxTerm;
  int nConstraint;
  int seenIn = 0;              /* True if an IN operator is seen */
  int seenVar = 0;             /* True if a non-constant constraint is seen */
  int iPhase;                  /* 0: const w/o IN, 1: const, 2: no IN,  2: IN */
  WhereLoop *pNew;
  int rc = SQLITE_OK;

  pWInfo = pBuilder->pWInfo;
  pParse = pWInfo->pParse;
  db = pParse->db;
  pWC = pBuilder->pWC;
  pNew = pBuilder->pNew;
  pSrc = &pWInfo->pTabList->a[pNew->iTab];
  pTab = pSrc->pTab;
  assert( IsVirtual(pTab) );
  pIdxInfo = allocateIndexInfo(pParse, pWC, pSrc, pBuilder->pOrderBy);
  if( pIdxInfo==0 ) return SQLITE_NOMEM;
  pNew->prereq = 0;
  pNew->rSetup = 0;
  pNew->wsFlags = WHERE_VIRTUALTABLE;
  pNew->nLTerm = 0;
  pNew->u.vtab.needFree = 0;
  pUsage = pIdxInfo->aConstraintUsage;
  nConstraint = pIdxInfo->nConstraint;
  if( whereLoopResize(db, pNew, nConstraint) ){
    sqlite3DbFree(db, pIdxInfo);
    return SQLITE_NOMEM;
  }

  for(iPhase=0; iPhase<=3; iPhase++){
    if( !seenIn && (iPhase&1)!=0 ){
      iPhase++;
      if( iPhase>3 ) break;
    }
    if( !seenVar && iPhase>1 ) break;
    pIdxCons = *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint;
    for(i=0; i<pIdxInfo->nConstraint; i++, pIdxCons++){
      j = pIdxCons->iTermOffset;
      pTerm = &pWC->a[j];
      switch( iPhase ){
        case 0:    /* Constants without IN operator */
          pIdxCons->usable = 0;
          if( (pTerm->eOperator & WO_IN)!=0 ){
            seenIn = 1;
          }
          if( pTerm->prereqRight!=0 ){
            seenVar = 1;
          }else if( (pTerm->eOperator & WO_IN)==0 ){
            pIdxCons->usable = 1;
          }
          break;
        case 1:    /* Constants with IN operators */
          assert( seenIn );
          pIdxCons->usable = (pTerm->prereqRight==0);
          break;
        case 2:    /* Variables without IN */
          assert( seenVar );
          pIdxCons->usable = (pTerm->eOperator & WO_IN)==0;
          break;
        default:   /* Variables with IN */
          assert( seenVar && seenIn );
          pIdxCons->usable = 1;
          break;
      }
    }
    memset(pUsage, 0, sizeof(pUsage[0])*pIdxInfo->nConstraint);
    if( pIdxInfo->needToFreeIdxStr ) sqlite3_free(pIdxInfo->idxStr);
    pIdxInfo->idxStr = 0;
    pIdxInfo->idxNum = 0;
    pIdxInfo->needToFreeIdxStr = 0;
    pIdxInfo->orderByConsumed = 0;
    pIdxInfo->estimatedCost = SQLITE_BIG_DBL / (double)2;
    rc = vtabBestIndex(pParse, pTab, pIdxInfo);
    if( rc ) goto whereLoopAddVtab_exit;
    pIdxCons = *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint;
    pNew->prereq = 0;
    mxTerm = -1;
    assert( pNew->nLSlot>=nConstraint );
    for(i=0; i<nConstraint; i++) pNew->aLTerm[i] = 0;
    pNew->u.vtab.omitMask = 0;
    for(i=0; i<nConstraint; i++, pIdxCons++){
      if( (iTerm = pUsage[i].argvIndex - 1)>=0 ){
        j = pIdxCons->iTermOffset;
        if( iTerm>=nConstraint
         || j<0
         || j>=pWC->nTerm
         || pNew->aLTerm[iTerm]!=0
        ){
          rc = SQLITE_ERROR;
          sqlite3ErrorMsg(pParse, "%s.xBestIndex() malfunction", pTab->zName);
          goto whereLoopAddVtab_exit;
        }
        testcase( iTerm==nConstraint-1 );
        testcase( j==0 );
        testcase( j==pWC->nTerm-1 );
        pTerm = &pWC->a[j];
        pNew->prereq |= pTerm->prereqRight;
        assert( iTerm<pNew->nLSlot );
        pNew->aLTerm[iTerm] = pTerm;
        if( iTerm>mxTerm ) mxTerm = iTerm;
        testcase( iTerm==15 );
        testcase( iTerm==16 );
        if( iTerm<16 && pUsage[i].omit ) pNew->u.vtab.omitMask |= 1<<iTerm;
        if( (pTerm->eOperator & WO_IN)!=0 ){
          if( pUsage[i].omit==0 ){
            /* Do not attempt to use an IN constraint if the virtual table
            ** says that the equivalent EQ constraint cannot be safely omitted.
            ** If we do attempt to use such a constraint, some rows might be
            ** repeated in the output. */
            break;
          }
          /* A virtual table that is constrained by an IN clause may not
          ** consume the ORDER BY clause because (1) the order of IN terms
          ** is not necessarily related to the order of output terms and
          ** (2) Multiple outputs from a single IN value will not merge
          ** together.  */
          pIdxInfo->orderByConsumed = 0;
        }
      }
    }
    if( i>=nConstraint ){
      pNew->nLTerm = mxTerm+1;
      assert( pNew->nLTerm<=pNew->nLSlot );
      pNew->u.vtab.idxNum = pIdxInfo->idxNum;
      pNew->u.vtab.needFree = pIdxInfo->needToFreeIdxStr;
      pIdxInfo->needToFreeIdxStr = 0;
      pNew->u.vtab.idxStr = pIdxInfo->idxStr;
      pNew->u.vtab.isOrdered = (u8)((pIdxInfo->nOrderBy!=0)
                                     && pIdxInfo->orderByConsumed);
      pNew->rSetup = 0;
      pNew->rRun = whereCostFromDouble(pIdxInfo->estimatedCost);
      /* TUNING: Every virtual table query returns 25 rows */
      pNew->nOut = 46;  assert( 46==whereCost(25) );
      whereLoopInsert(pBuilder, pNew);
      if( pNew->u.vtab.needFree ){
        sqlite3_free(pNew->u.vtab.idxStr);
        pNew->u.vtab.needFree = 0;
      }
    }
  }  

whereLoopAddVtab_exit:
  if( pIdxInfo->needToFreeIdxStr ) sqlite3_free(pIdxInfo->idxStr);
  sqlite3DbFree(db, pIdxInfo);
  return rc;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** Add WhereLoop entries to handle OR terms.  This works for either
** btrees or virtual tables.
*/
static int whereLoopAddOr(WhereLoopBuilder *pBuilder, Bitmask mExtra){
  WhereInfo *pWInfo = pBuilder->pWInfo;
  WhereClause *pWC;
  WhereLoop *pNew;
  WhereTerm *pTerm, *pWCEnd;
  int rc = SQLITE_OK;
  int iCur;
  WhereClause tempWC;
  WhereLoopBuilder sSubBuild;
  WhereOrSet sSum, sCur, sPrev;
  struct SrcList_item *pItem;
  
  pWC = pBuilder->pWC;
  if( pWInfo->wctrlFlags & WHERE_AND_ONLY ) return SQLITE_OK;
  pWCEnd = pWC->a + pWC->nTerm;
  pNew = pBuilder->pNew;
  memset(&sSum, 0, sizeof(sSum));

  for(pTerm=pWC->a; pTerm<pWCEnd && rc==SQLITE_OK; pTerm++){
    if( (pTerm->eOperator & WO_OR)!=0
     && (pTerm->u.pOrInfo->indexable & pNew->maskSelf)!=0 
    ){
      WhereClause * const pOrWC = &pTerm->u.pOrInfo->wc;
      WhereTerm * const pOrWCEnd = &pOrWC->a[pOrWC->nTerm];
      WhereTerm *pOrTerm;
      int once = 1;
      int i, j;
    
      pItem = pWInfo->pTabList->a + pNew->iTab;
      iCur = pItem->iCursor;
      sSubBuild = *pBuilder;
      sSubBuild.pOrderBy = 0;
      sSubBuild.pOrSet = &sCur;

      for(pOrTerm=pOrWC->a; pOrTerm<pOrWCEnd; pOrTerm++){
        if( (pOrTerm->eOperator & WO_AND)!=0 ){
          sSubBuild.pWC = &pOrTerm->u.pAndInfo->wc;
        }else if( pOrTerm->leftCursor==iCur ){
          tempWC.pWInfo = pWC->pWInfo;
          tempWC.pOuter = pWC;
          tempWC.op = TK_AND;
          tempWC.nTerm = 1;
          tempWC.a = pOrTerm;
          sSubBuild.pWC = &tempWC;
        }else{
          continue;
        }
        sCur.n = 0;
#ifndef SQLITE_OMIT_VIRTUALTABLE
        if( IsVirtual(pItem->pTab) ){
          rc = whereLoopAddVirtual(&sSubBuild);
          for(i=0; i<sCur.n; i++) sCur.a[i].prereq |= mExtra;
        }else
#endif
        {
          rc = whereLoopAddBtree(&sSubBuild, mExtra);
        }
        assert( rc==SQLITE_OK || sCur.n==0 );
        if( sCur.n==0 ){
          sSum.n = 0;
          break;
        }else if( once ){
          whereOrMove(&sSum, &sCur);
          once = 0;
        }else{
          whereOrMove(&sPrev, &sSum);
          sSum.n = 0;
          for(i=0; i<sPrev.n; i++){
            for(j=0; j<sCur.n; j++){
              whereOrInsert(&sSum, sPrev.a[i].prereq | sCur.a[j].prereq,
                            whereCostAdd(sPrev.a[i].rRun, sCur.a[j].rRun),
                            whereCostAdd(sPrev.a[i].nOut, sCur.a[j].nOut));
            }
          }
        }
      }
      pNew->nLTerm = 1;
      pNew->aLTerm[0] = pTerm;
      pNew->wsFlags = WHERE_MULTI_OR;
      pNew->rSetup = 0;
      pNew->iSortIdx = 0;
      memset(&pNew->u, 0, sizeof(pNew->u));
      for(i=0; rc==SQLITE_OK && i<sSum.n; i++){
        /* TUNING: Multiple by 3.5 for the secondary table lookup */
        pNew->rRun = sSum.a[i].rRun + 18;
        pNew->nOut = sSum.a[i].nOut;
        pNew->prereq = sSum.a[i].prereq;
        rc = whereLoopInsert(pBuilder, pNew);
      }
    }
  }
  return rc;
}

/*
** Add all WhereLoop objects for all tables 
*/
static int whereLoopAddAll(WhereLoopBuilder *pBuilder){
  WhereInfo *pWInfo = pBuilder->pWInfo;
  Bitmask mExtra = 0;
  Bitmask mPrior = 0;
  int iTab;
  SrcList *pTabList = pWInfo->pTabList;
  struct SrcList_item *pItem;
  sqlite3 *db = pWInfo->pParse->db;
  int nTabList = pWInfo->nLevel;
  int rc = SQLITE_OK;
  u8 priorJoinType = 0;
  WhereLoop *pNew;

  /* Loop over the tables in the join, from left to right */
  pNew = pBuilder->pNew;
  whereLoopInit(pNew);
  for(iTab=0, pItem=pTabList->a; iTab<nTabList; iTab++, pItem++){
    pNew->iTab = iTab;
    pNew->maskSelf = getMask(&pWInfo->sMaskSet, pItem->iCursor);
    if( ((pItem->jointype|priorJoinType) & (JT_LEFT|JT_CROSS))!=0 ){
      mExtra = mPrior;
    }
    priorJoinType = pItem->jointype;
    if( IsVirtual(pItem->pTab) ){
      rc = whereLoopAddVirtual(pBuilder);
    }else{
      rc = whereLoopAddBtree(pBuilder, mExtra);
    }
    if( rc==SQLITE_OK ){
      rc = whereLoopAddOr(pBuilder, mExtra);
    }
    mPrior |= pNew->maskSelf;
    if( rc || db->mallocFailed ) break;
  }
  whereLoopClear(db, pNew);
  return rc;
}

/*
** Examine a WherePath (with the addition of the extra WhereLoop of the 5th
** parameters) to see if it outputs rows in the requested ORDER BY
** (or GROUP BY) without requiring a separate sort operation.  Return:
** 
**    0:  ORDER BY is not satisfied.  Sorting required
**    1:  ORDER BY is satisfied.      Omit sorting
**   -1:  Unknown at this time
**
** Note that processing for WHERE_GROUPBY and WHERE_DISTINCTBY is not as
** strict.  With GROUP BY and DISTINCT the only requirement is that
** equivalent rows appear immediately adjacent to one another.  GROUP BY
** and DISTINT do not require rows to appear in any particular order as long
** as equivelent rows are grouped together.  Thus for GROUP BY and DISTINCT
** the pOrderBy terms can be matched in any order.  With ORDER BY, the 
** pOrderBy terms must be matched in strict left-to-right order.
*/
static int wherePathSatisfiesOrderBy(
  WhereInfo *pWInfo,    /* The WHERE clause */
  ExprList *pOrderBy,   /* ORDER BY or GROUP BY or DISTINCT clause to check */
  WherePath *pPath,     /* The WherePath to check */
  u16 wctrlFlags,       /* Might contain WHERE_GROUPBY or WHERE_DISTINCTBY */
  u16 nLoop,            /* Number of entries in pPath->aLoop[] */
  WhereLoop *pLast,     /* Add this WhereLoop to the end of pPath->aLoop[] */
  Bitmask *pRevMask     /* OUT: Mask of WhereLoops to run in reverse order */
){
  u8 revSet;            /* True if rev is known */
  u8 rev;               /* Composite sort order */
  u8 revIdx;            /* Index sort order */
  u8 isOrderDistinct;   /* All prior WhereLoops are order-distinct */
  u8 distinctColumns;   /* True if the loop has UNIQUE NOT NULL columns */
  u8 isMatch;           /* iColumn matches a term of the ORDER BY clause */
  u16 nColumn;          /* Number of columns in pIndex */
  u16 nOrderBy;         /* Number terms in the ORDER BY clause */
  int iLoop;            /* Index of WhereLoop in pPath being processed */
  int i, j;             /* Loop counters */
  int iCur;             /* Cursor number for current WhereLoop */
  int iColumn;          /* A column number within table iCur */
  WhereLoop *pLoop = 0; /* Current WhereLoop being processed. */
  WhereTerm *pTerm;     /* A single term of the WHERE clause */
  Expr *pOBExpr;        /* An expression from the ORDER BY clause */
  CollSeq *pColl;       /* COLLATE function from an ORDER BY clause term */
  Index *pIndex;        /* The index associated with pLoop */
  sqlite3 *db = pWInfo->pParse->db;  /* Database connection */
  Bitmask obSat = 0;    /* Mask of ORDER BY terms satisfied so far */
  Bitmask obDone;       /* Mask of all ORDER BY terms */
  Bitmask orderDistinctMask;  /* Mask of all well-ordered loops */
  Bitmask ready;              /* Mask of inner loops */

  /*
  ** We say the WhereLoop is "one-row" if it generates no more than one
  ** row of output.  A WhereLoop is one-row if all of the following are true:
  **  (a) All index columns match with WHERE_COLUMN_EQ.
  **  (b) The index is unique
  ** Any WhereLoop with an WHERE_COLUMN_EQ constraint on the rowid is one-row.
  ** Every one-row WhereLoop will have the WHERE_ONEROW bit set in wsFlags.
  **
  ** We say the WhereLoop is "order-distinct" if the set of columns from
  ** that WhereLoop that are in the ORDER BY clause are different for every
  ** row of the WhereLoop.  Every one-row WhereLoop is automatically
  ** order-distinct.   A WhereLoop that has no columns in the ORDER BY clause
  ** is not order-distinct. To be order-distinct is not quite the same as being
  ** UNIQUE since a UNIQUE column or index can have multiple rows that 
  ** are NULL and NULL values are equivalent for the purpose of order-distinct.
  ** To be order-distinct, the columns must be UNIQUE and NOT NULL.
  **
  ** The rowid for a table is always UNIQUE and NOT NULL so whenever the
  ** rowid appears in the ORDER BY clause, the corresponding WhereLoop is
  ** automatically order-distinct.
  */

  assert( pOrderBy!=0 );

  /* Sortability of virtual tables is determined by the xBestIndex method
  ** of the virtual table itself */
  if( pLast->wsFlags & WHERE_VIRTUALTABLE ){
    testcase( nLoop>0 );  /* True when outer loops are one-row and match 
                          ** no ORDER BY terms */
    return pLast->u.vtab.isOrdered;
  }
  if( nLoop && OptimizationDisabled(db, SQLITE_OrderByIdxJoin) ) return 0;

  nOrderBy = pOrderBy->nExpr;
  testcase( nOrderBy==BMS-1 );
  if( nOrderBy>BMS-1 ) return 0;  /* Cannot optimize overly large ORDER BYs */
  isOrderDistinct = 1;
  obDone = MASKBIT(nOrderBy)-1;
  orderDistinctMask = 0;
  ready = 0;
  for(iLoop=0; isOrderDistinct && obSat<obDone && iLoop<=nLoop; iLoop++){
    if( iLoop>0 ) ready |= pLoop->maskSelf;
    pLoop = iLoop<nLoop ? pPath->aLoop[iLoop] : pLast;
    assert( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0 );
    iCur = pWInfo->pTabList->a[pLoop->iTab].iCursor;

    /* Mark off any ORDER BY term X that is a column in the table of
    ** the current loop for which there is term in the WHERE
    ** clause of the form X IS NULL or X=? that reference only outer
    ** loops.
    */
    for(i=0; i<nOrderBy; i++){
      if( MASKBIT(i) & obSat ) continue;
      pOBExpr = sqlite3ExprSkipCollate(pOrderBy->a[i].pExpr);
      if( pOBExpr->op!=TK_COLUMN ) continue;
      if( pOBExpr->iTable!=iCur ) continue;
      pTerm = findTerm(&pWInfo->sWC, iCur, pOBExpr->iColumn,
                       ~ready, WO_EQ|WO_ISNULL, 0);
      if( pTerm==0 ) continue;
      if( (pTerm->eOperator&WO_EQ)!=0 && pOBExpr->iColumn>=0 ){
        const char *z1, *z2;
        pColl = sqlite3ExprCollSeq(pWInfo->pParse, pOrderBy->a[i].pExpr);
        if( !pColl ) pColl = db->pDfltColl;
        z1 = pColl->zName;
        pColl = sqlite3ExprCollSeq(pWInfo->pParse, pTerm->pExpr);
        if( !pColl ) pColl = db->pDfltColl;
        z2 = pColl->zName;
        if( sqlite3StrICmp(z1, z2)!=0 ) continue;
      }
      obSat |= MASKBIT(i);
    }

    if( (pLoop->wsFlags & WHERE_ONEROW)==0 ){
      if( pLoop->wsFlags & WHERE_IPK ){
        pIndex = 0;
        nColumn = 0;
      }else if( (pIndex = pLoop->u.btree.pIndex)==0 || pIndex->bUnordered ){
        return 0;
      }else{
        nColumn = pIndex->nColumn;
        isOrderDistinct = pIndex->onError!=OE_None;
      }

      /* Loop through all columns of the index and deal with the ones
      ** that are not constrained by == or IN.
      */
      rev = revSet = 0;
      distinctColumns = 0;
      for(j=0; j<=nColumn; j++){
        u8 bOnce;   /* True to run the ORDER BY search loop */

        /* Skip over == and IS NULL terms */
        if( j<pLoop->u.btree.nEq
         && ((i = pLoop->aLTerm[j]->eOperator) & (WO_EQ|WO_ISNULL))!=0
        ){
          if( i & WO_ISNULL ){
            testcase( isOrderDistinct );
            isOrderDistinct = 0;
          }
          continue;  
        }

        /* Get the column number in the table (iColumn) and sort order
        ** (revIdx) for the j-th column of the index.
        */
        if( j<nColumn ){
          /* Normal index columns */
          iColumn = pIndex->aiColumn[j];
          revIdx = pIndex->aSortOrder[j];
          if( iColumn==pIndex->pTable->iPKey ) iColumn = -1;
        }else{
          /* The ROWID column at the end */
          assert( j==nColumn );
          iColumn = -1;
          revIdx = 0;
        }

        /* An unconstrained column that might be NULL means that this
        ** WhereLoop is not well-ordered 
        */
        if( isOrderDistinct
         && iColumn>=0
         && j>=pLoop->u.btree.nEq
         && pIndex->pTable->aCol[iColumn].notNull==0
        ){
          isOrderDistinct = 0;
        }

        /* Find the ORDER BY term that corresponds to the j-th column
        ** of the index and and mark that ORDER BY term off 
        */
        bOnce = 1;
        isMatch = 0;
        for(i=0; bOnce && i<nOrderBy; i++){
          if( MASKBIT(i) & obSat ) continue;
          pOBExpr = sqlite3ExprSkipCollate(pOrderBy->a[i].pExpr);
          testcase( wctrlFlags & WHERE_GROUPBY );
          testcase( wctrlFlags & WHERE_DISTINCTBY );
          if( (wctrlFlags & (WHERE_GROUPBY|WHERE_DISTINCTBY))==0 ) bOnce = 0;
          if( pOBExpr->op!=TK_COLUMN ) continue;
          if( pOBExpr->iTable!=iCur ) continue;
          if( pOBExpr->iColumn!=iColumn ) continue;
          if( iColumn>=0 ){
            pColl = sqlite3ExprCollSeq(pWInfo->pParse, pOrderBy->a[i].pExpr);
            if( !pColl ) pColl = db->pDfltColl;
            if( sqlite3StrICmp(pColl->zName, pIndex->azColl[j])!=0 ) continue;
          }
          isMatch = 1;
          break;
        }
        if( isMatch ){
          if( iColumn<0 ){
            testcase( distinctColumns==0 );
            distinctColumns = 1;
          }
          obSat |= MASKBIT(i);
          if( (pWInfo->wctrlFlags & WHERE_GROUPBY)==0 ){
            /* Make sure the sort order is compatible in an ORDER BY clause.
            ** Sort order is irrelevant for a GROUP BY clause. */
            if( revSet ){
              if( (rev ^ revIdx)!=pOrderBy->a[i].sortOrder ) return 0;
            }else{
              rev = revIdx ^ pOrderBy->a[i].sortOrder;
              if( rev ) *pRevMask |= MASKBIT(iLoop);
              revSet = 1;
            }
          }
        }else{
          /* No match found */
          if( j==0 || j<nColumn ){
            testcase( isOrderDistinct!=0 );
            isOrderDistinct = 0;
          }
          break;
        }
      } /* end Loop over all index columns */
      if( distinctColumns ){
        testcase( isOrderDistinct==0 );
        isOrderDistinct = 1;
      }
    } /* end-if not one-row */

    /* Mark off any other ORDER BY terms that reference pLoop */
    if( isOrderDistinct ){
      orderDistinctMask |= pLoop->maskSelf;
      for(i=0; i<nOrderBy; i++){
        Expr *p;
        if( MASKBIT(i) & obSat ) continue;
        p = pOrderBy->a[i].pExpr;
        if( (exprTableUsage(&pWInfo->sMaskSet, p)&~orderDistinctMask)==0 ){
          obSat |= MASKBIT(i);
        }
      }
    }
  } /* End the loop over all WhereLoops from outer-most down to inner-most */
  if( obSat==obDone ) return 1;
  if( !isOrderDistinct ) return 0;
  return -1;
}

#ifdef WHERETRACE_ENABLED
/* For debugging use only: */
static const char *wherePathName(WherePath *pPath, int nLoop, WhereLoop *pLast){
  static char zName[65];
  int i;
  for(i=0; i<nLoop; i++){ zName[i] = pPath->aLoop[i]->cId; }
  if( pLast ) zName[i++] = pLast->cId;
  zName[i] = 0;
  return zName;
}
#endif


/*
** Given the list of WhereLoop objects at pWInfo->pLoops, this routine
** attempts to find the lowest cost path that visits each WhereLoop
** once.  This path is then loaded into the pWInfo->a[].pWLoop fields.
**
** Assume that the total number of output rows that will need to be sorted
** will be nRowEst (in the 10*log2 representation).  Or, ignore sorting
** costs if nRowEst==0.
**
** Return SQLITE_OK on success or SQLITE_NOMEM of a memory allocation
** error occurs.
*/
static int wherePathSolver(WhereInfo *pWInfo, WhereCost nRowEst){
  int mxChoice;             /* Maximum number of simultaneous paths tracked */
  int nLoop;                /* Number of terms in the join */
  Parse *pParse;            /* Parsing context */
  sqlite3 *db;              /* The database connection */
  int iLoop;                /* Loop counter over the terms of the join */
  int ii, jj;               /* Loop counters */
  WhereCost rCost;             /* Cost of a path */
  WhereCost mxCost = 0;        /* Maximum cost of a set of paths */
  WhereCost rSortCost;         /* Cost to do a sort */
  int nTo, nFrom;           /* Number of valid entries in aTo[] and aFrom[] */
  WherePath *aFrom;         /* All nFrom paths at the previous level */
  WherePath *aTo;           /* The nTo best paths at the current level */
  WherePath *pFrom;         /* An element of aFrom[] that we are working on */
  WherePath *pTo;           /* An element of aTo[] that we are working on */
  WhereLoop *pWLoop;        /* One of the WhereLoop objects */
  WhereLoop **pX;           /* Used to divy up the pSpace memory */
  char *pSpace;             /* Temporary memory used by this routine */

  pParse = pWInfo->pParse;
  db = pParse->db;
  nLoop = pWInfo->nLevel;
  /* TUNING: For simple queries, only the best path is tracked.
  ** For 2-way joins, the 5 best paths are followed.
  ** For joins of 3 or more tables, track the 10 best paths */
  mxChoice = (nLoop==1) ? 1 : (nLoop==2 ? 5 : 10);
  assert( nLoop<=pWInfo->pTabList->nSrc );
  WHERETRACE(0x002, ("---- begin solver\n"));

  /* Allocate and initialize space for aTo and aFrom */
  ii = (sizeof(WherePath)+sizeof(WhereLoop*)*nLoop)*mxChoice*2;
  pSpace = sqlite3DbMallocRaw(db, ii);
  if( pSpace==0 ) return SQLITE_NOMEM;
  aTo = (WherePath*)pSpace;
  aFrom = aTo+mxChoice;
  memset(aFrom, 0, sizeof(aFrom[0]));
  pX = (WhereLoop**)(aFrom+mxChoice);
  for(ii=mxChoice*2, pFrom=aTo; ii>0; ii--, pFrom++, pX += nLoop){
    pFrom->aLoop = pX;
  }

  /* Seed the search with a single WherePath containing zero WhereLoops.
  **
  ** TUNING: Do not let the number of iterations go above 25.  If the cost
  ** of computing an automatic index is not paid back within the first 25
  ** rows, then do not use the automatic index. */
  aFrom[0].nRow = MIN(pParse->nQueryLoop, 46);  assert( 46==whereCost(25) );
  nFrom = 1;

  /* Precompute the cost of sorting the final result set, if the caller
  ** to sqlite3WhereBegin() was concerned about sorting */
  rSortCost = 0;
  if( pWInfo->pOrderBy==0 || nRowEst==0 ){
    aFrom[0].isOrderedValid = 1;
  }else{
    /* TUNING: Estimated cost of sorting is N*log2(N) where N is the
    ** number of output rows. */
    rSortCost = nRowEst + estLog(nRowEst);
    WHERETRACE(0x002,("---- sort cost=%-3d\n", rSortCost));
  }

  /* Compute successively longer WherePaths using the previous generation
  ** of WherePaths as the basis for the next.  Keep track of the mxChoice
  ** best paths at each generation */
  for(iLoop=0; iLoop<nLoop; iLoop++){
    nTo = 0;
    for(ii=0, pFrom=aFrom; ii<nFrom; ii++, pFrom++){
      for(pWLoop=pWInfo->pLoops; pWLoop; pWLoop=pWLoop->pNextLoop){
        Bitmask maskNew;
        Bitmask revMask = 0;
        u8 isOrderedValid = pFrom->isOrderedValid;
        u8 isOrdered = pFrom->isOrdered;
        if( (pWLoop->prereq & ~pFrom->maskLoop)!=0 ) continue;
        if( (pWLoop->maskSelf & pFrom->maskLoop)!=0 ) continue;
        /* At this point, pWLoop is a candidate to be the next loop. 
        ** Compute its cost */
        rCost = whereCostAdd(pWLoop->rSetup,pWLoop->rRun + pFrom->nRow);
        rCost = whereCostAdd(rCost, pFrom->rCost);
        maskNew = pFrom->maskLoop | pWLoop->maskSelf;
        if( !isOrderedValid ){
          switch( wherePathSatisfiesOrderBy(pWInfo,
                       pWInfo->pOrderBy, pFrom, pWInfo->wctrlFlags,
                       iLoop, pWLoop, &revMask) ){
            case 1:  /* Yes.  pFrom+pWLoop does satisfy the ORDER BY clause */
              isOrdered = 1;
              isOrderedValid = 1;
              break;
            case 0:  /* No.  pFrom+pWLoop will require a separate sort */
              isOrdered = 0;
              isOrderedValid = 1;
              rCost = whereCostAdd(rCost, rSortCost);
              break;
            default: /* Cannot tell yet.  Try again on the next iteration */
              break;
          }
        }else{
          revMask = pFrom->revLoop;
        }
        /* Check to see if pWLoop should be added to the mxChoice best so far */
        for(jj=0, pTo=aTo; jj<nTo; jj++, pTo++){
          if( pTo->maskLoop==maskNew && pTo->isOrderedValid==isOrderedValid ){
            testcase( jj==nTo-1 );
            break;
          }
        }
        if( jj>=nTo ){
          if( nTo>=mxChoice && rCost>=mxCost ){
#ifdef WHERETRACE_ENABLED
            if( sqlite3WhereTrace&0x4 ){
              sqlite3DebugPrintf("Skip   %s cost=%3d order=%c\n",
                  wherePathName(pFrom, iLoop, pWLoop), rCost,
                  isOrderedValid ? (isOrdered ? 'Y' : 'N') : '?');
            }
#endif
            continue;
          }
          /* Add a new Path to the aTo[] set */
          if( nTo<mxChoice ){
            /* Increase the size of the aTo set by one */
            jj = nTo++;
          }else{
            /* New path replaces the prior worst to keep count below mxChoice */
            for(jj=nTo-1; aTo[jj].rCost<mxCost; jj--){ assert(jj>0); }
          }
          pTo = &aTo[jj];
#ifdef WHERETRACE_ENABLED
          if( sqlite3WhereTrace&0x4 ){
            sqlite3DebugPrintf("New    %s cost=%-3d order=%c\n",
                wherePathName(pFrom, iLoop, pWLoop), rCost,
                isOrderedValid ? (isOrdered ? 'Y' : 'N') : '?');
          }
#endif
        }else{
          if( pTo->rCost<=rCost ){
#ifdef WHERETRACE_ENABLED
            if( sqlite3WhereTrace&0x4 ){
              sqlite3DebugPrintf(
                  "Skip   %s cost=%-3d order=%c",
                  wherePathName(pFrom, iLoop, pWLoop), rCost,
                  isOrderedValid ? (isOrdered ? 'Y' : 'N') : '?');
              sqlite3DebugPrintf("   vs %s cost=%-3d order=%c\n",
                  wherePathName(pTo, iLoop+1, 0), pTo->rCost,
                  pTo->isOrderedValid ? (pTo->isOrdered ? 'Y' : 'N') : '?');
            }
#endif
            testcase( pTo->rCost==rCost );
            continue;
          }
          testcase( pTo->rCost==rCost+1 );
          /* A new and better score for a previously created equivalent path */
#ifdef WHERETRACE_ENABLED
          if( sqlite3WhereTrace&0x4 ){
            sqlite3DebugPrintf(
                "Update %s cost=%-3d order=%c",
                wherePathName(pFrom, iLoop, pWLoop), rCost,
                isOrderedValid ? (isOrdered ? 'Y' : 'N') : '?');
            sqlite3DebugPrintf("  was %s cost=%-3d order=%c\n",
                wherePathName(pTo, iLoop+1, 0), pTo->rCost,
                pTo->isOrderedValid ? (pTo->isOrdered ? 'Y' : 'N') : '?');
          }
#endif
        }
        /* pWLoop is a winner.  Add it to the set of best so far */
        pTo->maskLoop = pFrom->maskLoop | pWLoop->maskSelf;
        pTo->revLoop = revMask;
        pTo->nRow = pFrom->nRow + pWLoop->nOut;
        pTo->rCost = rCost;
        pTo->isOrderedValid = isOrderedValid;
        pTo->isOrdered = isOrdered;
        memcpy(pTo->aLoop, pFrom->aLoop, sizeof(WhereLoop*)*iLoop);
        pTo->aLoop[iLoop] = pWLoop;
        if( nTo>=mxChoice ){
          mxCost = aTo[0].rCost;
          for(jj=1, pTo=&aTo[1]; jj<mxChoice; jj++, pTo++){
            if( pTo->rCost>mxCost ) mxCost = pTo->rCost;
          }
        }
      }
    }

#ifdef WHERETRACE_ENABLED
    if( sqlite3WhereTrace>=2 ){
      sqlite3DebugPrintf("---- after round %d ----\n", iLoop);
      for(ii=0, pTo=aTo; ii<nTo; ii++, pTo++){
        sqlite3DebugPrintf(" %s cost=%-3d nrow=%-3d order=%c",
           wherePathName(pTo, iLoop+1, 0), pTo->rCost, pTo->nRow,
           pTo->isOrderedValid ? (pTo->isOrdered ? 'Y' : 'N') : '?');
        if( pTo->isOrderedValid && pTo->isOrdered ){
          sqlite3DebugPrintf(" rev=0x%llx\n", pTo->revLoop);
        }else{
          sqlite3DebugPrintf("\n");
        }
      }
    }
#endif

    /* Swap the roles of aFrom and aTo for the next generation */
    pFrom = aTo;
    aTo = aFrom;
    aFrom = pFrom;
    nFrom = nTo;
  }

  if( nFrom==0 ){
    sqlite3ErrorMsg(pParse, "no query solution");
    sqlite3DbFree(db, pSpace);
    return SQLITE_ERROR;
  }
  
  /* Find the lowest cost path.  pFrom will be left pointing to that path */
  pFrom = aFrom;
  assert( nFrom==1 );
#if 0 /* The following is needed if nFrom is ever more than 1 */
  for(ii=1; ii<nFrom; ii++){
    if( pFrom->rCost>aFrom[ii].rCost ) pFrom = &aFrom[ii];
  }
#endif
  assert( pWInfo->nLevel==nLoop );
  /* Load the lowest cost path into pWInfo */
  for(iLoop=0; iLoop<nLoop; iLoop++){
    WhereLevel *pLevel = pWInfo->a + iLoop;
    pLevel->pWLoop = pWLoop = pFrom->aLoop[iLoop];
    pLevel->iFrom = pWLoop->iTab;
    pLevel->iTabCur = pWInfo->pTabList->a[pLevel->iFrom].iCursor;
  }
  if( (pWInfo->wctrlFlags & WHERE_WANT_DISTINCT)!=0
   && (pWInfo->wctrlFlags & WHERE_DISTINCTBY)==0
   && pWInfo->eDistinct==WHERE_DISTINCT_NOOP
   && nRowEst
  ){
    Bitmask notUsed;
    int rc = wherePathSatisfiesOrderBy(pWInfo, pWInfo->pResultSet, pFrom,
                 WHERE_DISTINCTBY, nLoop-1, pFrom->aLoop[nLoop-1], &notUsed);
    if( rc==1 ) pWInfo->eDistinct = WHERE_DISTINCT_ORDERED;
  }
  if( pFrom->isOrdered ){
    if( pWInfo->wctrlFlags & WHERE_DISTINCTBY ){
      pWInfo->eDistinct = WHERE_DISTINCT_ORDERED;
    }else{
      pWInfo->bOBSat = 1;
      pWInfo->revMask = pFrom->revLoop;
    }
  }
  pWInfo->nRowOut = pFrom->nRow;

  /* Free temporary memory and return success */
  sqlite3DbFree(db, pSpace);
  return SQLITE_OK;
}

/*
** Most queries use only a single table (they are not joins) and have
** simple == constraints against indexed fields.  This routine attempts
** to plan those simple cases using much less ceremony than the
** general-purpose query planner, and thereby yield faster sqlite3_prepare()
** times for the common case.
**
** Return non-zero on success, if this query can be handled by this
** no-frills query planner.  Return zero if this query needs the 
** general-purpose query planner.
*/
static int whereShortCut(WhereLoopBuilder *pBuilder){
  WhereInfo *pWInfo;
  struct SrcList_item *pItem;
  WhereClause *pWC;
  WhereTerm *pTerm;
  WhereLoop *pLoop;
  int iCur;
  int j;
  Table *pTab;
  Index *pIdx;
  
  pWInfo = pBuilder->pWInfo;
  if( pWInfo->wctrlFlags & WHERE_FORCE_TABLE ) return 0;
  assert( pWInfo->pTabList->nSrc>=1 );
  pItem = pWInfo->pTabList->a;
  pTab = pItem->pTab;
  if( IsVirtual(pTab) ) return 0;
  if( pItem->zIndex ) return 0;
  iCur = pItem->iCursor;
  pWC = &pWInfo->sWC;
  pLoop = pBuilder->pNew;
  pLoop->wsFlags = 0;
  pTerm = findTerm(pWC, iCur, -1, 0, WO_EQ, 0);
  if( pTerm ){
    pLoop->wsFlags = WHERE_COLUMN_EQ|WHERE_IPK|WHERE_ONEROW;
    pLoop->aLTerm[0] = pTerm;
    pLoop->nLTerm = 1;
    pLoop->u.btree.nEq = 1;
    /* TUNING: Cost of a rowid lookup is 10 */
    pLoop->rRun = 33;  /* 33==whereCost(10) */
  }else{
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      assert( pLoop->aLTermSpace==pLoop->aLTerm );
      assert( ArraySize(pLoop->aLTermSpace)==4 );
      if( pIdx->onError==OE_None 
       || pIdx->pPartIdxWhere!=0 
       || pIdx->nColumn>ArraySize(pLoop->aLTermSpace) 
      ) continue;
      for(j=0; j<pIdx->nColumn; j++){
        pTerm = findTerm(pWC, iCur, pIdx->aiColumn[j], 0, WO_EQ, pIdx);
        if( pTerm==0 ) break;
        pLoop->aLTerm[j] = pTerm;
      }
      if( j!=pIdx->nColumn ) continue;
      pLoop->wsFlags = WHERE_COLUMN_EQ|WHERE_ONEROW|WHERE_INDEXED;
      if( (pItem->colUsed & ~columnsInIndex(pIdx))==0 ){
        pLoop->wsFlags |= WHERE_IDX_ONLY;
      }
      pLoop->nLTerm = j;
      pLoop->u.btree.nEq = j;
      pLoop->u.btree.pIndex = pIdx;
      /* TUNING: Cost of a unique index lookup is 15 */
      pLoop->rRun = 39;  /* 39==whereCost(15) */
      break;
    }
  }
  if( pLoop->wsFlags ){
    pLoop->nOut = (WhereCost)1;
    pWInfo->a[0].pWLoop = pLoop;
    pLoop->maskSelf = getMask(&pWInfo->sMaskSet, iCur);
    pWInfo->a[0].iTabCur = iCur;
    pWInfo->nRowOut = 1;
    if( pWInfo->pOrderBy ) pWInfo->bOBSat =  1;
    if( pWInfo->wctrlFlags & WHERE_WANT_DISTINCT ){
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }
#ifdef SQLITE_DEBUG
    pLoop->cId = '0';
#endif
    return 1;
  }
  return 0;
}

/*
** Generate the beginning of the loop used for WHERE clause processing.
** The return value is a pointer to an opaque structure that contains
** information needed to terminate the loop.  Later, the calling routine
** should invoke sqlite3WhereEnd() with the return value of this function
** in order to complete the WHERE clause processing.
**
** If an error occurs, this routine returns NULL.
**
** The basic idea is to do a nested loop, one loop for each table in
** the FROM clause of a select.  (INSERT and UPDATE statements are the
** same as a SELECT with only a single table in the FROM clause.)  For
** example, if the SQL is this:
**
**       SELECT * FROM t1, t2, t3 WHERE ...;
**
** Then the code generated is conceptually like the following:
**
**      foreach row1 in t1 do       \    Code generated
**        foreach row2 in t2 do      |-- by sqlite3WhereBegin()
**          foreach row3 in t3 do   /
**            ...
**          end                     \    Code generated
**        end                        |-- by sqlite3WhereEnd()
**      end                         /
**
** Note that the loops might not be nested in the order in which they
** appear in the FROM clause if a different order is better able to make
** use of indices.  Note also that when the IN operator appears in
** the WHERE clause, it might result in additional nested loops for
** scanning through all values on the right-hand side of the IN.
**
** There are Btree cursors associated with each table.  t1 uses cursor
** number pTabList->a[0].iCursor.  t2 uses the cursor pTabList->a[1].iCursor.
** And so forth.  This routine generates code to open those VDBE cursors
** and sqlite3WhereEnd() generates the code to close them.
**
** The code that sqlite3WhereBegin() generates leaves the cursors named
** in pTabList pointing at their appropriate entries.  The [...] code
** can use OP_Column and OP_Rowid opcodes on these cursors to extract
** data from the various tables of the loop.
**
** If the WHERE clause is empty, the foreach loops must each scan their
** entire tables.  Thus a three-way join is an O(N^3) operation.  But if
** the tables have indices and there are terms in the WHERE clause that
** refer to those indices, a complete table scan can be avoided and the
** code will run much faster.  Most of the work of this routine is checking
** to see if there are indices that can be used to speed up the loop.
**
** Terms of the WHERE clause are also used to limit which rows actually
** make it to the "..." in the middle of the loop.  After each "foreach",
** terms of the WHERE clause that use only terms in that loop and outer
** loops are evaluated and if false a jump is made around all subsequent
** inner loops (or around the "..." if the test occurs within the inner-
** most loop)
**
** OUTER JOINS
**
** An outer join of tables t1 and t2 is conceptally coded as follows:
**
**    foreach row1 in t1 do
**      flag = 0
**      foreach row2 in t2 do
**        start:
**          ...
**          flag = 1
**      end
**      if flag==0 then
**        move the row2 cursor to a null row
**        goto start
**      fi
**    end
**
** ORDER BY CLAUSE PROCESSING
**
** pOrderBy is a pointer to the ORDER BY clause (or the GROUP BY clause
** if the WHERE_GROUPBY flag is set in wctrlFlags) of a SELECT statement
** if there is one.  If there is no ORDER BY clause or if this routine
** is called from an UPDATE or DELETE statement, then pOrderBy is NULL.
*/
WhereInfo *sqlite3WhereBegin(
  Parse *pParse,        /* The parser context */
  SrcList *pTabList,    /* FROM clause: A list of all tables to be scanned */
  Expr *pWhere,         /* The WHERE clause */
  ExprList *pOrderBy,   /* An ORDER BY clause, or NULL */
  ExprList *pResultSet, /* Result set of the query */
  u16 wctrlFlags,       /* One of the WHERE_* flags defined in sqliteInt.h */
  int iIdxCur           /* If WHERE_ONETABLE_ONLY is set, index cursor number */
){
  int nByteWInfo;            /* Num. bytes allocated for WhereInfo struct */
  int nTabList;              /* Number of elements in pTabList */
  WhereInfo *pWInfo;         /* Will become the return value of this function */
  Vdbe *v = pParse->pVdbe;   /* The virtual database engine */
  Bitmask notReady;          /* Cursors that are not yet positioned */
  WhereLoopBuilder sWLB;     /* The WhereLoop builder */
  WhereMaskSet *pMaskSet;    /* The expression mask set */
  WhereLevel *pLevel;        /* A single level in pWInfo->a[] */
  WhereLoop *pLoop;          /* Pointer to a single WhereLoop object */
  int ii;                    /* Loop counter */
  sqlite3 *db;               /* Database connection */
  int rc;                    /* Return code */


  /* Variable initialization */
  db = pParse->db;
  memset(&sWLB, 0, sizeof(sWLB));
  sWLB.pOrderBy = pOrderBy;

  /* Disable the DISTINCT optimization if SQLITE_DistinctOpt is set via
  ** sqlite3_test_ctrl(SQLITE_TESTCTRL_OPTIMIZATIONS,...) */
  if( OptimizationDisabled(db, SQLITE_DistinctOpt) ){
    wctrlFlags &= ~WHERE_WANT_DISTINCT;
  }

  /* The number of tables in the FROM clause is limited by the number of
  ** bits in a Bitmask 
  */
  testcase( pTabList->nSrc==BMS );
  if( pTabList->nSrc>BMS ){
    sqlite3ErrorMsg(pParse, "at most %d tables in a join", BMS);
    return 0;
  }

  /* This function normally generates a nested loop for all tables in 
  ** pTabList.  But if the WHERE_ONETABLE_ONLY flag is set, then we should
  ** only generate code for the first table in pTabList and assume that
  ** any cursors associated with subsequent tables are uninitialized.
  */
  nTabList = (wctrlFlags & WHERE_ONETABLE_ONLY) ? 1 : pTabList->nSrc;

  /* Allocate and initialize the WhereInfo structure that will become the
  ** return value. A single allocation is used to store the WhereInfo
  ** struct, the contents of WhereInfo.a[], the WhereClause structure
  ** and the WhereMaskSet structure. Since WhereClause contains an 8-byte
  ** field (type Bitmask) it must be aligned on an 8-byte boundary on
  ** some architectures. Hence the ROUND8() below.
  */
  nByteWInfo = ROUND8(sizeof(WhereInfo)+(nTabList-1)*sizeof(WhereLevel));
  pWInfo = sqlite3DbMallocZero(db, nByteWInfo + sizeof(WhereLoop));
  if( db->mallocFailed ){
    sqlite3DbFree(db, pWInfo);
    pWInfo = 0;
    goto whereBeginError;
  }
  pWInfo->nLevel = nTabList;
  pWInfo->pParse = pParse;
  pWInfo->pTabList = pTabList;
  pWInfo->pOrderBy = pOrderBy;
  pWInfo->pResultSet = pResultSet;
  pWInfo->iBreak = sqlite3VdbeMakeLabel(v);
  pWInfo->wctrlFlags = wctrlFlags;
  pWInfo->savedNQueryLoop = pParse->nQueryLoop;
  pMaskSet = &pWInfo->sMaskSet;
  sWLB.pWInfo = pWInfo;
  sWLB.pWC = &pWInfo->sWC;
  sWLB.pNew = (WhereLoop*)(((char*)pWInfo)+nByteWInfo);
  assert( EIGHT_BYTE_ALIGNMENT(sWLB.pNew) );
  whereLoopInit(sWLB.pNew);
#ifdef SQLITE_DEBUG
  sWLB.pNew->cId = '*';
#endif

  /* Split the WHERE clause into separate subexpressions where each
  ** subexpression is separated by an AND operator.
  */
  initMaskSet(pMaskSet);
  whereClauseInit(&pWInfo->sWC, pWInfo);
  sqlite3ExprCodeConstants(pParse, pWhere);
  whereSplit(&pWInfo->sWC, pWhere, TK_AND);
  sqlite3CodeVerifySchema(pParse, -1); /* Insert the cookie verifier Goto */
    
  /* Special case: a WHERE clause that is constant.  Evaluate the
  ** expression and either jump over all of the code or fall thru.
  */
  if( pWhere && (nTabList==0 || sqlite3ExprIsConstantNotJoin(pWhere)) ){
    sqlite3ExprIfFalse(pParse, pWhere, pWInfo->iBreak, SQLITE_JUMPIFNULL);
    pWhere = 0;
  }

  /* Special case: No FROM clause
  */
  if( nTabList==0 ){
    if( pOrderBy ) pWInfo->bOBSat = 1;
    if( wctrlFlags & WHERE_WANT_DISTINCT ){
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }
  }

  /* Assign a bit from the bitmask to every term in the FROM clause.
  **
  ** When assigning bitmask values to FROM clause cursors, it must be
  ** the case that if X is the bitmask for the N-th FROM clause term then
  ** the bitmask for all FROM clause terms to the left of the N-th term
  ** is (X-1).   An expression from the ON clause of a LEFT JOIN can use
  ** its Expr.iRightJoinTable value to find the bitmask of the right table
  ** of the join.  Subtracting one from the right table bitmask gives a
  ** bitmask for all tables to the left of the join.  Knowing the bitmask
  ** for all tables to the left of a left join is important.  Ticket #3015.
  **
  ** Note that bitmasks are created for all pTabList->nSrc tables in
  ** pTabList, not just the first nTabList tables.  nTabList is normally
  ** equal to pTabList->nSrc but might be shortened to 1 if the
  ** WHERE_ONETABLE_ONLY flag is set.
  */
  for(ii=0; ii<pTabList->nSrc; ii++){
    createMask(pMaskSet, pTabList->a[ii].iCursor);
  }
#ifndef NDEBUG
  {
    Bitmask toTheLeft = 0;
    for(ii=0; ii<pTabList->nSrc; ii++){
      Bitmask m = getMask(pMaskSet, pTabList->a[ii].iCursor);
      assert( (m-1)==toTheLeft );
      toTheLeft |= m;
    }
  }
#endif

  /* Analyze all of the subexpressions.  Note that exprAnalyze() might
  ** add new virtual terms onto the end of the WHERE clause.  We do not
  ** want to analyze these virtual terms, so start analyzing at the end
  ** and work forward so that the added virtual terms are never processed.
  */
  exprAnalyzeAll(pTabList, &pWInfo->sWC);
  if( db->mallocFailed ){
    goto whereBeginError;
  }

  /* If the ORDER BY (or GROUP BY) clause contains references to general
  ** expressions, then we won't be able to satisfy it using indices, so
  ** go ahead and disable it now.
  */
  if( pOrderBy && (wctrlFlags & WHERE_WANT_DISTINCT)!=0 ){
    for(ii=0; ii<pOrderBy->nExpr; ii++){
      Expr *pExpr = sqlite3ExprSkipCollate(pOrderBy->a[ii].pExpr);
      if( pExpr->op!=TK_COLUMN ){
        pWInfo->pOrderBy = pOrderBy = 0;
        break;
      }else if( pExpr->iColumn<0 ){
        break;
      }
    }
  }

  if( wctrlFlags & WHERE_WANT_DISTINCT ){
    if( isDistinctRedundant(pParse, pTabList, &pWInfo->sWC, pResultSet) ){
      /* The DISTINCT marking is pointless.  Ignore it. */
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }else if( pOrderBy==0 ){
      /* Try to ORDER BY the result set to make distinct processing easier */
      pWInfo->wctrlFlags |= WHERE_DISTINCTBY;
      pWInfo->pOrderBy = pResultSet;
    }
  }

  /* Construct the WhereLoop objects */
  WHERETRACE(0xffff,("*** Optimizer Start ***\n"));
  if( nTabList!=1 || whereShortCut(&sWLB)==0 ){
    rc = whereLoopAddAll(&sWLB);
    if( rc ) goto whereBeginError;
  
    /* Display all of the WhereLoop objects if wheretrace is enabled */
#ifdef WHERETRACE_ENABLED
    if( sqlite3WhereTrace ){
      WhereLoop *p;
      int i;
      static char zLabel[] = "0123456789abcdefghijklmnopqrstuvwyxz"
                                       "ABCDEFGHIJKLMNOPQRSTUVWYXZ";
      for(p=pWInfo->pLoops, i=0; p; p=p->pNextLoop, i++){
        p->cId = zLabel[i%sizeof(zLabel)];
        whereLoopPrint(p, pTabList);
      }
    }
#endif
  
    wherePathSolver(pWInfo, 0);
    if( db->mallocFailed ) goto whereBeginError;
    if( pWInfo->pOrderBy ){
       wherePathSolver(pWInfo, pWInfo->nRowOut+1);
       if( db->mallocFailed ) goto whereBeginError;
    }
  }
  if( pWInfo->pOrderBy==0 && (db->flags & SQLITE_ReverseOrder)!=0 ){
     pWInfo->revMask = (Bitmask)(-1);
  }
  if( pParse->nErr || NEVER(db->mallocFailed) ){
    goto whereBeginError;
  }
#ifdef WHERETRACE_ENABLED
  if( sqlite3WhereTrace ){
    int ii;
    sqlite3DebugPrintf("---- Solution nRow=%d", pWInfo->nRowOut);
    if( pWInfo->bOBSat ){
      sqlite3DebugPrintf(" ORDERBY=0x%llx", pWInfo->revMask);
    }
    switch( pWInfo->eDistinct ){
      case WHERE_DISTINCT_UNIQUE: {
        sqlite3DebugPrintf("  DISTINCT=unique");
        break;
      }
      case WHERE_DISTINCT_ORDERED: {
        sqlite3DebugPrintf("  DISTINCT=ordered");
        break;
      }
      case WHERE_DISTINCT_UNORDERED: {
        sqlite3DebugPrintf("  DISTINCT=unordered");
        break;
      }
    }
    sqlite3DebugPrintf("\n");
    for(ii=0; ii<pWInfo->nLevel; ii++){
      whereLoopPrint(pWInfo->a[ii].pWLoop, pTabList);
    }
  }
#endif
  /* Attempt to omit tables from the join that do not effect the result */
  if( pWInfo->nLevel>=2
   && pResultSet!=0
   && OptimizationEnabled(db, SQLITE_OmitNoopJoin)
  ){
    Bitmask tabUsed = exprListTableUsage(pMaskSet, pResultSet);
    if( sWLB.pOrderBy ) tabUsed |= exprListTableUsage(pMaskSet, sWLB.pOrderBy);
    while( pWInfo->nLevel>=2 ){
      WhereTerm *pTerm, *pEnd;
      pLoop = pWInfo->a[pWInfo->nLevel-1].pWLoop;
      if( (pWInfo->pTabList->a[pLoop->iTab].jointype & JT_LEFT)==0 ) break;
      if( (wctrlFlags & WHERE_WANT_DISTINCT)==0
       && (pLoop->wsFlags & WHERE_ONEROW)==0
      ){
        break;
      }
      if( (tabUsed & pLoop->maskSelf)!=0 ) break;
      pEnd = sWLB.pWC->a + sWLB.pWC->nTerm;
      for(pTerm=sWLB.pWC->a; pTerm<pEnd; pTerm++){
        if( (pTerm->prereqAll & pLoop->maskSelf)!=0
         && !ExprHasProperty(pTerm->pExpr, EP_FromJoin)
        ){
          break;
        }
      }
      if( pTerm<pEnd ) break;
      WHERETRACE(0xffff, ("-> drop loop %c not used\n", pLoop->cId));
      pWInfo->nLevel--;
      nTabList--;
    }
  }
  WHERETRACE(0xffff,("*** Optimizer Finished ***\n"));
  pWInfo->pParse->nQueryLoop += pWInfo->nRowOut;

  /* If the caller is an UPDATE or DELETE statement that is requesting
  ** to use a one-pass algorithm, determine if this is appropriate.
  ** The one-pass algorithm only works if the WHERE clause constraints
  ** the statement to update a single row.
  */
  assert( (wctrlFlags & WHERE_ONEPASS_DESIRED)==0 || pWInfo->nLevel==1 );
  if( (wctrlFlags & WHERE_ONEPASS_DESIRED)!=0 
   && (pWInfo->a[0].pWLoop->wsFlags & WHERE_ONEROW)!=0 ){
    pWInfo->okOnePass = 1;
    pWInfo->a[0].pWLoop->wsFlags &= ~WHERE_IDX_ONLY;
  }

  /* Open all tables in the pTabList and any indices selected for
  ** searching those tables.
  */
  notReady = ~(Bitmask)0;
  for(ii=0, pLevel=pWInfo->a; ii<nTabList; ii++, pLevel++){
    Table *pTab;     /* Table to open */
    int iDb;         /* Index of database containing table/index */
    struct SrcList_item *pTabItem;

    pTabItem = &pTabList->a[pLevel->iFrom];
    pTab = pTabItem->pTab;
    iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
    pLoop = pLevel->pWLoop;
    if( (pTab->tabFlags & TF_Ephemeral)!=0 || pTab->pSelect ){
      /* Do nothing */
    }else
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( (pLoop->wsFlags & WHERE_VIRTUALTABLE)!=0 ){
      const char *pVTab = (const char *)sqlite3GetVTable(db, pTab);
      int iCur = pTabItem->iCursor;
      sqlite3VdbeAddOp4(v, OP_VOpen, iCur, 0, 0, pVTab, P4_VTAB);
    }else if( IsVirtual(pTab) ){
      /* noop */
    }else
#endif
    if( (pLoop->wsFlags & WHERE_IDX_ONLY)==0
         && (wctrlFlags & WHERE_OMIT_OPEN_CLOSE)==0 ){
      int op = pWInfo->okOnePass ? OP_OpenWrite : OP_OpenRead;
      sqlite3OpenTable(pParse, pTabItem->iCursor, iDb, pTab, op);
      testcase( !pWInfo->okOnePass && pTab->nCol==BMS-1 );
      testcase( !pWInfo->okOnePass && pTab->nCol==BMS );
      if( !pWInfo->okOnePass && pTab->nCol<BMS ){
        Bitmask b = pTabItem->colUsed;
        int n = 0;
        for(; b; b=b>>1, n++){}
        sqlite3VdbeChangeP4(v, sqlite3VdbeCurrentAddr(v)-1, 
                            SQLITE_INT_TO_PTR(n), P4_INT32);
        assert( n<=pTab->nCol );
      }
    }else{
      sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);
    }
    if( pLoop->wsFlags & WHERE_INDEXED ){
      Index *pIx = pLoop->u.btree.pIndex;
      KeyInfo *pKey = sqlite3IndexKeyinfo(pParse, pIx);
      /* FIXME:  As an optimization use pTabItem->iCursor if WHERE_IDX_ONLY */
      int iIndexCur = pLevel->iIdxCur = iIdxCur ? iIdxCur : pParse->nTab++;
      assert( pIx->pSchema==pTab->pSchema );
      assert( iIndexCur>=0 );
      sqlite3VdbeAddOp4(v, OP_OpenRead, iIndexCur, pIx->tnum, iDb,
                        (char*)pKey, P4_KEYINFO_HANDOFF);
      VdbeComment((v, "%s", pIx->zName));
    }
    sqlite3CodeVerifySchema(pParse, iDb);
    notReady &= ~getMask(&pWInfo->sMaskSet, pTabItem->iCursor);
  }
  pWInfo->iTop = sqlite3VdbeCurrentAddr(v);
  if( db->mallocFailed ) goto whereBeginError;

  /* Generate the code to do the search.  Each iteration of the for
  ** loop below generates code for a single nested loop of the VM
  ** program.
  */
  notReady = ~(Bitmask)0;
  for(ii=0; ii<nTabList; ii++){
    pLevel = &pWInfo->a[ii];
#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
    if( (pLevel->pWLoop->wsFlags & WHERE_AUTO_INDEX)!=0 ){
      constructAutomaticIndex(pParse, &pWInfo->sWC,
                &pTabList->a[pLevel->iFrom], notReady, pLevel);
      if( db->mallocFailed ) goto whereBeginError;
    }
#endif
    explainOneScan(pParse, pTabList, pLevel, ii, pLevel->iFrom, wctrlFlags);
    pLevel->addrBody = sqlite3VdbeCurrentAddr(v);
    notReady = codeOneLoopStart(pWInfo, ii, notReady);
    pWInfo->iContinue = pLevel->addrCont;
  }

  /* Done. */
  return pWInfo;

  /* Jump here if malloc fails */
whereBeginError:
  if( pWInfo ){
    pParse->nQueryLoop = pWInfo->savedNQueryLoop;
    whereInfoFree(db, pWInfo);
  }
  return 0;
}

/*
** Generate the end of the WHERE loop.  See comments on 
** sqlite3WhereBegin() for additional information.
*/
void sqlite3WhereEnd(WhereInfo *pWInfo){
  Parse *pParse = pWInfo->pParse;
  Vdbe *v = pParse->pVdbe;
  int i;
  WhereLevel *pLevel;
  WhereLoop *pLoop;
  SrcList *pTabList = pWInfo->pTabList;
  sqlite3 *db = pParse->db;

  /* Generate loop termination code.
  */
  sqlite3ExprCacheClear(pParse);
  for(i=pWInfo->nLevel-1; i>=0; i--){
    pLevel = &pWInfo->a[i];
    pLoop = pLevel->pWLoop;
    sqlite3VdbeResolveLabel(v, pLevel->addrCont);
    if( pLevel->op!=OP_Noop ){
      sqlite3VdbeAddOp2(v, pLevel->op, pLevel->p1, pLevel->p2);
      sqlite3VdbeChangeP5(v, pLevel->p5);
    }
    if( pLoop->wsFlags & WHERE_IN_ABLE && pLevel->u.in.nIn>0 ){
      struct InLoop *pIn;
      int j;
      sqlite3VdbeResolveLabel(v, pLevel->addrNxt);
      for(j=pLevel->u.in.nIn, pIn=&pLevel->u.in.aInLoop[j-1]; j>0; j--, pIn--){
        sqlite3VdbeJumpHere(v, pIn->addrInTop+1);
        sqlite3VdbeAddOp2(v, pIn->eEndLoopOp, pIn->iCur, pIn->addrInTop);
        sqlite3VdbeJumpHere(v, pIn->addrInTop-1);
      }
      sqlite3DbFree(db, pLevel->u.in.aInLoop);
    }
    sqlite3VdbeResolveLabel(v, pLevel->addrBrk);
    if( pLevel->iLeftJoin ){
      int addr;
      addr = sqlite3VdbeAddOp1(v, OP_IfPos, pLevel->iLeftJoin);
      assert( (pLoop->wsFlags & WHERE_IDX_ONLY)==0
           || (pLoop->wsFlags & WHERE_INDEXED)!=0 );
      if( (pLoop->wsFlags & WHERE_IDX_ONLY)==0 ){
        sqlite3VdbeAddOp1(v, OP_NullRow, pTabList->a[i].iCursor);
      }
      if( pLoop->wsFlags & WHERE_INDEXED ){
        sqlite3VdbeAddOp1(v, OP_NullRow, pLevel->iIdxCur);
      }
      if( pLevel->op==OP_Return ){
        sqlite3VdbeAddOp2(v, OP_Gosub, pLevel->p1, pLevel->addrFirst);
      }else{
        sqlite3VdbeAddOp2(v, OP_Goto, 0, pLevel->addrFirst);
      }
      sqlite3VdbeJumpHere(v, addr);
    }
  }

  /* The "break" point is here, just past the end of the outer loop.
  ** Set it.
  */
  sqlite3VdbeResolveLabel(v, pWInfo->iBreak);

  /* Close all of the cursors that were opened by sqlite3WhereBegin.
  */
  assert( pWInfo->nLevel<=pTabList->nSrc );
  for(i=0, pLevel=pWInfo->a; i<pWInfo->nLevel; i++, pLevel++){
    Index *pIdx = 0;
    struct SrcList_item *pTabItem = &pTabList->a[pLevel->iFrom];
    Table *pTab = pTabItem->pTab;
    assert( pTab!=0 );
    pLoop = pLevel->pWLoop;
    if( (pTab->tabFlags & TF_Ephemeral)==0
     && pTab->pSelect==0
     && (pWInfo->wctrlFlags & WHERE_OMIT_OPEN_CLOSE)==0
    ){
      int ws = pLoop->wsFlags;
      if( !pWInfo->okOnePass && (ws & WHERE_IDX_ONLY)==0 ){
        sqlite3VdbeAddOp1(v, OP_Close, pTabItem->iCursor);
      }
      if( (ws & WHERE_INDEXED)!=0 && (ws & (WHERE_IPK|WHERE_AUTO_INDEX))==0 ){
        sqlite3VdbeAddOp1(v, OP_Close, pLevel->iIdxCur);
      }
    }

    /* If this scan uses an index, make VDBE code substitutions to read data
    ** from the index instead of from the table where possible.  In some cases
    ** this optimization prevents the table from ever being read, which can
    ** yield a significant performance boost.
    ** 
    ** Calls to the code generator in between sqlite3WhereBegin and
    ** sqlite3WhereEnd will have created code that references the table
    ** directly.  This loop scans all that code looking for opcodes
    ** that reference the table and converts them into opcodes that
    ** reference the index.
    */
    if( pLoop->wsFlags & (WHERE_INDEXED|WHERE_IDX_ONLY) ){
      pIdx = pLoop->u.btree.pIndex;
    }else if( pLoop->wsFlags & WHERE_MULTI_OR ){
      pIdx = pLevel->u.pCovidx;
    }
    if( pIdx && !db->mallocFailed ){
      int k, j, last;
      VdbeOp *pOp;

      last = sqlite3VdbeCurrentAddr(v);
      k = pLevel->addrBody;
      pOp = sqlite3VdbeGetOp(v, k);
      for(; k<last; k++, pOp++){
        if( pOp->p1!=pLevel->iTabCur ) continue;
        if( pOp->opcode==OP_Column ){
          for(j=0; j<pIdx->nColumn; j++){
            if( pOp->p2==pIdx->aiColumn[j] ){
              pOp->p2 = j;
              pOp->p1 = pLevel->iIdxCur;
              break;
            }
          }
          assert( (pLoop->wsFlags & WHERE_IDX_ONLY)==0 || j<pIdx->nColumn );
        }else if( pOp->opcode==OP_Rowid ){
          pOp->p1 = pLevel->iIdxCur;
          pOp->opcode = OP_IdxRowid;
        }
      }
    }
  }

  /* Final cleanup
  */
  pParse->nQueryLoop = pWInfo->savedNQueryLoop;
  whereInfoFree(db, pWInfo);
  return;
}
