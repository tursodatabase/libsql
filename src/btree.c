/*
** 2004 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** $Id: btree.c,v 1.143 2004/05/18 12:50:17 drh Exp $
**
** This file implements a external (disk-based) database using BTrees.
** For a detailed discussion of BTrees, refer to
**
**     Donald E. Knuth, THE ART OF COMPUTER PROGRAMMING, Volume 3:
**     "Sorting And Searching", pages 473-480. Addison-Wesley
**     Publishing Company, Reading, Massachusetts.
**
** The basic idea is that each page of the file contains N database
** entries and N+1 pointers to subpages.
**
**   ----------------------------------------------------------------
**   |  Ptr(0) | Key(0) | Ptr(1) | Key(1) | ... | Key(N) | Ptr(N+1) |
**   ----------------------------------------------------------------
**
** All of the keys on the page that Ptr(0) points to have values less
** than Key(0).  All of the keys on page Ptr(1) and its subpages have
** values greater than Key(0) and less than Key(1).  All of the keys
** on Ptr(N+1) and its subpages have values greater than Key(N).  And
** so forth.
**
** Finding a particular key requires reading O(log(M)) pages from the 
** disk where M is the number of entries in the tree.
**
** In this implementation, a single file can hold one or more separate 
** BTrees.  Each BTree is identified by the index of its root page.  The
** key and data for any entry are combined to form the "payload".  A
** fixed amount of payload can be carried directly on the database
** page.  If the payload is larger than the preset amount then surplus
** bytes are stored on overflow pages.  The payload for an entry
** and the preceding pointer are combined to form a "Cell".  Each 
** page has a small header which contains the Ptr(N+1) pointer and other
** information such as the size of key and data.
**
** FORMAT DETAILS
**
** The file is divided into pages.  The first page is called page 1,
** the second is page 2, and so forth.  A page number of zero indicates
** "no such page".  The page size can be anything between 512 and 65536.
** Each page can be either a btree page, a freelist page or an overflow
** page.
**
** The first page is always a btree page.  The first 100 bytes of the first
** page contain a special header that describes the file.  The format
** of that header is as follows:
**
**   OFFSET   SIZE    DESCRIPTION
**      0      16     Header string: "SQLite format 3\000"
**     16       2     Page size in bytes.  
**     18       1     File format write version
**     19       1     File format read version
**     20       1     Bytes of unused space at the end of each page
**     21       1     Max embedded payload fraction
**     22       1     Min embedded payload fraction
**     23       1     Min leaf payload fraction
**     24       4     File change counter
**     28       4     Reserved for future use
**     32       4     First freelist page
**     36       4     Number of freelist pages in the file
**     40      60     15 4-byte meta values passed to higher layers
**
** All of the integer values are big-endian (most significant byte first).
**
** The file change counter is incremented every time the database is more
** than once within the same second.  This counter, together with the
** modification time of the file, allows other processes to know
** when the file has changed and thus when they need to flush their
** cache.
**
** The max embedded payload fraction is the amount of the total usable
** space in a page that can be consumed by a single cell for standard
** B-tree (non-LEAFDATA) tables.  A value of 255 means 100%.  The default
** is to limit the maximum cell size so that at least 4 cells will fit
** on one pages.  Thus the default max embedded payload fraction is 64.
**
** If the payload for a cell is larger than the max payload, then extra
** payload is spilled to overflow pages.  Once an overflow page is allocated,
** as many bytes as possible are moved into the overflow pages without letting
** the cell size drop below the min embedded payload fraction.
**
** The min leaf payload fraction is like the min embedded payload fraction
** except that it applies to leaf nodes in a LEAFDATA tree.  The maximum
** payload fraction for a LEAFDATA tree is always 100% (or 255) and it
** not specified in the header.
**
** Each btree page begins with a header described below.  Note that the
** header for page one begins at byte 100.  For all other btree pages, the
** header begins on byte zero.
**
**   OFFSET   SIZE     DESCRIPTION
**      0       1      Flags. 1: intkey, 2: zerodata, 4: leafdata, 8: leaf
**      1       2      byte offset to the first freeblock
**      3       2      byte offset to the first cell
**      5       1      number of fragmented free bytes
**      6       4      Right child (the Ptr(N+1) value).  Omitted if leaf
**
** The flags define the format of this btree page.  The leaf flag means that
** this page has no children.  The zerodata flag means that this page carries
** only keys and no data.  The intkey flag means that the key is a single
** variable length integer at the beginning of the payload.
**
** A variable-length integer is 1 to 9 bytes where the lower 7 bits of each 
** byte are used.  The integer consists of all bytes that have bit 8 set and
** the first byte with bit 8 clear.  The most significant byte of the integer
** appears first.
**
**    0x00                      becomes  0x00000000
**    0x7f                      becomes  0x0000007f
**    0x81 0x00                 becomes  0x00000080
**    0x82 0x00                 becomes  0x00000100
**    0x80 0x7f                 becomes  0x0000007f
**    0x8a 0x91 0xd1 0xac 0x78  becomes  0x12345678
**    0x81 0x81 0x81 0x81 0x01  becomes  0x10204081
**
** Variable length integers are used for rowids and to hold the number of
** bytes of key and data in a btree cell.
**
** Unused space within a btree page is collected into a linked list of
** freeblocks.  Each freeblock is at least 4 bytes in size.  The byte offset
** to the first freeblock is given in the header.  Freeblocks occur in
** increasing order.  Because a freeblock is 4 bytes in size, the minimum
** size allocation on a btree page is 4 bytes.  Because a freeblock must be
** at least 4 bytes in size, any group of 3 or fewer unused bytes cannot
** exist on the freeblock chain.  The total number of such fragmented bytes
** is recorded in the page header at offset 5.
**
**    SIZE    DESCRIPTION
**      2     Byte offset of the next freeblock
**      2     Bytes in this freeblock
**
** Cells are of variable length.  The first cell begins on the byte defined
** in the page header.  Cells do not necessarily occur in order - they can
** skip around on the page.
**
**    SIZE    DESCRIPTION
**      2     Byte offset of the next cell.  0 if this is the last cell
**      4     Page number of the left child. Omitted if leaf flag is set.
**     var    Number of bytes of data. Omitted if the zerodata flag is set.
**     var    Number of bytes of key. Or the key itself if intkey flag is set.
**      *     Payload
**      4     First page of the overflow chain.  Omitted if no overflow
**
** Overflow pages form a linked list.  Each page except the last is completely
** filled with data (pagesize - 4 bytes).  The last page can have as little
** as 1 byte of data.
**
**    SIZE    DESCRIPTION
**      4     Page number of next overflow page
**      *     Data
**
** Freelist pages come in two subtypes: trunk pages and leaf pages.  The
** file header points to first in a linked list of trunk page.  Each trunk
** page points to multiple leaf pages.  The content of a leaf page is
** unspecified.  A trunk page looks like this:
**
**    SIZE    DESCRIPTION
**      4     Page number of next trunk page
**      4     Number of leaf pointers on this page
**      *     zero or more pages numbers of leaves
*/
#include "sqliteInt.h"
#include "pager.h"
#include "btree.h"
#include <assert.h>


/* Maximum page size.  The upper bound on this value is 65536 (a limit
** imposed by the 2-byte offset at the beginning of each cell.)  The
** maximum page size determines the amount of stack space allocated
** by many of the routines in this module.  On embedded architectures
** or any machine where memory and especially stack memory is limited,
** one may wish to chose a smaller value for the maximum page size.
*/
#ifndef MX_PAGE_SIZE
# define MX_PAGE_SIZE 1024
#endif

/* The following value is the maximum cell size assuming a maximum page
** size give above.
*/
#define MX_CELL_SIZE  (MX_PAGE_SIZE-10)

/* The maximum number of cells on a single page of the database.  This
** assumes a minimum cell size of 3 bytes.  Such small cells will be
** exceedingly rare, but they are possible.
*/
#define MX_CELL ((MX_PAGE_SIZE-10)/3)

/* Forward declarations */
typedef struct MemPage MemPage;

/*
** This is a magic string that appears at the beginning of every
** SQLite database in order to identify the file as a real database.
**                                  123456789 123456 */
static const char zMagicHeader[] = "SQLite format 3";

/*
** Page type flags.  An ORed combination of these flags appear as the
** first byte of every BTree page.
*/
#define PTF_INTKEY    0x01
#define PTF_ZERODATA  0x02
#define PTF_LEAFDATA  0x04
#define PTF_LEAF      0x08

/*
** As each page of the file is loaded into memory, an instance of the following
** structure is appended and initialized to zero.  This structure stores
** information about the page that is decoded from the raw file page.
**
** The pParent field points back to the parent page.  This allows us to
** walk up the BTree from any leaf to the root.  Care must be taken to
** unref() the parent page pointer when this page is no longer referenced.
** The pageDestructor() routine handles that chore.
*/
struct MemPage {
  u32 notUsed;
  u8 isInit;                     /* True if previously initialized */
  u8 idxShift;                   /* True if Cell indices have changed */
  u8 isOverfull;                 /* Some aCell[] do not fit on page */
  u8 intKey;                     /* True if intkey flag is set */
  u8 leaf;                       /* True if leaf flag is set */
  u8 zeroData;                   /* True if table stores keys only */
  u8 leafData;                   /* True if tables stores data on leaves only */
  u8 hasData;                    /* True if this page stores data */
  u8 hdrOffset;                  /* 100 for page 1.  0 otherwise */
  u8 needRelink;                 /* True if need to run relinkCellList() */
  int idxParent;                 /* Index in pParent->aCell[] of this node */
  int nFree;                     /* Number of free bytes on the page */
  int nCell;                     /* Number of entries on this page */
  int nCellAlloc;                /* Number of slots allocated in aCell[] */
  unsigned char **aCell;         /* Pointer to start of each cell */
  struct Btree *pBt;             /* Pointer back to BTree structure */

  unsigned char *aData;          /* Pointer back to the start of the page */
  Pgno pgno;                     /* Page number for this page */
  MemPage *pParent;              /* The parent of this page.  NULL for root */
};

/*
** The in-memory image of a disk page has the auxiliary information appended
** to the end.  EXTRA_SIZE is the number of bytes of space needed to hold
** that extra information.
*/
#define EXTRA_SIZE sizeof(MemPage)

/*
** Everything we need to know about an open database
*/
struct Btree {
  Pager *pPager;        /* The page cache */
  BtCursor *pCursor;    /* A list of all open cursors */
  MemPage *pPage1;      /* First page of the database */
  u8 inTrans;           /* True if a transaction is in progress */
  u8 inStmt;            /* True if there is a checkpoint on the transaction */
  u8 readOnly;          /* True if the underlying file is readonly */
  int pageSize;         /* Total number of bytes on a page */
  int usableSize;       /* Number of usable bytes on each page */
  int maxLocal;         /* Maximum local payload in non-LEAFDATA tables */
  int minLocal;         /* Minimum local payload in non-LEAFDATA tables */
  int maxLeaf;          /* Maximum local payload in a LEAFDATA table */
  int minLeaf;          /* Minimum local payload in a LEAFDATA table */
  u8 maxEmbedFrac;      /* Maximum payload as % of total page size */
  u8 minEmbedFrac;      /* Minimum payload as % of total page size */
  u8 minLeafFrac;       /* Minimum leaf payload as % of total page size */
};
typedef Btree Bt;

/*
** An instance of the following structure is used to hold information
** about a cell.  The parseCell() function fills the structure in.
*/
typedef struct CellInfo CellInfo;
struct CellInfo {
  i64 nKey;      /* The key for INTKEY tables, or number of bytes in key */
  u32 nData;     /* Number of bytes of data */
  u16 nHeader;   /* Size of the header in bytes */
  u16 nLocal;    /* Amount of payload held locally */
  u16 iOverflow; /* Offset to overflow page number.  Zero if none */
  u16 nSize;     /* Size of the cell */
};

/*
** A cursor is a pointer to a particular entry in the BTree.
** The entry is identified by its MemPage and the index in
** MemPage.aCell[] of the entry.
*/
struct BtCursor {
  Btree *pBt;               /* The Btree to which this cursor belongs */
  BtCursor *pNext, *pPrev;  /* Forms a linked list of all cursors */
  BtCursor *pShared;        /* Loop of cursors with the same root page */
  int (*xCompare)(void*,int,const void*,int,const void*); /* Key comp func */
  void *pArg;               /* First arg to xCompare() */
  Pgno pgnoRoot;            /* The root page of this tree */
  MemPage *pPage;           /* Page that contains the entry */
  int idx;                  /* Index of the entry in pPage->aCell[] */
  CellInfo info;            /* A parse of the cell we are pointing at */
  u8 infoValid;             /* True if information in BtCursor.info is valid */
  u8 wrFlag;                /* True if writable */
  u8 iMatch;                /* compare result from last sqlite3BtreeMoveto() */
  u8 isValid;               /* TRUE if points to a valid entry */
  u8 status;                /* Set to SQLITE_ABORT if cursors is invalidated */
};

/*
** Read or write a two-, four-, and eight-byte big-endian integer values.
*/
static u32 get2byte(unsigned char *p){
  return (p[0]<<8) | p[1];
}
static u32 get4byte(unsigned char *p){
  return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}
static void put2byte(unsigned char *p, u32 v){
  p[0] = v>>8;
  p[1] = v;
}
static void put4byte(unsigned char *p, u32 v){
  p[0] = v>>24;
  p[1] = v>>16;
  p[2] = v>>8;
  p[3] = v;
}

/*
** Routines to read and write variable-length integers.
*/
#define getVarint    sqlite3GetVarint
#define getVarint32  sqlite3GetVarint32
#define putVarint    sqlite3PutVarint

/*
** Parse a cell header and fill in the CellInfo structure.
*/
static void parseCell(
  MemPage *pPage,         /* Page containing the cell */
  unsigned char *pCell,   /* The cell */
  CellInfo *pInfo         /* Fill in this structure */
){
  int n;
  int nPayload;
  Btree *pBt;
  int minLocal, maxLocal;
  if( pPage->leaf ){
    n = 2;
  }else{
    n = 6;
  }
  if( pPage->hasData ){
    n += getVarint32(&pCell[n], &pInfo->nData);
  }else{
    pInfo->nData = 0;
  }
  n += getVarint(&pCell[n], &pInfo->nKey);
  pInfo->nHeader = n;
  nPayload = pInfo->nData;
  if( !pPage->intKey ){
    nPayload += pInfo->nKey;
  }
  pBt = pPage->pBt;
  if( pPage->leafData ){
    minLocal = pBt->minLeaf;
    maxLocal = pBt->usableSize - 23;
  }else{
    minLocal = pBt->minLocal;
    maxLocal = pBt->maxLocal;
  }
  if( nPayload<=maxLocal ){
    pInfo->nLocal = nPayload;
    pInfo->iOverflow = 0;
    pInfo->nSize = nPayload + n;
  }else{
    int surplus = minLocal + (nPayload - minLocal)%(pBt->usableSize - 4);
    if( surplus <= maxLocal ){
      pInfo->nLocal = surplus;
    }else{
      pInfo->nLocal = minLocal;
    }
    pInfo->iOverflow = pInfo->nLocal + n;
    pInfo->nSize = pInfo->iOverflow + 4;
  }
}

/*
** Compute the total number of bytes that a Cell needs on the main
** database page.  The number returned includes the Cell header,
** local payload storage, and the pointer to overflow pages (if
** applicable).  Additional space allocated on overflow pages
** is NOT included in the value returned from this routine.
*/
static int cellSize(MemPage *pPage, unsigned char *pCell){
  CellInfo info;

  parseCell(pPage, pCell, &info);
  return info.nSize;
}

/*
** Do sanity checking on a page.  Throw an exception if anything is
** not right.
**
** This routine is used for internal error checking only.  It is omitted
** from most builds.
*/
#if defined(BTREE_DEBUG) && !defined(NDEBUG) && 0
static void _pageIntegrity(MemPage *pPage){
  int usableSize;
  u8 *data;
  int i, idx, c, pc, hdr, nFree;
  u8 used[MX_PAGE_SIZE];

  usableSize = pPage->pBt->usableSize;
  assert( pPage->aData==&((unsigned char*)pPage)[-pPage->pBt->pageSize] );
  hdr = pPage->hdrOffset;
  assert( hdr==(pPage->pgno==1 ? 100 : 0) );
  assert( pPage->pgno==sqlite3pager_pagenumber(pPage->aData) );
  c = pPage->aData[hdr];
  if( pPage->isInit ){
    assert( pPage->leaf == ((c & PTF_LEAF)!=0) );
    assert( pPage->zeroData == ((c & PTF_ZERODATA)!=0) );
    assert( pPage->leafData == ((c & PTF_LEAFDATA)!=0) );
    assert( pPage->intKey == ((c & (PTF_INTKEY|PTF_LEAFDATA))!=0) );
    assert( pPage->hasData ==
             !(pPage->zeroData || (!pPage->leaf && pPage->leafData)) );
  }
  data = pPage->aData;
  memset(used, 0, usableSize);
  for(i=0; i<hdr+10-pPage->leaf*4; i++) used[i] = 1;
  nFree = 0;
  pc = get2byte(&data[hdr+1]);
  while( pc ){
    int size;
    assert( pc>0 && pc<usableSize-4 );
    size = get2byte(&data[pc+2]);
    assert( pc+size<=usableSize );
    nFree += size;
    for(i=pc; i<pc+size; i++){
      assert( used[i]==0 );
      used[i] = 1;
    }
    pc = get2byte(&data[pc]);
  }
  assert( pPage->isInit==0 || pPage->nFree==nFree+data[hdr+5] );
  idx = 0;
  pc = get2byte(&data[hdr+3]);
  while( pc ){
    int size;
    assert( pPage->isInit==0 || idx<pPage->nCell );
    assert( pc>0 && pc<usableSize-4 );
    assert( pPage->isInit==0 || pPage->aCell[idx]==&data[pc] );
    size = cellSize(pPage, &data[pc]);
    assert( pc+size<=usableSize );
    for(i=pc; i<pc+size; i++){
      assert( used[i]==0 );
      used[i] = 1;
    }
    pc = get2byte(&data[pc]);
    idx++;
  }
  assert( idx==pPage->nCell );
  nFree = 0;
  for(i=0; i<usableSize; i++){
    assert( used[i]<=1 );
    if( used[i]==0 ) nFree++;
  }
  assert( nFree==data[hdr+5] );
}
#define pageIntegrity(X) _pageIntegrity(X)
#else
# define pageIntegrity(X)
#endif

/*
** Defragment the page given.  All Cells are moved to the
** beginning of the page and all free space is collected 
** into one big FreeBlk at the end of the page.
*/
static void defragmentPage(MemPage *pPage){
  int pc, i, n, addr;
  int start, hdr, size;
  int leftover;
  unsigned char *oldPage;
  unsigned char newPage[MX_PAGE_SIZE];

  assert( sqlite3pager_iswriteable(pPage->aData) );
  assert( pPage->pBt!=0 );
  assert( pPage->pBt->usableSize <= MX_PAGE_SIZE );
  assert( !pPage->needRelink );
  assert( !pPage->isOverfull );
  oldPage = pPage->aData;
  hdr = pPage->hdrOffset;
  addr = 3+hdr;
  n = 6+hdr;
  if( !pPage->leaf ){
    n += 4;
  }
  memcpy(&newPage[hdr], &oldPage[hdr], n-hdr);
  start = n;
  pc = get2byte(&oldPage[addr]);
  i = 0;
  while( pc>0 ){
    assert( n<pPage->pBt->usableSize );
    size = cellSize(pPage, &oldPage[pc]);
    memcpy(&newPage[n], &oldPage[pc], size);
    put2byte(&newPage[addr],n);
    assert( pPage->aCell[i]==&oldPage[pc] );
    pPage->aCell[i++] = &oldPage[n];
    addr = n;
    n += size;
    pc = get2byte(&oldPage[pc]);
  }
  assert( i==pPage->nCell );
  leftover = pPage->pBt->usableSize - n;
  assert( leftover>=0 );
  assert( pPage->nFree==leftover );
  if( leftover<4 ){
    oldPage[hdr+5] = leftover;
    leftover = 0;
    n = pPage->pBt->usableSize;
  }
  memcpy(&oldPage[hdr], &newPage[hdr], n-hdr);
  if( leftover==0 ){
    put2byte(&oldPage[hdr+1], 0);
  }else if( leftover>=4 ){
    put2byte(&oldPage[hdr+1], n);
    put2byte(&oldPage[n], 0);
    put2byte(&oldPage[n+2], leftover);
    memset(&oldPage[n+4], 0, leftover-4);
  }
  oldPage[hdr+5] = 0;
}

/*
** Allocate nByte bytes of space on a page.  If nByte is less than
** 4 it is rounded up to 4.
**
** Return the index into pPage->aData[] of the first byte of
** the new allocation. Or return 0 if there is not enough free
** space on the page to satisfy the allocation request.
**
** If the page contains nBytes of free space but does not contain
** nBytes of contiguous free space, then this routine automatically
** calls defragementPage() to consolidate all free space before 
** allocating the new chunk.
**
** Algorithm:  Carve a piece off of the first freeblock that is
** nByte in size or that larger.
*/
static int allocateSpace(MemPage *pPage, int nByte){
  int addr, pc, hdr;
  int size;
  int nFrag;
  unsigned char *data;
#ifndef NDEBUG
  int cnt = 0;
#endif

  data = pPage->aData;
  assert( sqlite3pager_iswriteable(data) );
  assert( pPage->pBt );
  if( nByte<4 ) nByte = 4;
  if( pPage->nFree<nByte || pPage->isOverfull ) return 0;
  hdr = pPage->hdrOffset;
  nFrag = data[hdr+5];
  if( nFrag>=60 || nFrag>pPage->nFree-nByte ){
    defragmentPage(pPage);
  }
  addr = hdr+1;
  pc = get2byte(&data[addr]);
  assert( addr<pc );
  assert( pc<=pPage->pBt->usableSize-4 );
  while( (size = get2byte(&data[pc+2]))<nByte ){
    addr = pc;
    pc = get2byte(&data[addr]);
    assert( pc<=pPage->pBt->usableSize-4 );
    assert( pc>=addr+size+4 || pc==0 );
    if( pc==0 ){
      assert( (cnt++)==0 );
      defragmentPage(pPage);
      assert( data[hdr+5]==0 );
      addr = pPage->hdrOffset+1;
      pc = get2byte(&data[addr]);
    }
  }
  assert( pc>0 && size>=nByte );
  assert( pc+size<=pPage->pBt->usableSize );
  if( size>nByte+4 ){
    int newStart = pc+nByte;
    put2byte(&data[addr], newStart);
    put2byte(&data[newStart], get2byte(&data[pc]));
    put2byte(&data[newStart+2], size-nByte);
  }else{
    put2byte(&data[addr], get2byte(&data[pc]));
    data[hdr+5] += size-nByte;
  }
  pPage->nFree -= nByte;
  assert( pPage->nFree>=0 );
  return pc;
}

/*
** Return a section of the pPage->aData to the freelist.
** The first byte of the new free block is pPage->aDisk[start]
** and the size of the block is "size" bytes.
**
** Most of the effort here is involved in coalesing adjacent
** free blocks into a single big free block.
*/
static void freeSpace(MemPage *pPage, int start, int size){
  int end = start + size;  /* End of the segment being freed */
  int addr, pbegin;
#ifndef NDEBUG
  int tsize = 0;          /* Total size of all freeblocks */
#endif
  unsigned char *data = pPage->aData;

  assert( pPage->pBt!=0 );
  assert( sqlite3pager_iswriteable(data) );
  assert( start>=pPage->hdrOffset+6+(pPage->leaf?0:4) );
  assert( end<=pPage->pBt->usableSize );
  if( size<4 ) size = 4;

  /* Add the space back into the linked list of freeblocks */
  addr = pPage->hdrOffset + 1;
  while( (pbegin = get2byte(&data[addr]))<start && pbegin>0 ){
    assert( pbegin<=pPage->pBt->usableSize-4 );
    assert( pbegin>addr );
    addr = pbegin;
  }
  assert( pbegin<=pPage->pBt->usableSize-4 );
  assert( pbegin>addr || pbegin==0 );
  put2byte(&data[addr], start);
  put2byte(&data[start], pbegin);
  put2byte(&data[start+2], size);
  pPage->nFree += size;

  /* Coalesce adjacent free blocks */
  addr = pPage->hdrOffset + 1;
  while( (pbegin = get2byte(&data[addr]))>0 ){
    int pnext, psize;
    assert( pbegin>addr );
    assert( pbegin<pPage->pBt->usableSize-4 );
    pnext = get2byte(&data[pbegin]);
    psize = get2byte(&data[pbegin+2]);
    if( pbegin + psize + 3 >= pnext && pnext>0 ){
      int frag = pnext - (pbegin+psize);
      assert( frag<=data[pPage->hdrOffset+5] );
      data[pPage->hdrOffset+5] -= frag;
      put2byte(&data[pbegin], get2byte(&data[pnext]));
      put2byte(&data[pbegin+2], pnext+get2byte(&data[pnext+2])-pbegin);
    }else{
      assert( (tsize += psize)>0 );
      addr = pbegin;
    }
  }
  assert( tsize+data[pPage->hdrOffset+5]==pPage->nFree );
}

/*
** Resize the aCell[] array of the given page so that it is able to
** hold at least nNewSz entries.
**
** Return SQLITE_OK or SQLITE_NOMEM.
*/
static int resizeCellArray(MemPage *pPage, int nNewSz){
  if( pPage->nCellAlloc<nNewSz ){
    int n = nNewSz*sizeof(pPage->aCell[0]);
    if( pPage->aCell==0 ){
      pPage->aCell = sqliteMallocRaw( n );
    }else{
      pPage->aCell = sqliteRealloc(pPage->aCell, n);
    }
    if( sqlite3_malloc_failed ) return SQLITE_NOMEM;
    pPage->nCellAlloc = nNewSz;
  }
  return SQLITE_OK;
}

/*
** Initialize the auxiliary information for a disk block.
**
** The pParent parameter must be a pointer to the MemPage which
** is the parent of the page being initialized.  The root of a
** BTree has no parent and so for that page, pParent==NULL.
**
** Return SQLITE_OK on success.  If we see that the page does
** not contain a well-formed database page, then return 
** SQLITE_CORRUPT.  Note that a return of SQLITE_OK does not
** guarantee that the page is well-formed.  It only shows that
** we failed to detect any corruption.
*/
static int initPage(
  MemPage *pPage,        /* The page to be initialized */
  MemPage *pParent       /* The parent.  Might be NULL */
){
  int c, pc, i, hdr;
  unsigned char *data;
  int usableSize;
  int nCell, nFree;
  u8 *aCell[MX_PAGE_SIZE/2];


  assert( pPage->pBt!=0 );
  assert( pParent==0 || pParent->pBt==pPage->pBt );
  assert( pPage->pgno==sqlite3pager_pagenumber(pPage->aData) );
  assert( pPage->aData == &((unsigned char*)pPage)[-pPage->pBt->pageSize] );
  assert( pPage->pParent==0 || pPage->pParent==pParent );
  assert( pPage->pParent==pParent || !pPage->isInit );
  if( pPage->isInit ) return SQLITE_OK;
  if( pPage->pParent==0 && pParent!=0 ){
    pPage->pParent = pParent;
    sqlite3pager_ref(pParent->aData);
  }
  pPage->nCell = pPage->nCellAlloc = 0;
  assert( pPage->hdrOffset==(pPage->pgno==1 ? 100 : 0) );
  hdr = pPage->hdrOffset;
  data = pPage->aData;
  c = data[hdr];
  pPage->intKey = (c & (PTF_INTKEY|PTF_LEAFDATA))!=0;
  pPage->zeroData = (c & PTF_ZERODATA)!=0;
  pPage->leafData = (c & PTF_LEAFDATA)!=0;
  pPage->leaf = (c & PTF_LEAF)!=0;
  pPage->hasData = !(pPage->zeroData || (!pPage->leaf && pPage->leafData));
  pPage->isOverfull = 0;
  pPage->needRelink = 0;
  pPage->idxShift = 0;
  usableSize = pPage->pBt->usableSize;

  /* Initialize the cell count and cell pointers */
  i = 0;
  pc = get2byte(&data[hdr+3]);
  nCell = 0;
  while( pc>0 ){
    if( pc>=usableSize ) return SQLITE_CORRUPT;
    if( nCell>sizeof(aCell)/sizeof(aCell[0]) ) return SQLITE_CORRUPT;
    aCell[nCell++] = &data[pc];
    pc = get2byte(&data[pc]);
  }
  if( resizeCellArray(pPage, nCell) ){
    return SQLITE_NOMEM;
  }
  pPage->nCell = nCell;
  memcpy(pPage->aCell, aCell, nCell*sizeof(aCell[0]));

  /* Compute the total free space on the page */
  pc = get2byte(&data[hdr+1]);
  nFree = data[hdr+5];
  i = 0;
  while( pc>0 ){
    int next, size;
    if( pc>=usableSize ) return SQLITE_CORRUPT;
    if( i++>MX_PAGE_SIZE ) return SQLITE_CORRUPT;
    next = get2byte(&data[pc]);
    size = get2byte(&data[pc+2]);
    if( next>0 && next<=pc+size+3 ) return SQLITE_CORRUPT;
    nFree += size;
    pc = next;
  }
  pPage->nFree = nFree;
  if( nFree>=usableSize ) return SQLITE_CORRUPT;

  pPage->isInit = 1;
  pageIntegrity(pPage);
  return SQLITE_OK;
}

/*
** Set up a raw page so that it looks like a database page holding
** no entries.
*/
static void zeroPage(MemPage *pPage, int flags){
  unsigned char *data = pPage->aData;
  Btree *pBt = pPage->pBt;
  int hdr = pPage->hdrOffset;
  int first;

  assert( sqlite3pager_pagenumber(data)==pPage->pgno );
  assert( &data[pBt->pageSize] == (unsigned char*)pPage );
  assert( sqlite3pager_iswriteable(data) );
  memset(&data[hdr], 0, pBt->usableSize - hdr);
  data[hdr] = flags;
  first = hdr + 6 + 4*((flags&PTF_LEAF)==0);
  put2byte(&data[hdr+1], first);
  put2byte(&data[first+2], pBt->usableSize - first);
  sqliteFree(pPage->aCell);
  pPage->aCell = 0;
  pPage->nCell = 0;
  pPage->nCellAlloc = 0;
  pPage->nFree = pBt->usableSize - first;
  pPage->intKey = (flags & (PTF_INTKEY|PTF_LEAFDATA))!=0;
  pPage->zeroData = (flags & PTF_ZERODATA)!=0;
  pPage->leafData = (flags & PTF_LEAFDATA)!=0;
  pPage->leaf = (flags & PTF_LEAF)!=0;
  pPage->hasData = !(pPage->zeroData || (!pPage->leaf && pPage->leafData));
  pPage->hdrOffset = hdr;
  pPage->isOverfull = 0;
  pPage->needRelink = 0;
  pPage->idxShift = 0;
  pPage->isInit = 1;
  pageIntegrity(pPage);
}

/*
** Get a page from the pager.  Initialize the MemPage.pBt and
** MemPage.aData elements if needed.
*/
static int getPage(Btree *pBt, Pgno pgno, MemPage **ppPage){
  int rc;
  unsigned char *aData;
  MemPage *pPage;
  rc = sqlite3pager_get(pBt->pPager, pgno, (void**)&aData);
  if( rc ) return rc;
  pPage = (MemPage*)&aData[pBt->pageSize];
  pPage->aData = aData;
  pPage->pBt = pBt;
  pPage->pgno = pgno;
  pPage->hdrOffset = pPage->pgno==1 ? 100 : 0;
  *ppPage = pPage;
  return SQLITE_OK;
}

/*
** Get a page from the pager and initialize it.  This routine
** is just a convenience wrapper around separate calls to
** getPage() and initPage().
*/
static int getAndInitPage(
  Btree *pBt,          /* The database file */
  Pgno pgno,           /* Number of the page to get */
  MemPage **ppPage,    /* Write the page pointer here */
  MemPage *pParent     /* Parent of the page */
){
  int rc;
  rc = getPage(pBt, pgno, ppPage);
  if( rc==SQLITE_OK && (*ppPage)->isInit==0 ){
    rc = initPage(*ppPage, pParent);
  }
  return rc;
}

/*
** Release a MemPage.  This should be called once for each prior
** call to getPage.
*/
static void releasePage(MemPage *pPage){
  if( pPage ){
    assert( pPage->aData );
    assert( pPage->pBt );
    assert( &pPage->aData[pPage->pBt->pageSize]==(unsigned char*)pPage );
    sqlite3pager_unref(pPage->aData);
  }
}

/*
** This routine is called when the reference count for a page
** reaches zero.  We need to unref the pParent pointer when that
** happens.
*/
static void pageDestructor(void *pData, int pageSize){
  MemPage *pPage = (MemPage*)&((char*)pData)[pageSize];
  assert( pPage->isInit==0 || pPage->needRelink==0 );
  if( pPage->pParent ){
    MemPage *pParent = pPage->pParent;
    pPage->pParent = 0;
    releasePage(pParent);
  }
  sqliteFree(pPage->aCell);
  pPage->aCell = 0;
  pPage->isInit = 0;
}

/*
** Open a new database.
**
** Actually, this routine just sets up the internal data structures
** for accessing the database.  We do not open the database file 
** until the first page is loaded.
**
** zFilename is the name of the database file.  If zFilename is NULL
** a new database with a random name is created.  This randomly named
** database file will be deleted when sqlite3BtreeClose() is called.
*/
int sqlite3BtreeOpen(
  const char *zFilename,  /* Name of the file containing the BTree database */
  Btree **ppBtree,        /* Pointer to new Btree object written here */
  int nCache,             /* Number of cache pages */
  int flags               /* Options */
){
  Btree *pBt;
  int rc;

  /*
  ** The following asserts make sure that structures used by the btree are
  ** the right size.  This is to guard against size changes that result
  ** when compiling on a different architecture.
  */
  assert( sizeof(i64)==8 );
  assert( sizeof(u64)==8 );
  assert( sizeof(u32)==4 );
  assert( sizeof(u16)==2 );
  assert( sizeof(Pgno)==4 );
  assert( sizeof(ptr)==sizeof(char*) );
  assert( sizeof(uptr)==sizeof(ptr) );

  pBt = sqliteMalloc( sizeof(*pBt) );
  if( pBt==0 ){
    *ppBtree = 0;
    return SQLITE_NOMEM;
  }
  if( nCache<10 ) nCache = 10;
  rc = sqlite3pager_open(&pBt->pPager, zFilename, nCache, EXTRA_SIZE,
                        (flags & BTREE_OMIT_JOURNAL)==0);
  if( rc!=SQLITE_OK ){
    if( pBt->pPager ) sqlite3pager_close(pBt->pPager);
    sqliteFree(pBt);
    *ppBtree = 0;
    return rc;
  }
  sqlite3pager_set_destructor(pBt->pPager, pageDestructor);
  pBt->pCursor = 0;
  pBt->pPage1 = 0;
  pBt->readOnly = sqlite3pager_isreadonly(pBt->pPager);
  pBt->pageSize = SQLITE_PAGE_SIZE;  /* FIX ME - read from header */
  pBt->usableSize = pBt->pageSize;
  pBt->maxEmbedFrac = 64;            /* FIX ME - read from header */
  pBt->minEmbedFrac = 32;            /* FIX ME - read from header */
  pBt->minLeafFrac = 32;             /* FIX ME - read from header */

  *ppBtree = pBt;
  return SQLITE_OK;
}

/*
** Close an open database and invalidate all cursors.
*/
int sqlite3BtreeClose(Btree *pBt){
  while( pBt->pCursor ){
    sqlite3BtreeCloseCursor(pBt->pCursor);
  }
  sqlite3pager_close(pBt->pPager);
  sqliteFree(pBt);
  return SQLITE_OK;
}

/*
** Change the limit on the number of pages allowed in the cache.
**
** The maximum number of cache pages is set to the absolute
** value of mxPage.  If mxPage is negative, the pager will
** operate asynchronously - it will not stop to do fsync()s
** to insure data is written to the disk surface before
** continuing.  Transactions still work if synchronous is off,
** and the database cannot be corrupted if this program
** crashes.  But if the operating system crashes or there is
** an abrupt power failure when synchronous is off, the database
** could be left in an inconsistent and unrecoverable state.
** Synchronous is on by default so database corruption is not
** normally a worry.
*/
int sqlite3BtreeSetCacheSize(Btree *pBt, int mxPage){
  sqlite3pager_set_cachesize(pBt->pPager, mxPage);
  return SQLITE_OK;
}

/*
** Change the way data is synced to disk in order to increase or decrease
** how well the database resists damage due to OS crashes and power
** failures.  Level 1 is the same as asynchronous (no syncs() occur and
** there is a high probability of damage)  Level 2 is the default.  There
** is a very low but non-zero probability of damage.  Level 3 reduces the
** probability of damage to near zero but with a write performance reduction.
*/
int sqlite3BtreeSetSafetyLevel(Btree *pBt, int level){
  sqlite3pager_set_safety_level(pBt->pPager, level);
  return SQLITE_OK;
}

/*
** Get a reference to pPage1 of the database file.  This will
** also acquire a readlock on that file.
**
** SQLITE_OK is returned on success.  If the file is not a
** well-formed database file, then SQLITE_CORRUPT is returned.
** SQLITE_BUSY is returned if the database is locked.  SQLITE_NOMEM
** is returned if we run out of memory.  SQLITE_PROTOCOL is returned
** if there is a locking protocol violation.
*/
static int lockBtree(Btree *pBt){
  int rc;
  MemPage *pPage1;
  if( pBt->pPage1 ) return SQLITE_OK;
  rc = getPage(pBt, 1, &pPage1);
  if( rc!=SQLITE_OK ) return rc;
  

  /* Do some checking to help insure the file we opened really is
  ** a valid database file. 
  */
  rc = SQLITE_NOTADB;
  if( sqlite3pager_pagecount(pBt->pPager)>0 ){
    u8 *page1 = pPage1->aData;
    if( memcmp(page1, zMagicHeader, 16)!=0 ){
      goto page1_init_failed;
    }
    if( page1[18]>1 || page1[19]>1 ){
      goto page1_init_failed;
    }
    pBt->pageSize = get2byte(&page1[16]);
    pBt->usableSize = pBt->pageSize - page1[20];
    if( pBt->usableSize<500 ){
      goto page1_init_failed;
    }
    pBt->maxEmbedFrac = page1[21];
    pBt->minEmbedFrac = page1[22];
    pBt->minLeafFrac = page1[23];
  }

  /* maxLocal is the maximum amount of payload to store locally for
  ** a cell.  Make sure it is small enough so that at least minFanout
  ** cells can will fit on one page.  We assume a 10-byte page header.
  ** Besides the payload, the cell must store:
  **     2-byte pointer to next cell
  **     4-byte child pointer
  **     9-byte nKey value
  **     4-byte nData value
  **     4-byte overflow page pointer
  ** So a cell consists of a header which is as much as 19 bytes long,
  ** 0 to N bytes of payload, and an optional 4 byte overflow page pointer.
  */
  pBt->maxLocal = (pBt->usableSize-10)*pBt->maxEmbedFrac/255 - 23;
  pBt->minLocal = (pBt->usableSize-10)*pBt->minEmbedFrac/255 - 23;
  pBt->maxLeaf = pBt->usableSize - 33;
  pBt->minLeaf = (pBt->usableSize-10)*pBt->minLeafFrac/255 - 23;
  if( pBt->minLocal>pBt->maxLocal || pBt->maxLocal<0 ){
    goto page1_init_failed;
  }
  assert( pBt->maxLeaf + 23 <= MX_CELL_SIZE );
  pBt->pPage1 = pPage1;
  return SQLITE_OK;

page1_init_failed:
  releasePage(pPage1);
  pBt->pPage1 = 0;
  return rc;
}

/*
** If there are no outstanding cursors and we are not in the middle
** of a transaction but there is a read lock on the database, then
** this routine unrefs the first page of the database file which 
** has the effect of releasing the read lock.
**
** If there are any outstanding cursors, this routine is a no-op.
**
** If there is a transaction in progress, this routine is a no-op.
*/
static void unlockBtreeIfUnused(Btree *pBt){
  if( pBt->inTrans==0 && pBt->pCursor==0 && pBt->pPage1!=0 ){
    releasePage(pBt->pPage1);
    pBt->pPage1 = 0;
    pBt->inTrans = 0;
    pBt->inStmt = 0;
  }
}

/*
** Create a new database by initializing the first page of the
** file.
*/
static int newDatabase(Btree *pBt){
  MemPage *pP1;
  unsigned char *data;
  int rc;
  if( sqlite3pager_pagecount(pBt->pPager)>0 ) return SQLITE_OK;
  pP1 = pBt->pPage1;
  assert( pP1!=0 );
  data = pP1->aData;
  rc = sqlite3pager_write(data);
  if( rc ) return rc;
  memcpy(data, zMagicHeader, sizeof(zMagicHeader));
  assert( sizeof(zMagicHeader)==16 );
  put2byte(&data[16], pBt->pageSize);
  data[18] = 1;
  data[19] = 1;
  data[20] = pBt->pageSize - pBt->usableSize;
  data[21] = pBt->maxEmbedFrac;
  data[22] = pBt->minEmbedFrac;
  data[23] = pBt->minLeafFrac;
  memset(&data[24], 0, 100-24);
  zeroPage(pP1, PTF_INTKEY|PTF_LEAF|PTF_LEAFDATA );
  return SQLITE_OK;
}

/*
** Attempt to start a new transaction.
**
** A transaction must be started before attempting any changes
** to the database.  None of the following routines will work
** unless a transaction is started first:
**
**      sqlite3BtreeCreateTable()
**      sqlite3BtreeCreateIndex()
**      sqlite3BtreeClearTable()
**      sqlite3BtreeDropTable()
**      sqlite3BtreeInsert()
**      sqlite3BtreeDelete()
**      sqlite3BtreeUpdateMeta()
*/
int sqlite3BtreeBeginTrans(Btree *pBt){
  int rc;
  if( pBt->inTrans ) return SQLITE_ERROR;
  if( pBt->readOnly ) return SQLITE_READONLY;
  if( pBt->pPage1==0 ){
    rc = lockBtree(pBt);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }
  rc = sqlite3pager_begin(pBt->pPage1->aData);
  if( rc==SQLITE_OK ){
    rc = newDatabase(pBt);
  }
  if( rc==SQLITE_OK ){
    pBt->inTrans = 1;
    pBt->inStmt = 0;
  }else{
    unlockBtreeIfUnused(pBt);
  }
  return rc;
}

/*
** Commit the transaction currently in progress.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
int sqlite3BtreeCommit(Btree *pBt){
  int rc;
  rc = pBt->readOnly ? SQLITE_OK : sqlite3pager_commit(pBt->pPager);
  pBt->inTrans = 0;
  pBt->inStmt = 0;
  unlockBtreeIfUnused(pBt);
  return rc;
}

/*
** Invalidate all cursors
*/
static void invalidateCursors(Btree *pBt){
  BtCursor *pCur;
  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    MemPage *pPage = pCur->pPage;
    if( pPage /* && !pPage->isInit */ ){
      pageIntegrity(pPage);
      releasePage(pPage);
      pCur->pPage = 0;
      pCur->isValid = 0;
      pCur->status = SQLITE_ABORT;
    }
  }
}

#ifdef SQLITE_TEST
/*
** Print debugging information about all cursors to standard output.
*/
void sqlite3BtreeCursorList(Btree *pBt){
  BtCursor *pCur;
  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    MemPage *pPage = pCur->pPage;
    char *zMode = pCur->wrFlag ? "rw" : "ro";
    printf("CURSOR %08x rooted at %4d(%s) currently at %d.%d%s\n",
       (int)pCur, pCur->pgnoRoot, zMode,
       pPage ? pPage->pgno : 0, pCur->idx,
       pCur->isValid ? "" : " eof"
    );
  }
}
#endif

/*
** Rollback the transaction in progress.  All cursors will be
** invalided by this operation.  Any attempt to use a cursor
** that was open at the beginning of this operation will result
** in an error.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
int sqlite3BtreeRollback(Btree *pBt){
  int rc;
  MemPage *pPage1;
  if( pBt->inTrans==0 ) return SQLITE_OK;
  pBt->inTrans = 0;
  pBt->inStmt = 0;
  if( pBt->readOnly ){
    rc = SQLITE_OK;
  }else{
    rc = sqlite3pager_rollback(pBt->pPager);
    /* The rollback may have destroyed the pPage1->aData value.  So
    ** call getPage() on page 1 again to make sure pPage1->aData is
    ** set correctly. */
    if( getPage(pBt, 1, &pPage1)==SQLITE_OK ){
      releasePage(pPage1);
    }
  }
  invalidateCursors(pBt);
  unlockBtreeIfUnused(pBt);
  return rc;
}

/*
** Set the checkpoint for the current transaction.  The checkpoint serves
** as a sub-transaction that can be rolled back independently of the
** main transaction.  You must start a transaction before starting a
** checkpoint.  The checkpoint is ended automatically if the transaction
** commits or rolls back.
**
** Only one checkpoint may be active at a time.  It is an error to try
** to start a new checkpoint if another checkpoint is already active.
*/
int sqlite3BtreeBeginStmt(Btree *pBt){
  int rc;
  if( !pBt->inTrans || pBt->inStmt ){
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  rc = pBt->readOnly ? SQLITE_OK : sqlite3pager_stmt_begin(pBt->pPager);
  pBt->inStmt = 1;
  return rc;
}


/*
** Commit a checkpoint to transaction currently in progress.  If no
** checkpoint is active, this is a no-op.
*/
int sqlite3BtreeCommitStmt(Btree *pBt){
  int rc;
  if( pBt->inStmt && !pBt->readOnly ){
    rc = sqlite3pager_stmt_commit(pBt->pPager);
  }else{
    rc = SQLITE_OK;
  }
  pBt->inStmt = 0;
  return rc;
}

/*
** Rollback the checkpoint to the current transaction.  If there
** is no active checkpoint or transaction, this routine is a no-op.
**
** All cursors will be invalided by this operation.  Any attempt
** to use a cursor that was open at the beginning of this operation
** will result in an error.
*/
int sqlite3BtreeRollbackStmt(Btree *pBt){
  int rc;
  if( pBt->inStmt==0 || pBt->readOnly ) return SQLITE_OK;
  rc = sqlite3pager_stmt_rollback(pBt->pPager);
  invalidateCursors(pBt);
  pBt->inStmt = 0;
  return rc;
}

/*
** Default key comparison function to be used if no comparison function
** is specified on the sqlite3BtreeCursor() call.
*/
static int dfltCompare(
  void *NotUsed,             /* User data is not used */
  int n1, const void *p1,    /* First key to compare */
  int n2, const void *p2     /* Second key to compare */
){
  int c;
  c = memcmp(p1, p2, n1<n2 ? n1 : n2);
  if( c==0 ){
    c = n1 - n2;
  }
  return c;
}

/*
** Create a new cursor for the BTree whose root is on the page
** iTable.  The act of acquiring a cursor gets a read lock on 
** the database file.
**
** If wrFlag==0, then the cursor can only be used for reading.
** If wrFlag==1, then the cursor can be used for reading or for
** writing if other conditions for writing are also met.  These
** are the conditions that must be met in order for writing to
** be allowed:
**
** 1:  The cursor must have been opened with wrFlag==1
**
** 2:  No other cursors may be open with wrFlag==0 on the same table
**
** 3:  The database must be writable (not on read-only media)
**
** 4:  There must be an active transaction.
**
** Condition 2 warrants further discussion.  If any cursor is opened
** on a table with wrFlag==0, that prevents all other cursors from
** writing to that table.  This is a kind of "read-lock".  When a cursor
** is opened with wrFlag==0 it is guaranteed that the table will not
** change as long as the cursor is open.  This allows the cursor to
** do a sequential scan of the table without having to worry about
** entries being inserted or deleted during the scan.  Cursors should
** be opened with wrFlag==0 only if this read-lock property is needed.
** That is to say, cursors should be opened with wrFlag==0 only if they
** intend to use the sqlite3BtreeNext() system call.  All other cursors
** should be opened with wrFlag==1 even if they never really intend
** to write.
** 
** No checking is done to make sure that page iTable really is the
** root page of a b-tree.  If it is not, then the cursor acquired
** will not work correctly.
**
** The comparison function must be logically the same for every cursor
** on a particular table.  Changing the comparison function will result
** in incorrect operations.  If the comparison function is NULL, a
** default comparison function is used.  The comparison function is
** always ignored for INTKEY tables.
*/
int sqlite3BtreeCursor(
  Btree *pBt,                                 /* The btree */
  int iTable,                                 /* Root page of table to open */
  int wrFlag,                                 /* 1 to write. 0 read-only */
  int (*xCmp)(void*,int,const void*,int,const void*), /* Key Comparison func */
  void *pArg,                                 /* First arg to xCompare() */
  BtCursor **ppCur                            /* Write new cursor here */
){
  int rc;
  BtCursor *pCur, *pRing;

  if( pBt->readOnly && wrFlag ){
    *ppCur = 0;
    return SQLITE_READONLY;
  }
  if( pBt->pPage1==0 ){
    rc = lockBtree(pBt);
    if( rc!=SQLITE_OK ){
      *ppCur = 0;
      return rc;
    }
  }
  pCur = sqliteMalloc( sizeof(*pCur) );
  if( pCur==0 ){
    rc = SQLITE_NOMEM;
    goto create_cursor_exception;
  }
  pCur->pgnoRoot = (Pgno)iTable;
  if( iTable==1 && sqlite3pager_pagecount(pBt->pPager)==0 ){
    rc = SQLITE_EMPTY;
    goto create_cursor_exception;
  }
  rc = getAndInitPage(pBt, pCur->pgnoRoot, &pCur->pPage, 0);
  if( rc!=SQLITE_OK ){
    goto create_cursor_exception;
  }
  pCur->xCompare = xCmp ? xCmp : dfltCompare;
  pCur->pArg = pArg;
  pCur->pBt = pBt;
  pCur->wrFlag = wrFlag;
  pCur->idx = 0;
  pCur->infoValid = 0;
  pCur->pNext = pBt->pCursor;
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur;
  }
  pCur->pPrev = 0;
  pRing = pBt->pCursor;
  while( pRing && pRing->pgnoRoot!=pCur->pgnoRoot ){ pRing = pRing->pNext; }
  if( pRing ){
    pCur->pShared = pRing->pShared;
    pRing->pShared = pCur;
  }else{
    pCur->pShared = pCur;
  }
  pBt->pCursor = pCur;
  pCur->isValid = 0;
  pCur->status = SQLITE_OK;
  *ppCur = pCur;
  return SQLITE_OK;

create_cursor_exception:
  *ppCur = 0;
  if( pCur ){
    releasePage(pCur->pPage);
    sqliteFree(pCur);
  }
  unlockBtreeIfUnused(pBt);
  return rc;
}

void sqlite3BtreeSetCompare(
  BtCursor *pCur,
  int(* xCmp)(void*,int,const void*,int,const void*),
  void *pArg
){
  pCur->xCompare = xCmp ? xCmp : dfltCompare;
  pCur->pArg = pArg;
}

/*
** Close a cursor.  The read lock on the database file is released
** when the last cursor is closed.
*/
int sqlite3BtreeCloseCursor(BtCursor *pCur){
  Btree *pBt = pCur->pBt;
  if( pCur->pPrev ){
    pCur->pPrev->pNext = pCur->pNext;
  }else{
    pBt->pCursor = pCur->pNext;
  }
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur->pPrev;
  }
  releasePage(pCur->pPage);
  if( pCur->pShared!=pCur ){
    BtCursor *pRing = pCur->pShared;
    while( pRing->pShared!=pCur ){ pRing = pRing->pShared; }
    pRing->pShared = pCur->pShared;
  }
  unlockBtreeIfUnused(pBt);
  sqliteFree(pCur);
  return SQLITE_OK;
}

/*
** Make a temporary cursor by filling in the fields of pTempCur.
** The temporary cursor is not on the cursor list for the Btree.
*/
static void getTempCursor(BtCursor *pCur, BtCursor *pTempCur){
  memcpy(pTempCur, pCur, sizeof(*pCur));
  pTempCur->pNext = 0;
  pTempCur->pPrev = 0;
  if( pTempCur->pPage ){
    sqlite3pager_ref(pTempCur->pPage->aData);
  }
}

/*
** Delete a temporary cursor such as was made by the CreateTemporaryCursor()
** function above.
*/
static void releaseTempCursor(BtCursor *pCur){
  if( pCur->pPage ){
    sqlite3pager_unref(pCur->pPage->aData);
  }
}

/*
** Make sure the BtCursor.info field of the given cursor is valid.
*/
static void getCellInfo(BtCursor *pCur){
  MemPage *pPage = pCur->pPage;
  if( !pCur->infoValid ){
    parseCell(pPage, pPage->aCell[pCur->idx], &pCur->info);
    pCur->infoValid = 1;
  }else{
#ifndef NDEBUG
    CellInfo info;
    parseCell(pPage, pPage->aCell[pCur->idx], &info);
    assert( memcmp(&info, &pCur->info, sizeof(info))==0 );
#endif
  }
}

/*
** Set *pSize to the size of the buffer needed to hold the value of
** the key for the current entry.  If the cursor is not pointing
** to a valid entry, *pSize is set to 0. 
**
** For a table with the INTKEY flag set, this routine returns the key
** itself, not the number of bytes in the key.
*/
int sqlite3BtreeKeySize(BtCursor *pCur, i64 *pSize){
  if( !pCur->isValid ){
    *pSize = 0;
  }else{
    getCellInfo(pCur);
    *pSize = pCur->info.nKey;
  }
  return SQLITE_OK;
}

/*
** Set *pSize to the number of bytes of data in the entry the
** cursor currently points to.  Always return SQLITE_OK.
** Failure is not possible.  If the cursor is not currently
** pointing to an entry (which can happen, for example, if
** the database is empty) then *pSize is set to 0.
*/
int sqlite3BtreeDataSize(BtCursor *pCur, u32 *pSize){
  if( !pCur->isValid ){
    /* Not pointing at a valid entry - set *pSize to 0. */
    *pSize = 0;
  }else{
    getCellInfo(pCur);
    *pSize = pCur->info.nData;
  }
  return SQLITE_OK;
}

/*
** Read payload information from the entry that the pCur cursor is
** pointing to.  Begin reading the payload at "offset" and read
** a total of "amt" bytes.  Put the result in zBuf.
**
** This routine does not make a distinction between key and data.
** It just reads bytes from the payload area.
*/
static int getPayload(
  BtCursor *pCur,      /* Cursor pointing to entry to read from */
  int offset,          /* Begin reading this far into payload */
  int amt,             /* Read this many bytes */
  unsigned char *pBuf, /* Write the bytes into this buffer */ 
  int skipKey          /* offset begins at data if this is true */
){
  unsigned char *aPayload;
  Pgno nextPage;
  int rc;
  MemPage *pPage;
  Btree *pBt;
  int ovflSize;
  u32 nKey;

  assert( pCur!=0 && pCur->pPage!=0 );
  assert( pCur->isValid );
  pBt = pCur->pBt;
  pPage = pCur->pPage;
  pageIntegrity(pPage);
  assert( pCur->idx>=0 && pCur->idx<pPage->nCell );
  aPayload = pPage->aCell[pCur->idx];
  getCellInfo(pCur);
  aPayload += pCur->info.nHeader;
  if( pPage->intKey ){
    nKey = 0;
  }else{
    nKey = pCur->info.nKey;
  }
  assert( offset>=0 );
  if( skipKey ){
    offset += nKey;
  }
  if( offset+amt > nKey+pCur->info.nData ){
    return SQLITE_ERROR;
  }
  if( offset<pCur->info.nLocal ){
    int a = amt;
    if( a+offset>pCur->info.nLocal ){
      a = pCur->info.nLocal - offset;
    }
    memcpy(pBuf, &aPayload[offset], a);
    if( a==amt ){
      return SQLITE_OK;
    }
    offset = 0;
    pBuf += a;
    amt -= a;
  }else{
    offset -= pCur->info.nLocal;
  }
  if( amt>0 ){
    nextPage = get4byte(&aPayload[pCur->info.nLocal]);
  }
  ovflSize = pBt->usableSize - 4;
  while( amt>0 && nextPage ){
    rc = sqlite3pager_get(pBt->pPager, nextPage, (void**)&aPayload);
    if( rc!=0 ){
      return rc;
    }
    nextPage = get4byte(aPayload);
    if( offset<ovflSize ){
      int a = amt;
      if( a + offset > ovflSize ){
        a = ovflSize - offset;
      }
      memcpy(pBuf, &aPayload[offset+4], a);
      offset = 0;
      amt -= a;
      pBuf += a;
    }else{
      offset -= ovflSize;
    }
    sqlite3pager_unref(aPayload);
  }
  if( amt>0 ){
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}

/*
** Read part of the key associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3BtreeKey(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  assert( amt>=0 );
  assert( offset>=0 );
  if( pCur->isValid==0 ){
    return pCur->status;
  }
  assert( pCur->pPage!=0 );
  assert( pCur->pPage->intKey==0 );
  assert( pCur->idx>=0 && pCur->idx<pCur->pPage->nCell );
  return getPayload(pCur, offset, amt, (unsigned char*)pBuf, 0);
}

/*
** Read part of the data associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3BtreeData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  if( !pCur->isValid ){
    return pCur->status ? pCur->status : SQLITE_INTERNAL;
  }
  assert( amt>=0 );
  assert( offset>=0 );
  assert( pCur->pPage!=0 );
  assert( pCur->idx>=0 && pCur->idx<pCur->pPage->nCell );
  return getPayload(pCur, offset, amt, pBuf, 1);
}

/*
** Return a pointer to payload information from the entry that the 
** pCur cursor is pointing to.  The pointer is to the beginning of
** the key if skipKey==0 and it points to the beginning of data if
** skipKey==1.
**
** At least amt bytes of information must be available on the local
** page or else this routine returns NULL.  If amt<0 then the entire
** key/data must be available.
**
** This routine is an optimization.  It is common for the entire key
** and data to fit on the local page and for there to be no overflow
** pages.  When that is so, this routine can be used to access the
** key and data without making a copy.  If the key and/or data spills
** onto overflow pages, then getPayload() must be used to reassembly
** the key/data and copy it into a preallocated buffer.
**
** The pointer returned by this routine looks directly into the cached
** page of the database.  The data might change or move the next time
** any btree routine is called.
*/
static const unsigned char *fetchPayload(
  BtCursor *pCur,      /* Cursor pointing to entry to read from */
  int amt,             /* Amount requested */
  int skipKey          /* read beginning at data if this is true */
){
  unsigned char *aPayload;
  MemPage *pPage;
  Btree *pBt;
  u32 nKey;
  int nLocal;

  assert( pCur!=0 && pCur->pPage!=0 );
  assert( pCur->isValid );
  pBt = pCur->pBt;
  pPage = pCur->pPage;
  pageIntegrity(pPage);
  assert( pCur->idx>=0 && pCur->idx<pPage->nCell );
  aPayload = pPage->aCell[pCur->idx];
  getCellInfo(pCur);
  aPayload += pCur->info.nHeader;
  if( pPage->intKey ){
    nKey = 0;
  }else{
    nKey = pCur->info.nKey;
  }
  if( skipKey ){
    aPayload += nKey;
    nLocal = pCur->info.nLocal - nKey;
    if( amt<0 ) amt = pCur->info.nData;
    assert( amt<=pCur->info.nData );
  }else{
    nLocal = pCur->info.nLocal;
    if( amt<0 ) amt = nKey;
    assert( amt<=nKey );
  }
  if( amt>nLocal ){
    return 0;  /* If any of the data is not local, return nothing */
  }
  return aPayload;
}


/*
** Return a pointer to the first amt bytes of the key or data
** for record that cursor pCur is point to if the entire request
** exists in contiguous memory on the main tree page.  If any
** any part of the request is on an overflow page, return 0.
** If pCur is not pointing to a valid entry return 0.
**
** If amt<0 then return the entire key or data.
**
** The pointer returned is ephemeral.  The key/data may move
** or be destroyed on the next call to any Btree routine.
**
** These routines is used to get quick access to key and data
** in the common case where no overflow pages are used.
**
** It is a fatal error to call these routines with amt values that
** are larger than the key/data size.
*/
const void *sqlite3BtreeKeyFetch(BtCursor *pCur, int amt){
  return (const void*)fetchPayload(pCur, amt, 0);
}
const void *sqlite3BtreeDataFetch(BtCursor *pCur, int amt){
  return (const void*)fetchPayload(pCur, amt, 1);
}


/*
** Move the cursor down to a new child page.  The newPgno argument is the
** page number of the child page in the byte order of the disk image.
*/
static int moveToChild(BtCursor *pCur, u32 newPgno){
  int rc;
  MemPage *pNewPage;
  MemPage *pOldPage;
  Btree *pBt = pCur->pBt;

  assert( pCur->isValid );
  rc = getAndInitPage(pBt, newPgno, &pNewPage, pCur->pPage);
  if( rc ) return rc;
  pageIntegrity(pNewPage);
  pNewPage->idxParent = pCur->idx;
  pOldPage = pCur->pPage;
  pOldPage->idxShift = 0;
  releasePage(pOldPage);
  pCur->pPage = pNewPage;
  pCur->idx = 0;
  pCur->infoValid = 0;
  if( pNewPage->nCell<1 ){
    return SQLITE_CORRUPT;
  }
  return SQLITE_OK;
}

/*
** Return true if the page is the virtual root of its table.
**
** The virtual root page is the root page for most tables.  But
** for the table rooted on page 1, sometime the real root page
** is empty except for the right-pointer.  In such cases the
** virtual root page is the page that the right-pointer of page
** 1 is pointing to.
*/
static int isRootPage(MemPage *pPage){
  MemPage *pParent = pPage->pParent;
  if( pParent==0 ) return 1;
  if( pParent->pgno>1 ) return 0;
  if( get2byte(&pParent->aData[pParent->hdrOffset+3])==0 ) return 1;
  return 0;
}

/*
** Move the cursor up to the parent page.
**
** pCur->idx is set to the cell index that contains the pointer
** to the page we are coming from.  If we are coming from the
** right-most child page then pCur->idx is set to one more than
** the largest cell index.
*/
static void moveToParent(BtCursor *pCur){
  Pgno oldPgno;
  MemPage *pParent;
  MemPage *pPage;
  int idxParent;

  assert( pCur->isValid );
  pPage = pCur->pPage;
  assert( pPage!=0 );
  assert( !isRootPage(pPage) );
  pageIntegrity(pPage);
  pParent = pPage->pParent;
  assert( pParent!=0 );
  pageIntegrity(pParent);
  idxParent = pPage->idxParent;
  sqlite3pager_ref(pParent->aData);
  oldPgno = pPage->pgno;
  releasePage(pPage);
  pCur->pPage = pParent;
  pCur->infoValid = 0;
  assert( pParent->idxShift==0 );
  if( pParent->idxShift==0 ){
    pCur->idx = idxParent;
#ifndef NDEBUG  
    /* Verify that pCur->idx is the correct index to point back to the child
    ** page we just came from 
    */
    if( pCur->idx<pParent->nCell ){
      assert( get4byte(&pParent->aCell[idxParent][2])==oldPgno );
    }else{
      assert( get4byte(&pParent->aData[pParent->hdrOffset+6])==oldPgno );
    }
#endif
  }else{
    /* The MemPage.idxShift flag indicates that cell indices might have 
    ** changed since idxParent was set and hence idxParent might be out
    ** of date.  So recompute the parent cell index by scanning all cells
    ** and locating the one that points to the child we just came from.
    */
    int i;
    pCur->idx = pParent->nCell;
    for(i=0; i<pParent->nCell; i++){
      if( get4byte(&pParent->aCell[i][2])==oldPgno ){
        pCur->idx = i;
        break;
      }
    }
  }
}

/*
** Move the cursor to the root page
*/
static int moveToRoot(BtCursor *pCur){
  MemPage *pRoot;
  int rc;
  Btree *pBt = pCur->pBt;

  rc = getAndInitPage(pBt, pCur->pgnoRoot, &pRoot, 0);
  if( rc ){
    pCur->isValid = 0;
    return rc;
  }
  releasePage(pCur->pPage);
  pageIntegrity(pRoot);
  pCur->pPage = pRoot;
  pCur->idx = 0;
  pCur->infoValid = 0;
  if( pRoot->nCell==0 && !pRoot->leaf ){
    Pgno subpage;
    assert( pRoot->pgno==1 );
    subpage = get4byte(&pRoot->aData[pRoot->hdrOffset+6]);
    assert( subpage>0 );
    pCur->isValid = 1;
    rc = moveToChild(pCur, subpage);
  }
  pCur->isValid = pCur->pPage->nCell>0;
  return rc;
}

/*
** Move the cursor down to the left-most leaf entry beneath the
** entry to which it is currently pointing.
*/
static int moveToLeftmost(BtCursor *pCur){
  Pgno pgno;
  int rc;
  MemPage *pPage;

  assert( pCur->isValid );
  while( !(pPage = pCur->pPage)->leaf ){
    assert( pCur->idx>=0 && pCur->idx<pPage->nCell );
    pgno = get4byte(&pPage->aCell[pCur->idx][2]);
    rc = moveToChild(pCur, pgno);
    if( rc ) return rc;
  }
  return SQLITE_OK;
}

/*
** Move the cursor down to the right-most leaf entry beneath the
** page to which it is currently pointing.  Notice the difference
** between moveToLeftmost() and moveToRightmost().  moveToLeftmost()
** finds the left-most entry beneath the *entry* whereas moveToRightmost()
** finds the right-most entry beneath the *page*.
*/
static int moveToRightmost(BtCursor *pCur){
  Pgno pgno;
  int rc;
  MemPage *pPage;

  assert( pCur->isValid );
  while( !(pPage = pCur->pPage)->leaf ){
    pgno = get4byte(&pPage->aData[pPage->hdrOffset+6]);
    pCur->idx = pPage->nCell;
    rc = moveToChild(pCur, pgno);
    if( rc ) return rc;
  }
  pCur->idx = pPage->nCell - 1;
  pCur->infoValid = 0;
  return SQLITE_OK;
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  int rc;
  if( pCur->status ){
    return pCur->status;
  }
  rc = moveToRoot(pCur);
  if( rc ) return rc;
  if( pCur->isValid==0 ){
    assert( pCur->pPage->nCell==0 );
    *pRes = 1;
    return SQLITE_OK;
  }
  assert( pCur->pPage->nCell>0 );
  *pRes = 0;
  rc = moveToLeftmost(pCur);
  return rc;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
  int rc;
  if( pCur->status ){
    return pCur->status;
  }
  rc = moveToRoot(pCur);
  if( rc ) return rc;
  if( pCur->isValid==0 ){
    assert( pCur->pPage->nCell==0 );
    *pRes = 1;
    return SQLITE_OK;
  }
  assert( pCur->isValid );
  *pRes = 0;
  rc = moveToRightmost(pCur);
  return rc;
}

/* Move the cursor so that it points to an entry near pKey/nKey.
** Return a success code.
**
** For INTKEY tables, only the nKey parameter is used.  pKey is
** ignored.  For other tables, nKey is the number of bytes of data
** in nKey.  The comparison function specified when the cursor was
** created is used to compare keys.
**
** If an exact match is not found, then the cursor is always
** left pointing at a leaf page which would hold the entry if it
** were present.  The cursor might point to an entry that comes
** before or after the key.
**
** The result of comparing the key with the entry to which the
** cursor is left pointing is stored in pCur->iMatch.  The same
** value is also written to *pRes if pRes!=NULL.  The meaning of
** this value is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than pKey or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches pKey.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than pKey.
*/
int sqlite3BtreeMoveto(BtCursor *pCur, const void *pKey, i64 nKey, int *pRes){
  int rc;

  if( pCur->status ){
    return pCur->status;
  }
  rc = moveToRoot(pCur);
  if( rc ) return rc;
  assert( pCur->pPage );
  assert( pCur->pPage->isInit );
  if( pCur->isValid==0 ){
    *pRes = -1;
    assert( pCur->pPage->nCell==0 );
    return SQLITE_OK;
  }
  for(;;){
    int lwr, upr;
    Pgno chldPg;
    MemPage *pPage = pCur->pPage;
    int c = -1;  /* pRes return if table is empty must be -1 */
    lwr = 0;
    upr = pPage->nCell-1;
    pageIntegrity(pPage);
    while( lwr<=upr ){
      const void *pCellKey;
      i64 nCellKey;
      pCur->idx = (lwr+upr)/2;
      pCur->infoValid = 0;
      sqlite3BtreeKeySize(pCur, &nCellKey);
      if( pPage->intKey ){
        if( nCellKey<nKey ){
          c = -1;
        }else if( nCellKey>nKey ){
          c = +1;
        }else{
          c = 0;
        }
      }else if( (pCellKey = sqlite3BtreeKeyFetch(pCur, nCellKey))!=0 ){
        c = pCur->xCompare(pCur->pArg, nCellKey, pCellKey, nKey, pKey);
      }else{
        u8 *pCellKey = sqliteMalloc( nCellKey );
        if( pCellKey==0 ) return SQLITE_NOMEM;
        rc = sqlite3BtreeKey(pCur, 0, nCellKey, pCellKey);
        c = pCur->xCompare(pCur->pArg, nCellKey, pCellKey, nKey, pKey);
        sqliteFree(pCellKey);
        if( rc ) return rc;
      }
      if( c==0 ){
        if( pPage->leafData && !pPage->leaf ){
          lwr = pCur->idx;
          upr = lwr - 1;
          break;
        }else{
          pCur->iMatch = c;
          if( pRes ) *pRes = 0;
          return SQLITE_OK;
        }
      }
      if( c<0 ){
        lwr = pCur->idx+1;
      }else{
        upr = pCur->idx-1;
      }
    }
    assert( lwr==upr+1 );
    assert( pPage->isInit );
    if( pPage->leaf ){
      chldPg = 0;
    }else if( lwr>=pPage->nCell ){
      chldPg = get4byte(&pPage->aData[pPage->hdrOffset+6]);
    }else{
      chldPg = get4byte(&pPage->aCell[lwr][2]);
    }
    if( chldPg==0 ){
      pCur->iMatch = c;
      assert( pCur->idx>=0 && pCur->idx<pCur->pPage->nCell );
      if( pRes ) *pRes = c;
      return SQLITE_OK;
    }
    pCur->idx = lwr;
    pCur->infoValid = 0;
    rc = moveToChild(pCur, chldPg);
    if( rc ){
      return rc;
    }
  }
  /* NOT REACHED */
}

/*
** Return TRUE if the cursor is not pointing at an entry of the table.
**
** TRUE will be returned after a call to sqlite3BtreeNext() moves
** past the last entry in the table or sqlite3BtreePrev() moves past
** the first entry.  TRUE is also returned if the table is empty.
*/
int sqlite3BtreeEof(BtCursor *pCur){
  return pCur->isValid==0;
}

/*
** Advance the cursor to the next entry in the database.  If
** successful then set *pRes=0.  If the cursor
** was already pointing to the last entry in the database before
** this routine was called, then set *pRes=1.
*/
int sqlite3BtreeNext(BtCursor *pCur, int *pRes){
  int rc;
  MemPage *pPage = pCur->pPage;

  assert( pRes!=0 );
  if( pCur->isValid==0 ){
    *pRes = 1;
    return SQLITE_OK;
  }
  assert( pPage->isInit );
  assert( pCur->idx<pPage->nCell );
  pCur->idx++;
  pCur->infoValid = 0;
  if( pCur->idx>=pPage->nCell ){
    if( !pPage->leaf ){
      rc = moveToChild(pCur, get4byte(&pPage->aData[pPage->hdrOffset+6]));
      if( rc ) return rc;
      rc = moveToLeftmost(pCur);
      *pRes = 0;
      return rc;
    }
    do{
      if( isRootPage(pPage) ){
        *pRes = 1;
        pCur->isValid = 0;
        return SQLITE_OK;
      }
      moveToParent(pCur);
      pPage = pCur->pPage;
    }while( pCur->idx>=pPage->nCell );
    *pRes = 0;
    if( pPage->leafData ){
      rc = sqlite3BtreeNext(pCur, pRes);
    }else{
      rc = SQLITE_OK;
    }
    return rc;
  }
  *pRes = 0;
  if( pPage->leaf ){
    return SQLITE_OK;
  }
  rc = moveToLeftmost(pCur);
  return rc;
}

/*
** Step the cursor to the back to the previous entry in the database.  If
** successful then set *pRes=0.  If the cursor
** was already pointing to the first entry in the database before
** this routine was called, then set *pRes=1.
*/
int sqlite3BtreePrevious(BtCursor *pCur, int *pRes){
  int rc;
  Pgno pgno;
  MemPage *pPage;
  if( pCur->isValid==0 ){
    *pRes = 1;
    return SQLITE_OK;
  }
  pPage = pCur->pPage;
  assert( pPage->isInit );
  assert( pCur->idx>=0 );
  if( !pPage->leaf ){
    pgno = get4byte(&pPage->aCell[pCur->idx][2]);
    rc = moveToChild(pCur, pgno);
    if( rc ) return rc;
    rc = moveToRightmost(pCur);
  }else{
    while( pCur->idx==0 ){
      if( isRootPage(pPage) ){
        pCur->isValid = 0;
        *pRes = 1;
        return SQLITE_OK;
      }
      moveToParent(pCur);
      pPage = pCur->pPage;
    }
    pCur->idx--;
    pCur->infoValid = 0;
    if( pPage->leafData ){
      rc = sqlite3BtreePrevious(pCur, pRes);
    }else{
      rc = SQLITE_OK;
    }
  }
  *pRes = 0;
  return rc;
}

/*
** The TRACE macro will print high-level status information about the
** btree operation when the global variable sqlite3_btree_trace is
** enabled.
*/
#if SQLITE_TEST
# define TRACE(X)   if( sqlite3_btree_trace ){ printf X; fflush(stdout); }
#else
# define TRACE(X)
#endif
int sqlite3_btree_trace=0;  /* True to enable tracing */

/*
** Allocate a new page from the database file.
**
** The new page is marked as dirty.  (In other words, sqlite3pager_write()
** has already been called on the new page.)  The new page has also
** been referenced and the calling routine is responsible for calling
** sqlite3pager_unref() on the new page when it is done.
**
** SQLITE_OK is returned on success.  Any other return value indicates
** an error.  *ppPage and *pPgno are undefined in the event of an error.
** Do not invoke sqlite3pager_unref() on *ppPage if an error is returned.
**
** If the "nearby" parameter is not 0, then a (feeble) effort is made to 
** locate a page close to the page number "nearby".  This can be used in an
** attempt to keep related pages close to each other in the database file,
** which in turn can make database access faster.
*/
static int allocatePage(Btree *pBt, MemPage **ppPage, Pgno *pPgno, Pgno nearby){
  MemPage *pPage1;
  int rc;
  int n;     /* Number of pages on the freelist */
  int k;     /* Number of leaves on the trunk of the freelist */

  pPage1 = pBt->pPage1;
  n = get4byte(&pPage1->aData[36]);
  if( n>0 ){
    /* There are pages on the freelist.  Reuse one of those pages. */
    MemPage *pTrunk;
    rc = sqlite3pager_write(pPage1->aData);
    if( rc ) return rc;
    put4byte(&pPage1->aData[36], n-1);
    rc = getPage(pBt, get4byte(&pPage1->aData[32]), &pTrunk);
    if( rc ) return rc;
    rc = sqlite3pager_write(pTrunk->aData);
    if( rc ){
      releasePage(pTrunk);
      return rc;
    }
    k = get4byte(&pTrunk->aData[4]);
    if( k==0 ){
      /* The trunk has no leaves.  So extract the trunk page itself and
      ** use it as the newly allocated page */
      *pPgno = get4byte(&pPage1->aData[32]);
      memcpy(&pPage1->aData[32], &pTrunk->aData[0], 4);
      *ppPage = pTrunk;
      TRACE(("ALLOCATE: %d trunk - %d free pages left\n", *pPgno, n-1));
    }else{
      /* Extract a leaf from the trunk */
      int closest;
      unsigned char *aData = pTrunk->aData;
      if( nearby>0 ){
        int i, dist;
        closest = 0;
        dist = get4byte(&aData[8]) - nearby;
        if( dist<0 ) dist = -dist;
        for(i=1; i<k; i++){
          int d2 = get4byte(&aData[8+i*4]) - nearby;
          if( d2<0 ) d2 = -d2;
          if( d2<dist ) closest = i;
        }
      }else{
        closest = 0;
      }
      *pPgno = get4byte(&aData[8+closest*4]);
      TRACE(("ALLOCATE: %d was leaf %d of %d on trunk %d: %d more free pages\n",
             *pPgno, closest+1, k, pTrunk->pgno, n-1));
      if( closest<k-1 ){
        memcpy(&aData[8+closest*4], &aData[4+k*4], 4);
      }
      put4byte(&aData[4], k-1);
      rc = getPage(pBt, *pPgno, ppPage);
      releasePage(pTrunk);
      if( rc==SQLITE_OK ){
        sqlite3pager_dont_rollback((*ppPage)->aData);
        rc = sqlite3pager_write((*ppPage)->aData);
      }
    }
  }else{
    /* There are no pages on the freelist, so create a new page at the
    ** end of the file */
    *pPgno = sqlite3pager_pagecount(pBt->pPager) + 1;
    rc = getPage(pBt, *pPgno, ppPage);
    if( rc ) return rc;
    rc = sqlite3pager_write((*ppPage)->aData);
    TRACE(("ALLOCATE: %d from end of file\n", *pPgno));
  }
  return rc;
}

/*
** Add a page of the database file to the freelist.
**
** sqlite3pager_unref() is NOT called for pPage.
*/
static int freePage(MemPage *pPage){
  Btree *pBt = pPage->pBt;
  MemPage *pPage1 = pBt->pPage1;
  int rc, n, k;

  /* Prepare the page for freeing */
  assert( pPage->pgno>1 );
  pPage->isInit = 0;
  releasePage(pPage->pParent);
  pPage->pParent = 0;

  /* Increment the free page count on pPage1 */
  rc = sqlite3pager_write(pPage1->aData);
  if( rc ) return rc;
  n = get4byte(&pPage1->aData[36]);
  put4byte(&pPage1->aData[36], n+1);

  if( n==0 ){
    /* This is the first free page */
    rc = sqlite3pager_write(pPage->aData);
    if( rc ) return rc;
    memset(pPage->aData, 0, 8);
    put4byte(&pPage1->aData[32], pPage->pgno);
    TRACE(("FREE-PAGE: %d first\n", pPage->pgno));
  }else{
    /* Other free pages already exist.  Retrive the first trunk page
    ** of the freelist and find out how many leaves it has. */
    MemPage *pTrunk;
    rc = getPage(pBt, get4byte(&pPage1->aData[32]), &pTrunk);
    if( rc ) return rc;
    k = get4byte(&pTrunk->aData[4]);
    if( k==pBt->usableSize/4 - 8 ){
      /* The trunk is full.  Turn the page being freed into a new
      ** trunk page with no leaves. */
      rc = sqlite3pager_write(pPage->aData);
      if( rc ) return rc;
      put4byte(pPage->aData, pTrunk->pgno);
      put4byte(&pPage->aData[4], 0);
      put4byte(&pPage1->aData[32], pPage->pgno);
      TRACE(("FREE-PAGE: %d new trunk page replacing %d\n",
              pPage->pgno, pTrunk->pgno));
    }else{
      /* Add the newly freed page as a leaf on the current trunk */
      rc = sqlite3pager_write(pTrunk->aData);
      if( rc ) return rc;
      put4byte(&pTrunk->aData[4], k+1);
      put4byte(&pTrunk->aData[8+k*4], pPage->pgno);
      sqlite3pager_dont_write(pBt->pPager, pPage->pgno);
      TRACE(("FREE-PAGE: %d leaf on trunk page %d\n",pPage->pgno,pTrunk->pgno));
    }
    releasePage(pTrunk);
  }
  return rc;
}

/*
** Free any overflow pages associated with the given Cell.
*/
static int clearCell(MemPage *pPage, unsigned char *pCell){
  Btree *pBt = pPage->pBt;
  CellInfo info;
  Pgno ovflPgno;
  int rc;

  parseCell(pPage, pCell, &info);
  if( info.iOverflow==0 ){
    return SQLITE_OK;  /* No overflow pages. Return without doing anything */
  }
  ovflPgno = get4byte(&pCell[info.iOverflow]);
  while( ovflPgno!=0 ){
    MemPage *pOvfl;
    rc = getPage(pBt, ovflPgno, &pOvfl);
    if( rc ) return rc;
    ovflPgno = get4byte(pOvfl->aData);
    rc = freePage(pOvfl);
    if( rc ) return rc;
    sqlite3pager_unref(pOvfl->aData);
  }
  return SQLITE_OK;
}

/*
** Create the byte sequence used to represent a cell on page pPage
** and write that byte sequence into pCell[].  Overflow pages are
** allocated and filled in as necessary.  The calling procedure
** is responsible for making sure sufficient space has been allocated
** for pCell[].
**
** Note that pCell does not necessary need to point to the pPage->aData
** area.  pCell might point to some temporary storage.  The cell will
** be constructed in this temporary area then copied into pPage->aData
** later.
*/
static int fillInCell(
  MemPage *pPage,                /* The page that contains the cell */
  unsigned char *pCell,          /* Complete text of the cell */
  const void *pKey, i64 nKey,    /* The key */
  const void *pData,int nData,   /* The data */
  int *pnSize                    /* Write cell size here */
){
  int nPayload;
  const void *pSrc;
  int nSrc, n, rc;
  int spaceLeft;
  MemPage *pOvfl = 0;
  MemPage *pToRelease = 0;
  unsigned char *pPrior;
  unsigned char *pPayload;
  Btree *pBt = pPage->pBt;
  Pgno pgnoOvfl = 0;
  int nHeader;
  CellInfo info;

  /* Fill in the header. */
  nHeader = 2;
  if( !pPage->leaf ){
    nHeader += 4;
  }
  if( pPage->hasData ){
    nHeader += putVarint(&pCell[nHeader], nData);
  }else{
    nData = 0;
  }
  nHeader += putVarint(&pCell[nHeader], *(u64*)&nKey);
  parseCell(pPage, pCell, &info);
  assert( info.nHeader==nHeader );
  assert( info.nKey==nKey );
  assert( info.nData==nData );
  
  /* Fill in the payload */
  nPayload = nData;
  if( pPage->intKey ){
    pSrc = pData;
    nSrc = nData;
    nData = 0;
  }else{
    nPayload += nKey;
    pSrc = pKey;
    nSrc = nKey;
  }
  *pnSize = info.nSize;
  spaceLeft = info.nLocal;
  pPayload = &pCell[nHeader];
  pPrior = &pCell[info.iOverflow];

  while( nPayload>0 ){
    if( spaceLeft==0 ){
      rc =  allocatePage(pBt, &pOvfl, &pgnoOvfl, pgnoOvfl);
      if( rc ){
        releasePage(pToRelease);
        clearCell(pPage, pCell);
        return rc;
      }
      put4byte(pPrior, pgnoOvfl);
      releasePage(pToRelease);
      pToRelease = pOvfl;
      pPrior = pOvfl->aData;
      put4byte(pPrior, 0);
      pPayload = &pOvfl->aData[4];
      spaceLeft = pBt->usableSize - 4;
    }
    n = nPayload;
    if( n>spaceLeft ) n = spaceLeft;
    if( n>nSrc ) n = nSrc;
    memcpy(pPayload, pSrc, n);
    nPayload -= n;
    pPayload += n;
    pSrc += n;
    nSrc -= n;
    spaceLeft -= n;
    if( nSrc==0 ){
      nSrc = nData;
      pSrc = pData;
    }
  }
  releasePage(pToRelease);
  return SQLITE_OK;
}

/*
** Change the MemPage.pParent pointer on the page whose number is
** given in the second argument so that MemPage.pParent holds the
** pointer in the third argument.
*/
static void reparentPage(Btree *pBt, Pgno pgno, MemPage *pNewParent, int idx){
  MemPage *pThis;
  unsigned char *aData;

  if( pgno==0 ) return;
  assert( pBt->pPager!=0 );
  aData = sqlite3pager_lookup(pBt->pPager, pgno);
  if( aData ){
    pThis = (MemPage*)&aData[pBt->usableSize];
    if( pThis->isInit ){
      if( pThis->pParent!=pNewParent ){
        if( pThis->pParent ) sqlite3pager_unref(pThis->pParent->aData);
        pThis->pParent = pNewParent;
        if( pNewParent ) sqlite3pager_ref(pNewParent->aData);
      }
      pThis->idxParent = idx;
    }
    sqlite3pager_unref(aData);
  }
}

/*
** Change the pParent pointer of all children of pPage to point back
** to pPage.
**
** In other words, for every child of pPage, invoke reparentPage()
** to make sure that each child knows that pPage is its parent.
**
** This routine gets called after you memcpy() one page into
** another.
*/
static void reparentChildPages(MemPage *pPage){
  int i;
  Btree *pBt;

  if( pPage->leaf ) return;
  pBt = pPage->pBt;
  for(i=0; i<pPage->nCell; i++){
    reparentPage(pBt, get4byte(&pPage->aCell[i][2]), pPage, i);
  }
  reparentPage(pBt, get4byte(&pPage->aData[pPage->hdrOffset+6]), pPage, i);
  pPage->idxShift = 0;
}

/*
** Remove the i-th cell from pPage.  This routine effects pPage only.
** The cell content is not freed or deallocated.  It is assumed that
** the cell content has been copied someplace else.  This routine just
** removes the reference to the cell from pPage.
**
** "sz" must be the number of bytes in the cell.
**
** Try to maintain the integrity of the linked list of cells.  But if
** the cell being inserted does not fit on the page, this will not be
** possible.  If the linked list is not maintained, then just update
** pPage->aCell[] and set the pPage->needRelink flag so that we will
** know to rebuild the linked list later.
*/
static void dropCell(MemPage *pPage, int idx, int sz){
  int j, pc;
  u8 *data;
  assert( idx>=0 && idx<pPage->nCell );
  assert( sz==cellSize(pPage, pPage->aCell[idx]) );
  assert( sqlite3pager_iswriteable(pPage->aData) );
  assert( pPage->aCell[idx]>=pPage->aData );
  assert( pPage->aCell[idx]<=&pPage->aData[pPage->pBt->usableSize-sz] );
  data = pPage->aData;
  pc = Addr(pPage->aCell[idx]) - Addr(data);
  assert( pc>pPage->hdrOffset && pc+sz<=pPage->pBt->usableSize );
  freeSpace(pPage, pc, sz);
  for(j=idx; j<pPage->nCell-1; j++){
    pPage->aCell[j] = pPage->aCell[j+1];
  }
  pPage->nCell--;
  if( !pPage->isOverfull && !pPage->needRelink ){
    u8 *pPrev;
    if( idx==0 ){
      pPrev = &data[pPage->hdrOffset+3];
    }else{
      pPrev = pPage->aCell[idx-1];
    }
    if( idx<pPage->nCell ){
      pc = Addr(pPage->aCell[idx]) - Addr(data);
    }else{
      pc = 0;
    }
    put2byte(pPrev, pc);
    pageIntegrity(pPage);
  }else{
    pPage->needRelink = 1;
  }
  pPage->idxShift = 1;
}

/*
** Insert a new cell on pPage at cell index "i".  pCell points to the
** content of the cell.
**
** If the cell content will fit on the page, then put it there.  If it
** will not fit and pTemp is not NULL, then make a copy of the content
** into pTemp, set pPage->aCell[i] point to pTemp, and set pPage->isOverfull.
** If the content will not fit and pTemp is NULL, then make pPage->aCell[i]
** point to pCell and set pPage->isOverfull.
**
** Try to maintain the integrity of the linked list of cells.  But if
** the cell being inserted does not fit on the page, this will not be
** possible.  If the linked list is not maintained, then just update
** pPage->aCell[] and set the pPage->needRelink flag so that we will
** know to rebuild the linked list later.
*/
static void insertCell(
  MemPage *pPage,   /* Page into which we are copying */
  int i,            /* Which cell on pPage to insert after */
  u8 *pCell,        /* Text of the new cell to insert */
  int sz,           /* Bytes of data in pCell */
  u8 *pTemp         /* Temp storage space for pCell, if needed */
){
  int idx, j;
  assert( i>=0 && i<=pPage->nCell );
  assert( sz==cellSize(pPage, pCell) );
  assert( sqlite3pager_iswriteable(pPage->aData) );
  idx = pPage->needRelink ? 0 : allocateSpace(pPage, sz);
  resizeCellArray(pPage, pPage->nCell+1);
  for(j=pPage->nCell; j>i; j--){
    pPage->aCell[j] = pPage->aCell[j-1];
  }
  pPage->nCell++;
  if( idx<=0 ){
    pPage->isOverfull = 1;
    if( pTemp ){
      memcpy(pTemp, pCell, sz);
    }else{
      pTemp = pCell;
    }
    pPage->aCell[i] = pTemp;
  }else{
    u8 *data = pPage->aData;
    memcpy(&data[idx], pCell, sz);
    pPage->aCell[i] = &data[idx];
  }
  if( !pPage->isOverfull && !pPage->needRelink ){
    u8 *pPrev;
    int pc;
    if( i==0 ){
      pPrev = &pPage->aData[pPage->hdrOffset+3];
    }else{
      pPrev = pPage->aCell[i-1];
    }
    pc = get2byte(pPrev);
    put2byte(pPrev, idx);
    put2byte(pPage->aCell[i], pc);
    pageIntegrity(pPage);
  }else{
    pPage->needRelink = 1;
  }
  pPage->idxShift = 1;
}

/*
** Add a list of cells to a page.  The page should be initially empty.
** The cells are guaranteed to fit on the page.
*/
static void assemblePage(
  MemPage *pPage,   /* The page to be assemblied */
  int nCell,        /* The number of cells to add to this page */
  u8 **apCell,      /* Pointers to cell text */
  int *aSize        /* Sizes of the cells */
){
  int i;            /* Loop counter */
  int totalSize;    /* Total size of all cells */
  int hdr;          /* Index of page header */
  int pc, prevpc;   /* Addresses of cells being inserted */
  u8 *data;         /* Data for the page */

  assert( pPage->needRelink==0 );
  assert( pPage->isOverfull==0 );
  totalSize = 0;
  for(i=0; i<nCell; i++){
    totalSize += aSize[i];
  }
  assert( totalSize<=pPage->nFree );
  assert( pPage->nCell==0 );
  resizeCellArray(pPage, nCell);
  pc = allocateSpace(pPage, totalSize);
  data = pPage->aData;
  hdr = pPage->hdrOffset;
  prevpc = hdr+3;
  for(i=0; i<nCell; i++){
    memcpy(data+pc, apCell[i], aSize[i]);
    put2byte(data+prevpc, pc);
    pPage->aCell[i] = data+pc;
    prevpc = pc;
    pc += aSize[i];
    assert( pc<=pPage->pBt->usableSize );
  }
  pPage->nCell = nCell;
  put2byte(data+prevpc, 0);
}

/*
** Rebuild the linked list of cells on a page so that the cells
** occur in the order specified by the pPage->aCell[] array.  
** Invoke this routine once to repair damage after one or more
** invocations of either insertCell() or dropCell().
*/
static void relinkCellList(MemPage *pPage){
  int i, idxFrom;
  assert( sqlite3pager_iswriteable(pPage->aData) );
  if( !pPage->needRelink ) return;
  idxFrom = pPage->hdrOffset+3;
  for(i=0; i<pPage->nCell; i++){
    int idx = Addr(pPage->aCell[i]) - Addr(pPage->aData);
    assert( idx>pPage->hdrOffset && idx<pPage->pBt->usableSize );
    put2byte(&pPage->aData[idxFrom], idx);
    idxFrom = idx;
  }
  put2byte(&pPage->aData[idxFrom], 0);
  pPage->needRelink = 0;
}

/*
** GCC does not define the offsetof() macro so we'll have to do it
** ourselves.
*/
#ifndef offsetof
#define offsetof(STRUCTURE,FIELD) ((int)((char*)&((STRUCTURE*)0)->FIELD))
#endif

/*
** Move the content of the page at pFrom over to pTo.  The pFrom->aCell[]
** pointers that point into pFrom->aData[] must be adjusted to point
** into pTo->aData[] instead.  But some pFrom->aCell[] entries might
** not point to pFrom->aData[].  Those are unchanged.
**
** Over this operation completes, the meta data for pFrom is zeroed.
*/
static void movePage(MemPage *pTo, MemPage *pFrom){
  uptr from, to;
  int i;
  int usableSize;
  int ofst;

  assert( pTo->hdrOffset==0 );
  assert( pFrom->isInit );
  ofst = pFrom->hdrOffset;
  usableSize = pFrom->pBt->usableSize;
  sqliteFree(pTo->aCell);
  memcpy(pTo->aData, &pFrom->aData[ofst], usableSize - ofst);
  memcpy(pTo, pFrom, offsetof(MemPage, aData));
  pFrom->isInit = 0;
  pFrom->aCell = 0;
  assert( pTo->aData[5]<155 );
  pTo->aData[5] += ofst;
  pTo->isOverfull = pFrom->isOverfull;
  to = Addr(pTo->aData);
  from = Addr(&pFrom->aData[ofst]);
  for(i=0; i<pTo->nCell; i++){
    uptr x = Addr(pTo->aCell[i]);
    if( x>from && x<from+usableSize-ofst ){
      *((uptr*)&pTo->aCell[i]) = x + to - from;
    }
  }
}

/*
** The following parameters determine how many adjacent pages get involved
** in a balancing operation.  NN is the number of neighbors on either side
** of the page that participate in the balancing operation.  NB is the
** total number of pages that participate, including the target page and
** NN neighbors on either side.
**
** The minimum value of NN is 1 (of course).  Increasing NN above 1
** (to 2 or 3) gives a modest improvement in SELECT and DELETE performance
** in exchange for a larger degradation in INSERT and UPDATE performance.
** The value of NN appears to give the best results overall.
*/
#define NN 1             /* Number of neighbors on either side of pPage */
#define NB (NN*2+1)      /* Total pages involved in the balance */

/*
** This routine redistributes Cells on pPage and up to two siblings
** of pPage so that all pages have about the same amount of free space.
** Usually one sibling on either side of pPage is used in the balancing,
** though both siblings might come from one side if pPage is the first
** or last child of its parent.  If pPage has fewer than two siblings
** (something which can only happen if pPage is the root page or a 
** child of root) then all available siblings participate in the balancing.
**
** The number of siblings of pPage might be increased or decreased by
** one in an effort to keep pages between 66% and 100% full. The root page
** is special and is allowed to be less than 66% full. If pPage is 
** the root page, then the depth of the tree might be increased
** or decreased by one, as necessary, to keep the root page from being
** overfull or empty.
**
** This routine alwyas calls relinkCellList() on its input page regardless of
** whether or not it does any real balancing.  Client routines will typically
** invoke insertCell() or dropCell() before calling this routine, so we
** need to call relinkCellList() to clean up the mess that those other
** routines left behind.
**
** Note that when this routine is called, some of the Cells on pPage
** might not actually be stored in pPage->aData[].  This can happen
** if the page is overfull.  Part of the job of this routine is to
** make sure all Cells for pPage once again fit in pPage->aData[].
**
** In the course of balancing the siblings of pPage, the parent of pPage
** might become overfull or underfull.  If that happens, then this routine
** is called recursively on the parent.
**
** If this routine fails for any reason, it might leave the database
** in a corrupted state.  So if this routine fails, the database should
** be rolled back.
*/
static int balance(MemPage *pPage){
  MemPage *pParent;            /* The parent of pPage */
  Btree *pBt;                  /* The whole database */
  int nCell;                   /* Number of cells in aCell[] */
  int nOld;                    /* Number of pages in apOld[] */
  int nNew;                    /* Number of pages in apNew[] */
  int nDiv;                    /* Number of cells in apDiv[] */
  int i, j, k;                 /* Loop counters */
  int idx;                     /* Index of pPage in pParent->aCell[] */
  int nxDiv;                   /* Next divider slot in pParent->aCell[] */
  int rc;                      /* The return code */
  int leafCorrection;          /* 4 if pPage is a leaf.  0 if not */
  int leafData;                /* True if pPage is a leaf of a LEAFDATA tree */
  int usableSpace;             /* Bytes in pPage beyond the header */
  int pageFlags;               /* Value of pPage->aData[0] */
  int subtotal;                /* Subtotal of bytes in cells on one page */
  int iSpace = 0;              /* First unused byte of aSpace[] */
  MemPage *extraUnref = 0;     /* Unref this page if not zero */
  MemPage *apOld[NB];          /* pPage and up to two siblings */
  Pgno pgnoOld[NB];            /* Page numbers for each page in apOld[] */
  MemPage *apCopy[NB];         /* Private copies of apOld[] pages */
  MemPage *apNew[NB+1];        /* pPage and up to NB siblings after balancing */
  Pgno pgnoNew[NB+1];          /* Page numbers for each page in apNew[] */
  int idxDiv[NB];              /* Indices of divider cells in pParent */
  u8 *apDiv[NB];               /* Divider cells in pParent */
  int cntNew[NB+1];            /* Index in aCell[] of cell after i-th page */
  int szNew[NB+1];             /* Combined size of cells place on i-th page */
  u8 *apCell[(MX_CELL+2)*NB];  /* All cells from pages being balanced */
  int szCell[(MX_CELL+2)*NB];  /* Local size of all cells */
  u8 aCopy[NB][MX_PAGE_SIZE+sizeof(MemPage)];  /* Space for apCopy[] */
  u8 aSpace[MX_PAGE_SIZE*4];   /* Space to copies of divider cells */

  /* 
  ** Return without doing any work if pPage is neither overfull nor
  ** underfull.
  */
  assert( pPage->isInit );
  assert( sqlite3pager_iswriteable(pPage->aData) );
  pBt = pPage->pBt;
  if( !pPage->isOverfull && pPage->nFree<pBt->usableSize*2/3
        && pPage->nCell>=2){
    relinkCellList(pPage);
    return SQLITE_OK;
  }

  /*
  ** Find the parent of the page to be balanced.  If there is no parent,
  ** it means this page is the root page and special rules apply.
  */
  pParent = pPage->pParent;
  if( pParent==0 ){
    Pgno pgnoChild;
    MemPage *pChild;
    assert( pPage->isInit );
    if( pPage->nCell==0 ){
      if( pPage->leaf ){
        /* The table is completely empty */
        relinkCellList(pPage);
        TRACE(("BALANCE: empty table %d\n", pPage->pgno));
      }else{
        /* The root page is empty but has one child.  Transfer the
        ** information from that one child into the root page if it 
        ** will fit.  This reduces the depth of the tree by one.
        **
        ** If the root page is page 1, it has less space available than
        ** its child (due to the 100 byte header that occurs at the beginning
        ** of the database fle), so it might not be able to hold all of the 
        ** information currently contained in the child.  If this is the 
        ** case, then do not do the transfer.  Leave page 1 empty except
        ** for the right-pointer to the child page.  The child page becomes
        ** the virtual root of the tree.
        */
        pgnoChild = get4byte(&pPage->aData[pPage->hdrOffset+6]);
        assert( pgnoChild>0 && pgnoChild<=sqlite3pager_pagecount(pBt->pPager) );
        rc = getPage(pBt, pgnoChild, &pChild);
        if( rc ) return rc;
        if( pPage->pgno==1 ){
          rc = initPage(pChild, pPage);
          if( rc ) return rc;
          if( pChild->nFree>=100 ){
            /* The child information will fit on the root page, so do the
            ** copy */
            zeroPage(pPage, pChild->aData[0]);
            for(i=0; i<pChild->nCell; i++){
              szCell[i]  = cellSize(pChild, pChild->aCell[i]);
            }
            assemblePage(pPage, pChild->nCell, pChild->aCell, szCell);
            freePage(pChild);
            TRACE(("BALANCE: child %d transfer to page 1\n", pChild->pgno));
          }else{
            /* The child has more information that will fit on the root.
            ** The tree is already balanced.  Do nothing. */
            TRACE(("BALANCE: child %d will not fit on page 1\n", pChild->pgno));
          }
        }else{
          memcpy(pPage->aData, pChild->aData, pBt->usableSize);
          pPage->isInit = 0;
          pPage->pParent = 0;
          rc = initPage(pPage, 0);
          assert( rc==SQLITE_OK );
          freePage(pChild);
          TRACE(("BALANCE: transfer child %d into root %d\n",
                  pChild->pgno, pPage->pgno));
        }
        reparentChildPages(pPage);
        releasePage(pChild);
      }
      return SQLITE_OK;
    }
    if( !pPage->isOverfull ){
      /* It is OK for the root page to be less than half full.
      */
      relinkCellList(pPage);
      TRACE(("BALANCE: root page %d is low - no changes\n", pPage->pgno));
      return SQLITE_OK;
    }
    /*
    ** If we get to here, it means the root page is overfull.
    ** When this happens, Create a new child page and copy the
    ** contents of the root into the child.  Then make the root
    ** page an empty page with rightChild pointing to the new
    ** child.  Then fall thru to the code below which will cause
    ** the overfull child page to be split.
    */
    rc = allocatePage(pBt, &pChild, &pgnoChild, pPage->pgno);
    if( rc ) return rc;
    assert( sqlite3pager_iswriteable(pChild->aData) );
    movePage(pChild, pPage);
    assert( pChild->aData[0]==pPage->aData[pPage->hdrOffset] );
    pChild->pParent = pPage;
    sqlite3pager_ref(pPage->aData);
    pChild->idxParent = 0;
    pChild->isOverfull = 1;
    zeroPage(pPage, pChild->aData[0] & ~PTF_LEAF);
    put4byte(&pPage->aData[pPage->hdrOffset+6], pChild->pgno);
    pParent = pPage;
    pPage = pChild;
    extraUnref = pChild;
    TRACE(("BALANCE: copy root %d into %d and balance %d\n",
            pParent->pgno, pPage->pgno, pPage->pgno));
  }else{
    TRACE(("BALANCE: begin page %d child of %d\n",
            pPage->pgno, pParent->pgno));
  }
  rc = sqlite3pager_write(pParent->aData);
  if( rc ) return rc;
  assert( pParent->isInit );
  
  /*
  ** Find the cell in the parent page whose left child points back
  ** to pPage.  The "idx" variable is the index of that cell.  If pPage
  ** is the rightmost child of pParent then set idx to pParent->nCell 
  */
  if( pParent->idxShift ){
    Pgno pgno;
    pgno = pPage->pgno;
    assert( pgno==sqlite3pager_pagenumber(pPage->aData) );
    for(idx=0; idx<pParent->nCell; idx++){
      if( get4byte(&pParent->aCell[idx][2])==pgno ){
        break;
      }
    }
    assert( idx<pParent->nCell
             || get4byte(&pParent->aData[pParent->hdrOffset+6])==pgno );
  }else{
    idx = pPage->idxParent;
  }

  /*
  ** Initialize variables so that it will be safe to jump
  ** directly to balance_cleanup at any moment.
  */
  nOld = nNew = 0;
  sqlite3pager_ref(pParent->aData);

  /*
  ** Find sibling pages to pPage and the cells in pParent that divide
  ** the siblings.  An attempt is made to find NN siblings on either
  ** side of pPage.  More siblings are taken from one side, however, if
  ** pPage there are fewer than NN siblings on the other side.  If pParent
  ** has NB or fewer children then all children of pParent are taken.
  */
  nxDiv = idx - NN;
  if( nxDiv + NB > pParent->nCell ){
    nxDiv = pParent->nCell - NB + 1;
  }
  if( nxDiv<0 ){
    nxDiv = 0;
  }
  nDiv = 0;
  for(i=0, k=nxDiv; i<NB; i++, k++){
    if( k<pParent->nCell ){
      idxDiv[i] = k;
      apDiv[i] = pParent->aCell[k];
      nDiv++;
      assert( !pParent->leaf );
      pgnoOld[i] = get4byte(&apDiv[i][2]);
    }else if( k==pParent->nCell ){
      pgnoOld[i] = get4byte(&pParent->aData[pParent->hdrOffset+6]);
    }else{
      break;
    }
    rc = getAndInitPage(pBt, pgnoOld[i], &apOld[i], pParent);
    if( rc ) goto balance_cleanup;
    apOld[i]->idxParent = k;
    apCopy[i] = 0;
    assert( i==nOld );
    nOld++;
  }

  /*
  ** Make copies of the content of pPage and its siblings into aOld[].
  ** The rest of this function will use data from the copies rather
  ** that the original pages since the original pages will be in the
  ** process of being overwritten.
  */
  for(i=0; i<nOld; i++){
    MemPage *p = apCopy[i] = (MemPage*)&aCopy[i+1][-sizeof(MemPage)];
    p->aData = &((u8*)p)[-pBt->usableSize];
    p->aCell = 0;
    p->hdrOffset = 0;
    movePage(p, apOld[i]);
  }

  /*
  ** Load pointers to all cells on sibling pages and the divider cells
  ** into the local apCell[] array.  Make copies of the divider cells
  ** into space obtained form aSpace[] and remove the the divider Cells
  ** from pParent.
  **
  ** If the siblings are on leaf pages, then the child pointers of the
  ** divider cells are stripped from the cells before they are copied
  ** into aSpace[].  In this way, all cells in apCell[] are without
  ** child pointers.  If siblings are not leaves, then all cell in
  ** apCell[] include child pointers.  Either way, all cells in apCell[]
  ** are alike.
  **
  ** leafCorrection:  4 if pPage is a leaf.  0 if pPage is not a leaf.
  **       leafData:  1 if pPage holds key+data and pParent holds only keys.
  */
  nCell = 0;
  leafCorrection = pPage->leaf*4;
  leafData = pPage->leafData && pPage->leaf;
  for(i=0; i<nOld; i++){
    MemPage *pOld = apCopy[i];
    for(j=0; j<pOld->nCell; j++){
      apCell[nCell] = pOld->aCell[j];
      szCell[nCell] = cellSize(pOld, apCell[nCell]);
      nCell++;
    }
    if( i<nOld-1 ){
      int sz = cellSize(pParent, apDiv[i]);
      if( leafData ){
        /* With the LEAFDATA flag, pParent cells hold only INTKEYs that
        ** are duplicates of keys on the child pages.  We need to remove
        ** the divider cells from pParent, but the dividers cells are not
        ** added to apCell[] because they are duplicates of child cells.
        */
        dropCell(pParent, nxDiv, sz);
      }else{
        u8 *pTemp;
        szCell[nCell] = sz;
        pTemp = &aSpace[iSpace];
        iSpace += sz;
        assert( iSpace<=sizeof(aSpace) );
        memcpy(pTemp, apDiv[i], sz);
        apCell[nCell] = pTemp+leafCorrection;
        dropCell(pParent, nxDiv, sz);
        szCell[nCell] -= leafCorrection;
        assert( get4byte(pTemp+2)==pgnoOld[i] );
        if( !pOld->leaf ){
          assert( leafCorrection==0 );
          /* The right pointer of the child page pOld becomes the left
          ** pointer of the divider cell */
          memcpy(&apCell[nCell][2], &pOld->aData[pOld->hdrOffset+6], 4);
        }else{
          assert( leafCorrection==4 );
        }
        nCell++;
      }
    }
  }

  /*
  ** Figure out the number of pages needed to hold all nCell cells.
  ** Store this number in "k".  Also compute szNew[] which is the total
  ** size of all cells on the i-th page and cntNew[] which is the index
  ** in apCell[] of the cell that divides page i from page i+1.  
  ** cntNew[k] should equal nCell.
  **
  ** Values computed by this block:
  **
  **           k: The total number of sibling pages
  **    szNew[i]: Spaced used on the i-th sibling page.
  **   cntNew[i]: Index in apCell[] and szCell[] for the first cell to
  **              the right of the i-th sibling page.
  ** usableSpace: Number of bytes of space available on each sibling.
  ** 
  */
  usableSpace = pBt->usableSize - 10 + leafCorrection;
  for(subtotal=k=i=0; i<nCell; i++){
    subtotal += szCell[i];
    if( subtotal > usableSpace ){
      szNew[k] = subtotal - szCell[i];
      cntNew[k] = i;
      if( leafData ){ i--; }
      subtotal = 0;
      k++;
    }
  }
  szNew[k] = subtotal;
  cntNew[k] = nCell;
  k++;

  /*
  ** The packing computed by the previous block is biased toward the siblings
  ** on the left side.  The left siblings are always nearly full, while the
  ** right-most sibling might be nearly empty.  This block of code attempts
  ** to adjust the packing of siblings to get a better balance.
  **
  ** This adjustment is more than an optimization.  The packing above might
  ** be so out of balance as to be illegal.  For example, the right-most
  ** sibling might be completely empty.  This adjustment is not optional.
  */
  for(i=k-1; i>0; i--){
    int szRight = szNew[i];  /* Size of sibling on the right */
    int szLeft = szNew[i-1]; /* Size of sibling on the left */
    int r;              /* Index of right-most cell in left sibling */
    int d;              /* Index of first cell to the left of right sibling */

    r = cntNew[i-1] - 1;
    d = r + 1 - leafData;
    while( szRight==0 || szRight+szCell[d]<=szLeft-szCell[r] ){
      szRight += szCell[d];
      szLeft -= szCell[r];
      cntNew[i-1]--;
      r = cntNew[i-1] - 1;
      d = r + 1 - leafData;
    }
    szNew[i] = szRight;
    szNew[i-1] = szLeft;
  }
  assert( cntNew[0]>0 );

  /*
  ** Allocate k new pages.  Reuse old pages where possible.
  */
  assert( pPage->pgno>1 );
  pageFlags = pPage->aData[0];
  for(i=0; i<k; i++){
    MemPage *pNew;
    if( i<nOld ){
      pNew = apNew[i] = apOld[i];
      pgnoNew[i] = pgnoOld[i];
      apOld[i] = 0;
      sqlite3pager_write(pNew->aData);
    }else{
      rc = allocatePage(pBt, &pNew, &pgnoNew[i], pgnoNew[i-1]);
      if( rc ) goto balance_cleanup;
      apNew[i] = pNew;
    }
    nNew++;
    zeroPage(pNew, pageFlags);
  }

  /* Free any old pages that were not reused as new pages.
  */
  while( i<nOld ){
    rc = freePage(apOld[i]);
    if( rc ) goto balance_cleanup;
    releasePage(apOld[i]);
    apOld[i] = 0;
    i++;
  }

  /*
  ** Put the new pages in accending order.  This helps to
  ** keep entries in the disk file in order so that a scan
  ** of the table is a linear scan through the file.  That
  ** in turn helps the operating system to deliver pages
  ** from the disk more rapidly.
  **
  ** An O(n^2) insertion sort algorithm is used, but since
  ** n is never more than NB (a small constant), that should
  ** not be a problem.
  **
  ** When NB==3, this one optimization makes the database
  ** about 25% faster for large insertions and deletions.
  */
  for(i=0; i<k-1; i++){
    int minV = pgnoNew[i];
    int minI = i;
    for(j=i+1; j<k; j++){
      if( pgnoNew[j]<(unsigned)minV ){
        minI = j;
        minV = pgnoNew[j];
      }
    }
    if( minI>i ){
      int t;
      MemPage *pT;
      t = pgnoNew[i];
      pT = apNew[i];
      pgnoNew[i] = pgnoNew[minI];
      apNew[i] = apNew[minI];
      pgnoNew[minI] = t;
      apNew[minI] = pT;
    }
  }
  TRACE(("BALANCE: old: %d %d %d  new: %d(%d) %d(%d) %d(%d) %d(%d)\n",
    pgnoOld[0], 
    nOld>=2 ? pgnoOld[1] : 0,
    nOld>=3 ? pgnoOld[2] : 0,
    pgnoNew[0], szNew[0],
    nNew>=2 ? pgnoNew[1] : 0, nNew>=2 ? szNew[1] : 0,
    nNew>=3 ? pgnoNew[2] : 0, nNew>=3 ? szNew[2] : 0,
    nNew>=4 ? pgnoNew[3] : 0, nNew>=4 ? szNew[3] : 0));


  /*
  ** Evenly distribute the data in apCell[] across the new pages.
  ** Insert divider cells into pParent as necessary.
  */
  j = 0;
  for(i=0; i<nNew; i++){
    MemPage *pNew = apNew[i];
    assert( pNew->pgno==pgnoNew[i] );
    resizeCellArray(pNew, cntNew[i] - j);
    assemblePage(pNew, cntNew[i]-j, &apCell[j], &szCell[j]);
    j = cntNew[i];
    assert( pNew->nCell>0 );
    assert( !pNew->isOverfull );
    relinkCellList(pNew);
    if( i<nNew-1 && j<nCell ){
      u8 *pCell;
      u8 *pTemp;
      int sz;
      pCell = apCell[j];
      sz = szCell[j] + leafCorrection;
      if( !pNew->leaf ){
        memcpy(&pNew->aData[6], pCell+2, 4);
        pTemp = 0;
      }else if( leafData ){
        CellInfo info;
        j--;
        parseCell(pNew, apCell[j], &info);
        pCell = &aSpace[iSpace];
        fillInCell(pParent, pCell, 0, info.nKey, 0, 0, &sz);
        iSpace += sz;
        assert( iSpace<=sizeof(aSpace) );
        pTemp = 0;
      }else{
        pCell -= 4;
        pTemp = &aSpace[iSpace];
        iSpace += sz;
        assert( iSpace<=sizeof(aSpace) );
      }
      insertCell(pParent, nxDiv, pCell, sz, pTemp);
      put4byte(&pParent->aCell[nxDiv][2], pNew->pgno);
      j++;
      nxDiv++;
    }
  }
  assert( j==nCell );
  if( (pageFlags & PTF_LEAF)==0 ){
    memcpy(&apNew[nNew-1]->aData[6], &apCopy[nOld-1]->aData[6], 4);
  }
  if( nxDiv==pParent->nCell ){
    /* Right-most sibling is the right-most child of pParent */
    put4byte(&pParent->aData[pParent->hdrOffset+6], pgnoNew[nNew-1]);
  }else{
    /* Right-most sibling is the left child of the first entry in pParent
    ** past the right-most divider entry */
    put4byte(&pParent->aCell[nxDiv][2], pgnoNew[nNew-1]);
  }

  /*
  ** Reparent children of all cells.
  */
  for(i=0; i<nNew; i++){
    reparentChildPages(apNew[i]);
  }
  reparentChildPages(pParent);

  /*
  ** Balance the parent page.  Note that the current page (pPage) might
  ** have been added to the freelist is it might no longer be initialized.
  ** But the parent page will always be initialized.
  */
  assert( pParent->isInit );
  /* assert( pPage->isInit ); // No! pPage might have been added to freelist */
  /* pageIntegrity(pPage);    // No! pPage might have been added to freelist */ 
  rc = balance(pParent);
  
  /*
  ** Cleanup before returning.
  */
balance_cleanup:
  for(i=0; i<nOld; i++){
    releasePage(apOld[i]);
    if( apCopy[i] ){
      sqliteFree(apCopy[i]->aCell);
    }
  }
  for(i=0; i<nNew; i++){
    releasePage(apNew[i]);
  }
  releasePage(pParent);
  releasePage(extraUnref);
  TRACE(("BALANCE: finished with %d: old=%d new=%d cells=%d\n",
          pPage->pgno, nOld, nNew, nCell));
  return rc;
}

/*
** This routine checks all cursors that point to the same table
** as pCur points to.  If any of those cursors were opened with
** wrFlag==0 then this routine returns SQLITE_LOCKED.  If all
** cursors point to the same table were opened with wrFlag==1
** then this routine returns SQLITE_OK.
**
** In addition to checking for read-locks (where a read-lock 
** means a cursor opened with wrFlag==0) this routine also moves
** all cursors other than pCur so that they are pointing to the 
** first Cell on root page.  This is necessary because an insert 
** or delete might change the number of cells on a page or delete
** a page entirely and we do not want to leave any cursors 
** pointing to non-existant pages or cells.
*/
static int checkReadLocks(BtCursor *pCur){
  BtCursor *p;
  assert( pCur->wrFlag );
  for(p=pCur->pShared; p!=pCur; p=p->pShared){
    assert( p );
    assert( p->pgnoRoot==pCur->pgnoRoot );
    assert( p->pPage->pgno==sqlite3pager_pagenumber(p->pPage->aData) );
    if( p->wrFlag==0 ) return SQLITE_LOCKED;
    if( p->pPage->pgno!=p->pgnoRoot ){
      moveToRoot(p);
    }
  }
  return SQLITE_OK;
}

/*
** Insert a new record into the BTree.  The key is given by (pKey,nKey)
** and the data is given by (pData,nData).  The cursor is used only to
** define what table the record should be inserted into.  The cursor
** is left pointing at a random location.
**
** For an INTKEY table, only the nKey value of the key is used.  pKey is
** ignored.  For a ZERODATA table, the pData and nData are both ignored.
*/
int sqlite3BtreeInsert(
  BtCursor *pCur,                /* Insert data into the table of this cursor */
  const void *pKey, i64 nKey,    /* The key of the new record */
  const void *pData, int nData   /* The data of the new record */
){
  int rc;
  int loc;
  int szNew;
  MemPage *pPage;
  Btree *pBt = pCur->pBt;
  unsigned char *oldCell;
  unsigned char newCell[MX_CELL_SIZE];

  if( pCur->status ){
    return pCur->status;  /* A rollback destroyed this cursor */
  }
  if( !pBt->inTrans ){
    /* Must start a transaction before doing an insert */
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  assert( !pBt->readOnly );
  if( !pCur->wrFlag ){
    return SQLITE_PERM;   /* Cursor not open for writing */
  }
  if( checkReadLocks(pCur) ){
    return SQLITE_LOCKED; /* The table pCur points to has a read lock */
  }
  rc = sqlite3BtreeMoveto(pCur, pKey, nKey, &loc);
  if( rc ) return rc;
  pPage = pCur->pPage;
  assert( pPage->intKey || nKey>=0 );
  assert( pPage->leaf || !pPage->leafData );
  TRACE(("INSERT: table=%d nkey=%lld ndata=%d page=%d %s\n",
          pCur->pgnoRoot, nKey, nData, pPage->pgno,
          loc==0 ? "overwrite" : "new entry"));
  assert( pPage->isInit );
  rc = sqlite3pager_write(pPage->aData);
  if( rc ) return rc;
  rc = fillInCell(pPage, newCell, pKey, nKey, pData, nData, &szNew);
  if( rc ) return rc;
  assert( szNew==cellSize(pPage, newCell) );
  assert( szNew<=sizeof(newCell) );
  if( loc==0 && pCur->isValid ){
    int szOld;
    assert( pCur->idx>=0 && pCur->idx<pPage->nCell );
    oldCell = pPage->aCell[pCur->idx];
    if( !pPage->leaf ){
      memcpy(&newCell[2], &oldCell[2], 4);
    }
    szOld = cellSize(pPage, oldCell);
    rc = clearCell(pPage, oldCell);
    if( rc ) return rc;
    dropCell(pPage, pCur->idx, szOld);
  }else if( loc<0 && pPage->nCell>0 ){
    assert( pPage->leaf );
    pCur->idx++;
    pCur->infoValid = 0;
  }else{
    assert( pPage->leaf );
  }
  insertCell(pPage, pCur->idx, newCell, szNew, 0);
  rc = balance(pPage);
  /* sqlite3BtreePageDump(pCur->pBt, pCur->pgnoRoot, 1); */
  /* fflush(stdout); */
  moveToRoot(pCur);
  return rc;
}

/*
** Delete the entry that the cursor is pointing to.  The cursor
** is left pointing at a random location.
*/
int sqlite3BtreeDelete(BtCursor *pCur){
  MemPage *pPage = pCur->pPage;
  unsigned char *pCell;
  int rc;
  Pgno pgnoChild;
  Btree *pBt = pCur->pBt;

  assert( pPage->isInit );
  if( pCur->status ){
    return pCur->status;  /* A rollback destroyed this cursor */
  }
  if( !pBt->inTrans ){
    /* Must start a transaction before doing a delete */
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  assert( !pBt->readOnly );
  if( pCur->idx >= pPage->nCell ){
    return SQLITE_ERROR;  /* The cursor is not pointing to anything */
  }
  if( !pCur->wrFlag ){
    return SQLITE_PERM;   /* Did not open this cursor for writing */
  }
  if( checkReadLocks(pCur) ){
    return SQLITE_LOCKED; /* The table pCur points to has a read lock */
  }
  rc = sqlite3pager_write(pPage->aData);
  if( rc ) return rc;
  pCell = pPage->aCell[pCur->idx];
  if( !pPage->leaf ){
    pgnoChild = get4byte(&pCell[2]);
  }
  clearCell(pPage, pCell);
  if( !pPage->leaf ){
    /*
    ** The entry we are about to delete is not a leaf so if we do not
    ** do something we will leave a hole on an internal page.
    ** We have to fill the hole by moving in a cell from a leaf.  The
    ** next Cell after the one to be deleted is guaranteed to exist and
    ** to be a leaf so we can use it.
    */
    BtCursor leafCur;
    unsigned char *pNext;
    int szNext;
    int notUsed;
    unsigned char tempCell[MX_CELL_SIZE];
    assert( !pPage->leafData );
    getTempCursor(pCur, &leafCur);
    rc = sqlite3BtreeNext(&leafCur, &notUsed);
    if( rc!=SQLITE_OK ){
      if( rc!=SQLITE_NOMEM ) rc = SQLITE_CORRUPT;
      return rc;
    }
    rc = sqlite3pager_write(leafCur.pPage->aData);
    if( rc ) return rc;
    TRACE(("DELETE: table=%d delete internal from %d replace from leaf %d\n",
       pCur->pgnoRoot, pPage->pgno, leafCur.pPage->pgno));
    dropCell(pPage, pCur->idx, cellSize(pPage, pCell));
    pNext = leafCur.pPage->aCell[leafCur.idx];
    szNext = cellSize(leafCur.pPage, pNext);
    assert( sizeof(tempCell)>=szNext+4 );
    insertCell(pPage, pCur->idx, pNext-4, szNext+4, tempCell);
    put4byte(pPage->aCell[pCur->idx]+2, pgnoChild);
    rc = balance(pPage);
    if( rc ) return rc;
    dropCell(leafCur.pPage, leafCur.idx, szNext);
    rc = balance(leafCur.pPage);
    releaseTempCursor(&leafCur);
  }else{
    TRACE(("DELETE: table=%d delete from leaf %d\n",
       pCur->pgnoRoot, pPage->pgno));
    dropCell(pPage, pCur->idx, cellSize(pPage, pCell));
    rc = balance(pPage);
  }
  moveToRoot(pCur);
  return rc;
}

/*
** Create a new BTree table.  Write into *piTable the page
** number for the root page of the new table.
**
** In the current implementation, BTree tables and BTree indices are the 
** the same.  In the future, we may change this so that BTree tables
** are restricted to having a 4-byte integer key and arbitrary data and
** BTree indices are restricted to having an arbitrary key and no data.
** But for now, this routine also serves to create indices.
*/
int sqlite3BtreeCreateTable(Btree *pBt, int *piTable, int flags){
  MemPage *pRoot;
  Pgno pgnoRoot;
  int rc;
  if( !pBt->inTrans ){
    /* Must start a transaction first */
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  if( pBt->readOnly ){
    return SQLITE_READONLY;
  }
  rc = allocatePage(pBt, &pRoot, &pgnoRoot, 1);
  if( rc ) return rc;
  assert( sqlite3pager_iswriteable(pRoot->aData) );
  zeroPage(pRoot, flags | PTF_LEAF);
  sqlite3pager_unref(pRoot->aData);
  *piTable = (int)pgnoRoot;
  return SQLITE_OK;
}

/*
** Erase the given database page and all its children.  Return
** the page to the freelist.
*/
static int clearDatabasePage(
  Btree *pBt,           /* The BTree that contains the table */
  Pgno pgno,            /* Page number to clear */
  MemPage *pParent,     /* Parent page.  NULL for the root */
  int freePageFlag      /* Deallocate page if true */
){
  MemPage *pPage;
  int rc;
  unsigned char *pCell;
  int i;

  rc = getAndInitPage(pBt, pgno, &pPage, pParent);
  if( rc ) return rc;
  rc = sqlite3pager_write(pPage->aData);
  if( rc ) return rc;
  for(i=0; i<pPage->nCell; i++){
    pCell = pPage->aCell[i];
    if( !pPage->leaf ){
      rc = clearDatabasePage(pBt, get4byte(&pCell[2]), pPage->pParent, 1);
      if( rc ) return rc;
    }
    rc = clearCell(pPage, pCell);
    if( rc ) return rc;
  }
  if( !pPage->leaf ){
    rc = clearDatabasePage(pBt, get4byte(&pPage->aData[6]), pPage->pParent, 1);
    if( rc ) return rc;
  }
  if( freePageFlag ){
    rc = freePage(pPage);
  }else{
    zeroPage(pPage, pPage->aData[0] | PTF_LEAF);
  }
  releasePage(pPage);
  return rc;
}

/*
** Delete all information from a single table in the database.
*/
int sqlite3BtreeClearTable(Btree *pBt, int iTable){
  int rc;
  BtCursor *pCur;
  if( !pBt->inTrans ){
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    if( pCur->pgnoRoot==(Pgno)iTable ){
      if( pCur->wrFlag==0 ) return SQLITE_LOCKED;
      moveToRoot(pCur);
    }
  }
  rc = clearDatabasePage(pBt, (Pgno)iTable, 0, 0);
  if( rc ){
    sqlite3BtreeRollback(pBt);
  }
  return rc;
}

/*
** Erase all information in a table and add the root of the table to
** the freelist.  Except, the root of the principle table (the one on
** page 2) is never added to the freelist.
*/
int sqlite3BtreeDropTable(Btree *pBt, int iTable){
  int rc;
  MemPage *pPage;
  BtCursor *pCur;
  if( !pBt->inTrans ){
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    if( pCur->pgnoRoot==(Pgno)iTable ){
      return SQLITE_LOCKED;  /* Cannot drop a table that has a cursor */
    }
  }
  rc = getPage(pBt, (Pgno)iTable, &pPage);
  if( rc ) return rc;
  rc = sqlite3BtreeClearTable(pBt, iTable);
  if( rc ) return rc;
  if( iTable>1 ){
    rc = freePage(pPage);
  }else{
    zeroPage(pPage, PTF_INTKEY|PTF_LEAF );
  }
  releasePage(pPage);
  return rc;  
}


/*
** Read the meta-information out of a database file.  Meta[0]
** is the number of free pages currently in the database.  Meta[1]
** through meta[15] are available for use by higher layers.  Meta[0]
** is read-only, the others are read/write.
** 
** The schema layer numbers meta values differently.  At the schema
** layer (and the SetCookie and ReadCookie opcodes) the number of
** free pages is not visible.  So Cookie[0] is the same as Meta[1].
*/
int sqlite3BtreeGetMeta(Btree *pBt, int idx, u32 *pMeta){
  int rc;
  unsigned char *pP1;

  assert( idx>=0 && idx<=15 );
  rc = sqlite3pager_get(pBt->pPager, 1, (void**)&pP1);
  if( rc ) return rc;
  *pMeta = get4byte(&pP1[36 + idx*4]);
  sqlite3pager_unref(pP1);
  return SQLITE_OK;
}

/*
** Write meta-information back into the database.  Meta[0] is
** read-only and may not be written.
*/
int sqlite3BtreeUpdateMeta(Btree *pBt, int idx, u32 iMeta){
  unsigned char *pP1;
  int rc;
  assert( idx>=1 && idx<=15 );
  if( !pBt->inTrans ){
    return pBt->readOnly ? SQLITE_READONLY : SQLITE_ERROR;
  }
  assert( pBt->pPage1!=0 );
  pP1 = pBt->pPage1->aData;
  rc = sqlite3pager_write(pP1);
  if( rc ) return rc;
  put4byte(&pP1[36 + idx*4], iMeta);
  return SQLITE_OK;
}

/*
** Return the flag byte at the beginning of the page that the cursor
** is currently pointing to.
*/
int sqlite3BtreeFlags(BtCursor *pCur){
  MemPage *pPage = pCur->pPage;
  return pPage ? pPage->aData[pPage->hdrOffset] : 0;
}

/******************************************************************************
** The complete implementation of the BTree subsystem is above this line.
** All the code the follows is for testing and troubleshooting the BTree
** subsystem.  None of the code that follows is used during normal operation.
******************************************************************************/

/*
** Print a disassembly of the given page on standard output.  This routine
** is used for debugging and testing only.
*/
#ifdef SQLITE_TEST
int sqlite3BtreePageDump(Btree *pBt, int pgno, int recursive){
  int rc;
  MemPage *pPage;
  int i, j, c;
  int nFree;
  u16 idx;
  int hdr;
  unsigned char *data;
  char range[20];
  unsigned char payload[20];

  rc = getPage(pBt, (Pgno)pgno, &pPage);
  if( rc ){
    return rc;
  }
  hdr = pPage->hdrOffset;
  data = pPage->aData;
  c = data[hdr];
  pPage->intKey = (c & (PTF_INTKEY|PTF_LEAFDATA))!=0;
  pPage->zeroData = (c & PTF_ZERODATA)!=0;
  pPage->leafData = (c & PTF_LEAFDATA)!=0;
  pPage->leaf = (c & PTF_LEAF)!=0;
  pPage->hasData = !(pPage->zeroData || (!pPage->leaf && pPage->leafData));
  printf("PAGE %d:  flags=0x%02x  frag=%d   parent=%d\n", pgno,
    data[hdr], data[hdr+5], 
    (pPage->isInit && pPage->pParent) ? pPage->pParent->pgno : 0);
  i = 0;
  assert( hdr == (pgno==1 ? 100 : 0) );
  idx = get2byte(&data[hdr+3]);
  while( idx>0 && idx<=pBt->usableSize ){
    CellInfo info;
    Pgno child;
    unsigned char *pCell = &data[idx];
    int sz;

    pCell = &data[idx];
    parseCell(pPage, pCell, &info);
    sz = info.nSize;
    sprintf(range,"%d..%d", idx, idx+sz-1);
    if( pPage->leaf ){
      child = 0;
    }else{
      child = get4byte(&pCell[2]);
    }
    sz = info.nData;
    if( !pPage->intKey ) sz += info.nKey;
    if( sz>sizeof(payload)-1 ) sz = sizeof(payload)-1;
    memcpy(payload, &pCell[info.nHeader], sz);
    for(j=0; j<sz; j++){
      if( payload[j]<0x20 || payload[j]>0x7f ) payload[j] = '.';
    }
    payload[sz] = 0;
    printf(
      "cell %2d: i=%-10s chld=%-4d nk=%-4lld nd=%-4d payload=%s\n",
      i, range, child, info.nKey, info.nData, payload
    );
    if( pPage->isInit && pPage->aCell[i]!=pCell ){
      printf("**** aCell[%d] does not match on prior entry ****\n", i);
    }
    i++;
    idx = get2byte(pCell);
  }
  if( idx!=0 ){
    printf("ERROR: next cell index out of range: %d\n", idx);
  }
  if( !pPage->leaf ){
    printf("right_child: %d\n", get4byte(&data[hdr+6]));
  }
  nFree = 0;
  i = 0;
  idx = get2byte(&data[hdr+1]);
  while( idx>0 && idx<pPage->pBt->usableSize ){
    int sz = get2byte(&data[idx+2]);
    sprintf(range,"%d..%d", idx, idx+sz-1);
    nFree += sz;
    printf("freeblock %2d: i=%-10s size=%-4d total=%d\n",
       i, range, sz, nFree);
    idx = get2byte(&data[idx]);
    i++;
  }
  if( idx!=0 ){
    printf("ERROR: next freeblock index out of range: %d\n", idx);
  }
  if( recursive && !pPage->leaf ){
    idx = get2byte(&data[hdr+3]);
    while( idx>0 && idx<pBt->usableSize ){
      unsigned char *pCell = &data[idx];
      sqlite3BtreePageDump(pBt, get4byte(&pCell[2]), 1);
      idx = get2byte(pCell);
    }
    sqlite3BtreePageDump(pBt, get4byte(&data[hdr+6]), 1);
  }
  sqlite3pager_unref(data);
  fflush(stdout);
  return SQLITE_OK;
}
#endif

#ifdef SQLITE_TEST
/*
** Fill aResult[] with information about the entry and page that the
** cursor is pointing to.
** 
**   aResult[0] =  The page number
**   aResult[1] =  The entry number
**   aResult[2] =  Total number of entries on this page
**   aResult[3] =  Size of this entry
**   aResult[4] =  Number of free bytes on this page
**   aResult[5] =  Number of free blocks on the page
**   aResult[6] =  Page number of the left child of this entry
**   aResult[7] =  Page number of the right child for the whole page
**
** This routine is used for testing and debugging only.
*/
int sqlite3BtreeCursorInfo(BtCursor *pCur, int *aResult){
  int cnt, idx;
  MemPage *pPage = pCur->pPage;

  pageIntegrity(pPage);
  assert( pPage->isInit );
  aResult[0] = sqlite3pager_pagenumber(pPage->aData);
  assert( aResult[0]==pPage->pgno );
  aResult[1] = pCur->idx;
  aResult[2] = pPage->nCell;
  if( pCur->idx>=0 && pCur->idx<pPage->nCell ){
    aResult[3] = cellSize(pPage, pPage->aCell[pCur->idx]);
    aResult[6] = pPage->leaf ? 0 : get4byte(&pPage->aCell[pCur->idx][2]);
  }else{
    aResult[3] = 0;
    aResult[6] = 0;
  }
  aResult[4] = pPage->nFree;
  cnt = 0;
  idx = get2byte(&pPage->aData[pPage->hdrOffset+1]);
  while( idx>0 && idx<pPage->pBt->usableSize ){
    cnt++;
    idx = get2byte(&pPage->aData[idx]);
  }
  aResult[5] = cnt;
  aResult[7] = pPage->leaf ? 0 : get4byte(&pPage->aData[pPage->hdrOffset+6]);
  return SQLITE_OK;
}
#endif

/*
** Return the pager associated with a BTree.  This routine is used for
** testing and debugging only.
*/
Pager *sqlite3BtreePager(Btree *pBt){
  return pBt->pPager;
}

/*
** This structure is passed around through all the sanity checking routines
** in order to keep track of some global state information.
*/
typedef struct IntegrityCk IntegrityCk;
struct IntegrityCk {
  Btree *pBt;    /* The tree being checked out */
  Pager *pPager; /* The associated pager.  Also accessible by pBt->pPager */
  int nPage;     /* Number of pages in the database */
  int *anRef;    /* Number of times each page is referenced */
  char *zErrMsg; /* An error message.  NULL of no errors seen. */
};

/*
** Append a message to the error message string.
*/
static void checkAppendMsg(IntegrityCk *pCheck, char *zMsg1, char *zMsg2){
  if( pCheck->zErrMsg ){
    char *zOld = pCheck->zErrMsg;
    pCheck->zErrMsg = 0;
    sqlite3SetString(&pCheck->zErrMsg, zOld, "\n", zMsg1, zMsg2, (char*)0);
    sqliteFree(zOld);
  }else{
    sqlite3SetString(&pCheck->zErrMsg, zMsg1, zMsg2, (char*)0);
  }
}

/*
** Add 1 to the reference count for page iPage.  If this is the second
** reference to the page, add an error message to pCheck->zErrMsg.
** Return 1 if there are 2 ore more references to the page and 0 if
** if this is the first reference to the page.
**
** Also check that the page number is in bounds.
*/
static int checkRef(IntegrityCk *pCheck, int iPage, char *zContext){
  if( iPage==0 ) return 1;
  if( iPage>pCheck->nPage || iPage<0 ){
    char zBuf[100];
    sprintf(zBuf, "invalid page number %d", iPage);
    checkAppendMsg(pCheck, zContext, zBuf);
    return 1;
  }
  if( pCheck->anRef[iPage]==1 ){
    char zBuf[100];
    sprintf(zBuf, "2nd reference to page %d", iPage);
    checkAppendMsg(pCheck, zContext, zBuf);
    return 1;
  }
  return  (pCheck->anRef[iPage]++)>1;
}

/*
** Check the integrity of the freelist or of an overflow page list.
** Verify that the number of pages on the list is N.
*/
static void checkList(
  IntegrityCk *pCheck,  /* Integrity checking context */
  int isFreeList,       /* True for a freelist.  False for overflow page list */
  int iPage,            /* Page number for first page in the list */
  int N,                /* Expected number of pages in the list */
  char *zContext        /* Context for error messages */
){
  int i;
  int expected = N;
  int iFirst = iPage;
  char zMsg[100];
  while( N-- > 0 ){
    unsigned char *pOvfl;
    if( iPage<1 ){
      sprintf(zMsg, "%d of %d pages missing from overflow list starting at %d",
          N+1, expected, iFirst);
      checkAppendMsg(pCheck, zContext, zMsg);
      break;
    }
    if( checkRef(pCheck, iPage, zContext) ) break;
    if( sqlite3pager_get(pCheck->pPager, (Pgno)iPage, (void**)&pOvfl) ){
      sprintf(zMsg, "failed to get page %d", iPage);
      checkAppendMsg(pCheck, zContext, zMsg);
      break;
    }
    if( isFreeList ){
      int n = get4byte(&pOvfl[4]);
      for(i=0; i<n; i++){
        checkRef(pCheck, get4byte(&pOvfl[8+i*4]), zContext);
      }
      N -= n;
    }
    iPage = get4byte(pOvfl);
    sqlite3pager_unref(pOvfl);
  }
}

/*
** Do various sanity checks on a single page of a tree.  Return
** the tree depth.  Root pages return 0.  Parents of root pages
** return 1, and so forth.
** 
** These checks are done:
**
**      1.  Make sure that cells and freeblocks do not overlap
**          but combine to completely cover the page.
**  NO  2.  Make sure cell keys are in order.
**  NO  3.  Make sure no key is less than or equal to zLowerBound.
**  NO  4.  Make sure no key is greater than or equal to zUpperBound.
**      5.  Check the integrity of overflow pages.
**      6.  Recursively call checkTreePage on all children.
**      7.  Verify that the depth of all children is the same.
**      8.  Make sure this page is at least 33% full or else it is
**          the root of the tree.
*/
static int checkTreePage(
  IntegrityCk *pCheck,  /* Context for the sanity check */
  int iPage,            /* Page number of the page to check */
  MemPage *pParent,     /* Parent page */
  char *zParentContext, /* Parent context */
  char *zLowerBound,    /* All keys should be greater than this, if not NULL */
  int nLower,           /* Number of characters in zLowerBound */
  char *zUpperBound,    /* All keys should be less than this, if not NULL */
  int nUpper            /* Number of characters in zUpperBound */
){
  MemPage *pPage;
  int i, rc, depth, d2, pgno, cnt;
  int hdr;
  u8 *data;
  BtCursor cur;
  Btree *pBt;
  int maxLocal, usableSize;
  char zMsg[100];
  char zContext[100];
  char hit[MX_PAGE_SIZE];

  /* Check that the page exists
  */
  cur.pBt = pBt = pCheck->pBt;
  usableSize = pBt->usableSize;
  if( iPage==0 ) return 0;
  if( checkRef(pCheck, iPage, zParentContext) ) return 0;
  if( (rc = getPage(pBt, (Pgno)iPage, &pPage))!=0 ){
    sprintf(zMsg, "unable to get the page. error code=%d", rc);
    checkAppendMsg(pCheck, zContext, zMsg);
    return 0;
  }
  maxLocal = pPage->leafData ? pBt->maxLeaf : pBt->maxLocal;
  if( (rc = initPage(pPage, pParent))!=0 ){
    sprintf(zMsg, "initPage() returns error code %d", rc);
    checkAppendMsg(pCheck, zContext, zMsg);
    releasePage(pPage);
    return 0;
  }

  /* Check out all the cells.
  */
  depth = 0;
  cur.pPage = pPage;
  for(i=0; i<pPage->nCell; i++){
    u8 *pCell;
    int sz;
    CellInfo info;

    /* Check payload overflow pages
    */
    sprintf(zContext, "On tree page %d cell %d: ", iPage, i);
    pCell = pPage->aCell[i];
    parseCell(pPage, pCell, &info);
    sz = info.nData;
    if( !pPage->intKey ) sz += info.nKey;
    if( sz>info.nLocal ){
      int nPage = (sz - info.nLocal + usableSize - 5)/(usableSize - 4);
      checkList(pCheck, 0, get4byte(&pCell[info.iOverflow]),nPage,zContext);
    }

    /* Check sanity of left child page.
    */
    if( !pPage->leaf ){
      pgno = get4byte(&pCell[2]);
      d2 = checkTreePage(pCheck,pgno,pPage,zContext,0,0,0,0);
      if( i>0 && d2!=depth ){
        checkAppendMsg(pCheck, zContext, "Child page depth differs");
      }
      depth = d2;
    }
  }
  if( !pPage->leaf ){
    pgno = get4byte(&pPage->aData[pPage->hdrOffset+6]);
    sprintf(zContext, "On page %d at right child: ", iPage);
    checkTreePage(pCheck, pgno, pPage, zContext,0,0,0,0);
  }
 
  /* Check for complete coverage of the page
  */
  memset(hit, 0, usableSize);
  memset(hit, 1, pPage->hdrOffset+10-4*(pPage->leaf));
  data = pPage->aData;
  hdr = pPage->hdrOffset;
  for(cnt=0, i=get2byte(&data[hdr+3]); i>0 && i<usableSize && cnt<10000; cnt++){
    int size = cellSize(pPage, &data[i]);
    int j;
    for(j=i+size-1; j>=i; j--) hit[j]++;
    i = get2byte(&data[i]);
  }
  for(cnt=0, i=get2byte(&data[hdr+1]); i>0 && i<usableSize && cnt<10000; cnt++){
    int size = get2byte(&data[i+2]);
    int j;
    for(j=i+size-1; j>=i; j--) hit[j]++;
    i = get2byte(&data[i]);
  }
  for(i=cnt=0; i<usableSize; i++){
    if( hit[i]==0 ){
      cnt++;
    }else if( hit[i]>1 ){
      sprintf(zMsg, "Multiple uses for byte %d of page %d", i, iPage);
      checkAppendMsg(pCheck, zMsg, 0);
      break;
    }
  }
  if( cnt!=data[hdr+5] ){
    sprintf(zMsg, "Fragmented space is %d byte reported as %d on page %d",
        cnt, data[hdr+5], iPage);
    checkAppendMsg(pCheck, zMsg, 0);
  }

  releasePage(pPage);
  return depth+1;
}

/*
** This routine does a complete check of the given BTree file.  aRoot[] is
** an array of pages numbers were each page number is the root page of
** a table.  nRoot is the number of entries in aRoot.
**
** If everything checks out, this routine returns NULL.  If something is
** amiss, an error message is written into memory obtained from malloc()
** and a pointer to that error message is returned.  The calling function
** is responsible for freeing the error message when it is done.
*/
char *sqlite3BtreeIntegrityCheck(Btree *pBt, int *aRoot, int nRoot){
  int i;
  int nRef;
  IntegrityCk sCheck;

  nRef = *sqlite3pager_stats(pBt->pPager);
  if( lockBtree(pBt)!=SQLITE_OK ){
    return sqliteStrDup("Unable to acquire a read lock on the database");
  }
  sCheck.pBt = pBt;
  sCheck.pPager = pBt->pPager;
  sCheck.nPage = sqlite3pager_pagecount(sCheck.pPager);
  if( sCheck.nPage==0 ){
    unlockBtreeIfUnused(pBt);
    return 0;
  }
  sCheck.anRef = sqliteMallocRaw( (sCheck.nPage+1)*sizeof(sCheck.anRef[0]) );
  for(i=0; i<=sCheck.nPage; i++){ sCheck.anRef[i] = 0; }
  sCheck.zErrMsg = 0;

  /* Check the integrity of the freelist
  */
  checkList(&sCheck, 1, get4byte(&pBt->pPage1->aData[32]),
            get4byte(&pBt->pPage1->aData[36]), "Main freelist: ");

  /* Check all the tables.
  */
  for(i=0; i<nRoot; i++){
    if( aRoot[i]==0 ) continue;
    checkTreePage(&sCheck, aRoot[i], 0, "List of tree roots: ", 0,0,0,0);
  }

  /* Make sure every page in the file is referenced
  */
  for(i=1; i<=sCheck.nPage; i++){
    if( sCheck.anRef[i]==0 ){
      char zBuf[100];
      sprintf(zBuf, "Page %d is never used", i);
      checkAppendMsg(&sCheck, zBuf, 0);
    }
  }

  /* Make sure this analysis did not leave any unref() pages
  */
  unlockBtreeIfUnused(pBt);
  if( nRef != *sqlite3pager_stats(pBt->pPager) ){
    char zBuf[100];
    sprintf(zBuf, 
      "Outstanding page count goes from %d to %d during this analysis",
      nRef, *sqlite3pager_stats(pBt->pPager)
    );
    checkAppendMsg(&sCheck, zBuf, 0);
  }

  /* Clean  up and report errors.
  */
  sqliteFree(sCheck.anRef);
  return sCheck.zErrMsg;
}

/*
** Return the full pathname of the underlying database file.
*/
const char *sqlite3BtreeGetFilename(Btree *pBt){
  assert( pBt->pPager!=0 );
  return sqlite3pager_filename(pBt->pPager);
}

/*
** Copy the complete content of pBtFrom into pBtTo.  A transaction
** must be active for both files.
**
** The size of file pBtFrom may be reduced by this operation.
** If anything goes wrong, the transaction on pBtFrom is rolled back.
*/
int sqlite3BtreeCopyFile(Btree *pBtTo, Btree *pBtFrom){
  int rc = SQLITE_OK;
  Pgno i, nPage, nToPage;

  if( !pBtTo->inTrans || !pBtFrom->inTrans ) return SQLITE_ERROR;
  if( pBtTo->pCursor ) return SQLITE_BUSY;
  memcpy(pBtTo->pPage1, pBtFrom->pPage1, pBtFrom->usableSize);
  rc = sqlite3pager_overwrite(pBtTo->pPager, 1, pBtFrom->pPage1);
  nToPage = sqlite3pager_pagecount(pBtTo->pPager);
  nPage = sqlite3pager_pagecount(pBtFrom->pPager);
  for(i=2; rc==SQLITE_OK && i<=nPage; i++){
    void *pPage;
    rc = sqlite3pager_get(pBtFrom->pPager, i, &pPage);
    if( rc ) break;
    rc = sqlite3pager_overwrite(pBtTo->pPager, i, pPage);
    if( rc ) break;
    sqlite3pager_unref(pPage);
  }
  for(i=nPage+1; rc==SQLITE_OK && i<=nToPage; i++){
    void *pPage;
    rc = sqlite3pager_get(pBtTo->pPager, i, &pPage);
    if( rc ) break;
    rc = sqlite3pager_write(pPage);
    sqlite3pager_unref(pPage);
    sqlite3pager_dont_write(pBtTo->pPager, i);
  }
  if( !rc && nPage<nToPage ){
    rc = sqlite3pager_truncate(pBtTo->pPager, nPage);
  }
  if( rc ){
    sqlite3BtreeRollback(pBtTo);
  }
  return rc;  
}
