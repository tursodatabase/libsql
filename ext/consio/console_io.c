/*
** 2023 November 4
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
********************************************************************************
** This file implements various interfaces used for console and stream I/O
** by the SQLite project command-line tools, as explained in console_io.h .
** Functions prefixed by "SQLITE_INTERNAL_LINKAGE" behave as described there.
*/

#ifndef SQLITE_CDECL
# define SQLITE_CDECL
#endif

#ifndef SHELL_NO_SYSINC
# include <stdarg.h>
# include <string.h>
# include <stdlib.h>
# include <limits.h>
# include <assert.h>
# include "console_io.h"
# include "sqlite3.h"
#endif

#ifndef SQLITE_CIO_NO_TRANSLATE
# if (defined(_WIN32) || defined(WIN32)) && !SQLITE_OS_WINRT
#  ifndef SHELL_NO_SYSINC
#   include <io.h>
#   include <fcntl.h>
#   undef WIN32_LEAN_AND_MEAN
#   define WIN32_LEAN_AND_MEAN
#   include <windows.h>
#  endif
#  define CIO_WIN_WC_XLATE 1 /* Use WCHAR Windows APIs for console I/O */
# else
#  ifndef SHELL_NO_SYSINC
#   include <unistd.h>
#  endif
#  define CIO_WIN_WC_XLATE 0 /* Use plain C library stream I/O at console */
# endif
#else
# define CIO_WIN_WC_XLATE 0 /* Not exposing translation routines at all */
#endif

#if CIO_WIN_WC_XLATE
/* Character used to represent a known-incomplete UTF-8 char group (�) */
static WCHAR cBadGroup = 0xfffd;
#endif

#if CIO_WIN_WC_XLATE
static HANDLE handleOfFile(FILE *pf){
  int fileDesc = _fileno(pf);
  union { intptr_t osfh; HANDLE fh; } fid = {
    (fileDesc>=0)? _get_osfhandle(fileDesc) : (intptr_t)INVALID_HANDLE_VALUE
  };
  return fid.fh;
}
#endif

#ifndef SQLITE_CIO_NO_TRANSLATE
typedef struct PerStreamTags {
# if CIO_WIN_WC_XLATE
  HANDLE hx;
  DWORD consMode;
  char acIncomplete[4];
# else
  short reachesConsole;
# endif
  FILE *pf;
} PerStreamTags;

/* Define NULL-like value for things which can validly be 0. */
# define SHELL_INVALID_FILE_PTR ((FILE *)~0)
# if CIO_WIN_WC_XLATE
#  define SHELL_INVALID_CONS_MODE 0xFFFF0000
# endif

# if CIO_WIN_WC_XLATE
#  define PST_INITIALIZER { INVALID_HANDLE_VALUE, SHELL_INVALID_CONS_MODE, \
      {0,0,0,0}, SHELL_INVALID_FILE_PTR }
# else
#  define PST_INITIALIZER { 0, SHELL_INVALID_FILE_PTR }
# endif

/* Quickly say whether a known output is going to the console. */
# if CIO_WIN_WC_XLATE
static short pstReachesConsole(PerStreamTags *ppst){
  return (ppst->hx != INVALID_HANDLE_VALUE);
}
# else
#  define pstReachesConsole(ppst) 0
# endif

# if CIO_WIN_WC_XLATE
static void restoreConsoleArb(PerStreamTags *ppst){
  if( pstReachesConsole(ppst) ) SetConsoleMode(ppst->hx, ppst->consMode);
}
# else
#  define restoreConsoleArb(ppst)
# endif

/* Say whether FILE* appears to be a console, collect associated info. */
static short streamOfConsole(FILE *pf, /* out */ PerStreamTags *ppst){
# if CIO_WIN_WC_XLATE
  short rv = 0;
  DWORD dwCM = SHELL_INVALID_CONS_MODE;
  HANDLE fh = handleOfFile(pf);
  ppst->pf = pf;
  if( INVALID_HANDLE_VALUE != fh ){
    rv = (GetFileType(fh) == FILE_TYPE_CHAR && GetConsoleMode(fh,&dwCM));
  }
  ppst->hx = (rv)? fh : INVALID_HANDLE_VALUE;
  ppst->consMode = dwCM;
  return rv;
# else
  ppst->pf = pf;
  ppst->reachesConsole = ( (short)isatty(fileno(pf)) );
  return ppst->reachesConsole;
# endif
}

# if CIO_WIN_WC_XLATE
/* Define console modes for use with the Windows Console API. */
#  define SHELL_CONI_MODE \
  (ENABLE_ECHO_INPUT | ENABLE_INSERT_MODE | ENABLE_LINE_INPUT | 0x80 \
  | ENABLE_QUICK_EDIT_MODE | ENABLE_EXTENDED_FLAGS | ENABLE_PROCESSED_INPUT)
#  define SHELL_CONO_MODE (ENABLE_PROCESSED_OUTPUT | ENABLE_WRAP_AT_EOL_OUTPUT \
  | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
# endif

typedef struct ConsoleInfo {
  PerStreamTags pstSetup[3];
  PerStreamTags pstDesignated[3];
  StreamsAreConsole sacSetup;
} ConsoleInfo;

static short isValidStreamInfo(PerStreamTags *ppst){
  return (ppst->pf != SHELL_INVALID_FILE_PTR);
}

static ConsoleInfo consoleInfo = {
  { /* pstSetup */ PST_INITIALIZER, PST_INITIALIZER, PST_INITIALIZER },
  { /* pstDesignated[] */ PST_INITIALIZER, PST_INITIALIZER, PST_INITIALIZER },
  SAC_NoConsole /* sacSetup */
};

SQLITE_INTERNAL_LINKAGE FILE* invalidFileStream = (FILE *)~0;

# if CIO_WIN_WC_XLATE
static void maybeSetupAsConsole(PerStreamTags *ppst, short odir){
  if( pstReachesConsole(ppst) ){
    DWORD cm = odir? SHELL_CONO_MODE : SHELL_CONI_MODE;
    SetConsoleMode(ppst->hx, cm);
  }
}
# else
#  define maybeSetupAsConsole(ppst,odir)
# endif

SQLITE_INTERNAL_LINKAGE void consoleRenewSetup(void){
# if CIO_WIN_WC_XLATE
  int ix = 0;
  while( ix < 6 ){
    PerStreamTags *ppst = (ix<3)?
      &consoleInfo.pstSetup[ix] : &consoleInfo.pstDesignated[ix-3];
    maybeSetupAsConsole(ppst, (ix % 3)>0);
    ++ix;
  }
# endif
}

SQLITE_INTERNAL_LINKAGE StreamsAreConsole
consoleClassifySetup( FILE *pfIn, FILE *pfOut, FILE *pfErr ){
  StreamsAreConsole rv = SAC_NoConsole;
  FILE* apf[3] = { pfIn, pfOut, pfErr };
  int ix;
  for( ix = 2; ix >= 0; --ix ){
    PerStreamTags *ppst = &consoleInfo.pstSetup[ix];
    if( streamOfConsole(apf[ix], ppst) ){
      rv |= (SAC_InConsole<<ix);
    }
    consoleInfo.pstDesignated[ix] = *ppst;
    if( ix > 0 ) fflush(apf[ix]);
  }
  consoleInfo.sacSetup = rv;
  consoleRenewSetup();
  return rv;
}

SQLITE_INTERNAL_LINKAGE void SQLITE_CDECL consoleRestore( void ){
# if CIO_WIN_WC_XLATE
  static ConsoleInfo *pci = &consoleInfo;
  if( pci->sacSetup ){
    int ix;
    for( ix=0; ix<3; ++ix ){
      if( pci->sacSetup & (SAC_InConsole<<ix) ){
        PerStreamTags *ppst = &pci->pstSetup[ix];
        SetConsoleMode(ppst->hx, ppst->consMode);
      }
    }
  }
# endif
}
#endif /* !defined(SQLITE_CIO_NO_TRANSLATE) */

#ifdef SQLITE_CIO_INPUT_REDIR
/* Say whether given FILE* is among those known, via either
** consoleClassifySetup() or set{Output,Error}Stream, as
** readable, and return an associated PerStreamTags pointer
** if so. Otherwise, return 0.
*/
static PerStreamTags * isKnownReadable(FILE *pf){
  static PerStreamTags *apst[] = {
    &consoleInfo.pstDesignated[0], &consoleInfo.pstSetup[0], 0
  };
  int ix = 0;
  do {
    if( apst[ix]->pf == pf ) break;
  } while( apst[++ix] != 0 );
  return apst[ix];
}
#endif

#ifndef SQLITE_CIO_NO_TRANSLATE
/* Say whether given FILE* is among those known, via either
** consoleClassifySetup() or set{Output,Error}Stream, as
** writable, and return an associated PerStreamTags pointer
** if so. Otherwise, return 0.
*/
static PerStreamTags * isKnownWritable(FILE *pf){
  static PerStreamTags *apst[] = {
    &consoleInfo.pstDesignated[1], &consoleInfo.pstDesignated[2],
    &consoleInfo.pstSetup[1], &consoleInfo.pstSetup[2], 0
  };
  int ix = 0;
  do {
    if( apst[ix]->pf == pf ) break;
  } while( apst[++ix] != 0 );
  return apst[ix];
}

static FILE *designateEmitStream(FILE *pf, unsigned chix){
  FILE *rv = consoleInfo.pstDesignated[chix].pf;
  if( pf == invalidFileStream ) return rv;
  else{
    /* Setting a possibly new output stream. */
    PerStreamTags *ppst = isKnownWritable(pf);
    if( ppst != 0 ){
      PerStreamTags pst = *ppst;
      consoleInfo.pstDesignated[chix] = pst;
    }else streamOfConsole(pf, &consoleInfo.pstDesignated[chix]);
  }
  return rv;
}

SQLITE_INTERNAL_LINKAGE FILE *setOutputStream(FILE *pf){
  return designateEmitStream(pf, 1);
}
# ifdef CONSIO_SET_ERROR_STREAM
SQLITE_INTERNAL_LINKAGE FILE *setErrorStream(FILE *pf){
  return designateEmitStream(pf, 2);
}
# endif
#endif /* !defined(SQLITE_CIO_NO_TRANSLATE) */

#ifndef SQLITE_CIO_NO_SETMODE
# if CIO_WIN_WC_XLATE
static void setModeFlushQ(FILE *pf, short bFlush, int mode){
  if( bFlush ) fflush(pf);
  _setmode(_fileno(pf), mode);
}
# else
#  define setModeFlushQ(f, b, m) if(b) fflush(f)
# endif

SQLITE_INTERNAL_LINKAGE void setBinaryMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_BINARY);
}
SQLITE_INTERNAL_LINKAGE void setTextMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_TEXT);
}
# undef setModeFlushQ

#else /* defined(SQLITE_CIO_NO_SETMODE) */
# define setBinaryMode(f, bFlush) do{ if((bFlush)) fflush(f); }while(0)
# define setTextMode(f, bFlush) do{ if((bFlush)) fflush(f); }while(0)
#endif /* defined(SQLITE_CIO_NO_SETMODE) */

#ifndef SQLITE_CIO_NO_TRANSLATE
# if CIO_WIN_WC_XLATE
/* Write buffer cBuf as output to stream known to reach console,
** limited to ncTake char's. Return ncTake on success, else 0. */
static int conZstrEmit(PerStreamTags *ppst, const char *z, int ncTake){
  int rv = 0;
  if( z!=NULL ){
    int nwc = MultiByteToWideChar(CP_UTF8,0, z,ncTake, 0,0);
    if( nwc > 0 ){
      WCHAR *zw = sqlite3_malloc64(nwc*sizeof(WCHAR));
      if( zw!=NULL ){
        nwc = MultiByteToWideChar(CP_UTF8,0, z,ncTake, zw,nwc);
        if( nwc > 0 ){
          /* Translation from UTF-8 to UTF-16, then WCHARs out. */
          if( WriteConsoleW(ppst->hx, zw,nwc, 0, NULL) ){
            rv = ncTake;
          }
        }
        sqlite3_free(zw);
      }
    }
  }
  return rv;
}

/* For {f,o,e}PrintfUtf8() when stream is known to reach console. */
static int conioVmPrintf(PerStreamTags *ppst, const char *zFormat, va_list ap){
  char *z = sqlite3_vmprintf(zFormat, ap);
  if( z ){
    int rv = conZstrEmit(ppst, z, (int)strlen(z));
    sqlite3_free(z);
    return rv;
  }else return 0;
}
# endif /* CIO_WIN_WC_XLATE */

# ifdef CONSIO_GET_EMIT_STREAM
static PerStreamTags * getDesignatedEmitStream(FILE *pf, unsigned chix,
                                               PerStreamTags *ppst){
  PerStreamTags *rv = isKnownWritable(pf);
  short isValid = (rv!=0)? isValidStreamInfo(rv) : 0;
  if( rv != 0 && isValid ) return rv;
  streamOfConsole(pf, ppst);
  return ppst;
}
# endif

/* Get stream info, either for designated output or error stream when
** chix equals 1 or 2, or for an arbitrary stream when chix == 0.
** In either case, ppst references a caller-owned PerStreamTags
** struct which may be filled in if none of the known writable
** streams is being held by consoleInfo. The ppf parameter is an
** output when chix!=0 and an input when chix==0.
 */
static PerStreamTags *
getEmitStreamInfo(unsigned chix, PerStreamTags *ppst,
                  /* in/out */ FILE **ppf){
  PerStreamTags *ppstTry;
  FILE *pfEmit;
  if( chix > 0 ){
    ppstTry = &consoleInfo.pstDesignated[chix];
    if( !isValidStreamInfo(ppstTry) ){
      ppstTry = &consoleInfo.pstSetup[chix];
      pfEmit = ppst->pf;
    }else pfEmit = ppstTry->pf;
    if( !isValidStreamInfo(ppst) ){
      pfEmit = (chix > 1)? stderr : stdout;
      ppstTry = ppst;
      streamOfConsole(pfEmit, ppstTry);
    }
    *ppf = pfEmit;
  }else{
    ppstTry = isKnownWritable(*ppf);
    if( ppstTry != 0 ) return ppstTry;
    streamOfConsole(*ppf, ppst);
    return ppst;
  }
  return ppstTry;
}

SQLITE_INTERNAL_LINKAGE int oPrintfUtf8(const char *zFormat, ...){
  va_list ap;
  int rv;
  FILE *pfOut;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(1, &pst, &pfOut);
# else
  getEmitStreamInfo(1, &pst, &pfOut);
# endif
  assert(zFormat!=0);
  va_start(ap, zFormat);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    rv = conioVmPrintf(ppst, zFormat, ap);
  }else{
# endif
    rv = vfprintf(pfOut, zFormat, ap);
# if CIO_WIN_WC_XLATE
  }
# endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int ePrintfUtf8(const char *zFormat, ...){
  va_list ap;
  int rv;
  FILE *pfErr;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(2, &pst, &pfErr);
# else
  getEmitStreamInfo(2, &pst, &pfErr);
# endif
  assert(zFormat!=0);
  va_start(ap, zFormat);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    rv = conioVmPrintf(ppst, zFormat, ap);
  }else{
# endif
    rv = vfprintf(pfErr, zFormat, ap);
# if CIO_WIN_WC_XLATE
  }
# endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int fPrintfUtf8(FILE *pfO, const char *zFormat, ...){
  va_list ap;
  int rv;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(0, &pst, &pfO);
# else
  getEmitStreamInfo(0, &pst, &pfO);
# endif
  assert(zFormat!=0);
  va_start(ap, zFormat);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    maybeSetupAsConsole(ppst, 1);
    rv = conioVmPrintf(ppst, zFormat, ap);
    if( 0 == isKnownWritable(ppst->pf) ) restoreConsoleArb(ppst);
  }else{
# endif
    rv = vfprintf(pfO, zFormat, ap);
# if CIO_WIN_WC_XLATE
  }
# endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int fPutsUtf8(const char *z, FILE *pfO){
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(0, &pst, &pfO);
# else
  getEmitStreamInfo(0, &pst, &pfO);
# endif
  assert(z!=0);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    int rv;
    maybeSetupAsConsole(ppst, 1);
    rv = conZstrEmit(ppst, z, (int)strlen(z));
    if( 0 == isKnownWritable(ppst->pf) ) restoreConsoleArb(ppst);
    return rv;
  }else {
# endif
    return (fputs(z, pfO)<0)? 0 : (int)strlen(z);
# if CIO_WIN_WC_XLATE
  }
# endif
}

SQLITE_INTERNAL_LINKAGE int ePutsUtf8(const char *z){
  FILE *pfErr;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(2, &pst, &pfErr);
# else
  getEmitStreamInfo(2, &pst, &pfErr);
# endif
  assert(z!=0);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ) return conZstrEmit(ppst, z, (int)strlen(z));
  else {
# endif
    return (fputs(z, pfErr)<0)? 0 : (int)strlen(z);
# if CIO_WIN_WC_XLATE
  }
# endif
}

SQLITE_INTERNAL_LINKAGE int oPutsUtf8(const char *z){
  FILE *pfOut;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(1, &pst, &pfOut);
# else
  getEmitStreamInfo(1, &pst, &pfOut);
# endif
  assert(z!=0);
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ) return conZstrEmit(ppst, z, (int)strlen(z));
  else {
# endif
    return (fputs(z, pfOut)<0)? 0 : (int)strlen(z);
# if CIO_WIN_WC_XLATE
  }
# endif
}

#endif /* !defined(SQLITE_CIO_NO_TRANSLATE) */

#if !(defined(SQLITE_CIO_NO_UTF8SCAN) && defined(SQLITE_CIO_NO_TRANSLATE))
/* Skip over as much z[] input char sequence as is valid UTF-8,
** limited per nAccept char's or whole characters and containing
** no char cn such that ((1<<cn) & ccm)!=0. On return, the
** sequence z:return (inclusive:exclusive) is validated UTF-8.
** Limit: nAccept>=0 => char count, nAccept<0 => character
 */
SQLITE_INTERNAL_LINKAGE const char*
zSkipValidUtf8(const char *z, int nAccept, long ccm){
  int ng = (nAccept<0)? -nAccept : 0;
  const char *pcLimit = (nAccept>=0)? z+nAccept : 0;
  assert(z!=0);
  while( (pcLimit)? (z<pcLimit) : (ng-- != 0) ){
    char c = *z;
    if( (c & 0x80) == 0 ){
      if( ccm != 0L && c < 0x20 && ((1L<<c) & ccm) != 0 ) return z;
      ++z; /* ASCII */
    }else if( (c & 0xC0) != 0xC0 ) return z; /* not a lead byte */
    else{
      const char *zt = z+1; /* Got lead byte, look at trail bytes.*/
      do{
        if( pcLimit && zt >= pcLimit ) return z;
        else{
          char ct = *zt++;
          if( ct==0 || (zt-z)>4 || (ct & 0xC0)!=0x80 ){
            /* Trailing bytes are too few, too many, or invalid. */
            return z;
          }
        }
      } while( ((c <<= 1) & 0x40) == 0x40 ); /* Eat lead byte's count. */
      z = zt;
    }
  }
  return z;
}
#endif /*!(defined(SQLITE_CIO_NO_UTF8SCAN)&&defined(SQLITE_CIO_NO_TRANSLATE))*/

#ifndef SQLITE_CIO_NO_TRANSLATE
SQLITE_INTERNAL_LINKAGE int
fPutbUtf8(FILE *pfO, const char *cBuf, int nAccept){
  assert(pfO!=0);
# if CIO_WIN_WC_XLATE
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
  PerStreamTags *ppst = getEmitStreamInfo(0, &pst, &pfO);
  if( pstReachesConsole(ppst) ){
    int rv;
    maybeSetupAsConsole(ppst, 1);
    rv = conZstrEmit(ppst, cBuf, nAccept);
    if( 0 == isKnownWritable(ppst->pf) ) restoreConsoleArb(ppst);
    return rv;
  }else {
# endif
    return (int)fwrite(cBuf, 1, nAccept, pfO);
# if CIO_WIN_WC_XLATE
  }
# endif
}

SQLITE_INTERNAL_LINKAGE int
oPutbUtf8(const char *cBuf, int nAccept){
  FILE *pfOut;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
# if CIO_WIN_WC_XLATE
  PerStreamTags *ppst = getEmitStreamInfo(1, &pst, &pfOut);
# else
  getEmitStreamInfo(1, &pst, &pfOut);
# endif
# if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    return conZstrEmit(ppst, cBuf, nAccept);
  }else {
# endif
    return (int)fwrite(cBuf, 1, nAccept, pfOut);
# if CIO_WIN_WC_XLATE
  }
# endif
}

# ifdef CONSIO_EPUTB
SQLITE_INTERNAL_LINKAGE int
ePutbUtf8(const char *cBuf, int nAccept){
  FILE *pfErr;
  PerStreamTags pst = PST_INITIALIZER; /* for unknown streams */
  PerStreamTags *ppst = getEmitStreamInfo(2, &pst, &pfErr);
#  if CIO_WIN_WC_XLATE
  if( pstReachesConsole(ppst) ){
    return conZstrEmit(ppst, cBuf, nAccept);
  }else {
#  endif
    return (int)fwrite(cBuf, 1, nAccept, pfErr);
#  if CIO_WIN_WC_XLATE
  }
#  endif
}
# endif /* defined(CONSIO_EPUTB) */

SQLITE_INTERNAL_LINKAGE char* fGetsUtf8(char *cBuf, int ncMax, FILE *pfIn){
  if( pfIn==0 ) pfIn = stdin;
# if CIO_WIN_WC_XLATE
  if( pfIn == consoleInfo.pstSetup[0].pf
      && (consoleInfo.sacSetup & SAC_InConsole)!=0 ){
#  if CIO_WIN_WC_XLATE==1
#   define SHELL_GULP 150 /* Count of WCHARS to be gulped at a time */
    WCHAR wcBuf[SHELL_GULP+1];
    int lend = 0, noc = 0;
    if( ncMax > 0 ) cBuf[0] = 0;
    while( noc < ncMax-8-1 && !lend ){
      /* There is room for at least 2 more characters and a 0-terminator. */
      int na = (ncMax > SHELL_GULP*4+1 + noc)? SHELL_GULP : (ncMax-1 - noc)/4;
#   undef SHELL_GULP
      DWORD nbr = 0;
      BOOL bRC = ReadConsoleW(consoleInfo.pstSetup[0].hx, wcBuf, na, &nbr, 0);
      if( bRC && nbr>0 && (wcBuf[nbr-1]&0xF800)==0xD800 ){
        /* Last WHAR read is first of a UTF-16 surrogate pair. Grab its mate. */
        DWORD nbrx;
        bRC &= ReadConsoleW(consoleInfo.pstSetup[0].hx, wcBuf+nbr, 1, &nbrx, 0);
        if( bRC ) nbr += nbrx;
      }
      if( !bRC || (noc==0 && nbr==0) ) return 0;
      if( nbr > 0 ){
        int nmb = WideCharToMultiByte(CP_UTF8, 0, wcBuf,nbr,0,0,0,0);
        if( nmb != 0 && noc+nmb <= ncMax ){
          int iseg = noc;
          nmb = WideCharToMultiByte(CP_UTF8, 0, wcBuf,nbr,cBuf+noc,nmb,0,0);
          noc += nmb;
          /* Fixup line-ends as coded by Windows for CR (or "Enter".)
          ** This is done without regard for any setMode{Text,Binary}()
          ** call that might have been done on the interactive input.
          */
          if( noc > 0 ){
            if( cBuf[noc-1]=='\n' ){
              lend = 1;
              if( noc > 1 && cBuf[noc-2]=='\r' ) cBuf[--noc-1] = '\n';
            }
          }
          /* Check for ^Z (anywhere in line) too, to act as EOF. */
          while( iseg < noc ){
            if( cBuf[iseg]=='\x1a' ){
              noc = iseg; /* Chop ^Z and anything following. */
              lend = 1; /* Counts as end of line too. */
              break;
            }
            ++iseg;
          }
        }else break; /* Drop apparent garbage in. (Could assert.) */
      }else break;
    }
    /* If got nothing, (after ^Z chop), must be at end-of-file. */
    if( noc > 0 ){
      cBuf[noc] = 0;
      return cBuf;
    }else return 0;
#  endif
  }else{
# endif
    return fgets(cBuf, ncMax, pfIn);
# if CIO_WIN_WC_XLATE
  }
# endif
}
#endif /* !defined(SQLITE_CIO_NO_TRANSLATE) */

#undef CIO_WIN_WC_XLATE
#undef SHELL_INVALID_FILE_PTR
