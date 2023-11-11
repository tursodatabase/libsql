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

#if (defined(_WIN32) || defined(WIN32)) && !SQLITE_OS_WINRT
# ifndef SHELL_NO_SYSINC
#  include <io.h>
#  include <fcntl.h>
#  undef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
# endif
# ifdef SHELL_LEGACY_CONSOLE_IO
#  define SHELL_CON_TRANSLATE 2 /* Use UTF-8/MBCS translation for console I/O */
# else
#  define SHELL_CON_TRANSLATE 1 /* Use WCHAR Windows APIs for console I/O */
# endif
#else
# ifndef SHELL_NO_SYSINC
#  include <unistd.h>
# endif
# define SHELL_CON_TRANSLATE 0 /* Use plain C library stream I/O at console */
#endif

#if SHELL_CON_TRANSLATE
static HANDLE handleOfFile(FILE *pf){
  int fileDesc = _fileno(pf);
  union { intptr_t osfh; HANDLE fh; } fid = {
    (fileDesc>=0)? _get_osfhandle(fileDesc) : (intptr_t)INVALID_HANDLE_VALUE
  };
  return fid.fh;
}
#endif

typedef struct PerStreamTags {
#if SHELL_CON_TRANSLATE
  HANDLE hx;
  DWORD consMode;
#else
  short reachesConsole;
#endif
  FILE *pf;
} PerStreamTags;

/* Define NULL-like value for things which can validly be 0. */
#define SHELL_INVALID_FILE_PTR ((FILE *)~0)
#if SHELL_CON_TRANSLATE
# define SHELL_INVALID_CONS_MODE 0xFFFF0000
#endif

#if SHELL_CON_TRANSLATE
# define CI_INITIALIZER \
  { INVALID_HANDLE_VALUE, SHELL_INVALID_CONS_MODE, SHELL_INVALID_FILE_PTR }
#else
# define CI_INITIALIZER { 0, SHELL_INVALID_FILE_PTR }
#endif

/* Quickly say whether a known output is going to the console. */
static short pstReachesConsole(PerStreamTags *ppst){
#if SHELL_CON_TRANSLATE
  return (ppst->hx != INVALID_HANDLE_VALUE);
#else
  return (ppst->reachesConsole != 0);
#endif
}

#if SHELL_CON_TRANSLATE
static void restoreConsoleArb(PerStreamTags *ppst){
  if( pstReachesConsole(ppst) ) SetConsoleMode(ppst->hx, ppst->consMode);
}
#else
# define restoreConsoleArb(ppst)
#endif

/* Say whether FILE* appears to be a console, collect associated info. */
static short streamOfConsole(FILE *pf, /* out */ PerStreamTags *ppst){
#if SHELL_CON_TRANSLATE
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
#else
  ppst->pf = pf;
  ppst->reachesConsole = ( (short)isatty(fileno(pf)) );
  return ppst->reachesConsole;
#endif
}

#if SHELL_CON_TRANSLATE
/* Define console modes for use with the Windows Console API. */
# define SHELL_CONI_MODE \
  (ENABLE_ECHO_INPUT | ENABLE_INSERT_MODE | ENABLE_LINE_INPUT | 0x80 \
  | ENABLE_QUICK_EDIT_MODE | ENABLE_EXTENDED_FLAGS | ENABLE_PROCESSED_INPUT)
# define SHELL_CONO_MODE (ENABLE_PROCESSED_OUTPUT | ENABLE_WRAP_AT_EOL_OUTPUT \
  | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
#endif

typedef struct ConsoleInfo {
  PerStreamTags pstSetup[3];
  PerStreamTags pstDesignated[3];
  StreamsAreConsole sacSetup;
  StreamsAreConsole sacDesignated;
} ConsoleInfo;

static short isValidStreamInfo(PerStreamTags *ppst){
  return (ppst->pf != SHELL_INVALID_FILE_PTR);
}

static ConsoleInfo consoleInfo = {
  { /* pstSetup */ CI_INITIALIZER, CI_INITIALIZER, CI_INITIALIZER },
  { /* pstDesignated[] */ CI_INITIALIZER, CI_INITIALIZER, CI_INITIALIZER },
  SAC_NoConsole, SAC_NoConsole /* sacSetup, sacDesignated */
};
#undef SHELL_INVALID_FILE_PTR
#undef CI_INITIALIZER

SQLITE_INTERNAL_LINKAGE FILE* invalidFileStream = (FILE *)~0;

static void maybeSetupAsConsole(PerStreamTags *ppst, short odir){
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ){
    DWORD cm = odir? SHELL_CONO_MODE : SHELL_CONI_MODE;
    SetConsoleMode(ppst->hx, cm);
# if SHELL_CON_TRANSLATE == 2
    _setmode(_fileno(ppst->pf), _O_TEXT);
# endif
  }
#else
  (void)ppst;
  (void)odir;
#endif
}

SQLITE_INTERNAL_LINKAGE void consoleRenewSetup(void){
#if SHELL_CON_TRANSLATE
  int ix = 0;
  while( ix < 6 ){
    PerStreamTags *ppst = (ix<3)?
      &consoleInfo.pstSetup[ix] : &consoleInfo.pstDesignated[ix-3];
    maybeSetupAsConsole(ppst, (ix % 3)>0);
    ++ix;
  }
#endif
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
#if SHELL_CON_TRANSLATE == 2
    _setmode(_fileno(apf[ix]), _O_TEXT);
#endif
  }
  consoleInfo.sacSetup = rv;
  consoleRenewSetup();
  return rv;
}

SQLITE_INTERNAL_LINKAGE void SQLITE_CDECL consoleRestore( void ){
#if SHELL_CON_TRANSLATE
  static ConsoleInfo *pci = &consoleInfo;
  if( pci->sacSetup ){
    int ix;
    for( ix=0; ix<3; ++ix ){
      if( pci->sacSetup & (SAC_InConsole<<ix) ){
        PerStreamTags *ppst = &pci->pstSetup[ix];
# if SHELL_CON_TRANSLATE == 2
        static int tmode = _O_TEXT;
        /* Consider: Read this mode in consoleClassifySetup somehow.
        ** A _get_fmode() call almost works. But not with gcc, yet.
        ** This has to be done to make the CLI a callable function
        ** when legacy console I/O is done. (This may never happen.)
        */
        _setmode(_fileno(pci->pstSetup[ix].pf), tmode);
# endif
        SetConsoleMode(ppst->hx, ppst->consMode);
      }
    }
  }
#endif
}

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
SQLITE_INTERNAL_LINKAGE FILE *setErrorStream(FILE *pf){
  return designateEmitStream(pf, 2);
}

#if SHELL_CON_TRANSLATE
static void setModeFlushQ(FILE *pf, short bFlush, int mode){
  if( bFlush ) fflush(pf);
  _setmode(_fileno(pf), mode);
}
#else
# define setModeFlushQ(f, b, m) if(b) fflush(f)
#endif

SQLITE_INTERNAL_LINKAGE void setBinaryMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_BINARY);
}
SQLITE_INTERNAL_LINKAGE void setTextMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_TEXT);
}
#undef setModeFlushQ

#if SHELL_CON_TRANSLATE
/* Write plain 0-terminated output to stream known as reaching console. */
static int conioZstrOut(PerStreamTags *ppst, const char *z){
  int rv = 0;
  if( z!=NULL && *z!=0 ){
    int nc;
    int nwc;
# if SHELL_CON_TRANSLATE == 2
    UINT cocp = GetConsoleOutputCP();
    FILE *pfO = ppst->pf;
    if( cocp == CP_UTF8 ){
      /* This is not legacy action. But it can work better,
      ** when the console putatively can handle UTF-8. */
      return fputs(z, pfO)<0 ? 0 : (int)strlen(z);
    }
# endif
    nc = (int)strlen(z);
    nwc = MultiByteToWideChar(CP_UTF8,0, z,nc, 0,0);
    if( nwc > 0 ){
      WCHAR *zw = sqlite3_malloc64(nwc*sizeof(WCHAR));
      if( zw!=NULL ){
        nwc = MultiByteToWideChar(CP_UTF8,0, z,nc, zw,nwc);
        if( nwc > 0 ){
# if SHELL_CON_TRANSLATE == 2
          /* Legacy translation to active code page, then MBCS out. */
          rv = WideCharToMultiByte(cocp,0, zw,nwc, 0,0, 0,0);
          if( rv != 0 ){
            char *zmb = sqlite3_malloc64(rv+1);
            if( zmb != NULL ){
              rv = WideCharToMultiByte(cocp,0, zw,nwc, zmb,rv, 0,0);
              zmb[rv] = 0;
              if( fputs(zmb, pfO)<0 ) rv = 0;
              sqlite3_free(zmb);
            }
          }
# elif SHELL_CON_TRANSLATE == 1
          /* Translation from UTF-8 to UTF-16, then WCHARs out. */
          if( WriteConsoleW(ppst->hx, zw,nwc, 0, NULL) ){
            rv = nc;
          }
# endif
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
  int rv = conioZstrOut(ppst, z);
  sqlite3_free(z);
  return rv;
}
#endif /* SHELL_CON_TRANSLATE */


static PerStreamTags * getDesignatedEmitStream(FILE *pf, unsigned chix,
                                               PerStreamTags *ppst){
  PerStreamTags *rv = isKnownWritable(pf);
  short isValid = (rv!=0)? isValidStreamInfo(rv) : 0;
  if( rv != 0 && isValid ) return rv;
  streamOfConsole(pf, ppst);
  return ppst;
}

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
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(1, &pst, &pfOut);

  va_start(ap, zFormat);
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ){
    rv = conioVmPrintf(ppst, zFormat, ap);
  }else{
#endif
    rv = vfprintf(pfOut, zFormat, ap);
#if SHELL_CON_TRANSLATE
  }
#endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int ePrintfUtf8(const char *zFormat, ...){
  va_list ap;
  int rv;
  FILE *pfErr;
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(2, &pst, &pfErr);

  va_start(ap, zFormat);
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ){
    rv = conioVmPrintf(ppst, zFormat, ap);
  }else{
#endif
    rv = vfprintf(pfErr, zFormat, ap);
#if SHELL_CON_TRANSLATE
  }
#endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int fPrintfUtf8(FILE *pfO, const char *zFormat, ...){
  va_list ap;
  int rv;
#if SHELL_CON_TRANSLATE
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(0, &pst, &pfO);
#endif

  va_start(ap, zFormat);
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ){
    maybeSetupAsConsole(ppst, 1);
    rv = conioVmPrintf(ppst, zFormat, ap);
    if( 0 == isKnownWritable(ppst->pf) ) restoreConsoleArb(ppst);
  }else{
#endif
    rv = vfprintf(pfO, zFormat, ap);
#if SHELL_CON_TRANSLATE
  }
#endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int fPutsUtf8(const char *z, FILE *pfO){
#if SHELL_CON_TRANSLATE
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(0, &pst, &pfO);
  if( pstReachesConsole(ppst) ){
    int rv;
    maybeSetupAsConsole(ppst, 1);
    rv = conioZstrOut(ppst, z);
    if( 0 == isKnownWritable(ppst->pf) ) restoreConsoleArb(ppst);
    return rv;
  }else {
#endif
    return (fputs(z, pfO)<0)? 0 : (int)strlen(z);
#if SHELL_CON_TRANSLATE
  }
#endif
}

SQLITE_INTERNAL_LINKAGE int ePutsUtf8(const char *z){
  FILE *pfErr;
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(2, &pst, &pfErr);
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ) return conioZstrOut(ppst, z);
  else {
#endif
    return (fputs(z, pfErr)<0)? 0 : (int)strlen(z);
#if SHELL_CON_TRANSLATE
  }
#endif
}

SQLITE_INTERNAL_LINKAGE int oPutsUtf8(const char *z){
  FILE *pfOut;
  PerStreamTags pst; /* Needed only for heretofore unknown streams. */
  PerStreamTags *ppst = getEmitStreamInfo(1, &pst, &pfOut);
#if SHELL_CON_TRANSLATE
  if( pstReachesConsole(ppst) ) return conioZstrOut(ppst, z);
  else {
#endif
    return (fputs(z, pfOut)<0)? 0 : (int)strlen(z);
#if SHELL_CON_TRANSLATE
  }
#endif
}

#if SHELL_CON_TRANSLATE==2
static int mbcsToUtf8InPlaceIfValid(char *pc, int nci, int nco, UINT codePage){
  WCHAR wcOneCode[2];
  int nuo = 0;
  int nwConvert = MultiByteToWideChar(codePage, MB_ERR_INVALID_CHARS,
                                      pc, nci, wcOneCode, 2);
  if( nwConvert > 0 ){
    nuo = WideCharToMultiByte(CP_UTF8, 0, wcOneCode, nwConvert, pc, nco, 0,0);
  }
  return nuo;
}
#endif

SQLITE_INTERNAL_LINKAGE char* fGetsUtf8(char *cBuf, int ncMax, FILE *pfIn){
  if( pfIn==0 ) pfIn = stdin;
#if SHELL_CON_TRANSLATE
  if( pfIn == consoleInfo.pstSetup[0].pf ){
# if SHELL_CON_TRANSLATE==1
#  define SHELL_GULP 150 /* Count of WCHARS to be gulped at a time */
    WCHAR wcBuf[SHELL_GULP+1];
    int lend = 0, noc = 0;
    if( ncMax > 0 ) cBuf[0] = 0;
    while( noc < ncMax-8-1 && !lend ){
      /* There is room for at least 2 more characters and a 0-terminator. */
      int na = (ncMax > SHELL_GULP*4+1 + noc)? SHELL_GULP : (ncMax-1 - noc)/4;
#  undef SHELL_GULP
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
# elif SHELL_CON_TRANSLATE==2
    /* This is not done efficiently because it may never be used.
    ** Also, it is interactive input so it need not be fast.  */
    int nco = 0;
    /* For converstion to WCHAR, or pre-test of same. */
    UINT cicp = GetConsoleCP(); /* For translation from mbcs. */
    /* If input code page is CP_UTF8, must bypass MBCS input
    ** collection because getc() returns 0 for non-ASCII byte
    ** Instead, use fgets() which repects character boundaries. */
    if( cicp == CP_UTF8 ) return fgets(cBuf, ncMax, pfIn);
    while( ncMax-nco >= 5 ){
      /* Have space for max UTF-8 group and 0-term. */
      int nug = 0;
      int c = getc(pfIn);
      if( c < 0 ){
        if( nco > 0 ) break;
        else return 0;
      }
      cBuf[nco] = (char)c;
      if( c < 0x80 ){
        ++nco;
        if( c == '\n' ) break;
        continue;
      }
      /* Deal with possible mbcs lead byte. */
      nug = mbcsToUtf8InPlaceIfValid(cBuf+nco, 1, ncMax-nco-1, cicp);
      if( nug > 0 ){
        nco += nug;
      }else{
        /* Must have just mbcs lead byte; get the trail byte(s). */
        int ntb = 1, ct;
        while( ntb <= 3 ){ /* No more under any multi-byte code. */
          ct = getc(pfIn);
          if( ct < 0 || ct == '\n' ){
            /* Just drop whatever garbage preceded the newline or.
            ** EOF. It's not valid, should not happen, and there
            ** is no good way to deal with it, short of bailing. */
            if( ct > 0 ){
              cBuf[nco++] = (int)ct;
            }
            break;
          }
          /* Treat ct as bona fide MBCS trailing byte, if valid. */
          cBuf[nco+ntb] = ct;
          nug = mbcsToUtf8InPlaceIfValid(cBuf+nco, 1+ntb, ncMax-nco-1, cicp);
          if( nug > 0 ){
            nco += nug;
            break;
          }
        }
        if( ct < 0 ) break;
      }
    }
    cBuf[nco] = 0;
    return cBuf;
# endif
  }else{
#endif
    return fgets(cBuf, ncMax, pfIn);
#if SHELL_CON_TRANSLATE
  }
#endif
}
