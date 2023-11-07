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
** This file implements various interfaces used for console I/O by the
** SQLite project command-line tools, as explained in console_io.h .
*/

#ifndef SQLITE_CDECL
# define SQLITE_CDECL
#endif

#ifndef SHELL_NO_SYSINC
# include <stdarg.h>
# include <string.h>
# include <stdlib.h>
# include <limits.h>
# include "console_io.h"
# include "sqlite3.h"
#endif

#if defined(_WIN32) || defined(WIN32)
# ifndef SHELL_NO_SYSINC
#  include <io.h>
#  include <fcntl.h>
#  undef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
# endif
# ifdef SHELL_LEGACY_CONSOLE_IO
#  define SHELL_CON_TRANSLATE 2 /* Use UTF-8/MBCS translation for console I/O */
extern char *sqlite3_win32_utf8_to_mbcs_v2(const char *, int);
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
    (fileDesc!=-2)? _get_osfhandle(fileDesc) : (intptr_t)INVALID_HANDLE_VALUE
  };
  return fid.fh;
}
#endif

typedef struct PerStreamTags {
#if SHELL_CON_TRANSLATE
  DWORD consMode;
  HANDLE hx;
#endif
  FILE *pf;
} PerStreamTags;

static short fileOfConsole(FILE *pf, PerStreamTags *ppst){
#if SHELL_CON_TRANSLATE
  short rv = 0;
  DWORD dwj;
  HANDLE fh = handleOfFile(pf);
  if( INVALID_HANDLE_VALUE != fh ){
    rv = (GetFileType(fh) == FILE_TYPE_CHAR && GetConsoleMode(fh,&dwj));
    if( rv ){
      ppst->hx = fh;
      ppst->pf = pf;
      GetConsoleMode(fh, &ppst->consMode);
    }
  }
  return rv;
#else
  return (short)isatty(fileno(pf));
#endif
}

#define SHELL_INVALID_FILE_PTR ((FILE *)sizeof(FILE*))
#define SHELL_INVALID_CONS_MODE 0xFFFF0000

#if SHELL_CON_TRANSLATE
# define SHELL_CONI_MODE \
  (ENABLE_ECHO_INPUT | ENABLE_INSERT_MODE | ENABLE_LINE_INPUT | 0x80 \
  | ENABLE_QUICK_EDIT_MODE | ENABLE_EXTENDED_FLAGS | ENABLE_PROCESSED_INPUT)
# define SHELL_CONO_MODE (ENABLE_PROCESSED_OUTPUT | ENABLE_WRAP_AT_EOL_OUTPUT \
  | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
#endif

typedef struct ConsoleInfo {
  /* int iInitialFmode[3];
  ** Above only needed for legacy console I/O for callable CLI.
  ** Because that state cannot be obtained from each FILE *,
  ** there will be no exact restoration of console state for
  ** the CLI when built with SHELL_LEGACY_CONSOLE_IO defined.
  */
  PerStreamTags pst[3];
#if SHELL_CON_TRANSLATE
  unsigned char haveInput;
  unsigned char outputIx;
  unsigned char stdinEof;
#endif
  ConsoleStdConsStreams cscs;
} ConsoleInfo;

#if SHELL_CON_TRANSLATE
# define CI_INITIALIZER \
  {SHELL_INVALID_CONS_MODE, INVALID_HANDLE_VALUE, SHELL_INVALID_FILE_PTR }
#else
# define CI_INITIALIZER { SHELL_INVALID_FILE_PTR }
#endif

static ConsoleInfo consoleInfo = {
  /* {0,0,0}, // iInitialFmode */
  { /* pst */ CI_INITIALIZER, CI_INITIALIZER, CI_INITIALIZER },
#if SHELL_CON_TRANSLATE
  0, 0, 1, /* haveInput, outputIx, stdinEof */
#endif
  CSCS_NoConsole
};
#undef CI_INITIALIZER

SQLITE_INTERNAL_LINKAGE ConsoleStdConsStreams
consoleClassifySetup( FILE *pfIn, FILE *pfOut, FILE *pfErr ){
  ConsoleStdConsStreams rv = CSCS_NoConsole;
  FILE *apf[3] = { pfIn, pfOut, pfErr };
  int ix;
  for( ix = 2; ix >= 0; --ix ){
    PerStreamTags *ppst = &consoleInfo.pst[ix];
    if( fileOfConsole(apf[ix], ppst) ){
#if SHELL_CON_TRANSLATE
      DWORD cm = (ix==0)? SHELL_CONI_MODE : SHELL_CONO_MODE;
      if( ix==0 ){
        consoleInfo.haveInput = 1;
        consoleInfo.stdinEof = 0;
      }else{
        consoleInfo.outputIx |= ix;
      }
      SetConsoleMode(ppst->hx, cm);
#endif
      rv |= (CSCS_InConsole<<ix);
    }
    if( ix > 0 ) fflush(apf[ix]);
#if SHELL_CON_TRANSLATE == 2
    //    _setmode(_fileno(apf[ix]), _O_U8TEXT);
    _setmode(_fileno(apf[ix]), _O_TEXT);
#endif
  }
  consoleInfo.cscs = rv;
  return rv;
}

SQLITE_INTERNAL_LINKAGE void SQLITE_CDECL consoleRestore( void ){
  if( consoleInfo.cscs ){
    int ix;
    for( ix=0; ix<3; ++ix ){
      if( consoleInfo.cscs & (CSCS_InConsole<<ix) ){
        PerStreamTags *ppst = &consoleInfo.pst[ix];
#if SHELL_CON_TRANSLATE == 2
        static int tmode = _O_TEXT;//, xmode = _O_U8TEXT;
        /* Consider: Read these modes in consoleClassifySetup somehow.
        ** A _get_fmode() call almost works. But not with gcc, yet.
        ** This has to be done to make the CLI a callable function
        ** when legacy console I/O is done. (This may never happen.)
        */
        _setmode(_fileno(consoleInfo.pst[ix].pf), tmode);
        //_setmode(_fileno(consoleInfo.pst[ix].pf), xmode);
#endif
#if SHELL_CON_TRANSLATE
        SetConsoleMode(ppst->hx, ppst->consMode);
        ppst->hx = INVALID_HANDLE_VALUE;
#endif
        ppst->pf = SHELL_INVALID_FILE_PTR;
      }
      consoleInfo.cscs = CSCS_NoConsole;
#if SHELL_CON_TRANSLATE
      consoleInfo.stdinEof = consoleInfo.haveInput = consoleInfo.outputIx= 0;
#endif
    }
  }
}

static short isConOut(FILE *pf){
  if( pf==consoleInfo.pst[1].pf ) return 1;
  else if( pf==consoleInfo.pst[2].pf ) return 2;
  else return 0;
}

#if SHELL_CON_TRANSLATE
static void setModeFlushQ(FILE *pf, short bFlush, int mode){
  short ico = isConOut(pf);
  if( ico>1 || bFlush ) fflush(pf);
  _setmode(_fileno(pf), mode);
}
#else
# define setModeFlushQ(f, b, m) if(isConOut(f)>0||b) fflush(f)
#endif

SQLITE_INTERNAL_LINKAGE void setBinaryMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_BINARY);
}
SQLITE_INTERNAL_LINKAGE void setTextMode(FILE *pf, short bFlush){
  setModeFlushQ(pf, bFlush, _O_TEXT);
}
#undef setModeFlushQ

#if SHELL_CON_TRANSLATE
/* For fprintfUtf8() and printfUtf8() when stream is known as console. */
static int conioVmPrintf(int rch, const char *zFormat, va_list ap){
  int rv = 0;
  char *z1 = sqlite3_vmprintf(zFormat, ap);
# if SHELL_CON_TRANSLATE == 2
  if( z1!=NULL ){
    UINT ccp = GetConsoleOutputCP();
    FILE *pfO = consoleInfo.pst[rch].pf;
    /* Legacy translation to active code page, then MBCS chars out. */
    char *z2 = sqlite3_win32_utf8_to_mbcs_v2(z1, 0);
    if( z2!=NULL ){
      rv = (int)strlen(z2);
      fputs(z2, pfO);
      sqlite3_free(z2);
    }
    sqlite3_free(z1);
  }
# elif SHELL_CON_TRANSLATE == 1
  /* Translation from UTF-8 to UTF-16, then WCHAR characters out. */
  if( z1!=NULL ){
    int nwc;
    WCHAR *zw2 = 0;
    rv = (int)strlen(z1);
    nwc = MultiByteToWideChar(CP_UTF8,0,z1,rv,0,0);
    if( nwc>0 ){
      zw2 = sqlite3_malloc64((nwc+1)*sizeof(WCHAR));
      if( zw2!=NULL ){
        HANDLE ho = consoleInfo.pst[rch].hx;
        nwc = MultiByteToWideChar(CP_UTF8,0,z1,rv,zw2,nwc);
        zw2[nwc] = 0;
        WriteConsoleW(ho, zw2, nwc, 0, NULL);
        sqlite3_free(zw2);
      }else rv = 0;
    }
    sqlite3_free(z1);
  }
# endif
  return rv;
}
#endif

SQLITE_INTERNAL_LINKAGE int printfUtf8(const char *zFormat, ...){
  va_list ap;
  int rv;
  va_start(ap, zFormat);
#if SHELL_CON_TRANSLATE
  if( SHELL_INVALID_FILE_PTR != consoleInfo.pst[1].pf ){
    rv = conioVmPrintf(1, zFormat, ap);
  }else{
#endif
    rv = vfprintf(stdout, zFormat, ap);
#if SHELL_CON_TRANSLATE
  }
#endif
  va_end(ap);
  return rv;
}
#undef SHELL_INVALID_FILE_PTR

SQLITE_INTERNAL_LINKAGE int fprintfUtf8(FILE *pfO, const char *zFormat, ...){
  va_list ap;
  int rv;
  va_start(ap, zFormat);
#if SHELL_CON_TRANSLATE
  short rch = isConOut(pfO);
  if( rch > 0 ){
    rv = conioVmPrintf(rch, zFormat, ap);
  }else {
#endif
    rv = vfprintf(pfO, zFormat, ap);
#if SHELL_CON_TRANSLATE
  }
#endif
  va_end(ap);
  return rv;
}

SQLITE_INTERNAL_LINKAGE int fputsUtf8(const char *z, FILE *pfO){
#if SHELL_CON_TRANSLATE
  return fprintfUtf8(pfO, "%s", z);
#else
  return fputs(z, pfO);
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

SQLITE_INTERNAL_LINKAGE char* fgetsUtf8(char *cBuf, int ncMax, FILE *pfIn){
  if( pfIn==0 ) pfIn = stdin;
#if SHELL_CON_TRANSLATE
  if( pfIn == consoleInfo.pst[0].pf ){
# if SHELL_CON_TRANSLATE==1
#  define SHELL_GULP 150 /* Count of WCHARS to be gulped at a time */
    WCHAR wcBuf[SHELL_GULP+1];
    int lend = 0, noc = 0;
    if( consoleInfo.stdinEof ) return 0;
    if( ncMax > 0 ) cBuf[0] = 0;
    while( noc < ncMax-8-1 && !lend ){
      /* There is room for at least 2 more characters and a 0-terminator. */
      int na = (ncMax > SHELL_GULP*4+1 + noc)? SHELL_GULP : (ncMax-1 - noc)/4;
#  undef SHELL_GULP
      DWORD nbr = 0;
      BOOL bRC = ReadConsoleW(consoleInfo.pst[0].hx, wcBuf, na, &nbr, 0);
      if( bRC && nbr>0 && (wcBuf[nbr-1]&0xF800)==0xD800 ){
        /* Last WHAR read is first of a UTF-16 surrogate pair. Grab its mate. */
        DWORD nbrx;
        bRC &= ReadConsoleW(consoleInfo.pst[0].hx, wcBuf+nbr, 1, &nbrx, 0);
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
          ** Note that this is done without regard for any setModeText()
          ** call that might have been done on the interactive input.
          */
          if( noc > 0 ){
            if( cBuf[noc-1]=='\n' ){
              lend = 1;
              if( noc > 1 && cBuf[noc-2]=='\r' ){
                cBuf[noc-2] = '\n';
                --noc;
              }
            }
          }
          /* Check for ^Z (anywhere in line) too, to act as EOF. */
          while( iseg < noc ){
            if( cBuf[iseg]==0x1a ){
              consoleInfo.stdinEof = 1;
              noc = iseg; /* Chop ^Z and anything following. */
              break;
            }
            ++iseg;
          }
        }else break; /* Drop apparent garbage in. (Could assert.) */
      }else break;
    }
    /* If got nothing, (after ^Z chop), must be at end-of-file. */
    if( noc == 0 ) return 0;
    cBuf[noc] = 0;
    return cBuf;
# elif SHELL_CON_TRANSLATE==2
    /* This is not done efficiently because it may never be used.
    ** Also, it is interactive input so it need not be fast.  */
    int nco = 0;
    /* For converstion to WCHAR, or pre-test of same. */
    UINT ccp = GetConsoleCP(); /* For translation from mbcs. */
    if( ccp == CP_UTF8 ) return fgets(cBuf, ncMax, pfIn);
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
      nug = mbcsToUtf8InPlaceIfValid(cBuf+nco, 1, ncMax-nco-1, ccp);
      if( nug > 0 ){
        nco += nug;
      }else{
        /* Must have just mbcs lead byte; get the trail byte. */
        int ct = getc(pfIn);
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
        cBuf[nco+1] = ct;
        nug = mbcsToUtf8InPlaceIfValid(cBuf+nco, 2, ncMax-nco-1, ccp);
        nco += nug;
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
