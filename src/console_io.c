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
*************************************************************************
** This file implements various interfaces used for console I/O by the
** SQLite project command-line tools, as explained in console_io.h .
*/

#ifndef SQLITE_CDECL
# define SQLITE_CDECL
#endif

#include <stdarg.h>
#include <string.h>
#include "console_io.h"
#include "sqlite3.h"

#if defined(_WIN32) || defined(WIN32)
# include <io.h>
# include <fcntl.h>
# ifdef SHELL_LEGACY_CONSOLE_IO
#  define SHELL_CON_TRANSLATE 2 /* Use UTF-8/MBCS translation for console I/O */
# else
#  define SHELL_CON_TRANSLATE 1 /* Use wchar APIs for console I/O */
# endif
#else
# include <unistd.h>
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

static short fileOfConsole(FILE *pf){
#if SHELL_CON_TRANSLATE
  DWORD dwj;
  HANDLE fh = handleOfFile(pf);
  if( INVALID_HANDLE_VALUE != fh ){
    return (GetFileType(fh) == FILE_TYPE_CHAR && GetConsoleMode(fh,&dwj));
  }else return 0;
#else
  return (short)isatty(fileno(pf));
#endif
}

#define SHELL_INVALID_FILE_PTR ((FILE *)sizeof(FILE*))

typedef struct ConsoleInfo {
  /* int iDefaultFmode; */
  ConsoleStdConsStreams cscs;
#if SHELL_CON_TRANSLATE
  HANDLE hIn; HANDLE hOut; HANDLE hErr;
  HANDLE hLowest;
#endif
  FILE *pfIn; FILE *pfOut; FILE *pfErr;
} ConsoleInfo;

static ConsoleInfo consoleInfo = {
  /* 0, iDefaultFmode */
  CSCS_NoConsole,
#if SHELL_CON_TRANSLATE
  INVALID_HANDLE_VALUE, INVALID_HANDLE_VALUE, INVALID_HANDLE_VALUE,
  INVALID_HANDLE_VALUE,
#endif
  SHELL_INVALID_FILE_PTR, SHELL_INVALID_FILE_PTR, SHELL_INVALID_FILE_PTR
};
#undef SHELL_INVALID_FILE_PTR

#if SHELL_CON_TRANSLATE == 1
# define SHELL_CON_MODE_CSZ _O_U16TEXT
#elif SHELL_CON_TRANSLATE == 2
# define SHELL_CON_MODE_CSZ _O_U8TEXT
#endif

INT_LINKAGE ConsoleStdConsStreams
consoleClassifySetup( FILE *pfIn, FILE *pfOut, FILE *pfErr ){
  ConsoleStdConsStreams rv = CSCS_NoConsole;
  if( fileOfConsole(pfErr) ){
    rv |= CSCS_ErrConsole;
    consoleInfo.pfErr = pfErr;
#if SHELL_CON_TRANSLATE
    fflush(pfErr);
# if SHELL_CON_TRANSLATE == 1
    _setmode(_fileno(pfErr), _O_U16TEXT);
    _setmode(_fileno(pfErr), _O_BINARY);
# elif SHELL_CON_TRANSLATE == 2
    _setmode(_fileno(pfErr), _O_U8TEXT);
    _setmode(_fileno(pfErr), _O_TEXT);
# endif
    consoleInfo.hLowest = consoleInfo.hErr = handleOfFile(pfErr);
#endif
  }
  if( fileOfConsole(pfOut) ){
    rv |= CSCS_OutConsole;
    consoleInfo.pfOut = pfOut;
#if SHELL_CON_TRANSLATE
    fflush(pfOut);
# if SHELL_CON_TRANSLATE == 1
    _setmode(_fileno(pfOut), _O_U16TEXT);
    _setmode(_fileno(pfOut), _O_BINARY);
# elif SHELL_CON_TRANSLATE == 2
    _setmode(_fileno(pfOut), _O_U8TEXT);
    _setmode(_fileno(pfOut), _O_TEXT);
# endif
    consoleInfo.hLowest = consoleInfo.hOut = handleOfFile(pfOut);
#endif
  }
  if( fileOfConsole(pfIn) ){
    rv |= CSCS_InConsole;
    consoleInfo.pfIn = pfIn;
#if SHELL_CON_TRANSLATE == 1
    _setmode(_fileno(pfIn), _O_U16TEXT);
    _setmode(_fileno(pfIn), _O_BINARY);
    consoleInfo.hLowest = consoleInfo.hIn = handleOfFile(pfIn);
#elif SHELL_CON_TRANSLATE == 2
    _setmode(_fileno(pfIn), _O_U8TEXT);
    _setmode(_fileno(pfIn), _O_TEXT);
    consoleInfo.hLowest = consoleInfo.hIn = handleOfFile(pfIn);
#endif
  }
  consoleInfo.cscs = rv;
  return rv;
}

INT_LINKAGE void SQLITE_CDECL consoleRestore( void ){
#if SHELL_CON_TRANSLATE
  if( consoleInfo.cscs ){
    /* ToDo: Read these modes in consoleClassifySetup somehow.
    ** A _get_fmode() call almost works. But not with gcc, yet.
    ** This has to be done to make the CLI a callable function.
    */
    int tmode = _O_TEXT, xmode = _O_U8TEXT;
    if( consoleInfo.cscs & CSCS_InConsole ){
      _setmode(_fileno(consoleInfo.pfIn), tmode);
      _setmode(_fileno(consoleInfo.pfIn), xmode);
    }
    if( consoleInfo.cscs & CSCS_OutConsole ){
      _setmode(_fileno(consoleInfo.pfOut), tmode);
      _setmode(_fileno(consoleInfo.pfOut), xmode);
    }
    if( consoleInfo.cscs & CSCS_ErrConsole ){
      _setmode(_fileno(consoleInfo.pfErr), tmode);
      _setmode(_fileno(consoleInfo.pfErr), xmode);
    }
  }
#endif
#ifdef TEST_CIO
#endif
}

static short isConOut(FILE *pf){
  if( pf==consoleInfo.pfOut ) return 1;
  else if( pf==consoleInfo.pfErr ) return 2;
  else return 0;
}

INT_LINKAGE void setBinaryMode(FILE *pf, short bFlush){
  short ico = isConOut(pf);
  if( ico || bFlush ) fflush(pf);
#if SHELL_CON_TRANSLATE == 2
  _setmode(_fileno(pf), _O_BINARY);
#elif SHELL_CON_TRANSLATE == 1
  /* Never change between text/binary on UTF-16 console streamss. */
  if( !ico && !(consoleInfo.pfIn==pf)) _setmode(_fileno(pf), _O_BINARY);
#endif
}
INT_LINKAGE void setTextMode(FILE *pf, short bFlush){
  short ico = isConOut(pf);
  if( ico || bFlush ) fflush(pf);
#if SHELL_CON_TRANSLATE == 2
  _setmode(_fileno(pf), _O_TEXT);
#elif SHELL_CON_TRANSLATE == 1
  /* Never change between text/binary on UTF-16 console streamss. */
  if( !ico && !(consoleInfo.pfIn==pf)) _setmode(_fileno(pf), _O_TEXT);
#endif
}
/* Later: Factor common code out of above 2 procs. */

INT_LINKAGE int fprintfUtf8(FILE *pfO, const char *zFormat, ...){
  va_list ap;
  int rv = 0;
  short on = isConOut(pfO);
  va_start(ap, zFormat);
  if( on > 0 ){
#if SHELL_CON_TRANSLATE
    char *z1 = sqlite3_vmprintf(zFormat, ap);
# if SHELL_CON_TRANSLATE == 2
    /* Legacy translation to active code page, then MBCS chars out. */
    char *z2 = sqlite3_win32_utf8_to_mbcs_v2(z1, 0);
    if( z2!=NULL ){
      rv = strlen(z2);
      vfprintf(pfO, "%s", z2);
      sqlite3_free(z2);
    }
# else
    /* Translation from UTF-8 to UTF-16, then WCHAR characters out. */
    if( z1!=NULL ){
      int nwc;
      WCHAR *zw2 = 0;
      rv = strlen(z1);
      nwc = MultiByteToWideChar(CP_UTF8,0,z1,rv,0,0);
      if( nwc>0 ){
        zw2 = sqlite3_malloc64((nwc+1)*sizeof(WCHAR));
        if( zw2!=NULL ){
          HANDLE ho = (on==1)? consoleInfo.hOut : consoleInfo.hErr;
          nwc = MultiByteToWideChar(CP_UTF8,0,z1,rv,zw2,nwc);
          zw2[nwc] = 0;
          WriteConsoleW(ho, zw2, nwc, 0, NULL);
          sqlite3_free(zw2);
        }else rv = 0;
      }
    }
# endif
    sqlite3_free(z1);
#else
#endif
  }else{
    rv = vfprintf(pfO, zFormat, ap);
  }
  va_end(ap);
  return rv;
}

INT_LINKAGE int fgetsUtf8(char *buf, int ncMax, FILE *pfIn){
  return 0;
}

#ifdef TEST_CIO
// cl -Zi -I. -DWIN32 -DTEST_CIO sqlite3.c src/console_io.c -Fecio.exe
// gcc -I. -DWIN32 -DTEST_CIO -o cio sqlite3.c src/console_io.c -o cio.exe
const char *prompts[] = { "main", "cont" };
Prompts goofy = { 2, prompts };

int main(int na, char *av[]){
  ConsoleStdConsStreams cc = consoleClassifySetup(stdin, stdout, stderr);
  setTextMode(stdout, 1);
  setTextMode(stderr, 1);
  fprintfUtf8(stderr, "%d\n", cc);
  fprintfUtf8(stdout, "%s=%d\n", "∑(1st 7 primes)", 42);
  fprintfUtf8(stderr, "%s\n", "∫ (1/x) dx ≡ ln(x)");
  consoleRestore();
  return 0;
}
#endif /* defined(TEST_CIO) */
