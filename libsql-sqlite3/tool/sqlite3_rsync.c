/*
** 2024-09-10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This is a utility program that makes a copy of a live SQLite database
** using a bandwidth-efficient protocol, similar to "rsync".
*/
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#include "sqlite3.h"

static const char zUsage[] =
  "sqlite3_rsync ORIGIN REPLICA ?OPTIONS?\n"
  "\n"
  "One of ORIGIN or REPLICA is a pathname to a database on the local\n"
  "machine and the other is of the form \"USER@HOST:PATH\" describing\n"
  "a database on a remote machine.  This utility makes REPLICA into a\n"
  "copy of ORIGIN\n"
  "\n"
  "OPTIONS:\n"
  "\n"
  "   --exe PATH      Name of the sqlite3_rsync program on the remote side\n"
  "   --help          Show this help screen\n"
  "   --protocol N    Use sync protocol version N.\n"
  "   --ssh PATH      Name of the SSH program used to reach the remote side\n"
  "   -v              Verbose.  Multiple v's for increasing output\n"
  "   --version       Show detailed version information\n"
  "   --wal-only      Do not sync unless both databases are in WAL mode\n"
;

typedef unsigned char u8;
typedef sqlite3_uint64 u64;

/* Context for the run */
typedef struct SQLiteRsync SQLiteRsync;
struct SQLiteRsync {
  const char *zOrigin;     /* Name of the origin */
  const char *zReplica;    /* Name of the replica */
  const char *zErrFile;    /* Append error messages to this file */
  const char *zDebugFile;  /* Append debugging messages to this file */
  FILE *pOut;              /* Transmit to the other side */
  FILE *pIn;               /* Receive from the other side */
  FILE *pLog;              /* Duplicate output here if not NULL */
  FILE *pDebug;            /* Write debug info here if not NULL */
  sqlite3 *db;             /* Database connection */
  int nErr;                /* Number of errors encountered */
  int nWrErr;              /* Number of failed attempts to write on the pipe */
  u8 eVerbose;             /* Bigger for more output.  0 means none. */
  u8 bCommCheck;           /* True to debug the communication protocol */
  u8 isRemote;             /* On the remote side of a connection */
  u8 isReplica;            /* True if running on the replica side */
  u8 iProtocol;            /* Protocol version number */
  u8 wrongEncoding;        /* ATTACH failed due to wrong encoding */
  u8 bWalOnly;             /* Require WAL mode */
  sqlite3_uint64 nOut;     /* Bytes transmitted */
  sqlite3_uint64 nIn;      /* Bytes received */
  unsigned int nPage;      /* Total number of pages in the database */
  unsigned int szPage;     /* Database page size */
  u64 nHashSent;           /* Hashes sent (replica to origin) */
  unsigned int nRound;     /* Number of hash batches (replica to origin) */
  unsigned int nPageSent;  /* Page contents sent (origin to replica) */
};

/* The version number of the protocol.  Sent in the *_BEGIN message
** to verify that both sides speak the same dialect.
*/
#define PROTOCOL_VERSION  2


/* Magic numbers to identify particular messages sent over the wire.
*/
/**** Baseline: protocol version 1 ****/
#define ORIGIN_BEGIN    0x41     /* Initial message */
#define ORIGIN_END      0x42     /* Time to quit */
#define ORIGIN_ERROR    0x43     /* Error message from the remote */
#define ORIGIN_PAGE     0x44     /* New page data */
#define ORIGIN_TXN      0x45     /* Transaction commit */
#define ORIGIN_MSG      0x46     /* Informational message */
/**** Added in protocol version 2 ****/
#define ORIGIN_DETAIL   0x47     /* Request finer-grain hash info */
#define ORIGIN_READY    0x48     /* Ready for next round of hash exchanges */

/**** Baseline: protocol version 1 ****/
#define REPLICA_BEGIN   0x61     /* Welcome message */
#define REPLICA_ERROR   0x62     /* Error.  Report and quit. */
#define REPLICA_END     0x63     /* Replica wants to stop */
#define REPLICA_HASH    0x64     /* One or more pages hashes to report */
#define REPLICA_READY   0x65     /* Read to receive page content */
#define REPLICA_MSG     0x66     /* Informational message */
/**** Added in protocol version 2 ****/
#define REPLICA_CONFIG  0x67     /* Hash exchange configuration */

/****************************************************************************
** Beginning of the popen2() implementation copied from Fossil  *************
****************************************************************************/
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <fcntl.h>
/*
** Print a fatal error and quit.
*/
static void win32_fatal_error(const char *zMsg){
  fprintf(stderr, "%s", zMsg);
  exit(1);
}
extern int _open_osfhandle(intptr_t,int);
#else
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#endif

/*
** The following macros are used to cast pointers to integers and
** integers to pointers.  The way you do this varies from one compiler
** to the next, so we have developed the following set of #if statements
** to generate appropriate macros for a wide range of compilers.
**
** The correct "ANSI" way to do this is to use the intptr_t type.
** Unfortunately, that typedef is not available on all compilers, or
** if it is available, it requires an #include of specific headers
** that vary from one machine to the next.
**
** This code is copied out of SQLite.
*/
#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define INT_TO_PTR(X)  ((void*)(X))
# define PTR_TO_INT(X)  ((int)(X))
#endif

/* Register SQL functions provided by ext/misc/sha1.c */
extern int sqlite3_sha_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
);

#ifdef _WIN32
/*
** On windows, create a child process and specify the stdin, stdout,
** and stderr channels for that process to use.
**
** Return the number of errors.
*/
static int win32_create_child_process(
  wchar_t *zCmd,       /* The command that the child process will run */
  HANDLE hIn,          /* Standard input */
  HANDLE hOut,         /* Standard output */
  HANDLE hErr,         /* Standard error */
  DWORD *pChildPid     /* OUT: Child process handle */
){
  STARTUPINFOW si;
  PROCESS_INFORMATION pi;
  BOOL rc;

  memset(&si, 0, sizeof(si));
  si.cb = sizeof(si);
  si.dwFlags = STARTF_USESTDHANDLES;
  SetHandleInformation(hIn, HANDLE_FLAG_INHERIT, TRUE);
  si.hStdInput  = hIn;
  SetHandleInformation(hOut, HANDLE_FLAG_INHERIT, TRUE);
  si.hStdOutput = hOut;
  SetHandleInformation(hErr, HANDLE_FLAG_INHERIT, TRUE);
  si.hStdError  = hErr;
  rc = CreateProcessW(
     NULL,  /* Application Name */
     zCmd,  /* Command-line */
     NULL,  /* Process attributes */
     NULL,  /* Thread attributes */
     TRUE,  /* Inherit Handles */
     0,     /* Create flags  */
     NULL,  /* Environment */
     NULL,  /* Current directory */
     &si,   /* Startup Info */
     &pi    /* Process Info */
  );
  if( rc ){
    CloseHandle( pi.hProcess );
    CloseHandle( pi.hThread );
    *pChildPid = pi.dwProcessId;
  }else{
    win32_fatal_error("cannot create child process");
  }
  return rc!=0;
}
void *win32_utf8_to_unicode(const char *zUtf8){
  int nByte = MultiByteToWideChar(CP_UTF8, 0, zUtf8, -1, 0, 0);
  wchar_t *zUnicode = malloc( nByte*2 );
  MultiByteToWideChar(CP_UTF8, 0, zUtf8, -1, zUnicode, nByte);
  return zUnicode;
}
#endif

/*
** Create a child process running shell command "zCmd".  *ppOut is
** a FILE that becomes the standard input of the child process.
** (The caller writes to *ppOut in order to send text to the child.)
** *ppIn is stdout from the child process.  (The caller
** reads from *ppIn in order to receive input from the child.)
** Note that *ppIn is an unbuffered file descriptor, not a FILE.
** The process ID of the child is written into *pChildPid.
**
** Return the number of errors.
*/
static int popen2(
  const char *zCmd,      /* Command to run in the child process */
  FILE **ppIn,           /* Read from child using this file descriptor */
  FILE **ppOut,          /* Write to child using this file descriptor */
  int *pChildPid,        /* PID of the child process */
  int bDirect            /* 0: run zCmd as a shell cmd.  1: run directly */
){
#ifdef _WIN32
  HANDLE hStdinRd, hStdinWr, hStdoutRd, hStdoutWr, hStderr;
  SECURITY_ATTRIBUTES saAttr;
  DWORD childPid = 0;
  int fd;

  saAttr.nLength = sizeof(saAttr);
  saAttr.bInheritHandle = TRUE;
  saAttr.lpSecurityDescriptor = NULL;
  hStderr = GetStdHandle(STD_ERROR_HANDLE);
  if( !CreatePipe(&hStdoutRd, &hStdoutWr, &saAttr, 4096) ){
    win32_fatal_error("cannot create pipe for stdout");
  }
  SetHandleInformation( hStdoutRd, HANDLE_FLAG_INHERIT, FALSE);

  if( !CreatePipe(&hStdinRd, &hStdinWr, &saAttr, 4096) ){
    win32_fatal_error("cannot create pipe for stdin");
  }
  SetHandleInformation( hStdinWr, HANDLE_FLAG_INHERIT, FALSE);

  win32_create_child_process(win32_utf8_to_unicode(zCmd),
                             hStdinRd, hStdoutWr, hStderr,&childPid);
  *pChildPid = childPid;
  fd = _open_osfhandle(PTR_TO_INT(hStdoutRd), 0);
  *ppIn = fdopen(fd, "rb");
  fd = _open_osfhandle(PTR_TO_INT(hStdinWr), 0);
  *ppOut = _fdopen(fd, "wb");
  CloseHandle(hStdinRd);
  CloseHandle(hStdoutWr);
  return 0;
#else
  int pin[2], pout[2];
  *ppIn = 0;
  *ppOut = 0;
  *pChildPid = 0;

  if( pipe(pin)<0 ){
    return 1;
  }
  if( pipe(pout)<0 ){
    close(pin[0]);
    close(pin[1]);
    return 1;
  }
  *pChildPid = fork();
  if( *pChildPid<0 ){
    close(pin[0]);
    close(pin[1]);
    close(pout[0]);
    close(pout[1]);
    *pChildPid = 0;
    return 1;
  }
  signal(SIGPIPE,SIG_IGN);
  if( *pChildPid==0 ){
    int fd;
    /* This is the child process */
    close(0);
    fd = dup(pout[0]);
    if( fd!=0 ) {
      fprintf(stderr,"popen2() failed to open file descriptor 0");
      exit(1);
    }
    close(pout[0]);
    close(pout[1]);
    close(1);
    fd = dup(pin[1]);
    if( fd!=1 ){
      fprintf(stderr,"popen() failed to open file descriptor 1");
      exit(1);
    }
    close(pin[0]);
    close(pin[1]);
    if( bDirect ){
      execl(zCmd, zCmd, (char*)0);
    }else{
      execl("/bin/sh", "/bin/sh", "-c", zCmd, (char*)0);
    }
    return 1;
  }else{
    /* This is the parent process */
    close(pin[1]);
    *ppIn = fdopen(pin[0], "r");
    close(pout[0]);
    *ppOut = fdopen(pout[1], "w");
    return 0;
  }
#endif
}

/*
** Close the connection to a child process previously created using
** popen2().
*/
static void pclose2(FILE *pIn, FILE *pOut, int childPid){
#ifdef _WIN32
  /* Not implemented, yet */
  fclose(pIn);
  fclose(pOut);
#else
  fclose(pIn);
  fclose(pOut);
  while( waitpid(0, 0, WNOHANG)>0 ) {}
#endif
}
/*****************************************************************************
** End of the popen2() implementation copied from Fossil *********************
*****************************************************************************/

/*****************************************************************************
** Beginning of the append_escaped_arg() routine, adapted from the Fossil   **
** subroutine nameed blob_append_escaped_arg()                              **
*****************************************************************************/
/*
** ASCII (for reference):
**    x0  x1  x2  x3  x4  x5  x6  x7  x8  x9  xa  xb  xc  xd  xe  xf
** 0x ^`  ^a  ^b  ^c  ^d  ^e  ^f  ^g  \b  \t  \n  ()  \f  \r  ^n  ^o
** 1x ^p  ^q  ^r  ^s  ^t  ^u  ^v  ^w  ^x  ^y  ^z  ^{  ^|  ^}  ^~  ^
** 2x ()  !   "   #   $   %   &   '   (   )   *   +   ,   -   .   /
** 3x 0   1   2   3   4   5   6   7   8   9   :   ;   <   =   >   ?
** 4x @   A   B   C   D   E   F   G   H   I   J   K   L   M   N   O
** 5x P   Q   R   S   T   U   V   W   X   Y   Z   [   \   ]   ^   _
** 6x `   a   b   c   d   e   f   g   h   i   j   k   l   m   n   o
** 7x p   q   r   s   t   u   v   w   x   y   z   {   |   }   ~   ^_
*/

/*
** Meanings for bytes in a filename:
**
**    0      Ordinary character.  No encoding required
**    1      Needs to be escaped
**    2      Illegal character.  Do not allow in a filename
**    3      First byte of a 2-byte UTF-8
**    4      First byte of a 3-byte UTF-8
**    5      First byte of a 4-byte UTF-8
*/
static const char aSafeChar[256] = {
#ifdef _WIN32
/* Windows
** Prohibit:  all control characters, including tab, \r and \n.
** Escape:    (space) " # $ % & ' ( ) * ; < > ? [ ] ^ ` { | }
*/
/*  x0  x1  x2  x3  x4  x5  x6  x7  x8  x9  xa  xb  xc  xd  xe  xf  */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 0x */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 1x */
     1,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  0,  0,  0, /* 2x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  0,  1,  1, /* 3x */
     1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* 4x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  1,  1,  0, /* 5x */
     1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* 6x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1,  0,  1, /* 7x */
#else
/* Unix
** Prohibit:  all control characters, including tab, \r and \n
** Escape:    (space) ! " # $ % & ' ( ) * ; < > ? [ \ ] ^ ` { | }
*/
/*  x0  x1  x2  x3  x4  x5  x6  x7  x8  x9  xa  xb  xc  xd  xe  xf  */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 0x */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 1x */
     1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  0,  0,  0, /* 2x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  0,  1,  1, /* 3x */
     1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* 4x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1,  1,  0, /* 5x */
     1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* 6x */
     0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1,  0,  1, /* 7x */
#endif
    /* all bytes 0x80 through 0xbf are unescaped, being secondary
    ** bytes to UTF8 characters.  Bytes 0xc0 through 0xff are the
    ** first byte of a UTF8 character and do get escaped */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 8x */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* 9x */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* ax */
     2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, /* bx */
     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3, /* cx */
     3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3, /* dx */
     4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4, /* ex */
     5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5  /* fx */
};

/*
** pStr is a shell command under construction.  This routine safely
** appends filename argument zIn.  It returns 0 on success or non-zero
** on any error.
**
** The argument is escaped if it contains white space or other characters
** that need to be escaped for the shell.  If zIn contains characters
** that cannot be safely escaped, then throw a fatal error.
**
** If the isFilename argument is true, then the argument is expected
** to be a filename.  As shell commands commonly have command-line
** options that begin with "-" and since we do not want an attacker
** to be able to invoke these switches using filenames that begin
** with "-", if zIn begins with "-", prepend an additional "./"
** (or ".\\" on Windows).
*/
int append_escaped_arg(sqlite3_str *pStr, const char *zIn, int isFilename){
  int i;
  unsigned char c;
  int needEscape = 0;
  int n = sqlite3_str_length(pStr);
  char *z = sqlite3_str_value(pStr);

  /* Look for illegal byte-sequences and byte-sequences that require
  ** escaping.  No control-characters are allowed.  All spaces and
  ** non-ASCII unicode characters and some punctuation characters require
  ** escaping. */
  for(i=0; (c = (unsigned char)zIn[i])!=0; i++){
    if( aSafeChar[c] ){
      unsigned char x = aSafeChar[c];
      needEscape = 1;
      if( x==2 ){
        /* Bad ASCII character */
        return 1;
      }else if( x>2 ){
        if( (zIn[i+1]&0xc0)!=0x80
         || (x>=4 && (zIn[i+2]&0xc0)!=0x80)
         || (x==5 && (zIn[i+3]&0xc0)!=0x80)
        ){
          /* Bad UTF8 character */
          return 1;
        }
        i += x-2;
      }
    }
  }

  /* Separate from the previous argument by a space */
  if( n>0 && !isspace(z[n-1]) ){
    sqlite3_str_appendchar(pStr, 1, ' ');
  }

  /* Check for characters that need quoting */
  if( !needEscape ){
    if( isFilename && zIn[0]=='-' ){
      sqlite3_str_appendchar(pStr, 1, '.');
#if defined(_WIN32)
      sqlite3_str_appendchar(pStr, 1, '\\');
#else
      sqlite3_str_appendchar(pStr, 1, '/');
#endif
    }
    sqlite3_str_appendall(pStr, zIn);
  }else{
#if defined(_WIN32)
    /* Quoting strategy for windows:
    ** Put the entire name inside of "...".  Any " characters within
    ** the name get doubled.
    */
    sqlite3_str_appendchar(pStr, 1, '"');
    if( isFilename && zIn[0]=='-' ){
      sqlite3_str_appendchar(pStr, 1, '.');
      sqlite3_str_appendchar(pStr, 1, '\\');
    }else if( zIn[0]=='/' ){
      sqlite3_str_appendchar(pStr, 1, '.');
    }
    for(i=0; (c = (unsigned char)zIn[i])!=0; i++){
      sqlite3_str_appendchar(pStr, 1, (char)c);
      if( c=='"' ) sqlite3_str_appendchar(pStr, 1, '"');
      if( c=='\\' ) sqlite3_str_appendchar(pStr, 1, '\\');
      if( c=='%' && isFilename ) sqlite3_str_append(pStr, "%cd:~,%", 7);
    }
    sqlite3_str_appendchar(pStr, 1, '"');
#else
    /* Quoting strategy for unix:
    ** If the name does not contain ', then surround the whole thing
    ** with '...'.   If there is one or more ' characters within the
    ** name, then put \ before each special character.
    */
    if( strchr(zIn,'\'') ){
      if( isFilename && zIn[0]=='-' ){
        sqlite3_str_appendchar(pStr, 1, '.');
        sqlite3_str_appendchar(pStr, 1, '/');
      }
      for(i=0; (c = (unsigned char)zIn[i])!=0; i++){
        if( aSafeChar[c] && aSafeChar[c]!=2 ){
          sqlite3_str_appendchar(pStr, 1, '\\');
        }
        sqlite3_str_appendchar(pStr, 1, (char)c);
      }
    }else{
      sqlite3_str_appendchar(pStr, 1, '\'');
      if( isFilename && zIn[0]=='-' ){
        sqlite3_str_appendchar(pStr, 1, '.');
        sqlite3_str_appendchar(pStr, 1, '/');
      }
      sqlite3_str_appendall(pStr, zIn);
      sqlite3_str_appendchar(pStr, 1, '\'');
    }
#endif
  }
  return 0;
}

/* Add an approprate PATH= argument to the SSH command under construction
** in pStr
**
** About This Feature
** ==================
**
** On some ssh servers (Macs in particular are guilty of this) the PATH
** variable in the shell that runs the command that is sent to the remote
** host contains a limited number of read-only system directories:
**
**      /usr/bin:/bin:/usr/sbin:/sbin
**
** The sqlite3_rsync executable cannot be installed into any of those
** directories because they are locked down, and so the "sqlite3_rsync"
** command cannot run.
**
** To work around this, the sqlite3_rsync command is prefixed with a PATH=
** argument, inserted by this function, to augment the PATH with additional
** directories in which the sqlite3_rsync executable can be installed.
**
** But other ssh servers are confused by this initial PATH= argument.
** Some ssh servers have a list of programs that they are allowed to run
** and will fail if the first argument is not on that list, and PATH=....
** is not on that list.
**
** So that sqlite3_rsync can invoke itself on a remote system using ssh
** on a variety of platforms, the following algorithm is used:
**
**   *  First try running the sqlite3_rsync without any PATH= argument.
**      If that works (and it does on a majority of systems) then we are
**      done.
**
**   *  If the first attempt fails, then try again after adding the
**      PATH= prefix argument.  (This function is what adds that
**      argument.)
**
** A consequence of this is that if the remote system is a Mac, the
** "ssh" command always ends up being invoked twice.  If anybody knows a
** way around that problem, please bring it to the attention of the
** developers.
*/
void add_path_argument(sqlite3_str *pStr){
  append_escaped_arg(pStr,
     "PATH=$HOME/bin:/usr/local/bin:/opt/homebrew/bin"
     ":/opt/local/bin:$PATH", 0);
}

/*****************************************************************************
** End of the append_escaped_arg() routine, adapted from the Fossil         **
*****************************************************************************/

/*****************************************************************************
** The Hash Engine
**
** This is basically SHA3, though with a 160-bit hash, and reducing the
** number of rounds in the KeccakF1600 step function from 24 to 6.
*/
/*
** Macros to determine whether the machine is big or little endian,
** and whether or not that determination is run-time or compile-time.
**
** For best performance, an attempt is made to guess at the byte-order
** using C-preprocessor macros.  If that is unsuccessful, or if
** -DHash_BYTEORDER=0 is set, then byte-order is determined
** at run-time.
*/
#ifndef Hash_BYTEORDER
# if defined(i386)     || defined(__i386__)   || defined(_M_IX86) ||    \
     defined(__x86_64) || defined(__x86_64__) || defined(_M_X64)  ||    \
     defined(_M_AMD64) || defined(_M_ARM)     || defined(__x86)   ||    \
     defined(__arm__)
#   define Hash_BYTEORDER    1234
# elif defined(sparc)    || defined(__ppc__)
#   define Hash_BYTEORDER    4321
# else
#   define Hash_BYTEORDER 0
# endif
#endif

/*
** State structure for a Hash hash in progress
*/
typedef struct HashContext HashContext;
struct HashContext {
  union {
    u64 s[25];                /* Keccak state. 5x5 lines of 64 bits each */
    unsigned char x[1600];    /* ... or 1600 bytes */
  } u;
  unsigned nRate;        /* Bytes of input accepted per Keccak iteration */
  unsigned nLoaded;      /* Input bytes loaded into u.x[] so far this cycle */
  unsigned ixMask;       /* Insert next input into u.x[nLoaded^ixMask]. */
  unsigned iSize;        /* 224, 256, 358, or 512 */
};

/*
** A single step of the Keccak mixing function for a 1600-bit state
*/
static void KeccakF1600Step(HashContext *p){
  int i;
  u64 b0, b1, b2, b3, b4;
  u64 c0, c1, c2, c3, c4;
  u64 d0, d1, d2, d3, d4;
  static const u64 RC[] = {
    0x0000000000000001ULL,  0x0000000000008082ULL,
    0x800000000000808aULL,  0x8000000080008000ULL,
    0x000000000000808bULL,  0x0000000080000001ULL,
    0x8000000080008081ULL,  0x8000000000008009ULL,
    0x000000000000008aULL,  0x0000000000000088ULL,
    0x0000000080008009ULL,  0x000000008000000aULL,
    0x000000008000808bULL,  0x800000000000008bULL,
    0x8000000000008089ULL,  0x8000000000008003ULL,
    0x8000000000008002ULL,  0x8000000000000080ULL,
    0x000000000000800aULL,  0x800000008000000aULL,
    0x8000000080008081ULL,  0x8000000000008080ULL,
    0x0000000080000001ULL,  0x8000000080008008ULL
  };
# define a00 (p->u.s[0])
# define a01 (p->u.s[1])
# define a02 (p->u.s[2])
# define a03 (p->u.s[3])
# define a04 (p->u.s[4])
# define a10 (p->u.s[5])
# define a11 (p->u.s[6])
# define a12 (p->u.s[7])
# define a13 (p->u.s[8])
# define a14 (p->u.s[9])
# define a20 (p->u.s[10])
# define a21 (p->u.s[11])
# define a22 (p->u.s[12])
# define a23 (p->u.s[13])
# define a24 (p->u.s[14])
# define a30 (p->u.s[15])
# define a31 (p->u.s[16])
# define a32 (p->u.s[17])
# define a33 (p->u.s[18])
# define a34 (p->u.s[19])
# define a40 (p->u.s[20])
# define a41 (p->u.s[21])
# define a42 (p->u.s[22])
# define a43 (p->u.s[23])
# define a44 (p->u.s[24])
# define ROL64(a,x) ((a<<x)|(a>>(64-x)))

  /*         v---- Number of rounds.  SHA3 has 24 here. */
  for(i=0; i<6; i++){
    c0 = a00^a10^a20^a30^a40;
    c1 = a01^a11^a21^a31^a41;
    c2 = a02^a12^a22^a32^a42;
    c3 = a03^a13^a23^a33^a43;
    c4 = a04^a14^a24^a34^a44;
    d0 = c4^ROL64(c1, 1);
    d1 = c0^ROL64(c2, 1);
    d2 = c1^ROL64(c3, 1);
    d3 = c2^ROL64(c4, 1);
    d4 = c3^ROL64(c0, 1);

    b0 = (a00^d0);
    b1 = ROL64((a11^d1), 44);
    b2 = ROL64((a22^d2), 43);
    b3 = ROL64((a33^d3), 21);
    b4 = ROL64((a44^d4), 14);
    a00 =   b0 ^((~b1)&  b2 );
    a00 ^= RC[i];
    a11 =   b1 ^((~b2)&  b3 );
    a22 =   b2 ^((~b3)&  b4 );
    a33 =   b3 ^((~b4)&  b0 );
    a44 =   b4 ^((~b0)&  b1 );

    b2 = ROL64((a20^d0), 3);
    b3 = ROL64((a31^d1), 45);
    b4 = ROL64((a42^d2), 61);
    b0 = ROL64((a03^d3), 28);
    b1 = ROL64((a14^d4), 20);
    a20 =   b0 ^((~b1)&  b2 );
    a31 =   b1 ^((~b2)&  b3 );
    a42 =   b2 ^((~b3)&  b4 );
    a03 =   b3 ^((~b4)&  b0 );
    a14 =   b4 ^((~b0)&  b1 );

    b4 = ROL64((a40^d0), 18);
    b0 = ROL64((a01^d1), 1);
    b1 = ROL64((a12^d2), 6);
    b2 = ROL64((a23^d3), 25);
    b3 = ROL64((a34^d4), 8);
    a40 =   b0 ^((~b1)&  b2 );
    a01 =   b1 ^((~b2)&  b3 );
    a12 =   b2 ^((~b3)&  b4 );
    a23 =   b3 ^((~b4)&  b0 );
    a34 =   b4 ^((~b0)&  b1 );

    b1 = ROL64((a10^d0), 36);
    b2 = ROL64((a21^d1), 10);
    b3 = ROL64((a32^d2), 15);
    b4 = ROL64((a43^d3), 56);
    b0 = ROL64((a04^d4), 27);
    a10 =   b0 ^((~b1)&  b2 );
    a21 =   b1 ^((~b2)&  b3 );
    a32 =   b2 ^((~b3)&  b4 );
    a43 =   b3 ^((~b4)&  b0 );
    a04 =   b4 ^((~b0)&  b1 );

    b3 = ROL64((a30^d0), 41);
    b4 = ROL64((a41^d1), 2);
    b0 = ROL64((a02^d2), 62);
    b1 = ROL64((a13^d3), 55);
    b2 = ROL64((a24^d4), 39);
    a30 =   b0 ^((~b1)&  b2 );
    a41 =   b1 ^((~b2)&  b3 );
    a02 =   b2 ^((~b3)&  b4 );
    a13 =   b3 ^((~b4)&  b0 );
    a24 =   b4 ^((~b0)&  b1 );
  }
}

/*
** Initialize a new hash.  iSize determines the size of the hash
** in bits and should be one of 224, 256, 384, or 512.  Or iSize
** can be zero to use the default hash size of 256 bits.
*/
static void HashInit(HashContext *p, int iSize){
  memset(p, 0, sizeof(*p));
  p->iSize = iSize;
  if( iSize>=128 && iSize<=512 ){
    p->nRate = (1600 - ((iSize + 31)&~31)*2)/8;
  }else{
    p->nRate = (1600 - 2*256)/8;
  }
#if Hash_BYTEORDER==1234
  /* Known to be little-endian at compile-time. No-op */
#elif Hash_BYTEORDER==4321
  p->ixMask = 7;  /* Big-endian */
#else
  {
    static unsigned int one = 1;
    if( 1==*(unsigned char*)&one ){
      /* Little endian.  No byte swapping. */
      p->ixMask = 0;
    }else{
      /* Big endian.  Byte swap. */
      p->ixMask = 7;
    }
  }
#endif
}

/*
** Make consecutive calls to the HashUpdate function to add new content
** to the hash
*/
static void HashUpdate(
  HashContext *p,
  const unsigned char *aData,
  unsigned int nData
){
  unsigned int i = 0;
  if( aData==0 ) return;
#if Hash_BYTEORDER==1234
  if( (p->nLoaded % 8)==0 && ((aData - (const unsigned char*)0)&7)==0 ){
    for(; i+7<nData; i+=8){
      p->u.s[p->nLoaded/8] ^= *(u64*)&aData[i];
      p->nLoaded += 8;
      if( p->nLoaded>=p->nRate ){
        KeccakF1600Step(p);
        p->nLoaded = 0;
      }
    }
  }
#endif
  for(; i<nData; i++){
#if Hash_BYTEORDER==1234
    p->u.x[p->nLoaded] ^= aData[i];
#elif Hash_BYTEORDER==4321
    p->u.x[p->nLoaded^0x07] ^= aData[i];
#else
    p->u.x[p->nLoaded^p->ixMask] ^= aData[i];
#endif
    p->nLoaded++;
    if( p->nLoaded==p->nRate ){
      KeccakF1600Step(p);
      p->nLoaded = 0;
    }
  }
}

/*
** After all content has been added, invoke HashFinal() to compute
** the final hash.  The function returns a pointer to the binary
** hash value.
*/
static unsigned char *HashFinal(HashContext *p){
  unsigned int i;
  if( p->nLoaded==p->nRate-1 ){
    const unsigned char c1 = 0x86;
    HashUpdate(p, &c1, 1);
  }else{
    const unsigned char c2 = 0x06;
    const unsigned char c3 = 0x80;
    HashUpdate(p, &c2, 1);
    p->nLoaded = p->nRate - 1;
    HashUpdate(p, &c3, 1);
  }
  for(i=0; i<p->nRate; i++){
    p->u.x[i+p->nRate] = p->u.x[i^p->ixMask];
  }
  return &p->u.x[p->nRate];
}

/*
** Implementation of the hash(X) function.
**
** Return a 160-bit BLOB which is the hash of X.
*/
static void hashFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  HashContext cx;
  int eType = sqlite3_value_type(argv[0]);
  int nByte = sqlite3_value_bytes(argv[0]);
  if( eType==SQLITE_NULL ) return;
  HashInit(&cx, 160);
  if( eType==SQLITE_BLOB ){
    HashUpdate(&cx, sqlite3_value_blob(argv[0]), nByte);
  }else{
    HashUpdate(&cx, sqlite3_value_text(argv[0]), nByte);
  }
  sqlite3_result_blob(context, HashFinal(&cx), 160/8, SQLITE_TRANSIENT);
}

/*
** Implementation of the agghash(X) function.
**
** Return a 160-bit BLOB which is the hash of the concatenation
** of all X inputs.
*/
static void agghashStep(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  HashContext *pCx;
  int eType = sqlite3_value_type(argv[0]);
  int nByte = sqlite3_value_bytes(argv[0]);
  if( eType==SQLITE_NULL ) return;
  pCx = (HashContext*)sqlite3_aggregate_context(context, sizeof(*pCx));
  if( pCx==0 ) return;
  if( pCx->iSize==0 ) HashInit(pCx, 160);
  if( eType==SQLITE_BLOB ){
    HashUpdate(pCx, sqlite3_value_blob(argv[0]), nByte);
  }else{
    HashUpdate(pCx, sqlite3_value_text(argv[0]), nByte);
  }
}
static void agghashFinal(sqlite3_context *context){
  HashContext *pCx = (HashContext*)sqlite3_aggregate_context(context, 0);
  if( pCx ){
    sqlite3_result_blob(context, HashFinal(pCx), 160/8, SQLITE_TRANSIENT);
  }
}

/* Register the hash function */
static int hashRegister(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "hash", 1,
                SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC,
                0, hashFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "agghash", 1,
                SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC,
                0, 0, agghashStep, agghashFinal);
  }
  return rc;
}

/* End of the hashing logic
*****************************************************************************/

/*
** Return the tail of a file pathname.  The tail is the last component
** of the path.  For example, the tail of "/a/b/c.d" is "c.d".
*/
const char *file_tail(const char *z){
  const char *zTail = z;
  if( !zTail ) return 0;
  while( z[0] ){
    if( z[0]=='/' ) zTail = &z[1];
    z++;
  }
  return zTail;
}

/*
** Append error message text to the error file, if an error file is
** specified.  In any case, increment the error count.
*/
static void logError(SQLiteRsync *p, const char *zFormat, ...){
  if( p->zErrFile ){
    FILE *pErr = fopen(p->zErrFile, "a");
    if( pErr ){
      va_list ap;
      va_start(ap, zFormat);
      vfprintf(pErr, zFormat, ap);
      va_end(ap);
      fclose(pErr);
    }
  }
  p->nErr++;
}

/*
** Append text to the debugging mesage file, if an that file is
** specified.
*/
static void debugMessage(SQLiteRsync *p, const char *zFormat, ...){
  if( p->zDebugFile ){
    if( p->pDebug==0 ){
      p->pDebug = fopen(p->zDebugFile, "wb");
    }
    if( p->pDebug ){
      va_list ap;
      va_start(ap, zFormat);
      vfprintf(p->pDebug, zFormat, ap);
      va_end(ap);
      fflush(p->pDebug);
    }
  }
}


/* Read a single big-endian 32-bit unsigned integer from the input
** stream.  Return 0 on success and 1 if there are any errors.
*/
static int readUint32(SQLiteRsync *p, unsigned int *pU){
  unsigned char buf[4];
  if( fread(buf, sizeof(buf), 1, p->pIn)==1 ){
    *pU = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
    p->nIn += 4;
    return 0;
  }else{
    logError(p, "failed to read a 32-bit integer\n");
    return 1;
  }
}

/* Write a single big-endian 32-bit unsigned integer to the output stream.
** Return 0 on success and 1 if there are any errors.
*/
static int writeUint32(SQLiteRsync *p, unsigned int x){
  unsigned char buf[4];
  buf[3] = x & 0xff;
  x >>= 8;
  buf[2] = x & 0xff;
  x >>= 8;
  buf[1] = x & 0xff;
  x >>= 8;
  buf[0] = x;
  if( p->pLog ) fwrite(buf, sizeof(buf), 1, p->pLog);
  if( fwrite(buf, sizeof(buf), 1, p->pOut)!=1 ){
    logError(p, "failed to write 32-bit integer 0x%x\n", x);
    p->nWrErr++;
    return 1;
  }
  p->nOut += 4;
  return 0;
}

/* Read a single byte from the wire.
*/
int readByte(SQLiteRsync *p){
  int c = fgetc(p->pIn);
  if( c!=EOF ) p->nIn++;
  return c;
}

/* Write a single byte into the wire.
*/
void writeByte(SQLiteRsync *p, int c){
  if( p->pLog ) fputc(c, p->pLog);
  fputc(c, p->pOut);
  p->nOut++;
}

/* Read a power of two encoded as a single byte.
*/
int readPow2(SQLiteRsync *p){
  int x = readByte(p);
  if( x<0 || x>=32 ){
    logError(p, "read invalid page size %d\n", x);
    return 0;
  }
  return 1<<x;
}

/* Write a power-of-two value onto the wire as a single byte.
*/
void writePow2(SQLiteRsync *p, int c){
  int n;
  if( c<0 || (c&(c-1))!=0 ){
    logError(p, "trying to read invalid page size %d\n", c);
  }
  for(n=0; c>1; n++){ c /= 2; }
  writeByte(p, n);
}

/* Read an array of bytes from the wire.
*/
void readBytes(SQLiteRsync *p, int nByte, void *pData){
  if( fread(pData, 1, nByte, p->pIn)==nByte ){
    p->nIn += nByte;
  }else{
    logError(p, "failed to read %d bytes\n", nByte);
  }
}

/* Write an array of bytes onto the wire.
*/
void writeBytes(SQLiteRsync *p, int nByte, const void *pData){
  if( p->pLog ) fwrite(pData, 1, nByte, p->pLog);
  if( fwrite(pData, 1, nByte, p->pOut)==nByte ){
    p->nOut += nByte;
  }else{
    logError(p, "failed to write %d bytes\n", nByte);
    p->nWrErr++;
  }
}

/* Report an error.
**
** If this happens on the remote side, we send back a *_ERROR
** message.  On the local side, the error message goes to stderr.
*/
static void reportError(SQLiteRsync *p, const char *zFormat, ...){
  va_list ap;
  char *zMsg;
  unsigned int nMsg;
  va_start(ap, zFormat);
  zMsg = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  nMsg = zMsg ? (unsigned int)strlen(zMsg) : 0;
  if( p->isRemote ){
    if( p->isReplica ){
      putc(REPLICA_ERROR, p->pOut);
    }else{
      putc(ORIGIN_ERROR, p->pOut);
    }
    writeUint32(p, nMsg);
    writeBytes(p, nMsg, zMsg);
    fflush(p->pOut);
  }else{
    fprintf(stderr, "%s\n", zMsg);
  }
  logError(p, "%s\n", zMsg);
  sqlite3_free(zMsg);
}

/* Send an informational message.
**
** If this happens on the remote side, we send back a *_MSG 
** message.  On the local side, the message goes to stdout.
*/
static void infoMsg(SQLiteRsync *p, const char *zFormat, ...){
  va_list ap;
  char *zMsg;
  unsigned int nMsg;
  va_start(ap, zFormat);
  zMsg = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  nMsg = zMsg ? (unsigned int)strlen(zMsg) : 0;
  if( p->isRemote ){
    if( p->isReplica ){
      putc(REPLICA_MSG, p->pOut);
    }else{
      putc(ORIGIN_MSG, p->pOut);
    }
    writeUint32(p, nMsg);
    writeBytes(p, nMsg, zMsg);
    fflush(p->pOut);
  }else{
    printf("%s\n", zMsg);
  }
  sqlite3_free(zMsg);
}

/* Receive and report an error message coming from the other side.
*/
static void readAndDisplayMessage(SQLiteRsync *p, int c){
  unsigned int n = 0;
  char *zMsg;
  const char *zPrefix;
  if( c==ORIGIN_ERROR || c==REPLICA_ERROR ){
    zPrefix = "ERROR: ";
  }else{
    zPrefix = "";
  }
  readUint32(p, &n);
  if( n==0 ){
    fprintf(stderr,"ERROR: unknown (possibly out-of-memory)\n");
  }else{
    zMsg = sqlite3_malloc64( n+1 );
    if( zMsg==0 ){
      fprintf(stderr, "ERROR: out-of-memory\n");
      return;
    }
    memset(zMsg, 0, n+1);
    readBytes(p, n, zMsg);
    fprintf(stderr,"%s%s\n", zPrefix, zMsg);
    if( zPrefix[0] ) logError(p, "%s%s\n", zPrefix, zMsg);
    sqlite3_free(zMsg);
  }
}

/* Construct a new prepared statement.  Report an error and return NULL
** if anything goes wrong.
*/
static sqlite3_stmt *prepareStmtVA(
  SQLiteRsync *p,
  char *zFormat,
  va_list ap
){
  sqlite3_stmt *pStmt = 0;
  char *zSql;
  char *zToFree = 0;
  int rc;

  if( strchr(zFormat,'%') ){
    zSql = sqlite3_vmprintf(zFormat, ap);
    if( zSql==0 ){
      reportError(p, "out-of-memory");
      return 0;
    }else{
      zToFree = zSql;
    }
  }else{
    zSql = zFormat;
  }
  rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, 0);
  if( rc || pStmt==0 ){
    reportError(p, "unable to prepare SQL [%s]: %s", zSql,
                sqlite3_errmsg(p->db));
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }
  if( zToFree ) sqlite3_free(zToFree);
  return pStmt;
}
static sqlite3_stmt *prepareStmt(
  SQLiteRsync *p,
  char *zFormat,
  ...
){
  sqlite3_stmt *pStmt;
  va_list ap;
  va_start(ap, zFormat);
  pStmt = prepareStmtVA(p, zFormat, ap);
  va_end(ap);
  return pStmt;
}

/* Run a single SQL statement.  Report an error if something goes
** wrong.
**
** As a special case, if the statement starts with "ATTACH" (but not
** "Attach") and if the error message is about an incorrect encoding,
** then do not report the error, but instead set the wrongEncoding flag.
** This is a kludgy work-around to the problem of attaching a database
** with a non-UTF8 encoding to the empty :memory: database that is
** opened on the replica.
*/
static void runSql(SQLiteRsync *p, char *zSql, ...){
  sqlite3_stmt *pStmt;
  va_list ap;

  va_start(ap, zSql);
  pStmt = prepareStmtVA(p, zSql, ap);
  va_end(ap);
  if( pStmt ){
    int rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ) rc = sqlite3_step(pStmt);
    if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
      const char *zErr = sqlite3_errmsg(p->db);
      if( strncmp(zSql,"ATTACH ", 7)==0
       && strstr(zErr,"must use the same text encoding")!=0
      ){
        p->wrongEncoding = 1;
      }else{
        reportError(p, "SQL statement [%s] failed: %s", zSql,
                    sqlite3_errmsg(p->db));
      }
    }
    sqlite3_finalize(pStmt);
  }
}

/* Run an SQL statement that returns a single unsigned 32-bit integer result
*/
static int runSqlReturnUInt(
  SQLiteRsync *p,
  unsigned int *pRes,
  char *zSql,
  ...
){
  sqlite3_stmt *pStmt;
  int res = 0;
  va_list ap;

  va_start(ap, zSql);
  pStmt = prepareStmtVA(p, zSql, ap);
  va_end(ap);
  if( pStmt==0 ){
    res = 1;
  }else{
    int rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      *pRes = (unsigned int)(sqlite3_column_int64(pStmt, 0)&0xffffffff);
    }else{
      reportError(p, "SQL statement [%s] failed: %s", zSql,
                  sqlite3_errmsg(p->db));
      res = 1;
    }
    sqlite3_finalize(pStmt);
  }
  return res;
}

/* Run an SQL statement that returns a single TEXT value that is no more
** than 99 bytes in length.
*/
static int runSqlReturnText(
  SQLiteRsync *p,
  char *pRes,
  char *zSql,
  ...
){
  sqlite3_stmt *pStmt;
  int res = 0;
  va_list ap;

  va_start(ap, zSql);
  pStmt = prepareStmtVA(p, zSql, ap);
  va_end(ap);
  pRes[0] = 0;
  if( pStmt==0 ){
    res = 1;
  }else{
    int rc = sqlite3_step(pStmt);
    if( rc==SQLITE_ROW ){
      const unsigned char *a = sqlite3_column_text(pStmt, 0);
      int n;
      if( a==0 ){
        pRes[0] = 0;
      }else{
        n = sqlite3_column_bytes(pStmt, 0);
        if( n>99 ) n = 99;
        memcpy(pRes, a, n);
        pRes[n] = 0;
      }
    }else{
      reportError(p, "SQL statement [%s] failed: %s", zSql,
                  sqlite3_errmsg(p->db));
      res = 1;
    }
    sqlite3_finalize(pStmt);
  }
  return res;
}

/* Close the database connection associated with p
*/
static void closeDb(SQLiteRsync *p){
  if( p->db ){
    sqlite3_close(p->db);
    p->db = 0;
  }
}

/*
** Run the origin-side protocol.
**
** Begin by sending the ORIGIN_BEGIN message with two arguments,
** nPage, and szPage.  Then enter a loop responding to message from
** the replica:
**
**    REPLICA_BEGIN  iProtocol
**
**         An optional message sent by the replica in response to the
**         prior ORIGIN_BEGIN with a counter-proposal for the protocol
**         level.  If seen, try to reduce the protocol level to what is
**         requested and send a new ORGIN_BEGIN.
**
**    REPLICA_ERROR  size  text
**
**         Report an error from the replica and quit
**
**    REPLICA_END
**
**         The replica is terminating.  Stop processing now.
**
**    REPLICA_HASH  hash
**
**         The argument is the 20-byte SHA1 hash for the next page or
**         block of pages.  Hashes appear in sequential order with no gaps,
**         unless there is an intervening REPLICA_CONFIG message.
**
**    REPLICA_CONFIG   pgno   cnt
**
**         Set counters used by REPLICA_HASH.  The next hash will start
**         on page pgno and all subsequent hashes will cover cnt pages
**         each.  Note that for a multi-page hash, the hash value is
**         actually a hash of the individual page hashes.
**
**    REPLICA_READY
**
**         The replica has sent all the hashes that it intends to send.
**         This side (the origin) can now start responding with page
**         content for pages that do not have a matching hash or with
**         ORIGIN_DETAIL messages with requests for more detail.
*/
static void originSide(SQLiteRsync *p){
  int rc = 0;
  int c = 0;
  unsigned int nPage = 0;
  unsigned int iHash = 1;               /* Pgno for next hash to receive */
  unsigned int nHash = 1;               /* Number of pages per hash received */
  unsigned int mxHash = 0;              /* Maximum hash value received */
  unsigned int lockBytePage = 0;
  unsigned int szPg = 0;
  sqlite3_stmt *pCkHash = 0;            /* Verify hash on a single page */
  sqlite3_stmt *pCkHashN = 0;           /* Verify a multi-page hash */
  sqlite3_stmt *pInsHash = 0;           /* Record a bad hash */
  char buf[200];

  p->isReplica = 0;
  if( p->bCommCheck ){
    infoMsg(p, "origin  zOrigin=%Q zReplica=%Q isRemote=%d protocol=%d",
               p->zOrigin, p->zReplica, p->isRemote, p->iProtocol);
    writeByte(p, ORIGIN_END);
    fflush(p->pOut);
  }else{
    /* Open the ORIGIN database. */
    rc = sqlite3_open_v2(p->zOrigin, &p->db, SQLITE_OPEN_READWRITE, 0);
    if( rc ){
      reportError(p, "cannot open origin \"%s\": %s",
                  p->zOrigin, sqlite3_errmsg(p->db));
      closeDb(p);
      return;
    }
    hashRegister(p->db);
    runSql(p, "BEGIN");
    if( p->bWalOnly ){
      runSqlReturnText(p, buf, "PRAGMA journal_mode");
      if( sqlite3_stricmp(buf,"wal")!=0 ){
        reportError(p, "Origin database is not in WAL mode");
      }
    }
    runSqlReturnUInt(p, &nPage, "PRAGMA page_count");
    runSqlReturnUInt(p, &szPg, "PRAGMA page_size");
  
    if( p->nErr==0 ){
      /* Send the ORIGIN_BEGIN message */
      writeByte(p, ORIGIN_BEGIN);
      writeByte(p, p->iProtocol);
      writePow2(p, szPg);
      writeUint32(p, nPage);
      fflush(p->pOut);
      if( p->zDebugFile ){
        debugMessage(p, "-> ORIGIN_BEGIN %u %u %u\n", p->iProtocol,szPg,nPage);
      }
      p->nPage = nPage;
      p->szPage = szPg;
      lockBytePage = (1<<30)/szPg + 1;
    }
  }
  
  /* Respond to message from the replica */
  while( p->nErr<=p->nWrErr && (c = readByte(p))!=EOF && c!=REPLICA_END ){
    switch( c ){
      case REPLICA_BEGIN: {
        /* This message is only sent if the replica received an origin-protocol
        ** that is larger than what it knows about.  The replica sends back
        ** a counter-proposal of an earlier protocol which the origin can
        ** accept by resending a new ORIGIN_BEGIN. */
        u8 newProtocol = readByte(p);
        if( p->zDebugFile ){
          debugMessage(p, "<- REPLICA_BEGIN %d\n", (int)newProtocol);
        }
        if( newProtocol < p->iProtocol ){
          p->iProtocol = newProtocol;
          writeByte(p, ORIGIN_BEGIN);
          writeByte(p, p->iProtocol);
          writePow2(p, p->szPage);
          writeUint32(p, p->nPage);
          fflush(p->pOut);
          if( p->zDebugFile ){
            debugMessage(p, "-> ORIGIN_BEGIN %d %d %u\n", p->iProtocol,
                         p->szPage, p->nPage);
          }
        }else{
          reportError(p, "Invalid REPLICA_BEGIN reply");
        }
        break;
      }
      case REPLICA_MSG:
      case REPLICA_ERROR: {
        readAndDisplayMessage(p, c);
        break;
      }
      case REPLICA_CONFIG: {
        readUint32(p, &iHash);
        readUint32(p, &nHash);
        if( p->zDebugFile ){
          debugMessage(p, "<- REPLICA_CONFIG %u %u\n", iHash, nHash);
        }
        break;
      }
      case REPLICA_HASH: {
        int bMatch = 0;
        if( pCkHash==0 ){
          runSql(p, "CREATE TEMP TABLE badHash("
                    " pgno INTEGER PRIMARY KEY,"
                    " sz INT)");
          pCkHash = prepareStmt(p,
            "SELECT hash(data)==?3 FROM sqlite_dbpage('main')"
            " WHERE pgno=?1"
          );
          if( pCkHash==0 ) break;
          pInsHash = prepareStmt(p, "INSERT INTO badHash VALUES(?1,?2)");
          if( pInsHash==0 ) break;
        }
        p->nHashSent++;
        readBytes(p, 20, buf);
        if( nHash>1 ){
          if( pCkHashN==0 ){
            pCkHashN = prepareStmt(p,
              "WITH c(n) AS "
              "  (VALUES(?1) UNION ALL SELECT n+1 FROM c WHERE n<?2)"
              "SELECT agghash(hash(data))==?3"
               " FROM c CROSS JOIN sqlite_dbpage('main') ON pgno=n"
            );
            if( pCkHashN==0 ) break;
          }
          sqlite3_bind_int64(pCkHashN, 1, iHash);
          sqlite3_bind_int64(pCkHashN, 2, iHash + nHash - 1);
          sqlite3_bind_blob(pCkHashN, 3, buf, 20, SQLITE_STATIC);
          rc = sqlite3_step(pCkHashN);
          if( rc==SQLITE_ROW ){
            bMatch = sqlite3_column_int(pCkHashN,0);
          }else if( rc==SQLITE_ERROR ){
            reportError(p, "SQL statement [%s] failed: %s",
                        sqlite3_sql(pCkHashN), sqlite3_errmsg(p->db));
          }
          sqlite3_reset(pCkHashN);
        }else{
          sqlite3_bind_int64(pCkHash, 1, iHash);
          sqlite3_bind_blob(pCkHash, 3, buf, 20, SQLITE_STATIC);
          rc = sqlite3_step(pCkHash);
          if( rc==SQLITE_ERROR ){
            reportError(p, "SQL statement [%s] failed: %s",
                        sqlite3_sql(pCkHash), sqlite3_errmsg(p->db));
          }else if( rc==SQLITE_ROW && sqlite3_column_int(pCkHash,0) ){
            bMatch = 1;
          }
          sqlite3_reset(pCkHash);
        }
        if( p->zDebugFile ){
          debugMessage(p, "<- REPLICA_HASH %u %u %s %08x...\n",
              iHash, nHash,
              bMatch ? "match" : "fail",
              *(unsigned int*)buf
          );
        }
        if( !bMatch ){
          sqlite3_bind_int64(pInsHash, 1, iHash);
          sqlite3_bind_int64(pInsHash, 2, nHash);
          rc = sqlite3_step(pInsHash);
          if( rc!=SQLITE_DONE ){
            reportError(p, "SQL statement [%s] failed: %s",
                sqlite3_sql(pInsHash), sqlite3_errmsg(p->db));
          }
          sqlite3_reset(pInsHash);
        }
        if( iHash+nHash>mxHash ) mxHash = iHash+nHash;
        iHash += nHash;
        break;
      }
      case REPLICA_READY: {
        int nMulti = 0;
        sqlite3_stmt *pStmt;
        if( p->zDebugFile ){
          debugMessage(p, "<- REPLICA_READY\n");
        }
        p->nRound++;
        pStmt = prepareStmt(p,"SELECT pgno, sz FROM badHash WHERE sz>1");
        if( pStmt==0 ) break;
        while( sqlite3_step(pStmt)==SQLITE_ROW ){
          unsigned int pgno = (unsigned int)sqlite3_column_int64(pStmt,0);
          unsigned int cnt = (unsigned int)sqlite3_column_int64(pStmt,1);
          writeByte(p, ORIGIN_DETAIL);
          writeUint32(p, pgno);
          writeUint32(p, cnt);
          nMulti++;
          if( p->zDebugFile ){
            debugMessage(p, "-> ORIGIN_DETAIL %u %u\n", pgno, cnt);
          }
        }
        sqlite3_finalize(pStmt);
        if( nMulti ){
          runSql(p, "DELETE FROM badHash WHERE sz>1");
          writeByte(p, ORIGIN_READY);
          if( p->zDebugFile ) debugMessage(p, "-> ORIGIN_READY\n");
        }else{
          sqlite3_finalize(pCkHash);
          sqlite3_finalize(pCkHashN);
          sqlite3_finalize(pInsHash);
          pCkHash = 0;
          pInsHash = 0;
          if( mxHash<=p->nPage ){
            runSql(p, "WITH RECURSIVE c(n) AS"
                      " (VALUES(%d) UNION ALL SELECT n+1 FROM c WHERE n<%d)"
                      " INSERT INTO badHash SELECT n, 1 FROM c",
                      mxHash, p->nPage);
          }
          runSql(p, "DELETE FROM badHash WHERE pgno=%d", lockBytePage);
          pStmt = prepareStmt(p,
                 "SELECT pgno, data"
                 "  FROM badHash JOIN sqlite_dbpage('main') USING(pgno)");
          if( pStmt==0 ) break;
          while( sqlite3_step(pStmt)==SQLITE_ROW
              && p->nErr==0
              && p->nWrErr==0
          ){
            unsigned int pgno = (unsigned int)sqlite3_column_int64(pStmt,0);
            const void *pContent = sqlite3_column_blob(pStmt, 1);
            writeByte(p, ORIGIN_PAGE);
            writeUint32(p, pgno);
            writeBytes(p, szPg, pContent);
            p->nPageSent++;
            if( p->zDebugFile ){
              debugMessage(p, "-> ORIGIN_PAGE %u\n", pgno);
            }
          }
          sqlite3_finalize(pStmt);
          writeByte(p, ORIGIN_TXN);
          writeUint32(p, nPage);
          if( p->zDebugFile ){
            debugMessage(p, "-> ORIGIN_TXN %u\n", nPage);
          }
          writeByte(p, ORIGIN_END);
        }
        fflush(p->pOut);
        break;
      }
      default: {
        reportError(p, "Unknown message 0x%02x %lld bytes into conversation",
                       c, p->nIn);
        break;
      }
    }
  }

  if( pCkHash ) sqlite3_finalize(pCkHash);
  if( pInsHash ) sqlite3_finalize(pInsHash);
  closeDb(p);
}

/*
** Send a REPLICA_HASH message for each entry in the sendHash table.
** The sendHash table looks like this:
**
**   CREATE TABLE sendHash(
**      fpg INTEGER PRIMARY KEY,   -- Page number of the hash
**      npg INT                    -- Number of pages in this hash
**   );
**
** If iHash is page number for the next page that the origin will
** be expecting, and nHash is the number of pages that the origin will
** be expecting in the hash that follows.  Send a REPLICA_CONFIG message
** if either of these values if not correct.
*/
static void sendHashMessages(
  SQLiteRsync *p,       /* The replica-side of the sync */
  unsigned int iHash,   /* Next page expected by origin */
  unsigned int nHash    /* Next number of pages expected by origin */
){
  sqlite3_stmt *pStmt;
  pStmt = prepareStmt(p,
    "SELECT if(npg==1,"
    "  (SELECT hash(data) FROM sqlite_dbpage('replica') WHERE pgno=fpg),"
    "  (WITH RECURSIVE c(n) AS"
    "     (SELECT fpg UNION ALL SELECT n+1 FROM c WHERE n<fpg+npg-1)"
    "   SELECT agghash(hash(data))"
    "     FROM c CROSS JOIN sqlite_dbpage('replica') ON pgno=n)) AS hash,"
    "  fpg,"
    "  npg"
    " FROM sendHash ORDER BY fpg"
  );
  while( sqlite3_step(pStmt)==SQLITE_ROW && p->nErr==0 && p->nWrErr==0 ){
    const unsigned char *a = sqlite3_column_blob(pStmt, 0);
    unsigned int pgno = (unsigned int)sqlite3_column_int64(pStmt, 1);
    unsigned int npg = (unsigned int)sqlite3_column_int64(pStmt, 2);
    if( pgno!=iHash || npg!=nHash ){
      writeByte(p, REPLICA_CONFIG);
      writeUint32(p, pgno);
      writeUint32(p, npg);
      if( p->zDebugFile ){
        debugMessage(p, "-> REPLICA_CONFIG %u %u\n", pgno, npg);
      }
    }
    if( a==0 ){
      if( p->zDebugFile ){
        debugMessage(p, "# Oops: No hash for %u %u\n", pgno, npg);
      }
    }else{
      writeByte(p, REPLICA_HASH);
      writeBytes(p, 20, a);
      if( p->zDebugFile ){
        debugMessage(p, "-> REPLICA_HASH %u %u (%08x...)\n",
        pgno, npg, *(unsigned int*)a);
      }
    }
    p->nHashSent++;
    iHash = pgno + npg;
    nHash = npg;
  }
  sqlite3_finalize(pStmt);
  runSql(p, "DELETE FROM sendHash");
  writeByte(p, REPLICA_READY);
  fflush(p->pOut);
  p->nRound++;
  if( p->zDebugFile ) debugMessage(p, "-> REPLICA_READY\n", iHash);
}

/*
** Make entries in the sendHash table to send hashes for
** npg (mnemonic: Number of PaGes) pages starting with fpg
** (mnemonic: First PaGe).
*/
static void subdivideHashRange(
  SQLiteRsync *p,       /* The replica-side of the sync */
  unsigned int fpg,     /* First page of the range */
  unsigned int npg      /* Number of pages */
){
  unsigned int nChunk;   /* How many pages to request per hash */
  sqlite3_uint64 iEnd;   /* One more than the last page */
  if( npg<=30 ){
    nChunk = 1;
  }else if( npg<=1000 ){
    nChunk = 30;
  }else{
    nChunk = 1000;
  }
  iEnd = fpg;
  iEnd += npg;
  runSql(p,
    "WITH RECURSIVE c(n) AS"
    "  (VALUES(%u) UNION ALL SELECT n+%u FROM c WHERE n<%llu)"
    "REPLACE INTO sendHash(fpg,npg)"
    " SELECT n, min(%llu-n,%u) FROM c",
    fpg, nChunk, iEnd-nChunk, iEnd, nChunk
  );
}

/*
** Run the replica-side protocol.  The protocol is passive in the sense
** that it only response to message from the origin side.
**
**    ORIGIN_BEGIN  idProtocol szPage nPage
**
**         The origin is reporting the protocol version number, the size of
**         each page in the origin database (sent as a single-byte power-of-2),
**         and the number of pages in the origin database.
**         This procedure checks compatibility, and if everything is ok,
**         it starts sending hashes back to the origin using REPLICA_HASH
**         and/or REPLICA_CONFIG message, followed by a single REPLICA_READY.
**         REPLICA_CONFIG is only sent if the protocol is 2 or greater.
**
**    ORIGIN_ERROR  size  text
**
**         Report an error and quit.
**
**    ORIGIN_DETAIL  pgno  cnt
**
**         The origin reports that a multi-page hash starting at pgno and
**         spanning cnt pages failed to match.  The origin is requesting
**         details (more REPLICA_HASH message with a smaller cnt).  The
**         replica must wait on ORIGIN_READY before sending its reply.
**
**    ORIGIN_READY
**
**         After sending one or more ORIGIN_DETAIL messages, the ORIGIN_READY
**         is sent by the origin to indicate that it has finished sending
**         requests for detail and is ready for the replicate to reply
**         with a new round of REPLICA_CONFIG and REPLICA_HASH messages.
**
**    ORIGIN_PAGE  pgno  content
**
**         Once the origin believes it knows exactly which pages need to be
**         updated in the replica, it starts sending those pages using these
**         messages.  These messages will only appear immediately after
**         REPLICA_READY.  The origin never mixes ORIGIN_DETAIL and
**         ORIGIN_PAGE messages in the same batch.
**
**    ORIGIN_TXN   pgno
**
**         Close the update transaction.  The total database size is pgno
**         pages.
**
**    ORIGIN_END
**
**         Expect no more transmissions from the origin.
*/
static void replicaSide(SQLiteRsync *p){
  int c;
  sqlite3_stmt *pIns = 0;
  unsigned int szOPage = 0;
  char eJMode = 0;               /* Journal mode prior to sync */
  char buf[65536];

  p->isReplica = 1;
  if( p->bCommCheck ){
    infoMsg(p, "replica zOrigin=%Q zReplica=%Q isRemote=%d protocol=%d",
               p->zOrigin, p->zReplica, p->isRemote, p->iProtocol);
    writeByte(p, REPLICA_END);
    fflush(p->pOut);
  }
  if( p->iProtocol<=0 ) p->iProtocol = PROTOCOL_VERSION;

  /* Respond to message from the origin.  The origin will initiate the
  ** the conversation with an ORIGIN_BEGIN message.
  */
  while( p->nErr<=p->nWrErr && (c = readByte(p))!=EOF && c!=ORIGIN_END ){
    switch( c ){
      case ORIGIN_MSG:
      case ORIGIN_ERROR: {
        readAndDisplayMessage(p, c);
        break;
      }
      case ORIGIN_BEGIN: {
        unsigned int nOPage = 0;
        unsigned int nRPage = 0, szRPage = 0;
        int rc = 0;
        u8 iProtocol;

        closeDb(p);
        iProtocol = readByte(p);
        szOPage = readPow2(p);
        readUint32(p, &nOPage);
        if( p->zDebugFile ){
          debugMessage(p, "<- ORIGIN_BEGIN %d %d %u\n", iProtocol, szOPage,
                       nOPage);
        }
        if( p->nErr ) break;
        if( iProtocol>p->iProtocol ){
          /* If the protocol version on the origin side is larger, send back
          ** a REPLICA_BEGIN message with the protocol version number of the
          ** replica side.  This gives the origin an opportunity to resend
          ** a new ORIGIN_BEGIN with a reduced protocol version. */
          writeByte(p, REPLICA_BEGIN);
          writeByte(p, p->iProtocol);
          fflush(p->pOut);
          if( p->zDebugFile ){
            debugMessage(p, "-> REPLICA_BEGIN %u\n", p->iProtocol);
          }
          break;
        }
        p->iProtocol = iProtocol;
        p->nPage = nOPage;
        p->szPage = szOPage;
        rc = sqlite3_open(":memory:", &p->db);
        if( rc ){
          reportError(p, "cannot open in-memory database: %s",
                      sqlite3_errmsg(p->db));
          closeDb(p);
          break;
        }
        sqlite3_db_config(p->db, SQLITE_DBCONFIG_WRITABLE_SCHEMA, 1, 0);
        runSql(p, "ATTACH %Q AS 'replica'", p->zReplica);
        if( p->wrongEncoding ){
          p->wrongEncoding = 0;
          runSql(p, "PRAGMA encoding=utf16le");
          runSql(p, "ATTACH %Q AS 'replica'", p->zReplica);
          if( p->wrongEncoding ){
            p->wrongEncoding = 0;
            runSql(p, "PRAGMA encoding=utf16be");
            runSql(p, "Attach %Q AS 'replica'", p->zReplica);
          }
        }
        if( p->nErr ){
          closeDb(p);
          break;
        }
        runSql(p,
          "CREATE TABLE sendHash("
          "  fpg INTEGER PRIMARY KEY,"   /* The page number of hash to send */
          "  npg INT"                    /* Number of pages in this hash */
          ")"
        );
        hashRegister(p->db);
        if( runSqlReturnUInt(p, &nRPage, "PRAGMA replica.page_count") ){
          break;
        }
        if( nRPage==0 ){
          runSql(p, "PRAGMA replica.page_size=%u", szOPage);
          runSql(p, "SELECT * FROM replica.sqlite_schema");
        }
        runSql(p, "BEGIN IMMEDIATE");
        runSqlReturnText(p, buf, "PRAGMA replica.journal_mode");
        if( strcmp(buf, "wal")!=0 ){
          if( p->bWalOnly && nRPage>0 ){
            reportError(p, "replica is not in WAL mode");
            break;
          }
          eJMode = 1;      /* Non-WAL mode prior to sync */
        }else{
          eJMode = 2;      /* WAL-mode prior to sync */
        }
        runSqlReturnUInt(p, &nRPage, "PRAGMA replica.page_count");
        runSqlReturnUInt(p, &szRPage, "PRAGMA replica.page_size");
        if( szRPage!=szOPage ){
          reportError(p, "page size mismatch; origin is %d bytes and "
                         "replica is %d bytes", szOPage, szRPage);
          break;
        }
        if( p->iProtocol<2 || nRPage<=100 ){
          runSql(p,
            "WITH RECURSIVE c(n) AS"
              "(VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<%d)"
            "INSERT INTO sendHash(fpg, npg) SELECT n, 1 FROM c",
            nRPage);
        }else{
          runSql(p,"INSERT INTO sendHash VALUES(1,1)");
          subdivideHashRange(p, 2, nRPage-1);
        }
        sendHashMessages(p, 1, 1);
        runSql(p, "PRAGMA writable_schema=ON");
        break;
      }
      case ORIGIN_DETAIL: {
        unsigned int fpg, npg;
        readUint32(p, &fpg);
        readUint32(p, &npg);
        if( p->zDebugFile ){
          debugMessage(p, "<- ORIGIN_DETAIL %u %u\n", fpg, npg);
        }
        subdivideHashRange(p, fpg, npg);
        break;
      }
      case ORIGIN_READY: {
        if( p->zDebugFile ){
          debugMessage(p, "<- ORIGIN_READY\n");
        }
        sendHashMessages(p, 0, 0);
        break;
      }
      case ORIGIN_TXN: {
        unsigned int nOPage = 0;
        readUint32(p, &nOPage);
        if( p->zDebugFile ){
          debugMessage(p, "<- ORIGIN_TXN %u\n", nOPage);
        }
        if( pIns==0 ){
          /* Nothing has changed */
          runSql(p, "COMMIT");
        }else if( p->nErr ){
          runSql(p, "ROLLBACK");
        }else{
          if( nOPage<0xffffffff ){
            int rc;
            sqlite3_bind_int64(pIns, 1, nOPage+1);
            sqlite3_bind_null(pIns, 2);
            rc = sqlite3_step(pIns);
            if( rc!=SQLITE_DONE ){
              reportError(p,
                  "SQL statement [%s] failed (pgno=%u, data=NULL): %s",
                  sqlite3_sql(pIns), nOPage, sqlite3_errmsg(p->db));
            }
            sqlite3_reset(pIns);
          }
          p->nPage = nOPage;
          runSql(p, "COMMIT");
        }
        break;
      }
      case ORIGIN_PAGE: {
        unsigned int pgno = 0;
        int rc;
        readUint32(p, &pgno);
        if( p->zDebugFile ){
          debugMessage(p, "<- ORIGIN_PAGE %u\n", pgno);
        }
        if( p->nErr ) break;
        if( pIns==0 ){
          pIns = prepareStmt(p,
            "INSERT INTO sqlite_dbpage(pgno,data,schema)VALUES(?1,?2,'replica')"
          );
          if( pIns==0 ) break;
        }
        readBytes(p, szOPage, buf);
        if( p->nErr ) break;
        if( pgno==1 &&  eJMode==2 && buf[18]==1 ){
          /* Do not switch the replica out of WAL mode if it started in 
          ** WAL mode */
          buf[18] = buf[19] = 2;
        }
        p->nPageSent++;
        sqlite3_bind_int64(pIns, 1, pgno);
        sqlite3_bind_blob(pIns, 2, buf, szOPage, SQLITE_STATIC);
        rc = sqlite3_step(pIns);
        if( rc!=SQLITE_DONE ){
          reportError(p, "SQL statement [%s] failed (pgno=%u): %s",
                 sqlite3_sql(pIns), pgno, sqlite3_errmsg(p->db));
        }
        sqlite3_reset(pIns);
        break;
      }
      default: {
        reportError(p, "Unknown message 0x%02x %lld bytes into conversation",
                       c, p->nIn);
        break;
      }
    }
  }

  if( pIns ) sqlite3_finalize(pIns);
  closeDb(p);
}

/*
** The argument might be -vvv...vv with any number of "v"s.  Return
** the number of "v"s.  Return 0 if the argument is not a -vvv...v.
*/
static int numVs(const char *z){
  int n = 0;
  if( z[0]!='-' ) return 0;
  z++;
  if( z[0]=='-' ) z++;
  while( z[0]=='v' ){ n++; z++; }
  if( z[0]==0 ) return n;
  return 0;
}

/*
** Get the argument to an --option.  Throw an error and die if no argument
** is available.
*/
static const char *cmdline_option_value(int argc, const char * const*argv,
                                        int i){
  if( i==argc ){
    fprintf(stderr,"%s: Error: missing argument to %s\n",
            argv[0], argv[argc-1]);
    exit(1);
  }
  return argv[i];
}

/*
** Return the current time in milliseconds since the Julian epoch.
*/
sqlite3_int64 currentTime(void){
  sqlite3_int64 now = 0;
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  if( pVfs && pVfs->iVersion>=2 && pVfs->xCurrentTimeInt64!=0 ){
    pVfs->xCurrentTimeInt64(pVfs, &now);
  }
  return now;
}

/*
** Input string zIn might be in any of these formats:
**
**    (1) PATH
**    (2) HOST:PATH
**    (3) USER@HOST:PATH
**
** For format 1, return NULL.  For formats 2 and 3, return
** a pointer to the ':' character that separates the hostname
** from the path.
*/
static char *hostSeparator(const char *zIn){
  char *zPath = strchr(zIn, ':');
  if( zPath==0 ) return 0;
#ifdef _WIN32
  if( isalpha(zIn[0]) && zIn[1]==':' && (zIn[2]=='/' || zIn[2]=='\\') ){
    return 0;
  }
#endif
  while( zIn<zPath ){
    if( zIn[0]=='/' ) return 0;
    if( zIn[0]=='\\' ) return 0;
    zIn++;
  }
  return zPath;
}


/*
** Parse command-line arguments.  Dispatch subroutines to do the
** requested work.
**
** Input formats:
**
**  (1)    sqlite3_rsync  FILENAME1  USER@HOST:FILENAME2
**
**  (2)    sqlite3_rsync  USER@HOST:FILENAME1  FILENAME2
**
**  (3)    sqlite3_rsync --origin FILENAME1
**
**  (4)    sqlite3_rsync --replica FILENAME2
**
** The user types (1) or (2).  SSH launches (3) or (4).
**
** If (1) is seen then popen2 is used launch (4) on the remote and
** originSide() is called locally.
**
** If (2) is seen, then popen2() is used to launch (3) on the remote
** and replicaSide() is run locally.
**
** If (3) is seen, call originSide() on stdin and stdout.
**
q** If (4) is seen, call replicaSide() on stdin and stdout.
*/
int main(int argc, char const * const *argv){
  int isOrigin = 0;
  int isReplica = 0;
  int i;
  SQLiteRsync ctx;
  char *zDiv;
  FILE *pIn = 0;
  FILE *pOut = 0;
  int childPid = 0;
  const char *zSsh = "ssh";
  const char *zExe = "sqlite3_rsync";
  char *zCmd = 0;
  sqlite3_int64 tmStart;
  sqlite3_int64 tmEnd;
  sqlite3_int64 tmElapse;
  const char *zRemoteErrFile = 0;
  const char *zRemoteDebugFile = 0;

#define cli_opt_val cmdline_option_value(argc, argv, ++i)
  memset(&ctx, 0, sizeof(ctx));
  ctx.iProtocol = PROTOCOL_VERSION;
  sqlite3_initialize();
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' && z[1]=='-' && z[2]!=0 ) z++;
    if( strcmp(z,"-origin")==0 ){
      isOrigin = 1;
      continue;
    }
    if( strcmp(z,"-replica")==0 ){
      isReplica = 1;
      continue;
    }
    if( numVs(z) ){
      ctx.eVerbose += numVs(z);
      continue;
    }
    if( strcmp(z, "-protocol")==0 ){
      ctx.iProtocol = atoi(cli_opt_val);
      if( ctx.iProtocol<1 ){
        ctx.iProtocol = 1;
      }else if( ctx.iProtocol>PROTOCOL_VERSION ){
        ctx.iProtocol = PROTOCOL_VERSION;
      }
      continue;
    }
    if( strcmp(z, "-ssh")==0 ){
      zSsh = cli_opt_val;
      continue;
    }
    if( strcmp(z, "-exe")==0 ){
      zExe = cli_opt_val;
      continue;
    }
    if( strcmp(z, "-wal-only")==0 ){
      ctx.bWalOnly = 1;
      continue;
    }
    if( strcmp(z, "-version")==0 ){
      printf("%s\n", sqlite3_sourceid());
      return 0;
    }
    if( strcmp(z, "-help")==0 || strcmp(z, "--help")==0
     || strcmp(z, "-?")==0
    ){
      printf("%s", zUsage);
      return 0;
    }
    if( strcmp(z, "-logfile")==0 ){
      /* DEBUG OPTION:  --logfile FILENAME
      ** Cause all local output traffic to be duplicated in FILENAME */
      const char *zLog = cli_opt_val;
      if( ctx.pLog ) fclose(ctx.pLog);
      ctx.pLog = fopen(zLog, "wb");
      if( ctx.pLog==0 ){
        fprintf(stderr, "cannot open \"%s\" for writing\n", argv[i]);
        return 1;
      }
      continue;
    }
    if( strcmp(z, "-errorfile")==0 ){
      /* DEBUG OPTION:  --errorfile FILENAME
      ** Error messages on the local side are written into FILENAME */
      ctx.zErrFile = cli_opt_val;
      continue;
    }
    if( strcmp(z, "-remote-errorfile")==0 ){
      /* DEBUG OPTION:  --remote-errorfile FILENAME
      ** Error messages on the remote side are written into FILENAME on
      ** the remote side. */
      zRemoteErrFile = cli_opt_val;
      continue;
    }
    if( strcmp(z, "-debugfile")==0 ){
      /* DEBUG OPTION:  --debugfile FILENAME
      ** Debugging messages on the local side are written into FILENAME */
      ctx.zDebugFile = cli_opt_val;
      continue;
    }
    if( strcmp(z, "-remote-debugfile")==0 ){
      /* DEBUG OPTION:  --remote-debugfile FILENAME
      ** Error messages on the remote side are written into FILENAME on
      ** the remote side. */
      zRemoteDebugFile = cli_opt_val;
      continue;
    }
    if( strcmp(z,"-commcheck")==0 ){  /* DEBUG ONLY */
      /* Run a communication check with the remote side.  Do not attempt
      ** to exchange any database connection */
      ctx.bCommCheck = 1;
      continue;
    }
    if( strcmp(z,"-arg-escape-check")==0 ){  /* DEBUG ONLY */
      /* Test the append_escaped_arg() routine by using it to render a
      ** copy of the input command-line, assuming all arguments except
      ** this one are filenames. */
      sqlite3_str *pStr = sqlite3_str_new(0);
      int k;
      for(k=0; k<argc; k++){
        append_escaped_arg(pStr, argv[k], i!=k);
      }
      printf("%s\n", sqlite3_str_value(pStr));
      return 0;
    }
    if( z[i]=='-' ){
      fprintf(stderr,
         "unknown option: \"%s\". Use --help for more detail.\n", z);
      return 1;
    }
    if( ctx.zOrigin==0 ){
      ctx.zOrigin = z;
    }else if( ctx.zReplica==0 ){
      ctx.zReplica = z;
    }else{
      fprintf(stderr, "Unknown argument: \"%s\"\n", z);
      return 1;
    }
  }
  if( ctx.zOrigin==0 ){
    fprintf(stderr, "missing ORIGIN database filename\n");
    return 1;
  }
  if( ctx.zReplica==0 ){
    fprintf(stderr, "missing REPLICA database filename\n");
    return 1;
  }
  if( isOrigin && isReplica ){
    fprintf(stderr, "bad option combination\n");
    return 1;
  }
  if( isOrigin ){
    ctx.pIn = stdin;
    ctx.pOut = stdout;
    ctx.isRemote = 1;
#ifdef _WIN32
    _setmode(_fileno(ctx.pIn), _O_BINARY);
    _setmode(_fileno(ctx.pOut), _O_BINARY);
#endif
    originSide(&ctx);
    return 0;
  }
  if( isReplica ){
    ctx.pIn = stdin;
    ctx.pOut = stdout;
    ctx.isRemote = 1;
#ifdef _WIN32
    _setmode(_fileno(ctx.pIn), _O_BINARY);
    _setmode(_fileno(ctx.pOut), _O_BINARY);
#endif
    replicaSide(&ctx);
    return 0;
  }
  if( ctx.zReplica==0 ){
    fprintf(stderr, "missing REPLICA database filename\n");
    return 1;
  }
  tmStart = currentTime();
  zDiv = hostSeparator(ctx.zOrigin);
  if( zDiv ){
    int iRetry;
    if( hostSeparator(ctx.zReplica)!=0 ){
      fprintf(stderr,
         "At least one of ORIGIN and REPLICA must be a local database\n"
         "You provided two remote databases.\n");
      return 1;
    }
    *(zDiv++) = 0;
    /* Remote ORIGIN and local REPLICA */
    for(iRetry=0; 1 /*exit-via-break*/; iRetry++){
      sqlite3_str *pStr = sqlite3_str_new(0);
      append_escaped_arg(pStr, zSsh, 1);
      sqlite3_str_appendf(pStr, " -e none");
      append_escaped_arg(pStr, ctx.zOrigin, 0);
      if( iRetry ) add_path_argument(pStr);
      append_escaped_arg(pStr, zExe, 1);
      append_escaped_arg(pStr, "--origin", 0);
      if( ctx.bCommCheck ){
        append_escaped_arg(pStr, "--commcheck", 0);
        if( ctx.eVerbose==0 ) ctx.eVerbose = 1;
      }
      if( zRemoteErrFile ){
        append_escaped_arg(pStr, "--errorfile", 0);
        append_escaped_arg(pStr, zRemoteErrFile, 1);
      }
      if( zRemoteDebugFile ){
        append_escaped_arg(pStr, "--debugfile", 0);
        append_escaped_arg(pStr, zRemoteDebugFile, 1);
      }
      if( ctx.bWalOnly ){
        append_escaped_arg(pStr, "--wal-only", 0);
      }
      append_escaped_arg(pStr, zDiv, 1);
      append_escaped_arg(pStr, file_tail(ctx.zReplica), 1);
      if( ctx.eVerbose<2 && iRetry==0 ){
        append_escaped_arg(pStr, "2>/dev/null", 0);
      }
      zCmd = sqlite3_str_finish(pStr);
      if( ctx.eVerbose>=2 ) printf("%s\n", zCmd);
      if( popen2(zCmd, &ctx.pIn, &ctx.pOut, &childPid, 0) ){
        if( iRetry>=1 ){
          fprintf(stderr, "Could not start auxiliary process: %s\n", zCmd);
          return 1;
        }
        if( ctx.eVerbose>=2 ){
          printf("ssh FAILED.  Retry with a PATH= argument...\n");
        }
        continue;
      }
      replicaSide(&ctx);
      if( ctx.nHashSent==0 && iRetry==0 ) continue;
      break;
    }
  }else if( (zDiv = hostSeparator(ctx.zReplica))!=0 ){
    /* Local ORIGIN and remote REPLICA */
    int iRetry;
    *(zDiv++) = 0;
    for(iRetry=0; 1 /*exit-by-break*/; iRetry++){
      sqlite3_str *pStr = sqlite3_str_new(0);
      append_escaped_arg(pStr, zSsh, 1);
      sqlite3_str_appendf(pStr, " -e none");
      append_escaped_arg(pStr, ctx.zReplica, 0);
      if( iRetry==1 ) add_path_argument(pStr);
      append_escaped_arg(pStr, zExe, 1);
      append_escaped_arg(pStr, "--replica", 0);
      if( ctx.bCommCheck ){
        append_escaped_arg(pStr, "--commcheck", 0);
        if( ctx.eVerbose==0 ) ctx.eVerbose = 1;
      }
      if( zRemoteErrFile ){
        append_escaped_arg(pStr, "--errorfile", 0);
        append_escaped_arg(pStr, zRemoteErrFile, 1);
      }
      if( zRemoteDebugFile ){
        append_escaped_arg(pStr, "--debugfile", 0);
        append_escaped_arg(pStr, zRemoteDebugFile, 1);
      }
      if( ctx.bWalOnly ){
        append_escaped_arg(pStr, "--wal-only", 0);
      }
      append_escaped_arg(pStr, file_tail(ctx.zOrigin), 1);
      append_escaped_arg(pStr, zDiv, 1);
      if( ctx.eVerbose<2 && iRetry==0 ){
        append_escaped_arg(pStr, "2>/dev/null", 0);
      }
      zCmd = sqlite3_str_finish(pStr);
      if( ctx.eVerbose>=2 ) printf("%s\n", zCmd);
      if( popen2(zCmd, &ctx.pIn, &ctx.pOut, &childPid, 0) ){
        if( iRetry>=1 ){
          fprintf(stderr, "Could not start auxiliary process: %s\n", zCmd);
          return 1;
        }else if( ctx.eVerbose>=2 ){
          printf("ssh FAILED.  Retry with a PATH= argument...\n");
        }
        continue;
      }
      originSide(&ctx);
      if( ctx.nHashSent==0 && iRetry==0 ) continue;
      break;
    }
  }else{
    /* Local ORIGIN and REPLICA */
    sqlite3_str *pStr = sqlite3_str_new(0);
    append_escaped_arg(pStr, argv[0], 1);
    append_escaped_arg(pStr, "--replica", 0);
    if( ctx.bCommCheck ){
      append_escaped_arg(pStr, "--commcheck", 0);
    }
    if( zRemoteErrFile ){
      append_escaped_arg(pStr, "--errorfile", 0);
      append_escaped_arg(pStr, zRemoteErrFile, 1);
    }
    if( zRemoteDebugFile ){
      append_escaped_arg(pStr, "--debugfile", 0);
      append_escaped_arg(pStr, zRemoteDebugFile, 1);
    }
    append_escaped_arg(pStr, ctx.zOrigin, 1);
    append_escaped_arg(pStr, ctx.zReplica, 1);
    zCmd = sqlite3_str_finish(pStr);
    if( ctx.eVerbose>=2 ) printf("%s\n", zCmd);
    if( popen2(zCmd, &ctx.pIn, &ctx.pOut, &childPid, 0) ){
      fprintf(stderr, "Could not start auxiliary process: %s\n", zCmd);
      return 1;
    }
    originSide(&ctx);
  }
  pclose2(ctx.pIn, ctx.pOut, childPid);
  if( ctx.pLog ) fclose(ctx.pLog);
  tmEnd = currentTime();
  tmElapse = tmEnd - tmStart;  /* Elapse time in milliseconds */
  if( ctx.nErr ){
    printf("Databases were not synced due to errors\n");
  }
  if( ctx.eVerbose>=1 ){
    char *zMsg;
    sqlite3_int64 szTotal = (sqlite3_int64)ctx.nPage*(sqlite3_int64)ctx.szPage;
    sqlite3_int64 nIO = ctx.nOut +ctx.nIn;
    zMsg = sqlite3_mprintf("sent %,lld bytes, received %,lld bytes",
                           ctx.nOut, ctx.nIn);
    printf("%s", zMsg);
    sqlite3_free(zMsg);
    if( tmElapse>0 ){
      zMsg = sqlite3_mprintf(", %,.2f bytes/sec",
                             1000.0*(double)nIO/(double)tmElapse);
      printf("%s\n", zMsg);
      sqlite3_free(zMsg);
    }else{
      printf("\n");
    }
    if( ctx.nErr==0 ){
      if( nIO<=szTotal && nIO>0 ){
        zMsg = sqlite3_mprintf("total size %,lld  speedup is %.2f",
           szTotal, (double)szTotal/(double)nIO);
      }else{
        zMsg = sqlite3_mprintf("total size %,lld", szTotal);
      }
      printf("%s\n", zMsg);
      sqlite3_free(zMsg);
    }
    if( ctx.eVerbose>=3 ){
      printf("hashes: %lld  hash-rounds: %d"
             "  page updates: %d  protocol: %d\n",
             ctx.nHashSent, ctx.nRound, ctx.nPageSent, ctx.iProtocol);
    }
  }
  sqlite3_free(zCmd);
  if( pIn!=0 && pOut!=0 ){
    pclose2(pIn, pOut, childPid);
  }
  sqlite3_shutdown();
  return ctx.nErr;
}
