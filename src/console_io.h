/*
** 2023 November 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file exposes various interfaces used for console I/O by the
** SQLite project command-line tools. These interfaces are used at
** either source conglomeration time, compilation time, or run time.
** This source provides for either inclusion into conglomerated,
** "single-source" forms or separate compilation then linking. (TBD)
**
** Platform dependencies are "hidden" here by various stratagems so
** that, provided certain conditions are met, the programs using
** this source or object code compiled from it need no explicit
** conditional compilation in their source for their console I/O.
**
** The symbols and functionality exposed here are not a public API.
** This code may change in tandem with other project code as needed.
*/

#ifndef INT_LINKAGE
# define INT_LINKAGE extern /* Linkage external to translation unit. */
# include <stdio.h>
# include <stdlib.h>
# include <limits.h>
# if defined(_WIN32) || defined(WIN32)
#  undef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <io.h>
#  include <fcntl.h>
# endif
#else
# define SHELL_NO_SYSINC /* Better yet, modify mkshellc.tcl for this. */
#endif

#ifndef SQLITE3_H
# include "sqlite3.h"
#endif

/* Define enum for use with following function. */
typedef enum ConsoleStdConsStreams {
  CSCS_NoConsole = 0,
  CSCS_InConsole = 1, CSCS_OutConsole = 2, CSCS_ErrConsole = 4,
  CSCS_AnyConsole = 0x7
} ConsoleStdConsStreams;

/*
** Classify the three standard I/O streams according to whether
** they are connected to a console attached to the process.
**
** Returns the bit-wise OR of CSCS_{In,Out,Err}Console values,
** or CSCS_NoConsole if none of the streams reaches a console.
**
** This function should be called before any I/O is done with
** the given streams. As a side-effect, the given inputs are
** recorded so that later I/O operations on them may be done
** differently than the C library FILE* I/O would be done,
** iff the stream is used for the I/O functions that follow.
**
** On some platforms, stream or console mode alteration (aka
** "Setup") may be made which is undone by consoleRestore().
**
** Applications which run an inferior (child) process which
** inherits the same I/O streams may call this function after
** such a process exits to guard against console mode changes.
*/
INT_LINKAGE ConsoleStdConsStreams
consoleClassifySetup( FILE *pfIn, FILE *pfOut, FILE *pfErr );

/*
** Undo any side-effects left by consoleClassifySetup(...).
**
** This should be called after consoleClassifySetup() and
** before the process terminates normally. It is suitable
** for use with the atexit() C library procedure. After
** this call, no I/O should be done with the console
** until consoleClassifySetup(...) is called again.
**
** Applications which run an inferior (child) process that
** inherits the same I/O streams might call this procedure
** before so that said process will have a console setup
** however users have configured it or come to expect.
*/
INT_LINKAGE void SQLITE_CDECL consoleRestore( void );

/*
** Render output like fprintf(). If the output is going to the
** console and translation from UTF-8 is necessary, perform
** the needed translation. Otherwise, write formatted output
** to the provided stream almost as-is, possibly with newline
** translation as specified by set{Binary,Text}Mode().
*/
INT_LINKAGE int fprintfUtf8(FILE *pfO, const char *zFormat, ...);

/*
** Render output like fputs(). If the output is going to the
** console and translation from UTF-8 is necessary, perform
** the needed translation. Otherwise, write given text to the
** provided stream almost as-is, possibly with newline
** translation as specified by set{Binary,Text}Mode().
*/
INT_LINKAGE int fputsUtf8(const char *z, FILE *pfO);

/*
** Collect input like fgets(...) with special provisions for input
** from the console on platforms that require same. Defers to the
** C library fgets() when input is not from the console. Newline
** translation may be done as set by set{Binary,Text}Mode(). As a
** convenience, pfIn==NULL is treated as stdin.
*/
INT_LINKAGE char* fgetsUtf8(char *cBuf, int ncMax, FILE *pfIn);

/*
** Set given stream for binary mode, where newline translation is
** not done, or for text mode where, for some platforms, newlines
** are translated to the platform's conventional char sequence.
** If bFlush true, flush the stream.
**
** An additional side-effect is that if the stream is one passed
** to consoleClassifySetup() as an output, it is flushed first.
**
** Note that binary/text mode has no effect on console I/O
** translation. On all platforms, newline to the console starts
** a new line and CR,LF chars from the console become a newline.
*/
INT_LINKAGE void setBinaryMode(FILE *, short bFlush);
INT_LINKAGE void setTextMode(FILE *, short bFlush);

typedef struct Prompts {
  int numPrompts;
  const char **azPrompts;
} Prompts;

/*
** Macros for use of a line editor.
**
** The following macros define operations involving use of a
** line-editing library or simple console interaction.
** A "T" argument is a text (char *) buffer or filename.
** A "N" argument is an integer.
**
** SHELL_ADD_HISTORY(T) // Record text as line(s) of history.
** SHELL_READ_HISTORY(T) // Read history from file named by T.
** SHELL_WRITE_HISTORY(T) // Write history to file named by T.
** SHELL_STIFLE_HISTORY(N) // Limit history to N entries.
**
** A console program which does interactive console input is
** expected to call:
** SHELL_READ_HISTORY(T) before collecting such input;
** SHELL_ADD_HISTORY(T) as record-worthy input is taken;
** SHELL_STIFLE_HISTORY(N) after console input ceases; then
** SHELL_WRITE_HISTORY(T) before the program exits.
*/

/*
** Retrieve a single line of input text from an input stream.
**
** If pfIn is the input stream passed to consoleClassifySetup(),
** and azPrompt is not NULL, then a prompt is issued before the
** line is collected, as selected by the isContinuation flag.
** Array azPrompt[{0,1}] holds the {main,continuation} prompt.
**
** If zBufPrior is not NULL then it is a buffer from a prior
** call to this routine that can be reused, or will be freed.
**
** The result is stored in space obtained from malloc() and
** must either be freed by the caller or else passed back to
** this function as zBufPrior for reuse.
**
** This function may call upon services of a line-editing
** library to interactively collect line edited input.
*/
INT_LINKAGE char *
shellGetLine(FILE *pfIn, char *zBufPrior, int nLen,
             short isContinuation, Prompts azPrompt);

/*
** TBD: Define an interface for application(s) to generate
** completion candidates for use by the line-editor.
**
** This may be premature; the CLI is the only application
** that does this. Yet, getting line-editing melded into
** console I/O is desirable because a line-editing library
** may have to establish console operating mode, possibly
** in a way that interferes with the above functionality.
*/
