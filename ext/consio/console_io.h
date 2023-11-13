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
********************************************************************************
** This file exposes various interfaces used for console and other I/O
** by the SQLite project command-line tools. These interfaces are used
** at either source conglomeration time, compilation time, or run time.
** This source provides for either inclusion into conglomerated,
** "single-source" forms or separate compilation then linking.
**
** Platform dependencies are "hidden" here by various stratagems so
** that, provided certain conditions are met, the programs using this
** source or object code compiled from it need no explicit conditional
** compilation in their source for their console and stream I/O.
**
** The symbols and functionality exposed here are not a public API.
** This code may change in tandem with other project code as needed.
*/

#ifndef SQLITE_INTERNAL_LINKAGE
# define SQLITE_INTERNAL_LINKAGE extern /* external to translation unit */
# include <stdio.h>
#else
# define SHELL_NO_SYSINC /* Better yet, modify mkshellc.tcl for this. */
#endif

#ifndef SQLITE3_H
# include "sqlite3.h"
#endif

/* Define enum for use with following function. */
typedef enum StreamsAreConsole {
  SAC_NoConsole = 0,
  SAC_InConsole = 1, SAC_OutConsole = 2, SAC_ErrConsole = 4,
  SAC_AnyConsole = 0x7
} StreamsAreConsole;

/*
** Classify the three standard I/O streams according to whether
** they are connected to a console attached to the process.
**
** Returns the bit-wise OR of SAC_{In,Out,Err}Console values,
** or SAC_NoConsole if none of the streams reaches a console.
**
** This function should be called before any I/O is done with
** the given streams. As a side-effect, the given inputs are
** recorded so that later I/O operations on them may be done
** differently than the C library FILE* I/O would be done,
** iff the stream is used for the I/O functions that follow,
** and to support the ones that use an implicit stream.
**
** On some platforms, stream or console mode alteration (aka
** "Setup") may be made which is undone by consoleRestore().
*/
SQLITE_INTERNAL_LINKAGE StreamsAreConsole
consoleClassifySetup( FILE *pfIn, FILE *pfOut, FILE *pfErr );
/* A usual call for convenience: */
#define SQLITE_STD_CONSOLE_INIT() consoleClassifySetup(stdin,stdout,stderr)

/*
** After an initial call to consoleClassifySetup(...), renew
** the same setup it effected. (A call not after is an error.)
** This will restore state altered by consoleRestore();
**
** Applications which run an inferior (child) process which
** inherits the same I/O streams may call this function after
** such a process exits to guard against console mode changes.
*/
SQLITE_INTERNAL_LINKAGE void consoleRenewSetup(void);

/*
** Undo any side-effects left by consoleClassifySetup(...).
**
** This should be called after consoleClassifySetup() and
** before the process terminates normally. It is suitable
** for use with the atexit() C library procedure. After
** this call, no console I/O should be done until one of
** console{Classify or Renew}Setup(...) is called again.
**
** Applications which run an inferior (child) process that
** inherits the same I/O streams might call this procedure
** before so that said process will have a console setup
** however users have configured it or come to expect.
*/
SQLITE_INTERNAL_LINKAGE void SQLITE_CDECL consoleRestore( void );

/*
** Set stream to be used for the functions below which write
** to "the designated X stream", where X is Output or Error.
** Returns the previous value.
**
** Alternatively, pass the special value, invalidFileStream,
** to get the designated stream value without setting it.
**
** Before the designated streams are set, they default to
** those passed to consoleClassifySetup(...), and before
** that is called they default to stdout and stderr.
**
** It is error to close a stream so designated, then, without
** designating another, use the corresponding {o,e}Emit(...).
*/
SQLITE_INTERNAL_LINKAGE FILE *invalidFileStream;
SQLITE_INTERNAL_LINKAGE FILE *setOutputStream(FILE *pf);
#ifdef CONSIO_SET_ERROR_STREAM
SQLITE_INTERNAL_LINKAGE FILE *setErrorStream(FILE *pf);
#endif

/*
** Emit output like fprintf(). If the output is going to the
** console and translation from UTF-8 is necessary, perform
** the needed translation. Otherwise, write formatted output
** to the provided stream almost as-is, possibly with newline
** translation as specified by set{Binary,Text}Mode().
*/
SQLITE_INTERNAL_LINKAGE int fPrintfUtf8(FILE *pfO, const char *zFormat, ...);
/* Like fPrintfUtf8 except stream is always the designated output. */
SQLITE_INTERNAL_LINKAGE int oPrintfUtf8(const char *zFormat, ...);
/* Like fPrintfUtf8 except stream is always the designated error. */
SQLITE_INTERNAL_LINKAGE int ePrintfUtf8(const char *zFormat, ...);

/*
** Emit output like fputs(). If the output is going to the
** console and translation from UTF-8 is necessary, perform
** the needed translation. Otherwise, write given text to the
** provided stream almost as-is, possibly with newline
** translation as specified by set{Binary,Text}Mode().
*/
SQLITE_INTERNAL_LINKAGE int fPutsUtf8(const char *z, FILE *pfO);
/* Like fPutsUtf8 except stream is always the designated output. */
SQLITE_INTERNAL_LINKAGE int oPutsUtf8(const char *z);
/* Like fPutsUtf8 except stream is always the designated error. */
SQLITE_INTERNAL_LINKAGE int ePutsUtf8(const char *z);

/*
** Emit output like fPutsUtf8(), except that the length of the
** accepted char or character sequence may be limited by nAccept.
**
** The magnitude and sign of nAccept control what nAccept limits.
** If positive, nAccept limits the number of char's accepted.
** If negative, it limits the number of valid input characters.
** Obtain the behavior of {f,o,e}PutsUtf8 with nAccept==INT_MAX.
**
** Returns the number of accepted char values.
**
** When ctrlMask!=0, it specifies a set of control characters not
** accepted as input, so that cBuf[abs(N)] on return will be one
** of the non-accepted characters unless nAccept limited the scan.
** Each bit in ctrlMask, 1<<cn, directs cn to not be accepted.
**
** The cBuf content will only be accessad up to the lesser of the
** limits specified by nAccept or a terminator char. It need not
** have a sentinel unless the nAccept limit exceeds the content.
** A common sentinel is '\x00', selected with ctrlMask == 1L .
**
** Special-case treatment occurs when fPutbUtf8() is given a NULL
** pfOut argument; No output is attempted, but the return value
** will still reflect the above conditions.
*/
SQLITE_INTERNAL_LINKAGE int
fPutbUtf8(FILE *pfOut, const char *cBuf, int nAccept, long ctrlMask);
/* Like fPutbUtf8 except stream is always the designated output. */
SQLITE_INTERNAL_LINKAGE int
oPutbUtf8(const char *cBuf, int nAccept, long ctrlMask);
/* Like fPutbUtf8 except stream is always the designated error. */
#ifdef CONSIO_EPUTB
SQLITE_INTERNAL_LINKAGE int
ePutbUtf8(const char *cBuf, int nAccept, long ctrlMask);
#endif

/*
** Collect input like fgets(...) with special provisions for input
** from the console on platforms that require same. Defers to the
** C library fgets() when input is not from the console. Newline
** translation may be done as set by set{Binary,Text}Mode(). As a
** convenience, pfIn==NULL is treated as stdin.
*/
SQLITE_INTERNAL_LINKAGE char* fGetsUtf8(char *cBuf, int ncMax, FILE *pfIn);
/* Like fGetsUtf8 except stream is always the designated input. */
/* SQLITE_INTERNAL_LINKAGE char* iGetsUtf8(char *cBuf, int ncMax); */

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
SQLITE_INTERNAL_LINKAGE void setBinaryMode(FILE *, short bFlush);
SQLITE_INTERNAL_LINKAGE void setTextMode(FILE *, short bFlush);

#if 0 /* For use with line editor. (not yet used) */
typedef struct Prompts {
  int numPrompts;
  const char **azPrompts;
} Prompts;
#endif

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
#if 0 /* not yet implemented */
SQLITE_INTERNAL_LINKAGE char *
shellGetLine(FILE *pfIn, char *zBufPrior, int nLen,
             short isContinuation, Prompts azPrompt);
#endif
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
