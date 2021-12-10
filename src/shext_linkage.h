#ifndef SQLITE3SHX_H
#define SQLITE3SHX_H
#include "sqlite3ext.h"

#include "obj_interfaces.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Convey data to, from and/or between I/O handlers and meta-commands. */
typedef struct {
  /* A semi-transient holder of arbitrary data used during operations
   * not interrupted by meta-command invocations. Any not-null pointer
   * left after a meta-command has completed is, by contract, to be
   * freeable using sqlite3_free(). It is otherwise unconstrained. */
  void *pvHandlerData;

  /* The user's currently open and primary DB connection */
  sqlite3 *db;
  /* The DB connection used for shell's dynamical data */
  sqlite3 *dbShell;

  /* Input stream providing shell's command or query input */
  FILE *pCurrentInputStream;
  /* Output stream to which shell's text output to be written */
  FILE *pCurrentOutputStream;

  /* Whether to exit as command completes.
   * 0 => no exit
   * ~0 => a non-error (0) exit
   * other => exit with process exit code other
   * For embedded shell, "exit" means "return from REPL".
   */
  int shellExit;

  /* Number of lines written during a query result output */
  int resultCount;
  /* Whether to show column names for certain output modes */
  int showHeader;
  /* Column separator character for some modes */
  char *zFieldSeparator;
  /* Row separator character for some modes (MODE_Ascii) */
  char *zRecordSeparator;
  /* Row set prefix for some modes */
  char *zRecordLead;
  /* Row set suffix for some modes */
  char *zRecordTrail;
  /* Text to represent a NULL in external data formats */
  char *zNullValue;
  /* Number of column widths presently desired or tracked */
  int  numWidths; /* known allocation count of next 2 members */
  /* The column widths last specified via .width command */
  int  *pWantWidths;
  /* The column widths last observed in query results */
  int  *pHaveWidths;
} ShellExState;

/* The shell's state, shared among meta-command implementations.
 * The ShellStateX object includes a private partition whose content
 * and usage are opaque to shell extensions compiled separately
 * from the shell.c core. (As defined here, it is wholly opaque.)
 */
typedef struct ShellStateX {
  ShellExState sxs;       /* sizeof(ShellExState) will never shrink. */
  struct ShellState *pSS; /* The offset of this member is NOT STABLE. */
} ShellStateX;

/* This function pointer has the same signature as the sqlite3_X_init()
 * function that is called as SQLite3 completes loading an extension.
 */
typedef int (*ExtensionId)
  (sqlite3 *, char **, const struct sqlite3_api_routines *);

/*****************
 * See "Shell Extensions, Programming" for purposes and usage of the following
 * interfaces supporting extended meta-commands and import and output modes.
 */

/* An object implementing below interface is registered with the
 * shell to make new or overriding meta-commands available to it.
 */
INTERFACE_BEGIN( MetaCommand );
PURE_VMETHOD(const char *, name, MetaCommand, 0,());
PURE_VMETHOD(const char *, help, MetaCommand, 1,(int more));
PURE_VMETHOD(struct {unsigned minArgs; unsigned maxArgs;},
             argsRange, MetaCommand, 0,());
PURE_VMETHOD(int, execute, MetaCommand,
             4,(ShellStateX *, char **pzErrMsg, int nArgs, char *azArgs[]));
INTERFACE_END( MetaCommand );

/* Define error codes to be returned either by a meta-command during
 * its own checking or by the dispatcher for bad argument counts.
 */
#define SHELL_INVALID_ARGS SQLITE_MISUSE
#define SHELL_FORBIDDEN_OP 0x7ffe /* Action disallowed under --safe.*/

/* An object implementing below interface is registered with the
 * shell to make new or overriding output modes available to it.
 */
INTERFACE_BEGIN( OutModeHandler );
PURE_VMETHOD(const char *, name, OutModeHandler, 0,());
PURE_VMETHOD(const char *, help, OutModeHandler, 1,(int more));
PURE_VMETHOD(int, openResultsOutStream, OutModeHandler,
             5,( ShellExState *pSES, char **pzErr,
                 int numArgs, char *azArgs[], const char * zName ));
PURE_VMETHOD(int, prependResultsOut, OutModeHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(int, rowResultsOut, OutModeHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(int, appendResultsOut, OutModeHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(void, closeResultsOutStream, OutModeHandler,
             2,( ShellExState *pSES, char **pzErr ));
INTERFACE_END( OutModeHandlerVtable );

/* An object implementing below interface is registered with the
 * shell to make new or overriding data importers available to it.
 */
INTERFACE_BEGIN( ImportHandler );
PURE_VMETHOD(const char *, name, ImportHandler, 0,());
PURE_VMETHOD(const char *, help, ImportHandler, 1,( int more ));
PURE_VMETHOD(int,  openDataInStream, ImportHandler,
             5,( ShellExState *pSES, char **pzErr,
                 int numArgs, char *azArgs[], const char * zName ));
PURE_VMETHOD(int, prepareDataInput, ImportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt * *ppStmt ));
PURE_VMETHOD(int, rowDataInput, ImportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(int, finishDataInput, ImportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(void, closeDataInStream, ImportHandler,
             2,( ShellExState *pSES, char **pzErr ));
INTERFACE_END( ImportHandlerVtable );

typedef struct {
  int helperCount;
  union ExtHelp {
    struct {
      void (*failIfSafeMode)(ShellStateX *p, const char *zErrMsg, ...);
    } named ;
    void (*nameless[2])(); /* Same as named but anonymous plus a sentinel. */
  } helpers;
} ExtensionHelpers;

#define SHELLEXT_VALIDITY_MARK "ExtensibleShell"

typedef struct ShellExtensionLink {
  char validityMark[16]; /* Preset to contain "ExtensibleShell\x00" */
  char *zErrMsg;         /* Extension puts error message here if any. */
  int sizeOfThis;           /* sizeof(struct ShellExtensionLink) */
  const char *shellVersion; /* Preset to "3.??.??\x00" or similar */

  /* An init "out" parameter, used as the loaded extension ID. Unless
   * this is set within sqlite3_X_init() prior to register*() calls,
   * the extension cannot be unloaded. 
   */
  ExtensionId eid;

  /* Another init "out" parameter, a destructor for extension overall.
   * Set to 0 on input and may be left so if no destructor is needed.
   */
  void (*extensionDestruct)(void *);

  /* Various shell extension helpers and feature registration functions
   */
  ExtensionHelpers * pExtHelp;

  union ShellExtensionAPI {
    struct ShExtAPI {
      /* Register a meta-command */
      int (*registerMetaCommand)(ExtensionId eid, MetaCommand *pMC);
      /* Register an output data display (or other disposition) mode */
      int (*registerOutMode)(ExtensionId eid, OutModeHandler *pOMH);
      /* Register an import variation from (various sources) for .import */
      int (*registerImporter)(ExtensionId eid, ImportHandler *pIH);
      /* Preset to 0 at extension load, a sentinel for expansion */
      void (*pExtra)(void); 
    } named;
    void (*pFunctions[4])(); /* 0-terminated sequence of function pointers */
  } api;
} ShellExtensionLink;

/* Test whether a char ** references a ShellExtensionLink instance's
 * validityMark, and if so return the instance's address, else return 0.
 * This macro may be used by a shell extension's sqlite3_X_init() function
 * to obtain a pointer to the ShellExtensionLink struct, derived from the
 * error message pointer (pzErrMsg) passed as the 2nd argument. This enables
 * the extension to incorporate its features into a running shell process.
 */
#define EXTENSION_LINKAGE_PTR(pzem) ( \
  pzem != 0 && *pzem != 0 && strcmp(*pzem, SHELLEXT_VALIDITY_MARK) == 0 \
  && *pzem == (char *)pzem \
  + offsetof(ShellExtensionLink, validityMark) \
  - offsetof(ShellExtensionLink, zErrMsg) ) \
  ? (ShellExtensionLink *) \
    ((char *)pzem-offsetof(ShellExtensionLink,zErrMsg)) \
  : 0

/* String used with SQLite "Pointer Passing Interfaces" as a type marker. 
 * That API subset is used by the shell to pass its extension API to the
 * sqlite3_X_init() function of extensions, via the DB parameter.
 */
#define SHELLEXT_API_POINTERS "shellext_api_pointers"

/* Pre-write a function to retrieve a ShellExtensionLink pointer from the
 * shell's DB. This is an alternative to use of the EXTENSION_LINKAGE_PTR
 * macro above. It takes some more code, replicated across extensions.
 */
#define DEFINE_SHDB_TO_SHEXT_API(func_name) \
 static ShellExtensionLink * func_name(sqlite3 * db){ \
  ShellExtensionLink *rv = 0; sqlite3_stmt *pStmt = 0; \
  if( SQLITE_OK==sqlite3_prepare(db,"SELECT shext_pointer(0)",-1,&pStmt,0) \
      && SQLITE_ROW == sqlite3_step(pStmt) ) \
    rv = (ShellExtensionLink *)sqlite3_value_pointer \
     (sqlite3_column_value(pStmt, 0), SHELLEXT_API_POINTERS); \
  sqlite3_finalize(pStmt); return rv; \
 }

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* !defined(SQLITE3SHX_H) */
