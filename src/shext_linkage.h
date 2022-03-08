#ifndef SQLITE3SHX_H
#define SQLITE3SHX_H
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "sqlite3ext.h"

#include "obj_interfaces.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Convey data to, from and/or between I/O handlers and meta-commands. */
typedef struct ShellExState {
  /* A sizeof(*) to permit extensions to guard against too-old hosts */
  int sizeofThis;

  /* A semi-transient holder of arbitrary data used during operations
   * not interrupted by meta-command invocations. Any not-null pointer
   * left after a meta-command has completed is, by contract, to be
   * freeable using sqlite3_free(), unless freeHandlerData is non-zero,
   * in which case it is used for the free, then zeroed too. This
   * pointer's use is otherwise unconstrained. */
  void *pvHandlerData;
  void (*freeHandlerData)(void *);

  /* The user's currently open and primary DB connection
   * Extensions may use this DB, but must not modify this pointer.
   */
  sqlite3 *dbUser;
  /* DB connection for shell dynamical data and extension management
   * Extensions may use this DB, but should not alter content created
   * by the shell nor depend upon its schema. Names with prefix "shell_"
   * or "shext_" are reserved for the shell's use.
   */
  sqlite3 *dbShell;

  /* Output stream to which shell's text output to be written (reference) */
  FILE **ppCurrentOutput;

  /* Whether to exit as command completes.
   * 0 => no exit
   * ~0 => a non-error (0) exit
   * other => exit with process exit code other
   * For embedded shell, "exit" means "return from REPL function".
   */
  int shellAbruptExit;

  /* Number of lines written during a query result output */
  int resultCount;
  /* Whether to show column names for certain output modes (reference) */
  u8 *pShowHeader;
  /* Column separator character for some modes, read-only */
  char *zFieldSeparator;
  /* Row separator character for some modes (MODE_Ascii), read-only */
  char *zRecordSeparator;
  /* Row set prefix for some modes if not 0 */
  char *zRecordLead;
  /* Row set suffix for some modes if not 0 */
  char *zRecordTail;
  /* Text to represent a NULL in external data formats, read-only */
  char *zNullValue;
  /* Name of table for which inserts are to be written or performed */
  const char *zDestTable;
  /* Next 3 members should be set and/or allocated by .width meta-command.
   * The values of pSpecWidths[i] and pHaveWidths[i] can be modified or
   * used by extensions, but setColumnWidths(...) must resize those lists.
   */
  /* Number of column widths presently desired or tracked, read-only */
  int  numWidths; /* known allocation count of next 2 members */
  /* The column widths last specified via .width command */
  int  *pSpecWidths;
  /* The column widths last observed in query results, read-only */
  int  *pHaveWidths;

  /* Internal and opaque shell state, not for use by extensions */
  struct ShellInState *pSIS; /* Offset of this member is NOT STABLE. */
} ShellExState;

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
PURE_VMETHOD(int, argsCheck, MetaCommand,
             3, (char **pzErrMsg, int nArgs, char *azArgs[]));
PURE_VMETHOD(int, execute, MetaCommand,
             4,(ShellExState *, char **pzErrMsg, int nArgs, char *azArgs[]));
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
  int helperCount; /* Helper count, not including sentinel */
  union ExtHelp {
    struct {
      int (*failIfSafeMode)(ShellExState *p, const char *zErrMsg, ...);
      FILE * (*currentOutputFile)(ShellExState *p);
      struct InSource * (*currentInputSource)(ShellExState *p);
      char * (*strLineGet)(char *zBuf, int ncMax, struct InSource *pInSrc);
      void (*setColumnWidths)(ShellExState *p, char *azWidths[], int nWidths);
      int (*nowInteractive)(ShellExState *p);
      void (*sentinel)(void);
    } named ;
    void (*nameless[5+1])(); /* Same as named but anonymous plus a sentinel. */
  } helpers;
} ExtensionHelpers;

/* Various shell extension helpers and feature registration functions */
typedef struct ShellExtensionAPI {
  /* Utility functions for use by extensions */
  ExtensionHelpers * pExtHelp;

  /* Functions for extension to register its implementors with shell */
  const int numRegistrars; /* 3 for this version */
  union {
    struct ShExtAPI {
      /* Register a meta-command */
      int (*registerMetaCommand)(ShellExState *p,
                                 ExtensionId eid, MetaCommand *pMC);
      /* Register query result data display (or other disposition) mode */
      int (*registerOutMode)(ShellExState *p,
                             ExtensionId eid, OutModeHandler *pOMH);
      /* Register an import variation from (various sources) for .import */
      int (*registerImporter)(ShellExState *p,
                              ExtensionId eid, ImportHandler *pIH);
      /* Preset to 0 at extension load, a sentinel for expansion */
      void (*sentinel)(void);
    } named;
    void (*pFunctions[4])(); /* 0-terminated sequence of function pointers */
  } api;
} ShellExtensionAPI;

/* Struct passed to extension init function to establish linkage. The
 * lifetime of instances spans only the init call itself. Extensions
 * should make a copy, if needed, of pShellExtensionAPI for later use.
 * Its referant is static, persisting for the process duration.
 */
typedef struct ShellExtensionLink {
  int sizeOfThis;        /* sizeof(ShellExtensionLink) for expansion */
  ShellExtensionAPI *pShellExtensionAPI;
  ShellExState *pSSX;    /* For use in extension feature registrations */
  char *zErrMsg;         /* Extension error messages land here, if any. */

  /* An init "out" parameter, used as the loaded extension ID. Unless
   * this is set within sqlite3_X_init() prior to register*() calls,
   * the extension cannot be unloaded.
   */
  ExtensionId eid;

  /* Another init "out" parameter, a destructor for extension overall.
   * Set to 0 on input and may be left so if no destructor is needed.
   */
  void (*extensionDestruct)(void *);

} ShellExtensionLink;

/* String used with SQLite "Pointer Passing Interfaces" as a type marker.
 * That API subset is used by the shell to pass its extension API to the
 * sqlite3_X_init() function of shell extensions, via the DB parameter.
 */
#define SHELLEXT_API_POINTERS "shellext_api_pointers"

/* Pre-write a function to retrieve a ShellExtensionLink pointer from the
 * shell's DB. This macro defines a function which will return either a
 * pointer to a ShellExtensionLink instance during an extension's *init*()
 * call (during shell extension load) or 0 (during SQLite extension load.)
 */
#define DEFINE_SHDB_TO_SHEXT_API(func_name) \
 static ShellExtensionLink * func_name(sqlite3 * db){ \
  ShellExtensionLink *rv = 0; sqlite3_stmt *pStmt = 0; \
  if( SQLITE_OK==sqlite3_prepare_v2(db,"SELECT shext_pointer(0)",-1,&pStmt,0) \
      && SQLITE_ROW == sqlite3_step(pStmt) ) \
    rv = (ShellExtensionLink *)sqlite3_value_pointer \
     (sqlite3_column_value(pStmt, 0), SHELLEXT_API_POINTERS); \
  sqlite3_finalize(pStmt); return rv; \
 }

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* !defined(SQLITE3SHX_H) */
