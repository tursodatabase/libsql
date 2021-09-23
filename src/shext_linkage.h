#ifndef SQLITE3SHX_H
#define SQLITE3SHX_H
#include "sqlite3ext.h"

typedef struct ShellState ShellState;

/* This function pointer has the same signature as the sqlite3_X_init()
 * function that is called as SQLite3 completes loading an extension.
 */
typedef int (*ExtensionId)
  (sqlite3 *, char **, const struct sqlite3_api_routines *);

/* An instance of below struct, possibly extended/subclassed, is registered
 * with the shell to make new or altered meta-commands available to it.
 */
typedef struct MetaCommand {
  struct MetaCommandVtable *pMCV;
} MetaCommand;

/* This vtable is for meta-command implementation and help linkage to shell.
 */
typedef struct MetaCommandVtable {
  void (*destruct_free)(MetaCommand *);
  const char * (*name)(MetaCommand *);
  const char * (*help)(MetaCommand *, int more);
  void (*argsRange)(MetaCommand *, int * pMinArgs, int * pMaxArgs);
  int (*execute)
    (MetaCommand *, ShellState *, char **pzErrMsg, char *azArgs[], int nArgs);
} MetaCommandVtable;

/* See "Shell Extensions, Programming" for purposes and usage of the following
 * structs supporting extended meta-commands and import and output modes.
 */

/* Convey data to, from and/or between I/O handlers. */
typedef struct {
  char *zFieldSeparator;
  char *zRecordSeparator;
  char *zRecordLead;
  char *zRecordTrail;
  char *zNullValue;
  char *zSQL;
  int  numWidths;
  int  *pWantWidths;
  int  *pHaveWidths;
  void *pvHandlerData; /* Lifetime is from mid-openX() to mid-closeX(). */
} FormatInfo;

/* An instance of below struct, possibly extended/subclassed, is registered
 * with the shell to make new or altered output modes available to it.
 */
typedef struct OutModeHandler {
  struct OutModeHandlerVtable *pOMV;
} OutModeHandler;

typedef struct OutModeHandlerVtable {
  void (*destruct_free)(OutModeHandler * pROS);
  const char * (*name)(OutModeHandler *);
  const char * (*help)(OutModeHandler *, int more);
  int (*openResultsOutStream)
    (OutModeHandler * pROS, FormatInfo *pFI, char **pzErr,
     const char * zLocus, const char * zName);
  int (*prependResultsOut)
    (OutModeHandler * pROS, FormatInfo *pFI, char **pzErr,
     sqlite3_stmt * pStmt);
  int (*rowResultsOut)
    (OutModeHandler * pROS, FormatInfo *pFI, char **pzErr,
     sqlite3_stmt * pStmt);
  int (*appendResultsOut)
    (OutModeHandler * pROS, FormatInfo *pFI, char **pzErr,
     sqlite3_stmt * pStmt);
  int (*closeResultsOutStream)
    (OutModeHandler * pROS, FormatInfo *pFI, char **pzErr);
} OutModeHandlerVtable;

/* An instance of below struct, possibly extended/subclassed, is registered
 * with the shell to make new or altered data importers available to it.
 */
typedef struct ImportHandler {
  struct ImportHandlerVtable *pIHV;
} ImportHandler;

typedef struct ImportHandlerVtable {
  void (*destruct_free)(ImportHandler * pIH);
  const char * (*name)(ImportHandler *);
  const char * (*help)(ImportHandler *, int more);
  int (*openDataInStream)
    (ImportHandler *pIH, FormatInfo *pFI, char **pzErr,
     const char * zLocus, const char * zName);
  int (*prepareDataInput)
    (ImportHandler *pIH, FormatInfo *pFI, char **pzErr, sqlite3_stmt * pStmt);
  int (*rowDataInput)
    (ImportHandler *pIH, FormatInfo *pFI, char **pzErr, sqlite3_stmt * pStmt);
  int (*finishDataInput)
    (ImportHandler *pIH, FormatInfo *pFI, char **pzErr, sqlite3_stmt * pStmt);
  int (*closeDataInStream)
    (ImportHandler *pIH, FormatInfo *pFI, char **pzErr);
} ImportHandlerVtable;

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
  void (*extensionDtor)(void *);

  /* Various shell extension feature registration functions
   */
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
    } *named;
    void (*pFunctions[4])(); /* 0-terminated sequence of function pointers */
  } api;
} ShellExtensionLink;

/* Test whether a char ** references a ShellExtensionLink instance's
 * validityMark, and if so return the instance's address, else return 0.
 * This macro may be used from a shell extension's sqlite3_X_init() function
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
  ShellExtensionLink *rv; sqlite3_stmt *pStmt; \
  if( SQLITE_OK!=sqlite3_prepare(db,"SELECT shext_pointer(0)",-1,&pStmt,0) \
      || SQLITE_ROW != sqlite3_step(pStmt) )  return 0; \
  rv = (ShellExtensionLink *)sqlite3_value_pointer \
    (sqlite3_column_value(pStmt, 0), SHELLEXT_API_POINTERS); \
  sqlite3_finalize(pStmt);  return rv; \
 }

#endif /* !defined(SQLITE3SHX_H) */
