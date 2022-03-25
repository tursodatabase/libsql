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

/*****************
 * See "Shell Extensions, Programming" for purposes and usage of the following
 * interfaces supporting extended meta-commands and import and output modes.
 */

/* Define status codes returned by a meta-command, either during its argument
 * checking or during its execution (to which checking may be deferred.) The
 * code has 1 or 2 parts. The low-valued codes, below MCR_ArgIxMask, have an
 * action part and an error flag. Higher-valued codes are bitwise-or'ed with
 * a small integer and indicate problems with the meta-command itself.
 */
typedef enum DotCmdRC {
  /* Post-execute action and success/error status (semi-ordered) */
  DCR_Ok          = 0,    /* ordinary success and continue */
  DCR_Error       = 1,    /* or'ed with low-valued codes upon error */
  DCR_Return      = 2,    /* return from present input source/script */
  DCR_ReturnError = 3,    /* return with error */
  DCR_Exit        = 4,    /* exit shell ( process or pseudo-main() ) */
  DCR_ExitError   = 5,    /* exit with error */
  DCR_Abort       = 6,    /* abort for unrecoverable cause (OOM) */
  DCR_AbortError  = 7,    /* abort with error (blocked unsafe) */
  /* Above are in reverse-priority order for process_input() returns. */

  /* Dispatch and argument errors */
  DCR_ArgIxMask = 0xfff,  /* mask to retain/exclude argument index */
  /* Below codes may be or'ed with the offending argument index */
  DCR_Unknown   = 0x1000, /* unknown command, subcommand or option */
  DCR_Ambiguous = 0x2000, /* ambiguous (sub)command (too abreviated) */
  DCR_Unpaired  = 0x3000, /* option value indicated but missing */
  DCR_TooMany   = 0x4000, /* excess arguments were provided */
  DCR_TooFew    = 0x5000, /* insufficient arguments provided */
  DCR_Missing   = 0x6000, /* required argument(s) missing */
  DCR_ArgWrong  = 0x7000, /* non-specific argument error, nothing emitted */

  /* This code indicates error and a usage message to be emitted to stderr. */
  DCR_SayUsage  = 0x7ffd, /* usage is at *pzErr or is to be generated */
  /* This code indicates nothing more need be put to stderr (or stdout.) */
  DCR_CmdErred  = 0x7fff  /* non-specific error for which complaint is done */
} DotCmdRC;

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
   * by the shell nor depend upon its schema. Names with prefix "Shell"
   * or "shext_" are reserved for the shell's use.
   */
  sqlite3 *dbShell;

  /* Output stream to which shell's text output to be written (reference) */
  FILE **ppCurrentOutput;

  /* Shell abrupt exit indicator with return code in LS-byte
   * 0 => no exit
   * 0x100 => a non-error (0) exit
   * 0x100|other => exit with process exit code other
   * Any value greater than 0x1ff indicates an abnormal exit.
   * For embedded shell, "exit" means "return from REPL function".
   */
  int shellAbruptExit;

  /* Number of lines written during a query result output */
  int resultCount;
  /* Whether to show column names for certain output modes (reference) */
  unsigned char *pShowHeader;
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

/* An object implementing below interface is registered with the
 * shell to make new or overriding meta-commands available to it.
 */
INTERFACE_BEGIN( MetaCommand );
PURE_VMETHOD(const char *, name, MetaCommand, 0,());
PURE_VMETHOD(const char *, help, MetaCommand, 1,(int more));
PURE_VMETHOD(DotCmdRC, argsCheck, MetaCommand,
             3, (char **pzErrMsg, int nArgs, char *azArgs[]));
PURE_VMETHOD(DotCmdRC, execute, MetaCommand,
             4,(ShellExState *, char **pzErrMsg, int nArgs, char *azArgs[]));
INTERFACE_END( MetaCommand );

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

/* Define an implementation's v-table matching the MetaCommand interface.
 * Method signatures are copied and pasted from above interface declaration.
 */
#define MetaCommand_IMPLEMENT_VTABLE(Derived, vtname) \
CONCRETE_BEGIN(MetaCommand, Derived); \
CONCRETE_METHOD(const char *, name, MetaCommand, 0,()); \
CONCRETE_METHOD(const char *, help, MetaCommand, 1,(int more)); \
CONCRETE_METHOD(DotCmdRC, argsCheck, MetaCommand, 3, \
         (char **pzErrMsg, int nArgs, char *azArgs[])); \
CONCRETE_METHOD(DotCmdRC, execute, MetaCommand, 4, \
         (ShellExState *, char **pzErrMsg, int nArgs, char *azArgs[])); \
CONCRETE_END(Derived) vtname = { \
  DECORATE_METHOD(Derived,destruct), \
  DECORATE_METHOD(Derived,name), \
  DECORATE_METHOD(Derived,help), \
  DECORATE_METHOD(Derived,argsCheck), \
  DECORATE_METHOD(Derived,execute) \
}

/* This function pointer has the same signature as the sqlite3_X_init()
 * function that is called as SQLite3 completes loading an extension.
 */
typedef int (*ExtensionId)
  (sqlite3 *, char **, const struct sqlite3_api_routines *);

/* Hooks for scripting language integration.
 *
 * If hookScripting(...) has been called to register an extension's
 * scripting support, and isScriptLeader(pvSS, zLineLead) returns true,
 * (where zLineLead is an input group's leading line), then the shell
 * will collect input lines until scriptIsComplete(pvSS, zLineGroup)
 * returns non-zero, whereupon, the same group is submitted to be run
 * via runScript(pvSS, zLineGroup, ...). The default behaviors (when
 * one of the function pointers is 0) are: return false; return true;
 * and return DCR_Error after doing nothing.
 *
 * An extension which has called hookScripting() should arrange to
 * free associated resources upon exit or when its destructor runs.
 *
 * The 1st member, pvScriptingState, is an arbitrary, opaque pointer.
 */
typedef struct ScriptHooks {
  void *pvScriptingState; /* passed into below functions as pvSS */
  int (*isScriptLeader)(void *pvSS, const char *zScript);
  int (*scriptIsComplete)(void *pvSS, const char *zScript);
  DotCmdRC (*runScript)(void *pvSS, const char *zScript,
                        ShellExState *, char **pzErrMsg);
} ScriptHooks;

typedef struct ExtensionHelpers {
  int helperCount; /* Helper count, not including sentinel */
  union {
    struct ExtHelpers {
      int (*failIfSafeMode)(ShellExState *p, const char *zErrMsg, ...);
      FILE * (*currentOutputFile)(ShellExState *p);
      struct InSource * (*currentInputSource)(ShellExState *p);
      char * (*strLineGet)(char *zBuf, int ncMax, struct InSource *pInSrc);
      MetaCommand * (*findMetaCommand)(const char *cmdName, ShellExState *p,
                                       /* out */ int *pnFound);
      DotCmdRC (*runMetaCommand)(MetaCommand *pmc, char *azArg[], int nArg,
                                 ShellExState *psx);
      void (*setColumnWidths)(ShellExState *p, char *azWidths[], int nWidths);
      int (*nowInteractive)(ShellExState *p);
      const char * (*shellInvokedAs)(void);
      const char * (*shellStartupDir)(void);
      int (*enable_load_extension)(sqlite3 *db, int onoff);
      void (*sentinel)(void);
    } named ;
    void (*nameless[10+1])(); /* Same as named but anonymous plus a sentinel. */
  } helpers;
} ExtensionHelpers;

/* This enum is stable excepting that it grows at the end. Members will not
 * change value across successive shell versions, except for NK_CountOf. An
 * extension which is built to rely upon particular notifications can pass
 * an NK_CountOf value upon which it relies to subscribe(...) as nkMin,
 * which will fail if the hosting shell's NK_CountOf value is lower.
 */
typedef enum {
  NK_Unsubscribe,      /* event handler is being unsubsribed
                        * Also passed to subscribeEvents(...) as nkMin
                        * to unsubscribe event handler(s) */
  NK_ShutdownImminent, /* a shell exit (or return) will soon occur */
  NK_DbUserAppeared,   /* a new ShellExState .dbUser value has been set */
  NK_DbUserVanishing,  /* current ShellExState .dbUser will soon vanish */
  NK_CountOf           /* present count of preceding members (evolves) */
} NoticeKind;

/* Callback signature for shell event handlers. */
typedef
int (*ShellEventNotify)(void *pvUserData, NoticeKind nk, ShellExState *psx);

/* Various shell extension helpers and feature registration functions */
typedef struct ShellExtensionAPI {
  /* Utility functions for use by extensions */
  ExtensionHelpers * pExtHelpers;

  /* Functions for an extension to register its implementors with shell */
  const int numRegistrars; /* 4 for this version */
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
      /* Provide scripting support to host shell. (See ScriptHooks above.) */
      int (*hookScripting)(ShellExState *p,
                           ExtensionId eid, ScriptHooks *pSH);
      /* Subscribe to (or unsubscribe from) messages about various changes. */
      int (*subscribeEvents)(ShellExState *p, ExtensionId eid, void *pvUserData,
                             NoticeKind nkMin, ShellEventNotify eventHandler);
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
  ShellExState *pSXS;    /* For use in extension feature registrations */
  char *zErrMsg;         /* Extension error messages land here, if any. */

  /* An init "out" parameter, used as the loaded extension ID. Unless
   * this is set within sqlite3_X_init() prior to register*() calls,
   * the extension cannot be unloaded.
   */
  ExtensionId eid;

  /* Two more init "out" parameters, a destructor for extension overall.
   * Set to 0 on input and left so if no destructor is needed. Otherwise,
   * upon exit or unload, extensionDestruct(pvExtensionObject) is called.
   */
  void (*extensionDestruct)(void *pvExtObj);
  void *pvExtensionObject;

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
#define DEFINE_SHDB_TO_SHEXTLINK(func_name) \
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
