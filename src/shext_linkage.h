/*
** 2022 April 8
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

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
 * interfaces supporting extended dot-commands and import and output modes.
 */

/* Define status codes returned by a dot-command, either during its argument
 * checking or during its execution (to which checking may be deferred.) The
 * code has 1 or 2 parts. The low-valued codes, below DCR_ArgIxMask, have an
 * action part and an error flag. Higher-valued codes are bitwise-or'ed with
 * a small integer and indicate problems with the dot-command itself.
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

/* Convey data to, from and/or between I/O handlers and dot-commands. */
typedef struct ShellExState {
  /* A sizeof(*) to permit extensions to guard against too-old hosts */
  int sizeofThis;

  /* The user's currently open and primary DB connection
   * Extensions may use this DB, but must not modify this pointer
   * and must never close the database. The shell is exclusively
   * responsible for creation and termination of this connection.
   * Extensions should not store a copy of this pointer without
   * provisions for maintaining validity of the copy. The shell
   * may alter this pointer apart from opening or closing a DB.
   * See ShellEvenNotify, NoticeKind and subscribeEvents below
   * for means of maintaining valid copies.  */
  sqlite3 *dbUser;

  /* DB connection for shell dynamical data and extension management
   * Extensions may use this DB, but should not alter content created
   * by the shell nor depend upon its schema. Names with prefix "Shell"
   * or "shext_" are reserved for the shell's use. */
  sqlite3 *dbShell;

  /* Shell abrupt exit indicator with return code in LS-byte
   * 0 => no exit
   * 0x100 => a non-error (0) exit
   * 0x100|other => exit with process exit code other
   * Any value greater than 0x1ff indicates an abnormal exit.
   * For embedded shell, "exit" means "return from top-level REPL function". */
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
  /* Next 3 members should be set and/or allocated by .width dot-command.
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
 * shell to make new or overriding dot-commands available to it.
 */
INTERFACE_BEGIN( DotCommand );
  /* The whole, true name for this command */
PURE_VMETHOD(const char *, name, DotCommand, 0,());
  /* Help text; zWhat=0 => primary, zWhat="" => secondary, other ? */
PURE_VMETHOD(const char *, help, DotCommand, 1,(const char *zWhat));
  /* Validate arguments, blocking execute for returns != DCR_Ok */
PURE_VMETHOD(DotCmdRC, argsCheck, DotCommand,
             3, (char **pzErrMsg, int nArgs, char *azArgs[]));
  /* Do whatever this command does, or return error of some kind */
PURE_VMETHOD(DotCmdRC, execute, DotCommand,
             4,(ShellExState *, char **pzErrMsg, int nArgs, char *azArgs[]));
INTERFACE_END( DotCommand );

/* An object implementing below interface is registered with the
 * shell to make new or overriding output modes available to it.
 */
INTERFACE_BEGIN( ExportHandler );
PURE_VMETHOD(const char *, name, ExportHandler, 0,());
PURE_VMETHOD(const char *, help, ExportHandler, 1,(const char *zWhat));
PURE_VMETHOD(int, openResultsOutStream, ExportHandler,
             5,( ShellExState *pSES, char **pzErr,
                 int numArgs, char *azArgs[], const char * zName ));
PURE_VMETHOD(int, prependResultsOut, ExportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(int, rowResultsOut, ExportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(int, appendResultsOut, ExportHandler,
             3,( ShellExState *pSES, char **pzErr, sqlite3_stmt *pStmt ));
PURE_VMETHOD(void, closeResultsOutStream, ExportHandler,
             2,( ShellExState *pSES, char **pzErr ));
INTERFACE_END( ExportHandler );

/* An object implementing below interface is registered with the
 * shell to make new or overriding data importers available to it.
 */
INTERFACE_BEGIN( ImportHandler );
PURE_VMETHOD(const char *, name, ImportHandler, 0,());
PURE_VMETHOD(const char *, help, ImportHandler, 1,(const char *zWhat));
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
INTERFACE_END( ImportHandler );

/* An object implementing this next interface is registered with the shell
 * to make scripting support available to it. Only one at a time can be used.
 *
 * If registerScripting() has been called to register an extension's support
 * for scripting, then its methods are called, and must respond, as follows:
 *
 * When the initial line of an "execution group" is collected by the shell,
 * it calls isScriptLeader(pObj, zLineLead) to determine whether the group
 * should be considered as (eventually) being one for the script handler
 * to execute. This does not indicate whether it is good input or runnable;
 * it is only for classification (so that different parsing/collection rules
 * may be applied for different categories of shell input.) The method should
 * return true iff the group should be parsed and run by this handler. If it
 * returns false, something else will be done with the group.
 *
 * As one or more lines of an "execution group" are collected by the shell,
 * scriptIsComplete(pObj, zLineGroup, pzWhyNot) is called with the group as
 * so far accumulated. If out parameter pzWhyNot is non-zero, the method may
 * output a message indicating in what way the input is incomplete, which is
 * then the shell's responsibility to pass to sqlite3_free(). The method must
 * return true if the group is ready to be executed, otherwise false. This is
 * not the time at which to execute the accumulated group.
 *
 * After the scriptIsComplete() method returns true, or whenever the script is
 * being ignored (due to end-of-stream or interrupt), the resetCompletionScan()
 * method is called. This may be used to reset the scanning state held across
 * calls to scriptIsComplete() so that it need not scan the whole script text
 * at each call. It might do other things; it is always called after a call to
 * isScriptLeader() has returned true and scriptIsComplete() has been called.
 *
 * If a script group is complete (as above-determined), then runScript() may
 * be called to execute that script group. (Or, it may not.) It must either
 * execute it successfully and return DCR_Ok, suffer an ordinary failure and
 * return DCR_Error, or return one of the codes DCR_{Return,Exit,Abort} or'ed
 * with DCR_Error or not, to indicate extraordinary post-execute actions.
 * DCR_Return is to indicate the present execution context should be left.
 * DCR_Exit is for shell exit requests. DCR_Abort means exit with prejudice.
 *
 * An extension which has called registerScripting() should arrange to
 * free associated resources upon exit or when its destructor runs.
 */
INTERFACE_BEGIN( ScriptSupport );
PURE_VMETHOD(const char *, name, ScriptSupport, 0,());
PURE_VMETHOD(const char *, help, ScriptSupport, 1,(const char *zWhat));
PURE_VMETHOD(int,  configure, ScriptSupport,
             4,( ShellExState *pSES, char **pzErr,
                 int numArgs, char *azArgs[] ));
PURE_VMETHOD(int, isScriptLeader, ScriptSupport,
             1,( const char *zScript ));
PURE_VMETHOD(int, scriptIsComplete, ScriptSupport,
             2,( const char *zScript, char **pzWhyNot ));
PURE_VMETHOD(void, resetCompletionScan, ScriptSupport, 0,());

PURE_VMETHOD(DotCmdRC, runScript, ScriptSupport,
             3,( const char *zScript, ShellExState *pSES, char **pzErr ));
INTERFACE_END( ScriptSupport );

/* Define a v-table implementation for ScriptSupport interface. */
#define ScriptSupport_IMPLEMENT_VTABLE(Derived, vtname) \
CONCRETE_BEGIN(ScriptSupport, Derived); \
CONCRETE_METHOD(const char *, name, ScriptSupport, 0,()); \
CONCRETE_METHOD(const char *, help, ScriptSupport, 1,(const char *zWhat)); \
CONCRETE_METHOD(int,  configure, ScriptSupport, \
  4,( ShellExState *pSES, char **pzErr, int numArgs, char *azArgs[] )); \
CONCRETE_METHOD(int, isScriptLeader, ScriptSupport, \
  1,( const char *zScript )); \
CONCRETE_METHOD(int, scriptIsComplete, ScriptSupport, \
   2,( const char *zScript, char **pzWhyNot )); \
CONCRETE_METHOD(void, resetCompletionScan, ScriptSupport, 0,()); \
CONCRETE_METHOD(DotCmdRC, runScript, ScriptSupport, \
  3,( const char *zScript, ShellExState *pSES, char **pzErr )); \
CONCRETE_END(Derived) vtname = { \
  DECORATE_METHOD(Derived,destruct), \
  DECORATE_METHOD(Derived,name), \
  DECORATE_METHOD(Derived,help), \
  DECORATE_METHOD(Derived,configure), \
  DECORATE_METHOD(Derived,isScriptLeader), \
  DECORATE_METHOD(Derived,scriptIsComplete), \
  DECORATE_METHOD(Derived,resetCompletionScan), \
  DECORATE_METHOD(Derived,runScript) \
}
/* Define an implementation's v-table matching the DotCommand interface.
 * Method signatures are copied and pasted from above interface declaration.
 */
#define DotCommand_IMPLEMENT_VTABLE(Derived, vtname) \
CONCRETE_BEGIN(DotCommand, Derived); \
CONCRETE_METHOD(const char *, name, DotCommand, 0,()); \
CONCRETE_METHOD(const char *, help, DotCommand, 1,(const char *zWhat)); \
CONCRETE_METHOD(DotCmdRC, argsCheck, DotCommand, 3, \
         (char **pzErrMsg, int nArgs, char *azArgs[])); \
CONCRETE_METHOD(DotCmdRC, execute, DotCommand, 4, \
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
 * It is used as a process-unique identifier for a loaded extension.
 */
typedef int (*ExtensionId)
  (sqlite3 *, char **, const struct sqlite3_api_routines *);

typedef struct Prompts {
  const char *zMain;
  const char *zContinue;
} Prompts;

AGGTYPE_BEGIN(ExtensionHelpers) {
  int helperCount; /* Helper count, not including sentinel */
  struct ExtHelpers {
    int (*failIfSafeMode)(ShellExState *p, const char *zErrMsg, ...);
    void (*utf8CurrentOutPrintf)(ShellExState *p, const char *zFmt, ...);
    struct InSource * (*currentInputSource)(ShellExState *p);
    char * (*strLineGet)(char *zBuf, int ncMax, struct InSource *pInSrc);
    DotCommand * (*findDotCommand)(const char *cmdName, ShellExState *p,
                                   /* out */ int *pnFound);
    DotCmdRC (*runDotCommand)(DotCommand *pmc, char *azArg[], int nArg,
                              ShellExState *psx);
    void (*setColumnWidths)(ShellExState *p, char *azWidths[], int nWidths);
    int (*nowInteractive)(ShellExState *p);
    const char * (*shellInvokedAs)(void);
    const char * (*shellStartupDir)(void);
    char * (*oneInputLine)(struct InSource *pInSrc, char *zPrior,
                           int isContinuation, Prompts *pCue);
    void (*freeInputLine)(char *zLine);
    int (*enable_load_extension)(sqlite3 *db, int onoff);
    void *pSentinel; /* Always set to 0, above never are. */
  } helpers;
} AGGTYPE_END(ExtensionHelpers);

/* This enum is stable excepting that it grows at the end. Members will not
 * change value across successive shell versions, except for NK_CountOf. An
 * extension which is built to rely upon particular notifications can pass
 * an NK_CountOf value upon which it relies to subscribeEvents(...) as nkMin,
 * which call will fail if the hosting shell's NK_CountOf value is lower.
 */
typedef enum {
  NK_Unsubscribe,      /* Event handler is being unsubsribed, pvSubject
                        * is the ExtensionId used to subscribe. Sent last.
                        * All event handlers eventually get this event, so
                        * it can be used to free a handler's resources.
                        * Also passed to subscribeEvents(...) as nkMin
                        * to unsubscribe some/all event handler(s). */
  NK_ShutdownImminent, /* Shell or module will soon be shut down, pvSubject
                        * is NULL. Event sent prior to above and extension
                        * destructor calls, and sent after all below, */
  NK_DbUserAppeared,   /* A new ShellExState .dbUser value has been set,
                        * pvSubject is the newly set .dbUser value. */
  NK_DbUserVanishing,  /* Current ShellExState .dbUser will soon vanish,
                        * pvSubject is the vanishing .dbUser value. */
  NK_DbAboutToClose,   /* A possibly ShellExState-visible DB will soon be
                        * closed, pvSubject is the DB's sqlite3 pointer. */
  NK_ExtensionUnload,  /* The ShellExState .dbShell DB will soon be closed,
                        * soon to be followed by unloading of all dynamic
                        * extensions; pvSubject is the DB's sqlite3 pointer. */
  NK_NewDotCommand,    /* A new DotCommand has been registered, pvSubject
                        * is the just-added DotCommand object (pointer). */
  NK_CountOf           /* Present count of preceding members (evolves) */
} NoticeKind;

/* Callback signature for shell event handlers. */
typedef int (*ShellEventNotify)(void *pvUserData, NoticeKind nk,
                                void *pvSubject, ShellExState *psx);

/* Various shell extension helpers and feature registration functions */
AGGTYPE_BEGIN(ShellExtensionAPI) {
  /* Utility functions for use by extensions */
  ExtensionHelpers * pExtHelpers;

  /* Functions for an extension to register its implementors with shell */
  const int numRegistrars; /* 6 for this version */
  struct ShExtAPI {
    /* Register a dot-command */
    int (*registerDotCommand)(ShellExState *p,
                              ExtensionId eid, DotCommand *pMC);
    /* Register query result data display (or other disposition) mode */
    int (*registerExporter)(ShellExState *p,
                            ExtensionId eid, ExportHandler *pEH);
    /* Register an import variation from (various sources) for .import */
    int (*registerImporter)(ShellExState *p,
                            ExtensionId eid, ImportHandler *pIH);
    /* Provide scripting support to host shell. (See ScriptSupport above.) */
    int (*registerScripting)(ShellExState *p,
                             ExtensionId eid, ScriptSupport *pSS);
    /* Subscribe to (or unsubscribe from) messages about various changes.
     * See above NoticeKind enum and ShellEventNotify callback typedef. */
    int (*subscribeEvents)(ShellExState *p, ExtensionId eid, void *pvUserData,
                           NoticeKind nkMin, ShellEventNotify eventHandler);
    /* Notify host shell that an ad-hoc dot command exists and provide for
     * its help text to appear in .help output. Only an extension which has
     * registered an "unknown" DotCommand may use this.
     * If zHelp==0, any such provision is removed. If zHelp!=0, original or
     * replacement help text is associated with command zName.
     * Help text before the first newline is primary, issued as summary help.
     * Text beyond that is secondary, issued as the complete command help. */
    int (*registerAdHocCommand)(ShellExState *p, ExtensionId eid,
                                const char *zName, const char *zHelp);
    void *pSentinel; /* Always set to 0, above never are. */
  } api;
} AGGTYPE_END(ShellExtensionAPI);

/* Struct passed to extension init function to establish linkage. The
 * lifetime of instances spans only the init call itself. Extensions
 * should make a copy, if needed, of pShellExtensionAPI for later use.
 * Its referent is static, persisting for the process duration.
 */
AGGTYPE_BEGIN(ShellExtensionLink) {
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

  /* If extra arguments were provided to the .shxload command, they are
   * available through these two members. Only azLoadArgs[0] through
   * azLoadArgs[nLoadArgs-1] may be referenced. (That may be none.)
   * If an extension keeps the argument values, copies must be made
   * because the pointers in azLoadArgs[] become invalid after loading.
   */
  int nLoadArgs;
  char **azLoadArgs;
} AGGTYPE_END(ShellExtensionLink);

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
#define DEFINE_SHDB_TO_SHEXTLINK(link_func_name) \
 static ShellExtensionLink * link_func_name(sqlite3 * db){ \
  ShellExtensionLink *rv = 0; sqlite3_stmt *pStmt = 0; \
  if( SQLITE_OK==sqlite3_prepare_v2(db,"SELECT shext_pointer(0)",-1,&pStmt,0) \
      && SQLITE_ROW == sqlite3_step(pStmt) ) \
    rv = (ShellExtensionLink *)sqlite3_value_pointer \
     (sqlite3_column_value(pStmt, 0), SHELLEXT_API_POINTERS); \
  sqlite3_finalize(pStmt); return rv; \
 }

/*
 * Define boilerplate macros analogous to SQLITE_EXTENSION_INIT#
 * Note that the argument names are reused across the macro set.
 * This reflects the fact that, for the macros to be useful, the
 * same objects must be referenced from different places. Hence,
 * the actual arguments must appear in all of the invocations.
 */

/* Place at file scope prior to usage of the arguments by extension code.
 * This defines 3 static objects, named per the arguments and set or used
 * for an extension to link into the shell host.
 */
#ifndef __cplusplus
#define SHELL_EXTENSION_INIT1( shell_api_ptr, ext_helpers_ptr, link_func ) \
  static struct ShExtAPI *shell_api_ptr = 0; \
  static struct ExtHelpers *ext_helpers_ptr = 0; \
  DEFINE_SHDB_TO_SHEXTLINK(link_func)
#else
#define SHELL_EXTENSION_INIT1( shell_api_ptr, ext_helpers_ptr, link_func ) \
  static ShellExtensionAPI::ShExtAPI *shell_api_ptr = 0; \
  static ExtensionHelpers::ExtHelpers *ext_helpers_ptr = 0; \
  DEFINE_SHDB_TO_SHEXTLINK(link_func)
#endif

/* Place within sqlite3_x_init() among its local variable declarations. */
#define SHELL_EXTENSION_INIT2( link_ptr, link_func, db_ptr ) \
  ShellExtensionLink * link_ptr = link_func(db_ptr)

/* Place within sqlite3_x_init() code prior to usage of the *_ptr arguments. */
#define SHELL_EXTENSION_INIT3( shell_api_ptr, ext_helpers_ptr, link_ptr ) \
 if( (link_ptr)!=0 ){ \
  shell_api_ptr = &link_ptr->pShellExtensionAPI->api; \
  ext_helpers_ptr = &link_ptr->pShellExtensionAPI->pExtHelpers->helpers; \
 }

/* This test may be used within sqlite3_x_init() after SHELL_EXTENSION_INIT3 */
#define SHELL_EXTENSION_LINKED(link_ptr) ((link_ptr)!=0)
/* These *_COUNT() macros help determine version compatibility.
 * They should only be used when the above test yields true.
 */
#define SHELL_API_COUNT(link_ptr) \
  (link_ptr->pShellExtensionAPI->numRegistrars)
#define SHELL_HELPER_COUNT(link_ptr) \
  (link_ptr->pShellExtensionAPI->pExtHelpers->helperCount)

/* Combining the above, safely, to provide a single test for extensions to
 * use for assurance that: (1) the load was as a shell extension (with the
 * -shext flag rather than bare .load); and (2) the loading host provides
 * stated minimum extension API and helper counts.
 */
#define SHELL_EXTENSION_LOADFAIL(link_ptr, minNumApi, minNumHelpers) \
  (!SHELL_EXTENSION_LINKED(link_ptr) \
   || SHELL_API_COUNT(link_ptr)<(minNumApi) \
   || SHELL_HELPER_COUNT(link_ptr)<(minNumHelpers) \
  )
/* Like above, except it is an enum expression. The value is EXLD_Ok for
 * success or one of the next three values telling why the load failed.
 */
typedef enum {
  EXLD_Ok, EXLD_NoLink, EXLD_OutdatedApi, EXLD_OutdatedHelpers
} ExtensionLoadStatus;
#define SHELL_EXTENSION_LOADFAIL_WHY(link_ptr, minNumApi, minNumHelpers) ( \
  (!SHELL_EXTENSION_LINKED(link_ptr) ? EXLD_NoLink \
   : SHELL_API_COUNT(link_ptr)<(minNumApi) ? EXLD_OutdatedApi \
   : SHELL_HELPER_COUNT(link_ptr)<(minNumHelpers) ? EXLD_OutdatedHelpers \
   : EXLD_Ok ) \
)

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* !defined(SQLITE3SHX_H) */
