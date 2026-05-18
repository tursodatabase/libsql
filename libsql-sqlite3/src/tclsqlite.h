/*
** 2024-07-30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface to TCL as used by SQLite.
** SQLite subcomponents that use TCL (the libsqlite3.c interface library
** and various test*.c pieces) should #include this file rather than
** including tcl.h directly.
*/
/******  Any edits to this file must mirrored in tclsqlite.c ***********/

/* When compiling for Windows using STDCALL instead of CDECL calling
** conventions, the MSVC makefile has to build a customized version of
** the "tcl.h" header that specifies the calling conventions for each
** interface.  That customized "tcl.h" is named "sqlite_tcl.h".
*/
#if defined(INCLUDE_SQLITE_TCL_H)
# include "sqlite_tcl.h"   /* Special case for Windows using STDCALL */
#else
# include <tcl.h>          /* All normal cases */
# ifndef SQLITE_TCLAPI
#   define SQLITE_TCLAPI
# endif
#endif

/******  Any edits to this file must mirrored in tclsqlite.c ***********/

/* Compatibility between Tcl8.6 and Tcl9.0 */
#if TCL_MAJOR_VERSION==9
# define CONST const
#elif !defined(Tcl_Size)
# define Tcl_Size int
#endif

/******  Any edits to this file must mirrored in tclsqlite.c ***********/
