/*
** 2013 November 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains code that is specific to Windows.
*/
#ifndef _OS_WIN_H_
#define _OS_WIN_H_

/*
** Include the primary Windows SDK header file.
*/
#include "windows.h"

#ifdef __CYGWIN__
# include <sys/cygwin.h>
# include <errno.h> /* amalgamator: dontcache */
#endif

/*
** Determine if we are dealing with Windows NT.
**
** We ought to be able to determine if we are compiling for Windows 9x or
** Windows NT using the _WIN32_WINNT macro as follows:
**
** #if defined(_WIN32_WINNT)
** # define SQLITE_OS_WINNT 1
** #else
** # define SQLITE_OS_WINNT 0
** #endif
**
** However, Visual Studio 2005 does not set _WIN32_WINNT by default, as
** it ought to, so the above test does not work.  We'll just assume that
** everything is Windows NT unless the programmer explicitly says otherwise
** by setting SQLITE_OS_WINNT to 0.
*/
#if SQLITE_OS_WIN && !defined(SQLITE_OS_WINNT)
# define SQLITE_OS_WINNT 1
#endif

/*
** Determine if we are dealing with Windows CE - which has a much reduced
** API.
*/
#if defined(_WIN32_WCE)
# define SQLITE_OS_WINCE 1
#else
# define SQLITE_OS_WINCE 0
#endif

/*
** Determine if we are dealing with WinRT, which provides only a subset of
** the full Win32 API.
*/
#if !defined(SQLITE_OS_WINRT)
# define SQLITE_OS_WINRT 0
#endif

#endif /* _OS_WIN_H_ */
