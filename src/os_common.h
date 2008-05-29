/*
** 2004 May 22
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
** This file contains macros and a little bit of code that is common to
** all of the platform-specific files (os_*.c) and is #included into those
** files.
**
** This file should be #included by the os_*.c files only.  It is not a
** general purpose header file.
**
** $Id: os_common.h,v 1.37 2008/05/29 20:22:37 shane Exp $
*/
#ifndef _OS_COMMON_H_
#define _OS_COMMON_H_

/*
** At least two bugs have slipped in because we changed the MEMORY_DEBUG
** macro to SQLITE_DEBUG and some older makefiles have not yet made the
** switch.  The following code should catch this problem at compile-time.
*/
#ifdef MEMORY_DEBUG
# error "The MEMORY_DEBUG macro is obsolete.  Use SQLITE_DEBUG instead."
#endif


/*
 * When testing, this global variable stores the location of the
 * pending-byte in the database file.
 */
#ifdef SQLITE_TEST
unsigned int sqlite3_pending_byte = 0x40000000;
#endif

#ifdef SQLITE_DEBUG
int sqlite3OSTrace = 0;
#define OSTRACE1(X)         if( sqlite3OSTrace ) sqlite3DebugPrintf(X)
#define OSTRACE2(X,Y)       if( sqlite3OSTrace ) sqlite3DebugPrintf(X,Y)
#define OSTRACE3(X,Y,Z)     if( sqlite3OSTrace ) sqlite3DebugPrintf(X,Y,Z)
#define OSTRACE4(X,Y,Z,A)   if( sqlite3OSTrace ) sqlite3DebugPrintf(X,Y,Z,A)
#define OSTRACE5(X,Y,Z,A,B) if( sqlite3OSTrace ) sqlite3DebugPrintf(X,Y,Z,A,B)
#define OSTRACE6(X,Y,Z,A,B,C) \
    if(sqlite3OSTrace) sqlite3DebugPrintf(X,Y,Z,A,B,C)
#define OSTRACE7(X,Y,Z,A,B,C,D) \
    if(sqlite3OSTrace) sqlite3DebugPrintf(X,Y,Z,A,B,C,D)
#else
#define OSTRACE1(X)
#define OSTRACE2(X,Y)
#define OSTRACE3(X,Y,Z)
#define OSTRACE4(X,Y,Z,A)
#define OSTRACE5(X,Y,Z,A,B)
#define OSTRACE6(X,Y,Z,A,B,C)
#define OSTRACE7(X,Y,Z,A,B,C,D)
#endif

/*
** Macros for performance tracing.  Normally turned off.  Only works
** on i486 hardware.
*/
#ifdef SQLITE_PERFORMANCE_TRACE

/* 
** hwtime.h contains inline assembler code for implementing 
** high-performance timing routines.
*/
#include "hwtime.h"

static sqlite_uint64 g_start;
static sqlite_uint64 g_elapsed;
#define TIMER_START       g_start=sqlite3Hwtime()
#define TIMER_END         g_elapsed=sqlite3Hwtime()-g_start
#define TIMER_ELAPSED     g_elapsed
#else
#define TIMER_START
#define TIMER_END
#define TIMER_ELAPSED     ((sqlite_uint64)0)
#endif

/*
** If we compile with the SQLITE_TEST macro set, then the following block
** of code will give us the ability to simulate a disk I/O error.  This
** is used for testing the I/O recovery logic.
*/
#ifdef SQLITE_TEST
int sqlite3_io_error_hit = 0;            /* Total number of I/O Errors */
int sqlite3_io_error_hardhit = 0;        /* Number of non-benign errors */
int sqlite3_io_error_pending = 0;        /* Count down to first I/O error */
int sqlite3_io_error_persist = 0;        /* True if I/O errors persist */
int sqlite3_io_error_benign = 0;         /* True if errors are benign */
int sqlite3_diskfull_pending = 0;
int sqlite3_diskfull = 0;
#define SimulateIOErrorBenign(X) sqlite3_io_error_benign=(X)
#define SimulateIOError(CODE)  \
  if( (sqlite3_io_error_persist && sqlite3_io_error_hit) \
       || sqlite3_io_error_pending-- == 1 )  \
              { local_ioerr(); CODE; }
static void local_ioerr(){
  IOTRACE(("IOERR\n"));
  sqlite3_io_error_hit++;
  if( !sqlite3_io_error_benign ) sqlite3_io_error_hardhit++;
}
#define SimulateDiskfullError(CODE) \
   if( sqlite3_diskfull_pending ){ \
     if( sqlite3_diskfull_pending == 1 ){ \
       local_ioerr(); \
       sqlite3_diskfull = 1; \
       sqlite3_io_error_hit = 1; \
       CODE; \
     }else{ \
       sqlite3_diskfull_pending--; \
     } \
   }
#else
#define SimulateIOErrorBenign(X)
#define SimulateIOError(A)
#define SimulateDiskfullError(A)
#endif

/*
** When testing, keep a count of the number of open files.
*/
#ifdef SQLITE_TEST
int sqlite3_open_file_count = 0;
#define OpenCounter(X)  sqlite3_open_file_count+=(X)
#else
#define OpenCounter(X)
#endif

#endif /* !defined(_OS_COMMON_H_) */
