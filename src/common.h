/*
** 2008 March 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Common includes/defines based on output of configure script
**
** @(#) $Id: common.h,v 1.2 2008/03/06 09:58:50 mlcreech Exp $
*/
#ifndef _COMMON_H_
#define _COMMON_H_

/*
** Include the configuration header output by 'configure' if it was run
** (otherwise we get an empty default).
*/
#include "config.h"

/* Needed for various definitions... */
#define _GNU_SOURCE

/*
** Include standard header files as necessary
*/
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

/*
** If possible, use the C99 intptr_t type to define an integral type of
** equivalent size to a pointer.  (Technically it's >= sizeof(void *), but
** practically it's == sizeof(void *)).  We fall back to an int if this type
** isn't defined.
*/
#ifdef HAVE_INTPTR_T
  typedef intptr_t sqlite3_intptr_t;
# define __sqlite3_intptr_defined
#endif

#endif
