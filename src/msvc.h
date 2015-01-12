/*
** 2015 January 12
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
#ifndef _MSVC_H_
#define _MSVC_H_

#if defined(_MSC_VER)
#pragma warning(disable : 4100)
#pragma warning(disable : 4127)
#pragma warning(disable : 4232)
#pragma warning(disable : 4244)
/* #pragma warning(disable : 4701) */
/* #pragma warning(disable : 4706) */
#endif

#endif /* _MSVC_H_ */
