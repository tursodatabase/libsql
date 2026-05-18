/*
** 2024-09-24
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This header file contains definitions of interfaces that provide 
** cross-platform I/O for UTF-8 content.
**
** On most platforms, the interfaces definitions in this file are
** just #defines.  For example sqlite3_fopen() is a macro that resolves
** to the standard fopen() in the C-library.
**
** But Windows does not have a standard C-library, at least not one that
** can handle UTF-8.  So for windows build, the interfaces resolve to new
** C-language routines contained in the separate sqlite3_stdio.c source file.
**
** So on all non-Windows platforms, simply #include this header file and
** use the interfaces defined herein.  Then to run your application on Windows,
** also link in the accompanying sqlite3_stdio.c source file when compiling
** to get compatible interfaces.
*/
#ifndef _SQLITE3_STDIO_H_
#define _SQLITE3_STDIO_H_ 1
#ifdef _WIN32
/**** Definitions For Windows ****/
#include <stdio.h>
#include <windows.h>

FILE *sqlite3_fopen(const char *zFilename, const char *zMode);
FILE *sqlite3_popen(const char *zCommand, const char *type);
char *sqlite3_fgets(char *s, int size, FILE *stream);
int sqlite3_fputs(const char *s, FILE *stream);
int sqlite3_fprintf(FILE *stream, const char *format, ...);
void sqlite3_fsetmode(FILE *stream, int mode);


#else
/**** Definitions For All Other Platforms ****/
#include <stdio.h>
#define sqlite3_fopen     fopen
#define sqlite3_popen     popen
#define sqlite3_fgets     fgets
#define sqlite3_fputs     fputs
#define sqlite3_fprintf   fprintf
#define sqlite3_fsetmode(F,X)   /*no-op*/

#endif
#endif /* _SQLITE3_STDIO_H_ */
