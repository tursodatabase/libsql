/*
** 2007 May 7
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
** This file defines various limits of what SQLite can process.
**
** @(#) $Id: limits.h,v 1.4 2007/05/08 15:15:02 drh Exp $
*/

/*
** The maximum length of a TEXT or BLOB in bytes.   This also
** limits the size of a row in a table or index.
**
** The hard limit is the ability of a 32-bit signed integer
** to count the size: 2^31-1 or 2147483647.
*/
#ifndef SQLITE_MAX_LENGTH
# define SQLITE_MAX_LENGTH 1000000000
#endif

/*
** This is the maximum number of
**
**    * Columns in a table
**    * Columns in an index
**    * Columns in a view
**    * Terms in the SET clause of an UPDATE statement
**    * Terms in the result set of a SELECT statement
**    * Terms in the GROUP BY or ORDER BY clauses of a SELECT statement.
**    * Terms in the VALUES clause of an INSERT statement
**
** The hard upper limit here is 32676.  Most database people will
** tell you that in a well-normalized database, you usually should
** not have more than a dozen or so columns in any table.  And if
** that is the case, there is no point in having more than a few
** dozen values in any of the other situations described above.
*/
#ifndef SQLITE_MAX_COLUMN
# define SQLITE_MAX_COLUMN 2000
#endif

/*
** The maximum length of a single SQL statement in bytes.
** The hard limit here is the same as SQLITE_MAX_LENGTH.
*/
#ifndef SQLITE_MAX_SQL_LENGTH
# define SQLITE_MAX_SQL_LENGTH 1000000
#endif

/*
** The maximum number of terms in an expression.
** This is limited to some extent by SQLITE_MAX_SQL_LENGTH.
** But sometime you might want to place more severe limits
** on the complexity of an expression.
*/
#ifndef SQLITE_MAX_EXPR_LENGTH
# define SQLITE_MAX_EXPR_LENGTH 5000
#endif

/*
** The maximum number of opcodes in a VDBE program.
*/
#ifndef SQLITE_MAX_VDBE_OP
# define SQLITE_MAX_VDBE_OP 25000
#endif

/*
** The maximum number of arguments to an SQL function.
*/
#ifndef SQLITE_MAX_FUNCTION_ARG
# define SQLITE_MAX_FUNCTION_ARG 100
#endif

/*
** The maximum number of in-memory pages to use for the main database
** table and for temporary tables.  The SQLITE_DEFAULT_CACHE_SIZE
*/
#ifndef SQLITE_DEFAULT_CACHE_SIZE
# define SQLITE_DEFAULT_CACHE_SIZE  2000
#endif
#ifndef SQLITE_DEFAULT_TEMP_CACHE_SIZE
# define SQLITE_DEFAULT_TEMP_CACHE_SIZE  500
#endif

/*
** The maximum number of attached databases.  This must be at least 2
** in order to support the main database file (0) and the file used to
** hold temporary tables (1).  And it must be less than 32 because
** we use a bitmask of databases with a u32 in places (for example
** the Parse.cookieMask field).
*/
#ifndef SQLITE_MAX_ATTACHED
# define SQLITE_MAX_ATTACHED 10
#endif


/*
** The maximum value of a ?nnn wildcard that the parser will accept.
*/
#ifndef SQLITE_MAX_VARIABLE_NUMBER
# define SQLITE_MAX_VARIABLE_NUMBER 999
#endif

/*
** The default size of a database page.
*/
#ifndef SQLITE_DEFAULT_PAGE_SIZE
# define SQLITE_DEFAULT_PAGE_SIZE 1024
#endif

/* Maximum page size.  The upper bound on this value is 32768.  This a limit
** imposed by the necessity of storing the value in a 2-byte unsigned integer
** and the fact that the page size must be a power of 2.
*/
#ifndef SQLITE_MAX_PAGE_SIZE
# define SQLITE_MAX_PAGE_SIZE 32768
#endif

/*
** Maximum number of pages in one database file.
**
** This is really just the default value for the max_page_count pragma.
** This value can be lowered (or raised) at run-time using that the
** max_page_count macro.
*/
#ifndef SQLITE_MAX_PAGE_COUNT
# define SQLITE_MAX_PAGE_COUNT 1073741823
#endif
