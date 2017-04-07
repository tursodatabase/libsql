/*
** 2017 April 07
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


#include "sqlite3.h"

typedef struct sqlite3expert sqlite3expert;

/*
** Create a new sqlite3expert object.
*/
sqlite3expert *sqlite3_expert_new(sqlite3 *db, char **pzErr);

/*
** Add an SQL statement to the analysis.
*/
int sqlite3_expert_sql(
  sqlite3expert *p,               /* From sqlite3_expert_new() */
  const char *zSql,               /* SQL statement to add */
  char **pzErr                    /* OUT: Error message (if any) */
);

int sqlite3_expert_analyze(sqlite3expert *p, char **pzErr);

/*
** Return the total number of SQL queries loaded via sqlite3_expert_sql().
*/
int sqlite3_expert_count(sqlite3expert*);

/*
** Return a component of the report.
*/
const char *sqlite3_expert_report(sqlite3expert*, int iStmt, int eReport);

/*
** Values for the third argument passed to sqlite3_expert_report().
*/
#define EXPERT_REPORT_SQL        1
#define EXPERT_REPORT_INDEXES    2
#define EXPERT_REPORT_PLAN       3

/*
** Free an (sqlite3expert*) handle allocated by sqlite3-expert_new().
*/
void sqlite3_expert_destroy(sqlite3expert*);


