#ifndef CRSQLITE_GETTABLE_H
#define CRSQLITE_GETTABLE_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

int crsql_get_table(
    sqlite3 *db,       /* The database on which the SQL executes */
    const char *zSql,  /* The SQL to be executed */
    char ***pazResult, /* Write the result table here */
    int *pnRow,        /* Write the number of rows in the result here */
    int *pnColumn,     /* Write the number of columns of result here */
    char **pzErrMsg    /* Write error messages here */
);
void crsql_free_table(
    char **azResult /* Result returned from sqlite3_get_table() */
);

#endif