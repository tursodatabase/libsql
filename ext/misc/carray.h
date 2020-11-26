/*
** Interface definitions for the CARRAY table-valued function
** extension.
*/

/* Use this interface to bind an array to the single-argument version
** of CARRAY().
*/
int sqlite3_carray_bind(
  sqlite3_stmt *pStmt,        /* Statement to be bound */
  int i,                      /* Parameter index */
  void *aData,                /* Pointer to array data */
  int nData,                  /* Number of data elements */
  int mFlags,                 /* CARRAY flags */
  void (*xDel)(void*)         /* Destructgor for aData*/
);

/* Allowed values for the mFlags parameter to sqlite3_carray_bind().
*/
#define CARRAY_INT32     0    /* Data is 32-bit signed integers */
#define CARRAY_INT64     1    /* Data is 64-bit signed integers */
#define CARRAY_DOUBLE    2    /* Data is doubles */
#define CARRAY_TEXT      3    /* Data is char* */
