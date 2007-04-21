/*
** This header file is used by programs that want to link against the
** FTS2 library.  All it does is declare the sqlite3Fts2Init() interface.
*/
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int sqlite3Fts2Init(sqlite3 *db);

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
