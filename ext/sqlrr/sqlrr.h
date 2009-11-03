/*
 *  sqlrr.h
 */

#ifndef _SQLRR_H_
#define _SQLRR_H_

/*
** Header constants
*/
#define SRR_FILE_SIGNATURE       "SQLRR"
#define SRR_FILE_SIGNATURE_LEN   5
#define SRR_FILE_VERSION         0x1 
#define SRR_FILE_VERSION_LEN     1

#if defined(SQLITE_ENABLE_SQLRR)

#include "sqlite3.h"
#include <sys/types.h>

#define SRRecOpen(A,B,C)        if(!rc){_SRRecOpen(A,B,C);}
#define SRRecPrepare(A,B,C,D,E) if(!rc){_SRRecPrepare(A,B,C,D,E);}

typedef enum {
    SRROpen = 0,
    SRRClose = 1,
    SRRExec = 8,
    SRRBindText = 16,
    SRRBindBlob = 17,
    SRRBindDouble = 18,
    SRRBindInt = 19,
    SRRBindNull = 20,
    SRRBindValue = 21,
    SRRBindClear = 22,
    SRRPrepare = 32,
    SRRStep = 33,
    SRRReset = 34,
    SRRFinalize = 35
} SRRCommand;

extern void SQLiteReplayRecorder(int flag);
extern void _SRRecOpen(sqlite3 *db, const char *path, int flags);
extern void SRRecClose(sqlite3 *db);
extern void SRRecExec(sqlite3 *db, const char *sql);
extern void SRRecExecEnd(sqlite3 *db);
extern void _SRRecPrepare(sqlite3 *db, const char *sql, int nBytes, int saveSql, sqlite3_stmt *stmt);
extern void SRRecStep(sqlite3_stmt *pStmt);
extern void SRRecStepEnd(sqlite3_stmt *pStmt);
extern void SRRecReset(sqlite3_stmt *pStmt);
extern void SRRecFinalize(sqlite3_stmt *pStmt);
extern void SRRecBindText(sqlite3_stmt *pStmt, int i, const char *zData, int64_t nData);
extern void SRRecBindBlob(sqlite3_stmt *pStmt, int i, const char *zData, int64_t nData);
extern void SRRecBindDouble(sqlite3_stmt *pStmt, int i, double value);
extern void SRRecBindInt64(sqlite3_stmt *pStmt, int i, int64_t value);
extern void SRRecBindNull(sqlite3_stmt *pStmt, int i);
extern void SRRecBindValue(sqlite3_stmt *pStmt, int i, const sqlite3_value *value);
extern void SRRecClearBindings(sqlite3_stmt *pStmt);

#endif /* defined(SQLITE_ENABLE_SQLRR) */

#endif /* _SQLRR_H_ */