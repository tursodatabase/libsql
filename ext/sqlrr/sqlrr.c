/*
 *  sqlrr.c
 */

#include "sqlrr.h"

#if defined(SQLITE_ENABLE_SQLRR)

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include <errno.h>
#include <pthread.h>
#include <libkern/OSAtomic.h>

#include "sqliteInt.h"
#include "vdbeInt.h"

#define LOGSUFFIXLEN 48

/* 
 * Data types 
 */
typedef struct SRRLogRef SRRLogRef;
struct SRRLogRef {
    int fd;
    sqlite3 *db;
    const char *dbPath;
    char *logPath;
    int connection;
    int depth;
    SRRLogRef *nextRef;
};

/* 
 * Globals 
 */
SRRLogRef *logRefHead = NULL;
int dbLogCount = 0;
static int srr_enabled = 1;
pthread_mutex_t srr_log_mutex;
static volatile int32_t srr_initialized = 0;

/* 
 * Log management 
 */
extern void SRRecInitialize() {
    int go = OSAtomicCompareAndSwap32Barrier(0, 1, &srr_initialized); 
    if( go ){
        pthread_mutex_init(&srr_log_mutex, NULL);
    }
}

static SRRLogRef *createLog(sqlite3 *db, const char *dbPath) {
    SRRLogRef *ref = NULL;
    char *baseDir = getenv("SQLITE_REPLAY_RECORD_DIR");
    char logPath[MAXPATHLEN] = "";
    char suffix[LOGSUFFIXLEN] = "";
    const char *dbName = dbPath;
    int len = 0;
    int index = 0;
    int fd = -1;
    size_t out;
    unsigned char version = SRR_FILE_VERSION;
    
    SRRecInitialize();

    /* construct the path for the log file 
     * ${SQLITE_REPLAY_DIR}/<dbname>_<pid>_<connection_number>.sqlrr
     */
    if (baseDir == NULL) {
        baseDir = "/tmp"; /* getenv(TMPDIR) */
    }
    len = strlen(baseDir);
    strlcat(logPath, baseDir, MAXPATHLEN);
    if ((len>0) && (baseDir[len-1] != '/')) {
        strlcat(logPath, "/", MAXPATHLEN);
    }
    len = strlen(dbPath);
    for (index = len-2; index >= 0; index --){
        if (dbPath[index] == '/') {
            dbName = &dbPath[index+1];
            break;
        }
    }
    strlcat(logPath, dbName, MAXPATHLEN);
    int cNum = ++dbLogCount;
    snprintf(suffix, sizeof(suffix), "_%d_%d_XXXX.sqlrr", getpid(), cNum);
    len = strlcat(logPath, suffix, MAXPATHLEN);
    /* make it unique if we have the space */
    if ((len + 1) < MAXPATHLEN) {
        fd = mkstemps(logPath, 6);
    } else {
        fprintf(stderr, "Failed to create sqlite replay log path for %s [%s]\n", dbPath, logPath);
        return NULL;
    }
    if (fd == -1) {
        fprintf(stderr, "Failed to create sqlite replay log file for %s with path %s [%s]\n", dbPath, logPath, strerror(errno));
        return NULL;
    }
    fprintf(stdout, "Writing sqlite replay log file %s\n", logPath);
    out = write(fd, SRR_FILE_SIGNATURE, SRR_FILE_SIGNATURE_LEN);
    if (out!=-1) {
        out = write(fd, &version, 1);
    }
    if (out == -1){
        fprintf(stderr, "Write failure on log [%s]: %s\n", logPath, strerror(errno));
        close(fd);
        return NULL;
    }

    len = strlen(logPath) + 1;
    ref = (SRRLogRef *)malloc(sizeof(SRRLogRef));

    ref->db = db;
    ref->dbPath = dbPath;
    ref->logPath = (char *)malloc(len * sizeof(char));
    strlcpy(ref->logPath, logPath, len);
    ref->fd = fd;
    ref->connection = cNum;
    ref->depth = 0;
    
    pthread_mutex_lock(&srr_log_mutex);
    ref->nextRef = logRefHead;
    logRefHead = ref;
    pthread_mutex_unlock(&srr_log_mutex);
    return ref;
}

static void closeLog(sqlite3 *db) {
    SRRLogRef *ref = NULL;
    SRRLogRef *lastRef = NULL;

    pthread_mutex_lock(&srr_log_mutex);
    for (ref = logRefHead; ref != NULL; ref = ref->nextRef) {
        if (ref->db == db) {
            if (lastRef == NULL) {
                logRefHead = ref->nextRef;
            } else {
                lastRef->nextRef = ref->nextRef;
            }
        }
    }
    pthread_mutex_unlock(&srr_log_mutex);

    if (ref != NULL) {
        fprintf(stdout, "Closing sqlite replay log file %s\n", ref->logPath);
        close(ref->fd);
        free(ref->logPath);
        free(ref);
    }
}

static SRRLogRef *getLog(sqlite3 *db) {
    pthread_mutex_lock(&srr_log_mutex);
    SRRLogRef *ref = logRefHead;
    for (ref = logRefHead; ref != NULL; ref = ref->nextRef) {
        if (ref->db == db) {
            pthread_mutex_unlock(&srr_log_mutex);
            return ref;
        }
    }
    pthread_mutex_unlock(&srr_log_mutex);
    return NULL;
}


/*
 * SQLite recording API
 */
void SQLiteReplayRecorder(int flag) {
    srr_enabled = flag;
}

// open-arg-data:		<connection><len><path><flags>
void _SRRecOpen(sqlite3 *db, const char *path, int flags) {
    if (!srr_enabled) return;
    if (db) {
        SRRLogRef *ref = createLog(db, path);
        if (ref) {
            SRRCommand code = SRROpen;
            int len = strlen(path);
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out=write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { out=write(ref->fd, &(ref->connection), sizeof(ref->connection)); }
            if (out!=-1) { out=write(ref->fd, &len, sizeof(len)); }
            if (out!=-1) { out=write(ref->fd, path, len); }
            if (out!=-1) { out=write(ref->fd, &flags, sizeof(flags)); }
            if (out==-1) {
                fprintf(stderr, "Error writing open to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(db);
            }
        }
    }
}

//close-arg-data:		<connection>
void SRRecClose(sqlite3 *db) {
    if (!srr_enabled) return;
    if (db) {
        SRRLogRef *ref = getLog(db);
        if (ref) {
            SRRCommand code = SRRClose;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { out = write(ref->fd, &(ref->connection), sizeof(ref->connection)); }
            if (out==-1) {
                fprintf(stderr, "Error writing close to log file [%s]: %s\n", ref->logPath, strerror(errno));
            }
            closeLog(db);
        }
    }
}

// exec-arg-data:		<connection><len><statement-text>
void SRRecExec(sqlite3 *db, const char *sql) {
    if (!srr_enabled) return;
    if (db) {
        SRRLogRef *ref = getLog(db);
        if (ref) {
            if (ref->depth == 0) {
                SRRCommand code = SRRExec;
                int len = strlen(sql);
                struct timeval tv;
                size_t out;
                
                ref->depth = 1;
                gettimeofday(&tv, NULL);
                out = write(ref->fd, &tv, sizeof(tv));
                if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
                if (out!=-1) { out = write(ref->fd, &(ref->connection), sizeof(ref->connection)); }
                if (out!=-1) { out = write(ref->fd, &len, sizeof(len)); }
                if (out!=-1) { out = write(ref->fd, sql, len); }
                if (out==-1) {
                    fprintf(stderr, "Error writing exec to log file [%s]: %s\n", ref->logPath, strerror(errno));
                    closeLog(db);
                }
            } else {
                ref->depth ++;
            }
        }
    }
}

void SRRecExecEnd(sqlite3 *db) {
    if (!srr_enabled) return;
    if (db) {
        SRRLogRef *ref = getLog(db);
        if (ref) {
            ref->depth --;
        }
    }
}
            
// prep-arg-data:		<connection><len><statement-text><savesql><statement-ref>
void _SRRecPrepare(sqlite3 *db, const char *sql, int nBytes, int saveSql, sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if ((db!=NULL)&&(pStmt!=NULL)) {
        SRRLogRef *ref = getLog(db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRPrepare;
            struct timeval tv;
            size_t out;
            int sqlLen = nBytes;

            if (sqlLen == -1) {
                sqlLen = strlen(sql);
            }
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { out = write(ref->fd, &(ref->connection), sizeof(ref->connection)); }
            if (out!=-1) { out = write(ref->fd, &sqlLen, sizeof(sqlLen)); }
            if (out!=-1) { out = write(ref->fd, sql, sqlLen); }
            if (out!=-1) { out = write(ref->fd, &saveSql, sizeof(saveSql)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out==-1) {
                fprintf(stderr, "Error writing prepare to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(db);
            }
        }
    }
}

//step-arg-data:		<statement-ref>
void SRRecStep(sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref) {
            if (ref->depth == 0) {
                SRRCommand code = SRRStep;
                struct timeval tv;
                size_t out;
                
                ref->depth = 1;
                gettimeofday(&tv, NULL);
                out = write(ref->fd, &tv, sizeof(tv));
                if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
                if (out!=-1) { 
                    int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                    out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
                }
                if (out==-1) {
                    fprintf(stderr, "Error writing step to log file [%s]: %s\n", ref->logPath, strerror(errno));
                    closeLog(ref->db);
                }
            } else {
                ref->depth ++;
            }            
        }
    }
}

void SRRecStepEnd(sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref) {
            ref->depth --;
        }
    }
}

// reset-arg-data:		<statement-ref>
void SRRecReset(sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRReset;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out==-1) {
                fprintf(stderr, "Error writing reset to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// finalize-arg-data:	<statement-ref>
void SRRecFinalize(sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRFinalize;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out==-1) {
                fprintf(stderr, "Error writing finalize to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-text-arg-data:	<statement-ref><index><len><data>
void SRRecBindText(sqlite3_stmt *pStmt, int i, const char *zData, int64_t nData) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindText;
            struct timeval tv;
            size_t out;
            int64_t textLen = nData;
            if (textLen == -1) {
                textLen = strlen(zData);
            }
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out!=-1) { out = write(ref->fd, &i, sizeof(i)); }
            if (out!=-1) { out = write(ref->fd, &textLen, sizeof(textLen)); }
            if (out!=-1) { out = write(ref->fd, zData, textLen); }
            if (out==-1) {
                fprintf(stderr, "Error writing bind text to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-blob-arg-data:	<statement-ref><index><len>[<data>]
void SRRecBindBlob(sqlite3_stmt *pStmt, int i, const char *zData, int64_t nData) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindBlob;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out!=-1) { out = write(ref->fd, &i, sizeof(i)); }
            if (zData == NULL) {
                int64_t negNData = -nData;
                if (out!=-1) { out = write(ref->fd, &negNData, sizeof(negNData)); }
            } else {
                if (out!=-1) { out = write(ref->fd, &nData, sizeof(nData)); }
                if (out!=-1) { out = write(ref->fd, zData, nData); }
            }
            if (out==-1) {
                fprintf(stderr, "Error writing bind blob to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-double-arg-data:	<statement-ref><index><data>
void SRRecBindDouble(sqlite3_stmt *pStmt, int i, double value) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindDouble;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out!=-1) { out = write(ref->fd, &i, sizeof(i)); }
            if (out!=-1) { out = write(ref->fd, &value, sizeof(value)); }
            if (out==-1) {
                fprintf(stderr, "Error writing bind double to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-int-arg-data:	<statement-ref><index><data>
void SRRecBindInt64(sqlite3_stmt *pStmt, int i, int64_t value) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindInt;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out!=-1) { out = write(ref->fd, &i, sizeof(i)); }
            if (out!=-1) { out = write(ref->fd, &value, sizeof(value)); }
            if (out==-1) {
                fprintf(stderr, "Error writing bind int to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-null-arg-data:	<statement-ref><index>
void SRRecBindNull(sqlite3_stmt *pStmt, int i) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindNull;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out!=-1) { out = write(ref->fd, &i, sizeof(i)); }
            if (out==-1) {
                fprintf(stderr, "Error writing bind null to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

// bind-value-arg-data:	<statement-ref><index><len><data> ???
void SRRecBindValue(sqlite3_stmt *pStmt, int i, const sqlite3_value *value) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            fprintf(stderr, "SRRecBindValue(sqlite3_bind_value) is not yet supported, closing [%s]: %s\n", ref->logPath, strerror(errno));
            closeLog(ref->db);
        }
    }
}

// bind-clear-arg-data:	<statement-ref>
void SRRecClearBindings(sqlite3_stmt *pStmt) {
    if (!srr_enabled) return;
    if(pStmt!=NULL) {
        Vdbe *v = (Vdbe *)pStmt;
        SRRLogRef *ref = getLog(v->db);
        if (ref && (ref->depth == 0)) {
            SRRCommand code = SRRBindClear;
            struct timeval tv;
            size_t out;
            
            gettimeofday(&tv, NULL);
            out = write(ref->fd, &tv, sizeof(tv));
            if (out!=-1) { out = write(ref->fd, &code, sizeof(SRRCommand)); }
            if (out!=-1) { 
                int64_t stmtInt = (int64_t)((intptr_t)(pStmt));
                out = write(ref->fd, &stmtInt, sizeof(int64_t)); 
            }
            if (out==-1) {
                fprintf(stderr, "Error writing clear bindings to log file [%s]: %s\n", ref->logPath, strerror(errno));
                closeLog(ref->db);
            }
        }
    }
}

#endif /* SQLITE_ENABLE_SQLRR */
