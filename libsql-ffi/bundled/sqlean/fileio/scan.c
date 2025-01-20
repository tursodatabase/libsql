// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// scanfile(name)
// Reads a file with the specified name line by line.
// Implemented as a table-valued function.

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#else
#include <sys/types.h>
#endif

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

/*
 * readline reads chars from the input `stream` until it encounters \n char.
 * Returns the number or characters read.
 *
 * `lineptr` points to the first character read.
 * `n` equals the current buffer size.
 */
static ssize_t readline(char** lineptr, size_t* n, FILE* stream) {
    char* bufptr = NULL;
    char* p = bufptr;
    size_t size;
    int c;

    if (lineptr == NULL) {
        return -1;
    }
    if (stream == NULL) {
        return -1;
    }
    if (n == NULL) {
        return -1;
    }
    bufptr = *lineptr;
    size = *n;

    c = fgetc(stream);
    if (c == EOF) {
        return -1;
    }
    if (bufptr == NULL) {
        bufptr = malloc(128);
        if (bufptr == NULL) {
            return -1;
        }
        size = 128;
    }
    p = bufptr;
    while (c != EOF) {
        if ((ssize_t)(p - bufptr) > (ssize_t)(size - 1)) {
            size = size + 128;
            bufptr = realloc(bufptr, size);
            if (bufptr == NULL) {
                return -1;
            }
        }
        *p++ = c;
        if (c == '\n') {
            break;
        }
        c = fgetc(stream);
    }

    *p++ = '\0';
    *lineptr = bufptr;
    *n = size;

    return p - bufptr - 1;
}

typedef struct {
    sqlite3_vtab base;
} Table;

typedef struct {
    sqlite3_vtab_cursor base;
    const char* name;
    FILE* in;
    bool eof;
    char* line;
    sqlite3_int64 rowid;
} Cursor;

#define COLUMN_ROWID -1
#define COLUMN_VALUE 0
#define COLUMN_NAME 1

// xconnect creates the virtual table.
static int xconnect(sqlite3* db,
                    void* aux,
                    int argc,
                    const char* const* argv,
                    sqlite3_vtab** vtabptr,
                    char** errptr) {
    (void)aux;
    (void)argc;
    (void)argv;
    (void)errptr;

    int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(value text, name hidden)");
    if (rc != SQLITE_OK) {
        return rc;
    }

    Table* table = sqlite3_malloc(sizeof(*table));
    *vtabptr = (sqlite3_vtab*)table;
    if (table == NULL) {
        return SQLITE_NOMEM;
    }
    memset(table, 0, sizeof(*table));
    sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
    return SQLITE_OK;
}

// xdisconnect destroys the virtual table.
static int xdisconnect(sqlite3_vtab* vtable) {
    Table* table = (Table*)vtable;
    sqlite3_free(table);
    return SQLITE_OK;
}

// xopen creates a new cursor.
static int xopen(sqlite3_vtab* vtable, sqlite3_vtab_cursor** curptr) {
    (void)vtable;
    Cursor* cursor = sqlite3_malloc(sizeof(*cursor));
    if (cursor == NULL) {
        return SQLITE_NOMEM;
    }
    memset(cursor, 0, sizeof(*cursor));
    *curptr = &cursor->base;
    return SQLITE_OK;
}

// xclose destroys the cursor.
static int xclose(sqlite3_vtab_cursor* cur) {
    Cursor* cursor = (Cursor*)cur;
    if (cursor->in != NULL) {
        fclose(cursor->in);
    }
    if (cursor->line != NULL) {
        free(cursor->line);
    }
    sqlite3_free(cur);
    return SQLITE_OK;
}

// xnext advances the cursor to its next row of output.
static int xnext(sqlite3_vtab_cursor* cur) {
    Cursor* cursor = (Cursor*)cur;
    cursor->rowid++;
    size_t bufsize = 0;
    ssize_t len = readline(&cursor->line, &bufsize, cursor->in);
    if (len == -1) {
        cursor->eof = true;
    }
    if (len >= 1 && cursor->line[len - 1] == '\n') {
        cursor->line[len - 1] = '\0';
    }
    if (len >= 2 && cursor->line[len - 2] == '\r') {
        cursor->line[len - 2] = '\0';
    }
    return SQLITE_OK;
}

// xcolumn returns the current cursor value.
static int xcolumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int col_idx) {
    (void)col_idx;
    Cursor* cursor = (Cursor*)cur;
    switch (col_idx) {
        case COLUMN_VALUE:
            sqlite3_result_text(ctx, (const char*)cursor->line, -1, SQLITE_TRANSIENT);
            break;

        case COLUMN_NAME:
            sqlite3_result_text(ctx, cursor->name, -1, SQLITE_TRANSIENT);
            break;

        default:
            break;
    }
    return SQLITE_OK;
}

// xrowid returns the rowid for the current row.
static int xrowid(sqlite3_vtab_cursor* cur, sqlite_int64* rowid_ptr) {
    Cursor* cursor = (Cursor*)cur;
    *rowid_ptr = cursor->rowid;
    return SQLITE_OK;
}

// xeof returns TRUE if the cursor has been moved off of the last row of output.
static int xeof(sqlite3_vtab_cursor* cur) {
    Cursor* cursor = (Cursor*)cur;
    return cursor->eof;
}

// xfilter rewinds the cursor back to the first row of output.
static int xfilter(sqlite3_vtab_cursor* cur,
                   int idx_num,
                   const char* idx_str,
                   int argc,
                   sqlite3_value** argv) {
    (void)idx_num;
    (void)idx_str;

    if (argc != 1) {
        return SQLITE_ERROR;
    }
    const char* name = (const char*)sqlite3_value_text(argv[0]);

    Cursor* cursor = (Cursor*)cur;
    sqlite3_vtab* vtable = (cursor->base).pVtab;

    // free resources from the previous file, if any
    if (cursor->in != NULL) {
        fclose(cursor->in);
    }
    if (cursor->line != NULL) {
        free(cursor->line);
    }

    // reset the cursor
    cursor->name = name;
    cursor->eof = false;
    cursor->line = NULL;
    cursor->rowid = 0;

    cursor->in = fopen(cursor->name, "r");
    if (cursor->in == NULL) {
        vtable->zErrMsg = sqlite3_mprintf("cannot open '%s' for reading", cursor->name);
        return SQLITE_ERROR;
    }

    return xnext(cur);
}

// xbest_index instructs SQLite to pass certain arguments to xFilter.
static int xbest_index(sqlite3_vtab* vtable, sqlite3_index_info* index_info) {
    // for (size_t i = 0; i < index_info->nConstraint; i++) {
    //     const struct sqlite3_index_constraint* constraint = index_info->aConstraint + i;
    //     printf("i=%zu iColumn=%d, op=%d, usable=%d\n", i, constraint->iColumn, constraint->op,
    //            constraint->usable);
    // }

    // only the name argument is supported
    if (index_info->nConstraint != 1) {
        vtable->zErrMsg = sqlite3_mprintf("scanfile() expects a single constraint (name)");
        return SQLITE_ERROR;
    }

    const struct sqlite3_index_constraint* constraint = index_info->aConstraint;
    if (constraint->iColumn != COLUMN_NAME) {
        vtable->zErrMsg = sqlite3_mprintf("scanfile() expects a name constraint)");
        return SQLITE_ERROR;
    }

    if (constraint->usable == 0) {
        // unusable contraint
        return SQLITE_CONSTRAINT;
    }

    // pass the name argument to xFilter
    index_info->aConstraintUsage[0].argvIndex = COLUMN_NAME;
    index_info->aConstraintUsage[0].omit = 1;
    index_info->estimatedCost = (double)1000;
    index_info->estimatedRows = 1000;
    return SQLITE_OK;
}

static sqlite3_module scan_module = {
    .xConnect = xconnect,
    .xBestIndex = xbest_index,
    .xDisconnect = xdisconnect,
    .xOpen = xopen,
    .xClose = xclose,
    .xFilter = xfilter,
    .xNext = xnext,
    .xEof = xeof,
    .xColumn = xcolumn,
    .xRowid = xrowid,
};

int fileio_scan_init(sqlite3* db) {
    sqlite3_create_module(db, "fileio_scan", &scan_module, 0);
    sqlite3_create_module(db, "scanfile", &scan_module, 0);
    return SQLITE_OK;
}
