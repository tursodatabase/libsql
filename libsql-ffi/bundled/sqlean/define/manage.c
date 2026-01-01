// Copyright (c) 2022 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Manage defined functions.

#include <stdio.h>
#include <stdlib.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#define DEFINE_CACHE 2

#pragma region statement cache

typedef struct cache_node {
    sqlite3_stmt* stmt;
    struct cache_node* next;
} cache_node;

static cache_node* cache_head = NULL;
static cache_node* cache_tail = NULL;

static int cache_add(sqlite3_stmt* stmt) {
    if (cache_head == NULL) {
        cache_head = (cache_node*)malloc(sizeof(cache_node));
        if (cache_head == NULL) {
            return SQLITE_ERROR;
        }
        cache_head->stmt = stmt;
        cache_head->next = NULL;
        cache_tail = cache_head;
        return SQLITE_OK;
    }
    cache_tail->next = (cache_node*)malloc(sizeof(cache_node));
    if (cache_tail->next == NULL) {
        return SQLITE_ERROR;
    }
    cache_tail = cache_tail->next;
    cache_tail->stmt = stmt;
    cache_tail->next = NULL;
    return SQLITE_OK;
}

static void cache_print() {
    if (cache_head == NULL) {
        printf("cache is empty");
        return;
    }
    cache_node* curr = cache_head;
    while (curr != NULL) {
        printf("%s\n", sqlite3_sql(curr->stmt));
        curr = curr->next;
    }
}

static void cache_free() {
    if (cache_head == NULL) {
        return;
    }
    cache_node* prev;
    cache_node* curr = cache_head;
    while (curr != NULL) {
        sqlite3_finalize(curr->stmt);
        prev = curr;
        curr = curr->next;
        free(prev);
    }
    cache_head = cache_tail = NULL;
}

/*
 * Prints prepared statements cache contents.
 */
static void define_cache(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    cache_print();
}

#pragma endregion

/*
 * Saves user-defined function into the database.
 */
int define_save_function(sqlite3* db, const char* name, const char* type, const char* body) {
    char* sql =
        "insert into sqlean_define(name, type, body) values (?, ?, ?) "
        "on conflict do nothing";
    sqlite3_stmt* stmt;
    int ret = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (ret != SQLITE_OK) {
        return ret;
    }
    sqlite3_bind_text(stmt, 1, name, -1, NULL);
    sqlite3_bind_text(stmt, 2, type, -1, NULL);
    sqlite3_bind_text(stmt, 3, body, -1, NULL);
    ret = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (ret != SQLITE_DONE) {
        return ret;
    }
    return SQLITE_OK;
}

// no cache at all
#if DEFINE_CACHE == 0

/*
 * Executes user-defined sql from the context.
 */
static void define_exec(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    int ret = SQLITE_OK;
    char* sql = sqlite3_user_data(ctx);
    sqlite3_stmt* stmt;
    // sqlite3_close requires all prepared statements to be closed before destroying functions, so
    // we have to re-create this every call
    if ((ret = sqlite3_prepare_v2(sqlite3_context_db_handle(ctx), sql, -1, &stmt, NULL)) !=
        SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
        return;
    }
    for (int i = 0; i < argc; i++)
        if ((ret = sqlite3_bind_value(stmt, i + 1, argv[i])) != SQLITE_OK)
            goto end;
    if ((ret = sqlite3_step(stmt)) != SQLITE_ROW) {
        if (ret == SQLITE_DONE)
            ret = SQLITE_MISUSE;
        goto end;
    }
    sqlite3_result_value(ctx, sqlite3_column_value(stmt, 0));

end:
    sqlite3_finalize(stmt);
    if (ret != SQLITE_ROW)
        sqlite3_result_error_code(ctx, ret);
}

/*
 * Creates user-defined function without caching the prepared statement.
 */
static int define_create(sqlite3* db, const char* name, const char* body) {
    char* sql = sqlite3_mprintf("select %s", body);
    if (!sql) {
        return SQLITE_NOMEM;
    }

    sqlite3_stmt* stmt;
    int ret = sqlite3_prepare_v3(db, sql, -1, SQLITE_PREPARE_PERSISTENT, &stmt, NULL);
    if (ret != SQLITE_OK) {
        sqlite3_free(sql);
        return ret;
    }
    int nparams = sqlite3_bind_parameter_count(stmt);
    sqlite3_finalize(stmt);

    return sqlite3_create_function_v2(db, name, nparams, SQLITE_UTF8, sql, define_exec, NULL, NULL,
                                      sqlite3_free);
}

/*
 * Creates user-defined function and saves it to the database.
 */
static void define_function(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    sqlite3* db = sqlite3_context_db_handle(ctx);
    const char* name = (const char*)sqlite3_value_text(argv[0]);
    const char* body = (const char*)sqlite3_value_text(argv[1]);
    int ret;
    if ((ret = define_create(db, name, body)) != SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
        return;
    }
    if ((ret = define_save_function(db, name, "scalar", body)) != SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
        return;
    }
}

/*
 * No-op as nothing is cached.
 */
static void define_free(sqlite3_context* ctx, int argc, sqlite3_value** argv) {}

// custom cache
#elif DEFINE_CACHE == 2

/*
 * Executes compiled prepared statement from the context.
 */
static void define_exec(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    int ret = SQLITE_OK;
    sqlite3_stmt* stmt = sqlite3_user_data(ctx);
    for (int i = 0; i < argc; i++) {
        if ((ret = sqlite3_bind_value(stmt, i + 1, argv[i])) != SQLITE_OK) {
            sqlite3_reset(stmt);
            sqlite3_result_error_code(ctx, ret);
            return;
        }
    }
    if ((ret = sqlite3_step(stmt)) != SQLITE_ROW) {
        if (ret == SQLITE_DONE) {
            ret = SQLITE_MISUSE;
        }
        sqlite3_reset(stmt);
        sqlite3_result_error_code(ctx, ret);
        return;
    }
    sqlite3_result_value(ctx, sqlite3_column_value(stmt, 0));
    sqlite3_reset(stmt);
}

/*
 * Creates user-defined function and caches the prepared statement.
 */
static int define_create(sqlite3* db, const char* name, const char* body) {
    char* sql = sqlite3_mprintf("select %s", body);
    if (!sql) {
        return SQLITE_NOMEM;
    }

    sqlite3_stmt* stmt;
    int ret = sqlite3_prepare_v3(db, sql, -1, SQLITE_PREPARE_PERSISTENT, &stmt, NULL);
    sqlite3_free(sql);
    if (ret != SQLITE_OK) {
        return ret;
    }
    int nparams = sqlite3_bind_parameter_count(stmt);
    // We are going to cache the statement in the function constructor and retrieve it later
    // when executing the function, using sqlite3_user_data(). But relying on this internal cache
    // is not enough.
    //
    // SQLite requires all prepared statements to be closed before calling the function destructor
    // when closing the connection. So we can't close the statement in the function destructor.
    // We have to cache it in the external cache and ask the user to manually free it
    // before closing the connection.
    //
    // Alternatively, we can cache via the sqlite3_set_auxdata() with a negative slot,
    // but that seems rather hacky.
    if ((ret = cache_add(stmt)) != SQLITE_OK) {
        return ret;
    }

    return sqlite3_create_function(db, name, nparams, SQLITE_UTF8, stmt, define_exec, NULL, NULL);
}

/*
 * Creates compiled user-defined function and saves it to the database.
 */
static void define_function(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    sqlite3* db = sqlite3_context_db_handle(ctx);
    const char* name = (const char*)sqlite3_value_text(argv[0]);
    const char* body = (const char*)sqlite3_value_text(argv[1]);
    int ret;
    if ((ret = define_create(db, name, body)) != SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
        return;
    }
    if ((ret = define_save_function(db, name, "scalar", body)) != SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
        return;
    }
}

/*
 * Frees prepared statements compiled by user-defined functions.
 */
static void define_free(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    cache_free();
}

#endif  // DEFINE_CACHE

/*
 * Deletes user-defined function (scalar or table-valued)
 */
static void define_undefine(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    char* template =
        "delete from sqlean_define where name = '%q';"
        "drop table if exists \"%w\";";
    const char* name = (const char*)sqlite3_value_text(argv[0]);
    char* sql = sqlite3_mprintf(template, name, name);
    if (!sql) {
        sqlite3_result_error_code(ctx, SQLITE_NOMEM);
        return;
    }

    sqlite3* db = sqlite3_context_db_handle(ctx);
    int ret = sqlite3_exec(db, sql, NULL, NULL, NULL);
    if (ret != SQLITE_OK) {
        sqlite3_result_error_code(ctx, ret);
    }
    sqlite3_free(sql);
}

/*
 * Loads user-defined functions from the database.
 */
static int define_load(sqlite3* db) {
    char* sql =
        "create table if not exists sqlean_define"
        "(name text primary key, type text, body text)";
    int ret = sqlite3_exec(db, sql, NULL, NULL, NULL);
    if (ret != SQLITE_OK) {
        return ret;
    }

    sqlite3_stmt* stmt;
    sql = "select name, body from sqlean_define where type = 'scalar'";
    if ((ret = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        return ret;
    }

    const char* name;
    const char* body;
    while (sqlite3_step(stmt) != SQLITE_DONE) {
        name = (const char*)sqlite3_column_text(stmt, 0);
        body = (const char*)sqlite3_column_text(stmt, 1);
        ret = define_create(db, name, body);
        if (ret != SQLITE_OK) {
            break;
        }
    }
    return sqlite3_finalize(stmt);
}

int define_manage_init(sqlite3* db) {
    const int flags = SQLITE_UTF8 | SQLITE_DIRECTONLY;
    sqlite3_create_function(db, "define", 2, flags, NULL, define_function, NULL, NULL);
    sqlite3_create_function(db, "define_free", 0, flags, NULL, define_free, NULL, NULL);
    sqlite3_create_function(db, "define_cache", 0, flags, NULL, define_cache, NULL, NULL);
    sqlite3_create_function(db, "undefine", 1, flags, NULL, define_undefine, NULL, NULL);
    return define_load(db);
}
