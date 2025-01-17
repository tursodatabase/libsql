// Created by 0x09, Public Domain
// https://github.com/0x09/sqlite-statement-vtab/blob/master/statement_vtab.c

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// Define table-valued functions.

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "define/define.h"

struct define_vtab {
    sqlite3_vtab base;
    sqlite3* db;
    char* sql;
    size_t sql_len;
    int num_inputs;
    int num_outputs;
};

struct define_cursor {
    sqlite3_vtab_cursor base;
    sqlite3_stmt* stmt;
    int rowid;
    int param_argc;
    sqlite3_value** param_argv;
};

static char* build_create_statement(sqlite3_stmt* stmt) {
    sqlite3_str* sql = sqlite3_str_new(NULL);
    sqlite3_str_appendall(sql, "CREATE TABLE x( ");
    for (int i = 0, nout = sqlite3_column_count(stmt); i < nout; i++) {
        const char* name = sqlite3_column_name(stmt, i);
        if (!name) {
            sqlite3_free(sqlite3_str_finish(sql));
            return NULL;
        }
        const char* type = sqlite3_column_decltype(stmt, i);
        sqlite3_str_appendf(sql, "%Q %s,", name, (type ? type : ""));
    }
    for (int i = 0, nargs = sqlite3_bind_parameter_count(stmt); i < nargs; i++) {
        const char* name = sqlite3_bind_parameter_name(stmt, i + 1);
        if (name)
            sqlite3_str_appendf(sql, "%Q hidden,", name + 1);
        else
            sqlite3_str_appendf(sql, "'%d' hidden,", i + 1);
    }
    if (sqlite3_str_length(sql))
        sqlite3_str_value(sql)[sqlite3_str_length(sql) - 1] = ')';
    return sqlite3_str_finish(sql);
}

static int define_vtab_destroy(sqlite3_vtab* pVTab) {
    sqlite3_free(((struct define_vtab*)pVTab)->sql);
    sqlite3_free(pVTab);
    return SQLITE_OK;
}

static int define_vtab_create(sqlite3* db,
                              void* pAux,
                              int argc,
                              const char* const* argv,
                              sqlite3_vtab** ppVtab,
                              char** pzErr) {
    size_t len;
    if (argc < 4 || (len = strlen(argv[3])) < 3) {
        if (!(*pzErr = sqlite3_mprintf("no statement provided")))
            return SQLITE_NOMEM;
        return SQLITE_MISUSE;
    }
    if (argv[3][0] != '(' || argv[3][len - 1] != ')') {
        if (!(*pzErr = sqlite3_mprintf("statement must be parenthesized")))
            return SQLITE_NOMEM;
        return SQLITE_MISUSE;
    }

    int ret;
    sqlite3_stmt* stmt = NULL;
    char* create = NULL;

    struct define_vtab* vtab = sqlite3_malloc64(sizeof(*vtab));
    if (!vtab) {
        return SQLITE_NOMEM;
    }
    memset(vtab, 0, sizeof(*vtab));
    *ppVtab = &vtab->base;

    vtab->db = db;
    vtab->sql_len = len - 2;
    if (!(vtab->sql = sqlite3_mprintf("%.*s", vtab->sql_len, argv[3] + 1))) {
        ret = SQLITE_NOMEM;
        goto error;
    }

    ret = sqlite3_prepare_v2(db, vtab->sql, vtab->sql_len, &stmt, NULL);
    if (ret != SQLITE_OK) {
        goto sqlite_error;
    }

    if (!sqlite3_stmt_readonly(stmt)) {
        ret = SQLITE_ERROR;
        if (!(*pzErr = sqlite3_mprintf("Statement must be read only.")))
            ret = SQLITE_NOMEM;
        goto error;
    }

    vtab->num_inputs = sqlite3_bind_parameter_count(stmt);
    vtab->num_outputs = sqlite3_column_count(stmt);

    if (!(create = build_create_statement(stmt))) {
        ret = SQLITE_NOMEM;
        goto error;
    }

    if ((ret = sqlite3_declare_vtab(db, create)) != SQLITE_OK) {
        goto sqlite_error;
    }

    if ((ret = define_save_function(db, argv[2], "table", argv[3])) != SQLITE_OK) {
        goto error;
    }

    sqlite3_free(create);
    sqlite3_finalize(stmt);
    return SQLITE_OK;

sqlite_error:
    if (!(*pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db))))
        ret = SQLITE_NOMEM;
error:
    sqlite3_free(create);
    sqlite3_finalize(stmt);
    define_vtab_destroy(*ppVtab);
    *ppVtab = NULL;
    return ret;
}

// if these point to the literal same function sqlite makes define_vtab eponymous, which we don't
// want
static int define_vtab_connect(sqlite3* db,
                               void* pAux,
                               int argc,
                               const char* const* argv,
                               sqlite3_vtab** ppVtab,
                               char** pzErr) {
    return define_vtab_create(db, pAux, argc, argv, ppVtab, pzErr);
}

static int define_vtab_open(sqlite3_vtab* pVTab, sqlite3_vtab_cursor** ppCursor) {
    struct define_vtab* vtab = (struct define_vtab*)pVTab;
    struct define_cursor* cur = sqlite3_malloc64(sizeof(*cur));
    if (!cur)
        return SQLITE_NOMEM;

    *ppCursor = &cur->base;
    cur->param_argv = sqlite3_malloc(sizeof(*cur->param_argv) * vtab->num_inputs);
    return sqlite3_prepare_v2(vtab->db, vtab->sql, vtab->sql_len, &cur->stmt, NULL);
}

static int define_vtab_close(sqlite3_vtab_cursor* cur) {
    struct define_cursor* stmtcur = (struct define_cursor*)cur;
    sqlite3_finalize(stmtcur->stmt);
    sqlite3_free(stmtcur->param_argv);
    sqlite3_free(cur);
    return SQLITE_OK;
}

static int define_vtab_next(sqlite3_vtab_cursor* cur) {
    struct define_cursor* stmtcur = (struct define_cursor*)cur;
    int ret = sqlite3_step(stmtcur->stmt);
    if (ret == SQLITE_ROW) {
        stmtcur->rowid++;
        return SQLITE_OK;
    }
    return ret == SQLITE_DONE ? SQLITE_OK : ret;
}

static int define_vtab_rowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid) {
    *pRowid = ((struct define_cursor*)cur)->rowid;
    return SQLITE_OK;
}

static int define_vtab_eof(sqlite3_vtab_cursor* cur) {
    return !sqlite3_stmt_busy(((struct define_cursor*)cur)->stmt);
}

static int define_vtab_column(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i) {
    struct define_cursor* stmtcur = (struct define_cursor*)cur;
    int num_outputs = ((struct define_vtab*)cur->pVtab)->num_outputs;
    if (i < num_outputs)
        sqlite3_result_value(ctx, sqlite3_column_value(stmtcur->stmt, i));
    else if (i - num_outputs < stmtcur->param_argc)
        sqlite3_result_value(ctx, stmtcur->param_argv[i - num_outputs]);
    return SQLITE_OK;
}

// parameter map encoding for xBestIndex/xFilter
// constraint -> param index mappings are stored in idxStr when not contiguous. idxStr is expected
// to be NUL terminated and printable, so we use a 6 bit encoding in the ASCII range. for simplicity
// encoded indexes are fixed to the length necessary to encode an int. this is overkill on most
// systems due to sqlite's current hard limit on number of columns but makes define_vtab agnostic
// to changes to this limit
const static size_t param_idx_size = (sizeof(int) * CHAR_BIT + 5) / 6;

static inline void encode_param_idx(int i, char* restrict param_map, int param_idx) {
    assert(param_idx >= 0);
    for (size_t j = 0; j < param_idx_size; j++)
        param_map[i * param_idx_size + j] = ((param_idx >> 6 * j) & 63) + 33;
}

static inline int decode_param_idx(int i, const char* param_map) {
    int param_idx = 0;
    for (size_t j = 0; j < param_idx_size; j++)
        param_idx |= (param_map[i * param_idx_size + j] - 33) << 6 * j;
    return param_idx;
}

// xBestIndex needs to communicate which columns are constrained by the where clause to xFilter;
// in terms of a statement table this translates to which parameters will be available to bind.
static int define_vtab_filter(sqlite3_vtab_cursor* cur,
                              int idxNum,
                              const char* idxStr,
                              int argc,
                              sqlite3_value** argv) {
    struct define_cursor* stmtcur = (struct define_cursor*)cur;
    stmtcur->rowid = 1;
    sqlite3_stmt* stmt = stmtcur->stmt;
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    int ret;
    for (int i = 0; i < argc; i++) {
        int param_idx = idxStr ? decode_param_idx(i, idxStr) : i + 1;
        if ((ret = sqlite3_bind_value(stmt, param_idx, argv[i])) != SQLITE_OK)
            return ret;
    }
    ret = sqlite3_step(stmt);
    if (!(ret == SQLITE_ROW || ret == SQLITE_DONE))
        return ret;

    assert(((struct define_vtab*)cur->pVtab)->num_inputs >= argc);
    if ((stmtcur->param_argc = argc))  // shallow copy args as these are explicitly retained in
                                       // sqlite3WhereCodeOneLoopStart
        memcpy(stmtcur->param_argv, argv, sizeof(*stmtcur->param_argv) * argc);

    return SQLITE_OK;
}

static int define_vtab_best_index(sqlite3_vtab* pVTab, sqlite3_index_info* index_info) {
    int num_outputs = ((struct define_vtab*)pVTab)->num_outputs;
    int out_constraints = 0;
    index_info->orderByConsumed = 0;
    index_info->estimatedCost = 1;
    index_info->estimatedRows = 1;
    int col_max = 0;
    sqlite3_uint64 used_cols = 0;
    for (int i = 0; i < index_info->nConstraint; i++) {
        // skip if this is a constraint on one of our output columns
        if (index_info->aConstraint[i].iColumn < num_outputs)
            continue;
        // a given query plan is only usable if all provided "input" columns are usable and have
        // equal constraints only is this redundant / an EQ constraint ever unusable?
        if (!index_info->aConstraint[i].usable ||
            index_info->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ)
            return SQLITE_CONSTRAINT;

        int col_index = index_info->aConstraint[i].iColumn - num_outputs;
        index_info->aConstraintUsage[i].argvIndex = col_index + 1;
        index_info->aConstraintUsage[i].omit = 1;

        if (col_index + 1 > col_max)
            col_max = col_index + 1;
        if (col_index < 64)
            used_cols |= 1ull << col_index;

        out_constraints++;
    }

    // if the constrained columns are contiguous then we can just tell sqlite to order the arg
    // vector provided to xFilter in the same order as our column bindings, so there's no need to
    // map between these (this will always be the case when calling the vtab as a table-valued
    // function) only support this optimization for up to 64 constrained columns since checking for
    // continuity more generally would cost nearly as much as just allocating the mapping
    sqlite_uint64 required_cols = (col_max < 64 ? 1ull << col_max : 0ull) - 1;
    if (!out_constraints ||
        (col_max <= 64 && used_cols == required_cols && out_constraints == col_max))
        return SQLITE_OK;

    // otherwise map the constraint index as provided to xFilter to column index for bindings
    // this will only be necessary when constraints are not contiguous e.g. where arg1 = x and arg3
    // = y in that case bound parameter indexes are encoded as a string in idxStr, in the order they
    // appear in constriants
    if ((size_t)out_constraints > (SIZE_MAX - 1) / param_idx_size) {
        sqlite3_free(pVTab->zErrMsg);
        if (!(pVTab->zErrMsg =
                  sqlite3_mprintf("Too many constraints to index: %d", out_constraints)))
            return SQLITE_NOMEM;
        return SQLITE_ERROR;
    }

    if (!(index_info->idxStr = sqlite3_malloc64(out_constraints * param_idx_size + 1)))
        return SQLITE_NOMEM;

    index_info->needToFreeIdxStr = 1;

    for (int i = 0, constraint_idx = 0; i < index_info->nConstraint; i++) {
        if (!index_info->aConstraintUsage[i].argvIndex)
            continue;
        encode_param_idx(constraint_idx, index_info->idxStr,
                         index_info->aConstraintUsage[i].argvIndex);
        index_info->aConstraintUsage[i].argvIndex = ++constraint_idx;
    }

    index_info->idxStr[out_constraints * param_idx_size] = '\0';

    return SQLITE_OK;
}

static sqlite3_module define_module = {
    .xCreate = define_vtab_create,
    .xConnect = define_vtab_connect,
    .xBestIndex = define_vtab_best_index,
    .xDisconnect = define_vtab_destroy,
    .xDestroy = define_vtab_destroy,
    .xOpen = define_vtab_open,
    .xClose = define_vtab_close,
    .xFilter = define_vtab_filter,
    .xNext = define_vtab_next,
    .xEof = define_vtab_eof,
    .xColumn = define_vtab_column,
    .xRowid = define_vtab_rowid,
};

int define_module_init(sqlite3* db) {
    sqlite3_create_module(db, "define", &define_module, NULL);
    return SQLITE_OK;
}
