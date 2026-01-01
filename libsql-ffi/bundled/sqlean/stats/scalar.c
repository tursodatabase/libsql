// Standard deviation and variance by Liam Healy, Public Domain
// extension-functions.c at https://sqlite.org/contrib/

// Percentile by D. Richard Hipp, Public Domain
// https://sqlite.org/src/file/ext/misc/percentile.c

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Statistical functions for SQLite.

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#pragma region Standard deviation and variance

/*
** An instance of the following structure holds the context of a
** stddev() or variance() aggregate computation.
** implementaion of http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Algorithm_II
** less prone to rounding errors
*/
typedef struct StddevCtx StddevCtx;
struct StddevCtx {
    double rM;
    double rS;
    int64_t cnt; /* number of elements */
};

/*
** called for each value received during a calculation of stddev or variance
*/
static void varianceStep(sqlite3_context* context, int argc, sqlite3_value** argv) {
    StddevCtx* p;

    double delta;
    double x;

    assert(argc == 1);
    p = sqlite3_aggregate_context(context, sizeof(*p));
    /* only consider non-null values */
    if (SQLITE_NULL != sqlite3_value_numeric_type(argv[0])) {
        p->cnt++;
        x = sqlite3_value_double(argv[0]);
        delta = (x - p->rM);
        p->rM += delta / p->cnt;
        p->rS += delta * (x - p->rM);
    }
}

/*
** Returns the sample standard deviation value
*/
static void stddevFinalize(sqlite3_context* context) {
    StddevCtx* p;
    p = sqlite3_aggregate_context(context, 0);
    if (p && p->cnt > 1) {
        sqlite3_result_double(context, sqrt(p->rS / (p->cnt - 1)));
    } else {
        sqlite3_result_double(context, 0.0);
    }
}

/*
** Returns the population standard deviation value
*/
static void stddevpopFinalize(sqlite3_context* context) {
    StddevCtx* p;
    p = sqlite3_aggregate_context(context, 0);
    if (p && p->cnt > 1) {
        sqlite3_result_double(context, sqrt(p->rS / p->cnt));
    } else {
        sqlite3_result_double(context, 0.0);
    }
}

/*
** Returns the sample variance value
*/
static void varianceFinalize(sqlite3_context* context) {
    StddevCtx* p;
    p = sqlite3_aggregate_context(context, 0);
    if (p && p->cnt > 1) {
        sqlite3_result_double(context, p->rS / (p->cnt - 1));
    } else {
        sqlite3_result_double(context, 0.0);
    }
}

/*
** Returns the population variance value
*/
static void variancepopFinalize(sqlite3_context* context) {
    StddevCtx* p;
    p = sqlite3_aggregate_context(context, 0);
    if (p && p->cnt > 1) {
        sqlite3_result_double(context, p->rS / p->cnt);
    } else {
        sqlite3_result_double(context, 0.0);
    }
}

#pragma endregion

#pragma region Percentile

/* The following object is the session context for a single percentile()
** function.  We have to remember all input Y values until the very end.
** Those values are accumulated in the Percentile.a[] array.
*/
typedef struct Percentile Percentile;
struct Percentile {
    unsigned nAlloc; /* Number of slots allocated for a[] */
    unsigned nUsed;  /* Number of slots actually used in a[] */
    double rPct;     /* 1.0 more than the value for P */
    double* a;       /* Array of Y values */
};

/*
** Return TRUE if the input floating-point number is an infinity.
*/
static int isInfinity(double r) {
    sqlite3_uint64 u;
    assert(sizeof(u) == sizeof(r));
    memcpy(&u, &r, sizeof(u));
    return ((u >> 52) & 0x7ff) == 0x7ff;
}

/*
** Return TRUE if two doubles differ by 0.001 or less
*/
static int sameValue(double a, double b) {
    a -= b;
    return a >= -0.001 && a <= 0.001;
}

/*
** The "step" function for percentile(Y,P) is called once for each
** input row.
*/
static void percentStep(sqlite3_context* pCtx, double rPct, int argc, sqlite3_value** argv) {
    Percentile* p;
    int eType;
    double y;

    /* Allocate the session context. */
    p = (Percentile*)sqlite3_aggregate_context(pCtx, sizeof(*p));
    if (p == 0)
        return;

    /* Remember the P value.  Throw an error if the P value is different
    ** from any prior row, per Requirement (2). */
    if (p->rPct == 0.0) {
        p->rPct = rPct + 1.0;
    } else if (!sameValue(p->rPct, rPct + 1.0)) {
        sqlite3_result_error(pCtx,
                             "2nd argument to percentile() is not the "
                             "same for all input rows",
                             -1);
        return;
    }

    /* Ignore rows for which Y is NULL */
    eType = sqlite3_value_type(argv[0]);
    if (eType == SQLITE_NULL)
        return;

    /* If not NULL, then Y must be numeric.  Otherwise throw an error.
    ** Requirement 4 */
    if (eType != SQLITE_INTEGER && eType != SQLITE_FLOAT) {
        sqlite3_result_error(pCtx,
                             "1st argument to percentile() is not "
                             "numeric",
                             -1);
        return;
    }

    /* Throw an error if the Y value is infinity or NaN */
    y = sqlite3_value_double(argv[0]);
    if (isInfinity(y)) {
        sqlite3_result_error(pCtx, "Inf input to percentile()", -1);
        return;
    }

    /* Allocate and store the Y */
    if (p->nUsed >= p->nAlloc) {
        unsigned n = p->nAlloc * 2 + 250;
        double* a = sqlite3_realloc64(p->a, sizeof(double) * n);
        if (a == 0) {
            sqlite3_free(p->a);
            memset(p, 0, sizeof(*p));
            sqlite3_result_error_nomem(pCtx);
            return;
        }
        p->nAlloc = n;
        p->a = a;
    }
    p->a[p->nUsed++] = y;
}

static void percentStepCustom(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 2);
    /* Requirement 3:  P must be a number between 0 and 100 */
    int eType = sqlite3_value_numeric_type(argv[1]);
    double rPct = sqlite3_value_double(argv[1]);
    if ((eType != SQLITE_INTEGER && eType != SQLITE_FLOAT) || rPct < 0.0 || rPct > 100.0) {
        sqlite3_result_error(pCtx,
                             "2nd argument to percentile() should be "
                             "a number between 0.0 and 100.0",
                             -1);
        return;
    }
    percentStep(pCtx, rPct, argc, argv);
}

static void percentStep25(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 25, argc, argv);
}

static void percentStep50(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 50, argc, argv);
}

static void percentStep75(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 75, argc, argv);
}

static void percentStep90(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 90, argc, argv);
}

static void percentStep95(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 95, argc, argv);
}

static void percentStep99(sqlite3_context* pCtx, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    percentStep(pCtx, 99, argc, argv);
}

/*
** Compare to doubles for sorting using qsort()
*/
static int SQLITE_CDECL doubleCmp(const void* pA, const void* pB) {
    double a = *(double*)pA;
    double b = *(double*)pB;
    if (a == b)
        return 0;
    if (a < b)
        return -1;
    return +1;
}

/*
** Called to compute the final output of percentile() and to clean
** up all allocated memory.
*/
static void percentFinal(sqlite3_context* pCtx) {
    Percentile* p;
    unsigned i1, i2;
    double v1, v2;
    double ix, vx;
    p = (Percentile*)sqlite3_aggregate_context(pCtx, 0);
    if (p == 0)
        return;
    if (p->a == 0)
        return;
    if (p->nUsed) {
        qsort(p->a, p->nUsed, sizeof(double), doubleCmp);
        ix = (p->rPct - 1.0) * (p->nUsed - 1) * 0.01;
        i1 = (unsigned)ix;
        i2 = ix == (double)i1 || i1 == p->nUsed - 1 ? i1 : i1 + 1;
        v1 = p->a[i1];
        v2 = p->a[i2];
        vx = v1 + (v2 - v1) * (ix - i1);
        sqlite3_result_double(pCtx, vx);
    }
    sqlite3_free(p->a);
    memset(p, 0, sizeof(*p));
}

#pragma endregion

int stats_scalar_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS;
    sqlite3_create_function(db, "stats_stddev", 1, flags, 0, 0, varianceStep, stddevFinalize);
    sqlite3_create_function(db, "stats_stddev_samp", 1, flags, 0, 0, varianceStep, stddevFinalize);
    sqlite3_create_function(db, "stats_stddev_pop", 1, flags, 0, 0, varianceStep,
                            stddevpopFinalize);
    sqlite3_create_function(db, "stats_var", 1, flags, 0, 0, varianceStep, varianceFinalize);
    sqlite3_create_function(db, "stats_var_samp", 1, flags, 0, 0, varianceStep, varianceFinalize);
    sqlite3_create_function(db, "stats_var_pop", 1, flags, 0, 0, varianceStep, variancepopFinalize);
    sqlite3_create_function(db, "stats_median", 1, flags, 0, 0, percentStep50, percentFinal);
    sqlite3_create_function(db, "stats_perc", 2, flags, 0, 0, percentStepCustom, percentFinal);
    sqlite3_create_function(db, "stats_p25", 1, flags, 0, 0, percentStep25, percentFinal);
    sqlite3_create_function(db, "stats_p75", 1, flags, 0, 0, percentStep75, percentFinal);
    sqlite3_create_function(db, "stats_p90", 1, flags, 0, 0, percentStep90, percentFinal);
    sqlite3_create_function(db, "stats_p95", 1, flags, 0, 0, percentStep95, percentFinal);
    sqlite3_create_function(db, "stats_p99", 1, flags, 0, 0, percentStep99, percentFinal);

    sqlite3_create_function(db, "stddev", 1, flags, 0, 0, varianceStep, stddevFinalize);
    sqlite3_create_function(db, "stddev_samp", 1, flags, 0, 0, varianceStep, stddevFinalize);
    sqlite3_create_function(db, "stddev_pop", 1, flags, 0, 0, varianceStep, stddevpopFinalize);
    sqlite3_create_function(db, "variance", 1, flags, 0, 0, varianceStep, varianceFinalize);
    sqlite3_create_function(db, "var_samp", 1, flags, 0, 0, varianceStep, varianceFinalize);
    sqlite3_create_function(db, "var_pop", 1, flags, 0, 0, varianceStep, variancepopFinalize);
    sqlite3_create_function(db, "median", 1, flags, 0, 0, percentStep50, percentFinal);
    sqlite3_create_function(db, "percentile", 2, flags, 0, 0, percentStepCustom, percentFinal);
    sqlite3_create_function(db, "percentile_25", 1, flags, 0, 0, percentStep25, percentFinal);
    sqlite3_create_function(db, "percentile_75", 1, flags, 0, 0, percentStep75, percentFinal);
    sqlite3_create_function(db, "percentile_90", 1, flags, 0, 0, percentStep90, percentFinal);
    sqlite3_create_function(db, "percentile_95", 1, flags, 0, 0, percentStep95, percentFinal);
    sqlite3_create_function(db, "percentile_99", 1, flags, 0, 0, percentStep99, percentFinal);

    return SQLITE_OK;
}
