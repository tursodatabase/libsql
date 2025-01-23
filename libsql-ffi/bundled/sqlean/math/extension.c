// Originally from SQLite 3.42.0 source code (func.c), Public Domain
// Updated as of 3.46.0

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// SQLite math functions.

#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#if defined(HAVE_STDINT_H) /* Use this case if we have ANSI headers */
#define SQLITE_PTR_TO_INT(X) ((int)(intptr_t)(X))
#elif defined(__PTRDIFF_TYPE__) /* This case should work for GCC */
#define SQLITE_PTR_TO_INT(X) ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__) /* Works for compilers other than LLVM */
#define SQLITE_PTR_TO_INT(X) ((int)(((char*)X) - (char*)0))
#else /* Generates a warning - but it always works */
#define SQLITE_PTR_TO_INT(X) ((int)(X))
#endif

/* Mathematical Constants */
#ifndef M_PI
#define M_PI 3.141592653589793238462643383279502884
#endif
#ifndef M_LN10
#define M_LN10 2.302585092994045684017991454684364208
#endif
#ifndef M_LN2
#define M_LN2 0.693147180559945309417232121458176568
#endif

/*
** Implementation SQL functions:
**
**   ceil(X)
**   ceiling(X)
**   floor(X)
**
** The sqlite3_user_data() pointer is a pointer to the libm implementation
** of the underlying C function.
*/
static void ceilingFunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    switch (sqlite3_value_numeric_type(argv[0])) {
        case SQLITE_INTEGER: {
            sqlite3_result_int64(context, sqlite3_value_int64(argv[0]));
            break;
        }
        case SQLITE_FLOAT: {
            double (*x)(double) = (double (*)(double))sqlite3_user_data(context);
            sqlite3_result_double(context, x(sqlite3_value_double(argv[0])));
            break;
        }
        default: {
            break;
        }
    }
}

/*
** On some systems, ceil() and floor() are intrinsic function.  You are
** unable to take a pointer to these functions.  Hence, we here wrap them
** in our own actual functions.
*/
static double xCeil(double x) {
    return ceil(x);
}
static double xFloor(double x) {
    return floor(x);
}

/*
** Some systems do not have log2() and log10() in their standard math
** libraries.
*/
#if defined(HAVE_LOG10) && HAVE_LOG10 == 0
#define log10(X) (0.4342944819032517867 * log(X))
#endif
#if defined(HAVE_LOG2) && HAVE_LOG2 == 0
#define log2(X) (1.442695040888963456 * log(X))
#endif

/*
** Implementation of SQL functions:
**
**   ln(X)       - natural logarithm
**   log(X)      - log X base 10
**   log10(X)    - log X base 10
**   log(B,X)    - log X base B
*/
static void logFunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    double x, b, ans;
    assert(argc == 1 || argc == 2);
    switch (sqlite3_value_numeric_type(argv[0])) {
        case SQLITE_INTEGER:
        case SQLITE_FLOAT:
            x = sqlite3_value_double(argv[0]);
            if (x <= 0.0)
                return;
            break;
        default:
            return;
    }
    if (argc == 2) {
        switch (sqlite3_value_numeric_type(argv[0])) {
            case SQLITE_INTEGER:
            case SQLITE_FLOAT:
                b = log(x);
                if (b <= 0.0)
                    return;
                x = sqlite3_value_double(argv[1]);
                if (x <= 0.0)
                    return;
                break;
            default:
                return;
        }
        ans = log(x) / b;
    } else {
        switch (SQLITE_PTR_TO_INT(sqlite3_user_data(context))) {
            case 1:
                ans = log10(x);
                break;
            case 2:
                ans = log2(x);
                break;
            default:
                ans = log(x);
                break;
        }
    }
    sqlite3_result_double(context, ans);
}

/*
** Functions to converts degrees to radians and radians to degrees.
*/
static double degToRad(double x) {
    return x * (M_PI / 180.0);
}
static double radToDeg(double x) {
    return x * (180.0 / M_PI);
}

/*
** Implementation of 1-argument SQL math functions:
**
**   exp(X)  - Compute e to the X-th power
*/
static void math1Func(sqlite3_context* context, int argc, sqlite3_value** argv) {
    int type0;
    double v0, ans;
    double (*x)(double);
    assert(argc == 1);
    type0 = sqlite3_value_numeric_type(argv[0]);
    if (type0 != SQLITE_INTEGER && type0 != SQLITE_FLOAT)
        return;
    v0 = sqlite3_value_double(argv[0]);
    x = (double (*)(double))sqlite3_user_data(context);
    ans = x(v0);
    sqlite3_result_double(context, ans);
}

/*
** Implementation of 2-argument SQL math functions:
**
**   power(X,Y)  - Compute X to the Y-th power
*/
static void math2Func(sqlite3_context* context, int argc, sqlite3_value** argv) {
    int type0, type1;
    double v0, v1, ans;
    double (*x)(double, double);
    assert(argc == 2);
    type0 = sqlite3_value_numeric_type(argv[0]);
    if (type0 != SQLITE_INTEGER && type0 != SQLITE_FLOAT)
        return;
    type1 = sqlite3_value_numeric_type(argv[1]);
    if (type1 != SQLITE_INTEGER && type1 != SQLITE_FLOAT)
        return;
    v0 = sqlite3_value_double(argv[0]);
    v1 = sqlite3_value_double(argv[1]);
    x = (double (*)(double, double))sqlite3_user_data(context);
    ans = x(v0, v1);
    sqlite3_result_double(context, ans);
}

/*
** Implementation of 0-argument pi() function.
*/
static void piFunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 0);
    (void)argv;
    sqlite3_result_double(context, M_PI);
}

/*
** Implementation of the round() function
*/
static void roundFunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    int n = 0;
    double r;
    char* zBuf;
    assert(argc == 1 || argc == 2);
    if (argc == 2) {
        if (SQLITE_NULL == sqlite3_value_type(argv[1]))
            return;
        n = sqlite3_value_int(argv[1]);
        if (n > 30)
            n = 30;
        if (n < 0)
            n = 0;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
        return;
    r = sqlite3_value_double(argv[0]);
    /* If Y==0 and X will fit in a 64-bit int,
    ** handle the rounding directly,
    ** otherwise use printf.
    */
    if (r < -4503599627370496.0 || r > +4503599627370496.0) {
        /* The value has no fractional part so there is nothing to round */
    } else if (n == 0) {
        r = (double)((sqlite_int64)(r + (r < 0 ? -0.5 : +0.5)));
    } else {
        zBuf = sqlite3_mprintf("%!.*f", n, r);
        if (zBuf == 0) {
            sqlite3_result_error_nomem(context);
            return;
        }
        // sqlite3AtoF(zBuf, &r, sqlite3Strlen30(zBuf), SQLITE_UTF8);
        r = strtod(zBuf, NULL);
        sqlite3_free(zBuf);
    }
    sqlite3_result_double(context, r);
}

int math_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;

    sqlite3_create_function(db, "math_round", 1, flags, 0, roundFunc, 0, 0);
    sqlite3_create_function(db, "math_round", 2, flags, 0, roundFunc, 0, 0);
    sqlite3_create_function(db, "math_ceil", 1, flags, xCeil, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "math_floor", 1, flags, xFloor, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "math_trunc", 1, flags, trunc, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "math_ln", 1, flags, 0, logFunc, 0, 0);
    sqlite3_create_function(db, "math_log", 1, flags, (void*)(1), logFunc, 0, 0);
    sqlite3_create_function(db, "math_log10", 1, flags, (void*)(1), logFunc, 0, 0);
    sqlite3_create_function(db, "math_log2", 1, flags, (void*)(2), logFunc, 0, 0);
    sqlite3_create_function(db, "math_log", 2, flags, 0, logFunc, 0, 0);
    sqlite3_create_function(db, "math_exp", 1, flags, exp, math1Func, 0, 0);
    sqlite3_create_function(db, "math_pow", 2, flags, pow, math2Func, 0, 0);
    sqlite3_create_function(db, "math_mod", 2, flags, fmod, math2Func, 0, 0);
    sqlite3_create_function(db, "math_acos", 1, flags, acos, math1Func, 0, 0);
    sqlite3_create_function(db, "math_asin", 1, flags, asin, math1Func, 0, 0);
    sqlite3_create_function(db, "math_atan", 1, flags, atan, math1Func, 0, 0);
    sqlite3_create_function(db, "math_atan2", 2, flags, atan2, math2Func, 0, 0);
    sqlite3_create_function(db, "math_cos", 1, flags, cos, math1Func, 0, 0);
    sqlite3_create_function(db, "math_sin", 1, flags, sin, math1Func, 0, 0);
    sqlite3_create_function(db, "math_tan", 1, flags, tan, math1Func, 0, 0);
    sqlite3_create_function(db, "math_cosh", 1, flags, cosh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_sinh", 1, flags, sinh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_tanh", 1, flags, tanh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_acosh", 1, flags, acosh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_asinh", 1, flags, asinh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_atanh", 1, flags, atanh, math1Func, 0, 0);
    sqlite3_create_function(db, "math_sqrt", 1, flags, sqrt, math1Func, 0, 0);
    sqlite3_create_function(db, "math_radians", 1, flags, degToRad, math1Func, 0, 0);
    sqlite3_create_function(db, "math_degrees", 1, flags, radToDeg, math1Func, 0, 0);
    sqlite3_create_function(db, "math_pi", 0, flags, 0, piFunc, 0, 0);

    sqlite3_create_function(db, "ceil", 1, flags, xCeil, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "ceiling", 1, flags, xCeil, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "floor", 1, flags, xFloor, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "trunc", 1, flags, trunc, ceilingFunc, 0, 0);
    sqlite3_create_function(db, "ln", 1, flags, 0, logFunc, 0, 0);
    sqlite3_create_function(db, "log", 1, flags, (void*)(1), logFunc, 0, 0);
    sqlite3_create_function(db, "log10", 1, flags, (void*)(1), logFunc, 0, 0);
    sqlite3_create_function(db, "log2", 1, flags, (void*)(2), logFunc, 0, 0);
    sqlite3_create_function(db, "log", 2, flags, 0, logFunc, 0, 0);
    sqlite3_create_function(db, "exp", 1, flags, exp, math1Func, 0, 0);
    sqlite3_create_function(db, "pow", 2, flags, pow, math2Func, 0, 0);
    sqlite3_create_function(db, "power", 2, flags, pow, math2Func, 0, 0);
    sqlite3_create_function(db, "mod", 2, flags, fmod, math2Func, 0, 0);
    sqlite3_create_function(db, "acos", 1, flags, acos, math1Func, 0, 0);
    sqlite3_create_function(db, "asin", 1, flags, asin, math1Func, 0, 0);
    sqlite3_create_function(db, "atan", 1, flags, atan, math1Func, 0, 0);
    sqlite3_create_function(db, "atan2", 2, flags, atan2, math2Func, 0, 0);
    sqlite3_create_function(db, "cos", 1, flags, cos, math1Func, 0, 0);
    sqlite3_create_function(db, "sin", 1, flags, sin, math1Func, 0, 0);
    sqlite3_create_function(db, "tan", 1, flags, tan, math1Func, 0, 0);
    sqlite3_create_function(db, "cosh", 1, flags, cosh, math1Func, 0, 0);
    sqlite3_create_function(db, "sinh", 1, flags, sinh, math1Func, 0, 0);
    sqlite3_create_function(db, "tanh", 1, flags, tanh, math1Func, 0, 0);
    sqlite3_create_function(db, "acosh", 1, flags, acosh, math1Func, 0, 0);
    sqlite3_create_function(db, "asinh", 1, flags, asinh, math1Func, 0, 0);
    sqlite3_create_function(db, "atanh", 1, flags, atanh, math1Func, 0, 0);
    sqlite3_create_function(db, "sqrt", 1, flags, sqrt, math1Func, 0, 0);
    sqlite3_create_function(db, "radians", 1, flags, degToRad, math1Func, 0, 0);
    sqlite3_create_function(db, "degrees", 1, flags, radToDeg, math1Func, 0, 0);
    sqlite3_create_function(db, "pi", 0, flags, 0, piFunc, 0, 0);

    return SQLITE_OK;
}
