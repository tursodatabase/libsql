// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite extension for working with time.

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "time/timex.h"

// result_blob converts a Time value to a blob and sets it as the result.
static void result_blob(sqlite3_context* context, Time t) {
    uint8_t buf[TIMEX_BLOB_SIZE];
    time_to_blob(t, buf);
    sqlite3_result_blob(context, buf, sizeof(buf), SQLITE_TRANSIENT);
}

// time_now()
static void fn_now(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 0);
    Time t = time_now();
    result_blob(context, t);
}

// time_date(year, month, day[, hour, min, sec[, nsec[, offset_sec]]])
static void fn_date(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3 || argc == 6 || argc == 7 || argc == 8);
    for (int i = 0; i < argc; i++) {
        if (sqlite3_value_type(argv[i]) != SQLITE_INTEGER) {
            sqlite3_result_error(context, "all parameters should be integers", -1);
            return;
        }
    }
    int year = sqlite3_value_int(argv[0]);
    int month = sqlite3_value_int(argv[1]);
    int day = sqlite3_value_int(argv[2]);

    int hour = 0;
    int min = 0;
    int sec = 0;
    if (argc >= 6) {
        hour = sqlite3_value_int(argv[3]);
        min = sqlite3_value_int(argv[4]);
        sec = sqlite3_value_int(argv[5]);
    }

    int nsec = 0;
    if (argc >= 7) {
        nsec = sqlite3_value_int(argv[6]);
    }

    int offset_sec = 0;
    if (argc == 8) {
        offset_sec = sqlite3_value_int(argv[7]);
    }

    Time t = time_date(year, month, day, hour, min, sec, nsec, offset_sec);
    result_blob(context, t);
}

// time_get_year(t)
// time_get_month(t)
// time_get_day(t)
// time_get_hour(t)
// time_get_minute(t)
// time_get_second(t)
// time_get_nano(t)
// time_get_weekday(t)
// time_get_yearday(t)
static void fn_extract(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    int (*extract)(Time t) = (int (*)(Time t))sqlite3_user_data(context);
    Time t = time_blob(sqlite3_value_blob(argv[0]));
    sqlite3_result_int(context, extract(t));
}

// time_get_isoyear(t)
static void fn_get_isoyear(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));
    int year, week;
    time_get_isoweek(t, &year, &week);
    sqlite3_result_int(context, year);
}

// time_get_isoweek(t)
static void fn_get_isoweek(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));
    int year, week;
    time_get_isoweek(t, &year, &week);
    sqlite3_result_int(context, week);
}

// get_field returns a part of the t according to a given field
static void get_field(sqlite3_context* context, Time t, const char* field) {
    // millennium, century, decade
    if (strcmp(field, "millennium") == 0) {
        int millennium = time_get_year(t) / 1000;
        sqlite3_result_int(context, millennium);
        return;
    }
    if (strcmp(field, "century") == 0) {
        int century = time_get_year(t) / 100;
        sqlite3_result_int(context, century);
        return;
    }
    if (strncmp(field, "decade", 6) == 0) {
        int decade = time_get_year(t) / 10;
        sqlite3_result_int(context, decade);
        return;
    }

    // year, quarter, month, day
    if (strcmp(field, "year") == 0 || strcmp(field, "years") == 0) {
        sqlite3_result_int(context, time_get_year(t));
        return;
    }
    if (strncmp(field, "quarter", 7) == 0) {
        int quarter = (time_get_month(t) - 1) / 3 + 1;
        sqlite3_result_int(context, quarter);
        return;
    }
    if (strncmp(field, "month", 5) == 0) {
        sqlite3_result_int(context, time_get_month(t));
        return;
    }
    if (strcmp(field, "day") == 0 || strcmp(field, "days") == 0) {
        sqlite3_result_int(context, time_get_day(t));
        return;
    }

    // hour, minute, second
    if (strncmp(field, "hour", 4) == 0) {
        sqlite3_result_int(context, time_get_hour(t));
        return;
    }
    if (strncmp(field, "minute", 6) == 0) {
        sqlite3_result_int(context, time_get_minute(t));
        return;
    }
    if (strncmp(field, "second", 6) == 0) {
        // including fractional part
        double sec = time_get_second(t) + t.nsec / 1e9;
        sqlite3_result_double(context, sec);
        return;
    }

    // millisecond, microsecond, nanosecond
    if (strncmp(field, "milli", 5) == 0) {
        int msec = time_get_nano(t) / 1000000;
        sqlite3_result_int(context, msec);
        return;
    }
    if (strncmp(field, "micro", 5) == 0) {
        int usec = time_get_nano(t) / 1000;
        sqlite3_result_int(context, usec);
        return;
    }
    if (strncmp(field, "nano", 4) == 0) {
        sqlite3_result_int(context, time_get_nano(t));
        return;
    }

    // isoyear, isoweek, isodow, yearday, weekday
    if (strcmp(field, "isoyear") == 0) {
        int year, week;
        time_get_isoweek(t, &year, &week);
        sqlite3_result_int(context, year);
        return;
    }
    if (strcmp(field, "isoweek") == 0 || strcmp(field, "week") == 0) {
        int year, week;
        time_get_isoweek(t, &year, &week);
        sqlite3_result_int(context, week);
        return;
    }
    if (strcmp(field, "isodow") == 0) {
        int isodow = time_get_weekday(t) == 0 ? 7 : time_get_weekday(t);
        sqlite3_result_int(context, isodow);
        return;
    }
    if (strcmp(field, "yearday") == 0 || strcmp(field, "doy") == 0 ||
        strcmp(field, "dayofyear") == 0) {
        sqlite3_result_int(context, time_get_yearday(t));
        return;
    }
    if (strcmp(field, "weekday") == 0 || strcmp(field, "dow") == 0 ||
        strcmp(field, "dayofweek") == 0) {
        sqlite3_result_int(context, time_get_weekday(t));
        return;
    }

    // epoch
    if (strcmp(field, "epoch") == 0) {
        // including fractional part
        double epoch = time_to_unix(t) + t.nsec / 1e9;
        sqlite3_result_double(context, epoch);
        return;
    }

    sqlite3_result_error(context, "unknown field", -1);
}

// time_get(t, field)
static void fn_get(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_TEXT) {
        sqlite3_result_error(context, "2nd parameter: should be a field name", -1);
        return;
    }
    const char* field = (const char*)sqlite3_value_text(argv[1]);

    get_field(context, t, field);
}

// date_part(field, t)
// Postgres-compatible.
static void date_part(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(context, "1st parameter: should be a field name", -1);
        return;
    }
    const char* field = (const char*)sqlite3_value_text(argv[0]);

    if (sqlite3_value_type(argv[1]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "2nd parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[1]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "2nd parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[1]));

    get_field(context, t, field);
}

// time_unix(sec[, nsec])
static void fn_unix(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1 || argc == 2);
    for (int i = 0; i < argc; i++) {
        if (sqlite3_value_type(argv[i]) != SQLITE_INTEGER) {
            sqlite3_result_error(context, "all parameters should be integers", -1);
            return;
        }
    }

    int64_t sec = sqlite3_value_int64(argv[0]);
    int64_t nsec = 0;
    if (argc == 2) {
        nsec = sqlite3_value_int64(argv[1]);
    }

    Time t = time_unix(sec, nsec);
    result_blob(context, t);
}

// time_milli(msec)
// time_micro(usec)
// time_nano(nsec)
static void fn_unix_n(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "parameter should be an integer", -1);
        return;
    }
    int64_t n = sqlite3_value_int64(argv[0]);
    Time (*convert)(int64_t n) = (Time(*)(int64_t))sqlite3_user_data(context);
    Time t = convert(n);
    result_blob(context, t);
}

// time_to_unix(t)
// time_to_milli(t)
// time_to_micro(t)
// time_to_nano(t)
static void fn_convert(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    int64_t (*convert)(Time t) = (int64_t(*)(Time t))sqlite3_user_data(context);
    Time t = time_blob(sqlite3_value_blob(argv[0]));
    sqlite3_result_int64(context, convert(t));
}

// time_after(t, u)
// time_before(t, u)
// time_compare(t, u)
// time_equal(t, u)
static void fn_compare(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "2nd parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[1]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "2nd parameter: invalid time blob size", -1);
        return;
    }
    Time u = time_blob(sqlite3_value_blob(argv[1]));

    int (*compare)(Time t, Time u) = (int (*)(Time, Time))sqlite3_user_data(context);
    sqlite3_result_int(context, compare(t, u));
}

// time_add(t, d)
static void fn_add(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "2nd parameter: should be an integer", -1);
        return;
    }
    Duration d = sqlite3_value_int64(argv[1]);

    Time r = time_add(t, d);
    result_blob(context, r);
}

// time_sub(t, u)
static void fn_sub(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "2nd parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[1]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "2nd parameter: invalid time blob size", -1);
        return;
    }
    Time u = time_blob(sqlite3_value_blob(argv[1]));

    Duration d = time_sub(t, u);
    sqlite3_result_int64(context, d);
}

// time_since(t)
static void fn_since(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    Duration d = time_since(t);
    sqlite3_result_int64(context, d);
}

// time_until(t)
static void fn_until(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "parameter should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    Duration d = time_until(t);
    sqlite3_result_int64(context, d);
}

// time_add_date(t, years[, months[, days]])
static void fn_add_date(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2 || argc == 3 || argc == 4);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "2nd parameter: should be an integer", -1);
        return;
    }
    int years = sqlite3_value_int(argv[1]);

    int months = 0;
    if (argc >= 3) {
        if (sqlite3_value_type(argv[2]) != SQLITE_INTEGER) {
            sqlite3_result_error(context, "3rd parameter: should be an integer", -1);
            return;
        }
        months = sqlite3_value_int(argv[2]);
    }

    int days = 0;
    if (argc == 4) {
        if (sqlite3_value_type(argv[3]) != SQLITE_INTEGER) {
            sqlite3_result_error(context, "4th parameter: should be an integer", -1);
            return;
        }
        days = sqlite3_value_int(argv[3]);
    }

    Time r = time_add_date(t, years, months, days);
    result_blob(context, r);
}

// trunc_field truncates t according to a given field
static void trunc_field(sqlite3_context* context, Time t, const char* field) {
    // millennium, century, decade
    if (strcmp(field, "millennium") == 0) {
        int year = time_get_year(t);
        int millennium = year / 1000 * 1000;
        Time r = time_date(millennium, January, 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "century") == 0) {
        int year = time_get_year(t);
        int century = year / 100 * 100;
        Time r = time_date(century, January, 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "decade") == 0) {
        int year = time_get_year(t);
        int decade = year / 10 * 10;
        Time r = time_date(decade, January, 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }

    // year, quarter, month, week, day
    if (strcmp(field, "year") == 0) {
        Time r = time_date(time_get_year(t), January, 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "quarter") == 0) {
        int quarter = (time_get_month(t) - 1) / 3;
        Time r = time_date(time_get_year(t), quarter * 3 + 1, 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "month") == 0) {
        Time r = time_date(time_get_year(t), time_get_month(t), 1, 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "week") == 0) {
        int year, week;
        time_get_isoweek(t, &year, &week);
        Time r = time_date(year, January, 1, 0, 0, 0, 0, 0);
        r = time_add_date(r, 0, 0, (week - 1) * 7);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "day") == 0) {
        Time r =
            time_date(time_get_year(t), time_get_month(t), time_get_day(t), 0, 0, 0, 0, TIMEX_UTC);
        result_blob(context, r);
        return;
    }

    // hour, minute, second, millisecond, microsecond
    if (strcmp(field, "hour") == 0) {
        Time r = time_truncate(t, Hour);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "minute") == 0) {
        Time r = time_truncate(t, Minute);
        result_blob(context, r);
        return;
    }
    if (strcmp(field, "second") == 0) {
        Time r = time_truncate(t, Second);
        result_blob(context, r);
        return;
    }
    if (strncmp(field, "milli", 5) == 0) {
        int64_t nsec = (t.nsec / 1000000) * 1000000;
        Time r = time_unix(time_to_unix(t), nsec);
        result_blob(context, r);
        return;
    }
    if (strncmp(field, "micro", 5) == 0) {
        int64_t nsec = (t.nsec / 1000) * 1000;
        Time r = time_unix(time_to_unix(t), nsec);
        result_blob(context, r);
        return;
    }

    sqlite3_result_error(context, "unknown field", -1);
}

// time_trunc(t, field)
// time_trunc(t, d)
static void fn_trunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    // first parameter is a time blob
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    // second parameter can be a custom duration
    if (sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
        // truncate to custom duration
        Duration d = sqlite3_value_int64(argv[1]);
        Time r = time_truncate(t, d);
        result_blob(context, r);
        return;
    }

    // or a field name
    if (sqlite3_value_type(argv[1]) != SQLITE_TEXT) {
        sqlite3_result_error(context, "2nd parameter: should be a field name", -1);
        return;
    }
    const char* field = (const char*)sqlite3_value_text(argv[1]);

    // truncate to field
    trunc_field(context, t, field);
}

// date_trunc(field, t)
// Postgres-compatible.
static void date_trunc(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    // first parameter is a field name
    if (sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
        sqlite3_result_error(context, "1st parameter: should be a field name", -1);
        return;
    }
    const char* field = (const char*)sqlite3_value_text(argv[0]);

    // second parameter is a time blob
    if (sqlite3_value_type(argv[1]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "2nd parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[1]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "2nd parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[1]));

    trunc_field(context, t, field);
}

// time_round(t, d)
static void fn_round(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "2nd parameter: should be an integer", -1);
        return;
    }
    Duration d = sqlite3_value_int64(argv[1]);

    Time r = time_round(t, d);
    result_blob(context, r);
}

// time_fmt_iso(t[, offset_sec])
// time_fmt_datetime(t[, offset_sec])
// time_fmt_date(t[, offset_sec])
// time_fmt_time(t[, offset_sec])
static void fn_format(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1 || argc == 2);
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB) {
        sqlite3_result_error(context, "1st parameter: should be a time blob", -1);
        return;
    }
    if (sqlite3_value_bytes(argv[0]) != TIMEX_BLOB_SIZE) {
        sqlite3_result_error(context, "1st parameter: invalid time blob size", -1);
        return;
    }
    Time t = time_blob(sqlite3_value_blob(argv[0]));

    int offset_sec = 0;
    if (argc == 2) {
        if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
            sqlite3_result_error(context, "2nd parameter: should be an integer", -1);
            return;
        }
        offset_sec = sqlite3_value_int(argv[1]);
    }

    char buf[36];
    size_t (*format)(char* buf, size_t size, Time t, int offset_sec) =
        (size_t(*)(char*, size_t, Time, int))sqlite3_user_data(context);
    format(buf, sizeof(buf), t, offset_sec);
    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);
}

// time_parse(v)
static void fn_parse(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    const char* val = (const char*)sqlite3_value_text(argv[0]);
    Time t = time_parse(val);
    result_blob(context, t);
}

// dur_h(), dur_m(), dur_s(), dur_ms(), dur_us(), dur_ns()
static void fn_dur_const(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 0);
    int64_t d = (intptr_t)sqlite3_user_data(context);
    sqlite3_result_int64(context, d);
}

int time_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    static const int flags_nd = SQLITE_UTF8 | SQLITE_INNOCUOUS;

    // constructors
    sqlite3_create_function(db, "time_now", 0, flags_nd, 0, fn_now, 0, 0);
    sqlite3_create_function(db, "time_date", 3, flags, 0, fn_date, 0, 0);
    sqlite3_create_function(db, "time_date", 6, flags, 0, fn_date, 0, 0);
    sqlite3_create_function(db, "time_date", 7, flags, 0, fn_date, 0, 0);
    sqlite3_create_function(db, "time_date", 8, flags, 0, fn_date, 0, 0);

    // time parts
    sqlite3_create_function(db, "time_get_year", 1, flags, time_get_year, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_month", 1, flags, time_get_month, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_day", 1, flags, time_get_day, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_hour", 1, flags, time_get_hour, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_minute", 1, flags, time_get_minute, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_second", 1, flags, time_get_second, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_nano", 1, flags, time_get_nano, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_weekday", 1, flags, time_get_weekday, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_yearday", 1, flags, time_get_yearday, fn_extract, 0, 0);
    sqlite3_create_function(db, "time_get_isoyear", 1, flags, 0, fn_get_isoyear, 0, 0);
    sqlite3_create_function(db, "time_get_isoweek", 1, flags, 0, fn_get_isoweek, 0, 0);
    sqlite3_create_function(db, "time_get", 2, flags, 0, fn_get, 0, 0);

    // unix time
    sqlite3_create_function(db, "time_unix", 1, flags, 0, fn_unix, 0, 0);
    sqlite3_create_function(db, "time_unix", 2, flags, 0, fn_unix, 0, 0);
    sqlite3_create_function(db, "time_milli", 1, flags, time_milli, fn_unix_n, 0, 0);
    sqlite3_create_function(db, "time_micro", 1, flags, time_micro, fn_unix_n, 0, 0);
    sqlite3_create_function(db, "time_nano", 1, flags, time_nano, fn_unix_n, 0, 0);
    sqlite3_create_function(db, "time_to_unix", 1, flags, time_to_unix, fn_convert, 0, 0);
    sqlite3_create_function(db, "time_to_milli", 1, flags, time_to_milli, fn_convert, 0, 0);
    sqlite3_create_function(db, "time_to_micro", 1, flags, time_to_micro, fn_convert, 0, 0);
    sqlite3_create_function(db, "time_to_nano", 1, flags, time_to_nano, fn_convert, 0, 0);

    // comparison
    sqlite3_create_function(db, "time_after", 2, flags, time_after, fn_compare, 0, 0);
    sqlite3_create_function(db, "time_before", 2, flags, time_before, fn_compare, 0, 0);
    sqlite3_create_function(db, "time_compare", 2, flags, time_compare, fn_compare, 0, 0);
    sqlite3_create_function(db, "time_equal", 2, flags, time_equal, fn_compare, 0, 0);

    // arithmetic
    sqlite3_create_function(db, "time_add", 2, flags, 0, fn_add, 0, 0);
    sqlite3_create_function(db, "time_sub", 2, flags, 0, fn_sub, 0, 0);
    sqlite3_create_function(db, "time_since", 1, flags_nd, 0, fn_since, 0, 0);
    sqlite3_create_function(db, "time_until", 1, flags_nd, 0, fn_until, 0, 0);
    sqlite3_create_function(db, "time_add_date", 2, flags, 0, fn_add_date, 0, 0);
    sqlite3_create_function(db, "time_add_date", 3, flags, 0, fn_add_date, 0, 0);
    sqlite3_create_function(db, "time_add_date", 4, flags, 0, fn_add_date, 0, 0);

    // rounding
    sqlite3_create_function(db, "time_trunc", 2, flags, 0, fn_trunc, 0, 0);
    sqlite3_create_function(db, "time_round", 2, flags, 0, fn_round, 0, 0);

    // formatting
    sqlite3_create_function(db, "time_fmt_iso", 1, flags, time_fmt_iso, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_iso", 2, flags, time_fmt_iso, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_datetime", 1, flags, time_fmt_datetime, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_datetime", 2, flags, time_fmt_datetime, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_date", 1, flags, time_fmt_date, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_date", 2, flags, time_fmt_date, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_time", 1, flags, time_fmt_time, fn_format, 0, 0);
    sqlite3_create_function(db, "time_fmt_time", 2, flags, time_fmt_time, fn_format, 0, 0);
    sqlite3_create_function(db, "time_parse", 1, flags, 0, fn_parse, 0, 0);

    // duration constants
    sqlite3_create_function(db, "dur_h", 0, flags, (void*)Hour, fn_dur_const, 0, 0);
    sqlite3_create_function(db, "dur_m", 0, flags, (void*)Minute, fn_dur_const, 0, 0);
    sqlite3_create_function(db, "dur_s", 0, flags, (void*)Second, fn_dur_const, 0, 0);
    sqlite3_create_function(db, "dur_ms", 0, flags, (void*)Millisecond, fn_dur_const, 0, 0);
    sqlite3_create_function(db, "dur_us", 0, flags, (void*)Microsecond, fn_dur_const, 0, 0);
    sqlite3_create_function(db, "dur_ns", 0, flags, (void*)Nanosecond, fn_dur_const, 0, 0);

    // postgres compatibility layer
    sqlite3_create_function(db, "age", 2, flags, 0, fn_sub, 0, 0);
    sqlite3_create_function(db, "date_add", 2, flags, 0, fn_add, 0, 0);
    sqlite3_create_function(db, "date_part", 2, flags, 0, date_part, 0, 0);
    sqlite3_create_function(db, "date_trunc", 2, flags, 0, date_trunc, 0, 0);
    sqlite3_create_function(db, "make_date", 3, flags, 0, fn_date, 0, 0);
    sqlite3_create_function(db, "make_timestamp", 6, flags, 0, fn_date, 0, 0);
    sqlite3_create_function(db, "now", 0, flags_nd, 0, fn_now, 0, 0);
    sqlite3_create_function(db, "to_timestamp", 1, flags, 0, fn_unix, 0, 0);

    return SQLITE_OK;
}
