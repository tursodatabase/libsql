// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Based on Go's time package, BSD 3-Clause License
// https://github.com/golang/go

// Time functions and methods.

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "time/timex.h"

#pragma region Private

static const int64_t seconds_per_minute = 60;
static const int64_t seconds_per_hour = 60 * seconds_per_minute;
static const int64_t seconds_per_day = 24 * seconds_per_hour;
static const int64_t seconds_per_week = 7 * seconds_per_day;
static const int64_t days_per_400_years = 365 * 400 + 97;
static const int64_t days_per_100_years = 365 * 100 + 24;
static const int64_t days_per_4_years = 365 * 4 + 1;

// The unsigned zero year for internal calculations.
// Must be 1 mod 400, and times before it will not compute correctly,
// but otherwise can be changed at will.
static const int64_t absolute_zero_year = -292277022399LL;

// Offsets to convert between internal and absolute or Unix times.
// = (absoluteZeroYear - internalYear) * 365.2425 * secondsPerDay
static const int64_t absolute_to_internal = -9223371966579724800LL;
static const int64_t internal_to_absolute = -absolute_to_internal;

static const int64_t unix_to_internal =
    (1969 * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400) * seconds_per_day;
static const int64_t internal_to_unix = -unix_to_internal;

// days_before[m] counts the number of days in a non-leap year
// before month m begins. There is an entry for m=12, counting
// the number of days before January of next year (365).
static const int days_before[] = {
    0,
    31,
    31 + 28,
    31 + 28 + 31,
    31 + 28 + 31 + 30,
    31 + 28 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
};

// norm returns nhi, nlo such that
//
//	hi * base + lo == nhi * base + nlo
//	0 <= nlo < base
static void norm(int hi, int lo, int base, int* nhi, int* nlo) {
    if (lo < 0) {
        int n = (-lo - 1) / base + 1;
        hi -= n;
        lo += n * base;
    }
    if (lo >= base) {
        int n = lo / base;
        hi += n;
        lo -= n * base;
    }
    *nhi = hi;
    *nlo = lo;
}

// days_since_epoch takes a year and returns the number of days from
// the absolute epoch to the start of that year.
// This is basically (year - zeroYear) * 365, but accounting for leap days.
static uint64_t days_since_epoch(int year) {
    uint64_t y = year - absolute_zero_year;

    // Add in days from 400-year cycles.
    uint64_t n = y / 400;
    y -= 400 * n;
    uint64_t d = days_per_400_years * n;

    // Add in 100-year cycles.
    n = y / 100;
    y -= 100 * n;
    d += days_per_100_years * n;

    // Add in 4-year cycles.
    n = y / 4;
    y -= 4 * n;
    d += days_per_4_years * n;

    // Add in non-leap years.
    n = y;
    d += 365 * n;

    return d;
}

// is_leap reports whether the year is a leap year.
static bool is_leap(int year) {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

static int64_t unix_sec(Time t) {
    return t.sec + internal_to_unix;
}

static Time unix_time(int64_t sec, int32_t nsec) {
    return (Time){sec + unix_to_internal, nsec};
}

// abs_time returns the time t as an absolute time, adjusted by the zone offset.
// It is called when computing a presentation property like Month or Hour.
static uint64_t abs_time(Time t) {
    return t.sec + internal_to_absolute;
}

// abs_weekday is like Weekday but operates on an absolute time.
static enum Weekday abs_weekday(uint64_t abs) {
    // January 1 of the absolute year, like January 1 of 2001, was a Monday.
    uint64_t sec = (abs + Monday * seconds_per_day) % seconds_per_week;
    return sec / seconds_per_day;
}

static void abs_date(uint64_t abs, int* year, int* yday) {
    // Split into time and day.
    uint64_t d = abs / seconds_per_day;

    // Account for 400 year cycles.
    uint64_t n = d / days_per_400_years;
    uint64_t y = 400 * n;
    d -= days_per_400_years * n;

    // Cut off 100-year cycles.
    // The last cycle has one extra leap year, so on the last day
    // of that year, day / days_per_100_years will be 4 instead of 3.
    // Cut it back down to 3 by subtracting n>>2.
    n = d / days_per_100_years;
    n -= n >> 2;
    y += 100 * n;
    d -= days_per_100_years * n;

    // Cut off 4-year cycles.
    // The last cycle has a missing leap year, which does not
    // affect the computation.
    n = d / days_per_4_years;
    y += 4 * n;
    d -= days_per_4_years * n;

    // Cut off years within a 4-year cycle.
    // The last year is a leap year, so on the last day of that year,
    // day / 365 will be 4 instead of 3. Cut it back down to 3
    // by subtracting n>>2.
    n = d / 365;
    n -= n >> 2;
    y += n;
    d -= 365 * n;

    *year = y + absolute_zero_year;
    *yday = d;
}

static void abs_date_full(uint64_t abs, int* year, enum Month* month, int* day, int* yday) {
    abs_date(abs, year, yday);

    *day = *yday;
    if (is_leap(*year)) {
        // Leap year
        if (*day > 31 + 29 - 1) {
            // After leap day; pretend it wasn't there.
            *day -= 1;
        }
        if (*day == 31 + 29 - 1) {
            // Leap day.
            *month = February;
            *day = 29;
            return;
        }
    }

    // Estimate month on assumption that every month has 31 days.
    // The estimate may be too low by at most one month, so adjust.
    *month = *day / 31;
    int end = days_before[(int)(*month) + 1];
    int begin;
    if (*day >= end) {
        *month += 1;
        begin = end;
    } else {
        begin = days_before[(int)(*month)];
    }

    *month += 1;  // because January is 1
    *day = *day - begin + 1;
}

void abs_clock(uint64_t abs, int* hour, int* min, int* sec) {
    *sec = abs % seconds_per_day;
    *hour = *sec / seconds_per_hour;
    *sec -= *hour * seconds_per_hour;
    *min = *sec / seconds_per_minute;
    *sec -= *min * seconds_per_minute;
}

// tless_than_half reports whether x+x < y but avoids overflow,
// assuming x and y are both positive (Duration is signed).
static bool tless_than_half(Duration x, Duration y) {
    return (uint64_t)x + (uint64_t)x < (uint64_t)y;
}

// time_div divides t by d and returns the remainder.
// Only supports d which is a multiple of 1 second.
static Duration time_div(Time t, Duration d) {
    if (d % Second != 0) {
        return 0;
    }

    bool neg = false;
    int64_t sec = t.sec;
    int64_t nsec = t.nsec;
    if (sec < 0) {
        // Operate on absolute value.
        neg = true;
        sec = -sec;
        nsec = -nsec;
        if (nsec < 0) {
            nsec += 1e9;
            sec--;  // sec >= 1 before the -- so safe
        }
    }

    // d is a multiple of 1 second.
    int64_t d1 = d / Second;
    Duration r = (sec % d1) * Second + nsec;

    if (neg && r != 0) {
        r = d - r;
    }
    return r;
}

#pragma endregion

#pragma region Constructors

// time_now returns the current time in UTC.
Time time_now(void) {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    return unix_time(ts.tv_sec, ts.tv_nsec);
}

// time_date returns the Time corresponding to
// yyyy-mm-dd hh:mm:ss + nsec nanoseconds
//
// The month, day, hour, min, sec, and nsec values may be outside
// their usual ranges and will be normalized during the conversion.
// For example, October 32 converts to November 1.
//
// The time is converted to UTC using offset_sec in seconds east of UTC.
Time time_date(int year,
               enum Month month,
               int day,
               int hour,
               int min,
               int sec,
               int nsec,
               int offset_sec) {
    // Normalize month, overflowing into year.
    int m = month - 1;
    norm(year, m, 12, &year, &m);
    month = m + 1;

    // Normalize nsec, sec, min, hour, overflowing into day.
    norm(sec, nsec, 1000000000, &sec, &nsec);
    norm(min, sec, 60, &min, &sec);
    norm(hour, min, 60, &hour, &min);
    norm(day, hour, 24, &day, &hour);

    // Compute days since the absolute epoch.
    uint64_t d = days_since_epoch(year);

    // Add in days before this month.
    d += days_before[month - 1];
    if (is_leap(year) && month >= March) {
        d++;  // February 29
    }

    // Add in days before today.
    d += day - 1;

    // Add in time elapsed today.
    uint64_t abs = d * seconds_per_day;
    abs += hour * seconds_per_hour + min * seconds_per_minute + sec;

    // Convert to UTC.
    abs -= offset_sec;

    return (Time){abs + absolute_to_internal, nsec};
}

#pragma endregion

#pragma region Time parts

// time_get_date returns the year, month, and day in which t occurs.
void time_get_date(Time t, int* year, enum Month* month, int* day) {
    uint64_t abs = abs_time(t);
    int yday;
    abs_date_full(abs, year, month, day, &yday);
}

// time_get_year returns the year in which t occurs.
int time_get_year(Time t) {
    uint64_t abs = abs_time(t);
    int year, yday;
    abs_date(abs, &year, &yday);
    return year;
}

// time_get_month returns the month of the year specified by t.
enum Month time_get_month(Time t) {
    uint64_t abs = abs_time(t);
    int year, day, yday;
    enum Month month;
    abs_date_full(abs, &year, &month, &day, &yday);
    return month;
}

// time_get_day returns the day of the month specified by t.
int time_get_day(Time t) {
    uint64_t abs = abs_time(t);
    int year, day, yday;
    enum Month month;
    abs_date_full(abs, &year, &month, &day, &yday);
    return day;
}

// time_get_clock returns the hour, minute, and second within the day specified by t.
void time_get_clock(Time t, int* hour, int* min, int* sec) {
    uint64_t abs = abs_time(t);
    abs_clock(abs, hour, min, sec);
}

// time_get_hour returns the hour within the day specified by t, in the range [0, 23].
int time_get_hour(Time t) {
    uint64_t abs = abs_time(t);
    return (abs % seconds_per_day) / seconds_per_hour;
}

// time_get_minute returns the minute offset within the hour specified by t, in the range [0, 59].
int time_get_minute(Time t) {
    uint64_t abs = abs_time(t);
    return (abs % seconds_per_hour) / seconds_per_minute;
}

// time_get_second returns the second offset within the minute specified by t, in the range [0, 59].
int time_get_second(Time t) {
    uint64_t abs = abs_time(t);
    return abs % seconds_per_minute;
}

// time_get_nano returns the nanosecond offset within the second specified by t,
// in the range [0, 999999999].
int time_get_nano(Time t) {
    return t.nsec;
}

// time_get_weekday returns the day of the week specified by t.
enum Weekday time_get_weekday(Time t) {
    uint64_t abs = abs_time(t);
    return abs_weekday(abs);
}

// time_get_yearday returns the day of the year specified by t, in the range [1,365] for non-leap
// years, and [1,366] in leap years.
int time_get_yearday(Time t) {
    uint64_t abs = abs_time(t);
    int year, yday;
    abs_date(abs, &year, &yday);
    return yday + 1;
}

// time_get_isoweek returns the ISO 8601 year and week number in which t occurs.
// Week ranges from 1 to 53. Jan 01 to Jan 03 of year n might belong to
// week 52 or 53 of year n-1, and Dec 29 to Dec 31 might belong to week 1 of year n+1.
void time_get_isoweek(Time t, int* year, int* week) {
    // According to the rule that the first calendar week of a calendar year is
    // the week including the first Thursday of that year, and that the last one is
    // the week immediately preceding the first calendar week of the next calendar year.
    // See https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en:term:3.1.1.23 for details.

    // weeks start with Monday
    // Monday Tuesday Wednesday Thursday Friday Saturday Sunday
    // 1      2       3         4        5      6        7
    // +3     +2      +1        0        -1     -2       -3
    // the offset to Thursday
    uint64_t abs = abs_time(t);
    int d = (Thursday - abs_weekday(abs));
    // handle Sunday
    if (d == 4) {
        d = -3;
    }
    // find the Thursday of the calendar week
    int yday;
    abs += d * seconds_per_day;
    abs_date(abs, year, &yday);
    *week = yday / 7 + 1;
}

#pragma endregion

#pragma region Unix time

// time_unix returns the Time corresponding to the given Unix time,
// sec seconds and nsec nanoseconds since January 1, 1970 UTC.
// It is valid to pass nsec outside the range [0, 999999999].
// Not all sec values have a corresponding time value. One such
// value is 1<<63-1 (the largest int64 value).
Time time_unix(int64_t sec, int64_t nsec) {
    if (nsec < 0 || nsec >= 1000000000) {
        int64_t n = nsec / 1000000000;
        sec += n;
        nsec -= n * 1000000000;
        if (nsec < 0) {
            nsec += 1000000000;
            sec--;
        }
    }
    return unix_time(sec, nsec);
}

// time_milli returns the Time corresponding to the given Unix time,
// msec milliseconds since January 1, 1970 UTC.
Time time_milli(int64_t msec) {
    return time_unix(msec / 1000, (msec % 1000) * 1000000);
}

// time_micro returns the Time corresponding to the given Unix time,
// usec microseconds since January 1, 1970 UTC.
Time time_micro(int64_t usec) {
    return time_unix(usec / 1000000, (usec % 1000000) * 1000);
}

// time_nano returns the Time corresponding to the given Unix time,
// nsec nanoseconds since January 1, 1970 UTC.
Time time_nano(int64_t nsec) {
    return time_unix(0, nsec);
}

// time_to_unix returns t as a Unix time, the number of seconds elapsed
// since January 1, 1970 UTC.
// Unix-like operating systems often record time as a 32-bit
// count of seconds, but since the method here returns a 64-bit
// value it is valid for billions of years into the past or future.
int64_t time_to_unix(Time t) {
    return unix_sec(t);
}

// time_to_milli returns t as a Unix time, the number of milliseconds elapsed since
// January 1, 1970 UTC. The result is undefined if the Unix time in
// milliseconds cannot be represented by an int64 (a date more than 292 million
// years before or after 1970).
int64_t time_to_milli(Time t) {
    return unix_sec(t) * 1000 + t.nsec / 1000000;
}

// time_to_micro returns t as a Unix time, the number of microseconds elapsed since
// January 1, 1970 UTC. The result is undefined if the Unix time in
// microseconds cannot be represented by an int64 (a date before year -290307 or
// after year 294246).
int64_t time_to_micro(Time t) {
    return unix_sec(t) * 1000000 + t.nsec / 1000;
}

// time_to_nano returns t as a Unix time, the number of nanoseconds elapsed
// since January 1, 1970 UTC. The result is undefined if the Unix time
// in nanoseconds cannot be represented by an int64 (a date before the year
// 1678 or after 2262). Note that this means the result of calling UnixNano
// on the zero Time is undefined.
int64_t time_to_nano(Time t) {
    return unix_sec(t) * 1000000000 + t.nsec;
}

#pragma endregion

#pragma region Calendar time

// time_tm returns the Time corresponding to the given calendar time at the given timezone offset.
Time time_tm(struct tm tm, int offset_sec) {
    int year = tm.tm_year + 1900;
    int month = tm.tm_mon + 1;
    int day = tm.tm_mday;
    int hour = tm.tm_hour;
    int min = tm.tm_min;
    int sec = tm.tm_sec;
    return time_date(year, month, day, hour, min, sec, 0, offset_sec);
}

// time_to_tm returns t in the given timezone offset as a calendar time.
struct tm time_to_tm(Time t, int offset_sec) {
    Time loc_t = time_add(t, offset_sec * Second);
    int year, day, hour, min, sec;
    enum Month month;
    time_get_date(loc_t, &year, &month, &day);
    time_get_clock(loc_t, &hour, &min, &sec);
    struct tm tm = {
        .tm_year = year - 1900,
        .tm_mon = month - 1,
        .tm_mday = day,
        .tm_hour = hour,
        .tm_min = min,
        .tm_sec = sec,
        .tm_isdst = -1,
    };
    return tm;
}

#pragma endregion

#pragma region Comparison

// time_after reports whether the time instant t is after u.
bool time_after(Time t, Time u) {
    return t.sec > u.sec || (t.sec == u.sec && t.nsec > u.nsec);
}

// time_before reports whether the time instant t is before u.
bool time_before(Time t, Time u) {
    return t.sec < u.sec || (t.sec == u.sec && t.nsec < u.nsec);
}

// time_compare compares the time instant t with u. If t is before u, it returns -1;
// if t is after u, it returns +1; if they're the same, it returns 0.
int time_compare(Time t, Time u) {
    if (time_before(t, u)) {
        return -1;
    }
    if (time_after(t, u)) {
        return +1;
    }
    return 0;
}

// time_equal reports whether t and u represent the same time instant.
bool time_equal(Time t, Time u) {
    return t.sec == u.sec && t.nsec == u.nsec;
}

// time_is_zero reports whether t represents the zero time instant,
// January 1, year 1, 00:00:00 UTC.
bool time_is_zero(Time t) {
    return t.sec == 0 && t.nsec == 0;
}

#pragma endregion

#pragma region Arithmetic

// time_add returns the time t+d.
Time time_add(Time t, Duration d) {
    int64_t dsec = d / Second;
    int64_t nsec = t.nsec + d % 1000000000;
    if (nsec >= 1e9) {
        dsec++;
        nsec -= 1e9;
    } else if (nsec < 0) {
        dsec--;
        nsec += 1e9;
    }
    return (Time){t.sec + dsec, nsec};
}

// time_sub returns the duration t-u. If the result exceeds the maximum (or minimum)
// value that can be stored in a Duration, the maximum (or minimum) duration
// will be returned.
Duration time_sub(Time t, Time u) {
    int64_t d = (t.sec - u.sec) * Second + (t.nsec - u.nsec);
    if (time_equal(time_add(u, d), t)) {
        return d;  // d is correct
    }
    if (time_before(t, u)) {
        return MIN_DURATION;  // t - u is negative out of range
    }
    return MAX_DURATION;  // t - u is positive out of range
}

// time_since returns the time elapsed since t.
// It is shorthand for time_sub(time_now(), t).
Duration time_since(Time t) {
    return time_sub(time_now(), t);
}

// time_until returns the duration until t.
// It is shorthand for time_sub(t, time_now()).
Duration time_until(Time t) {
    return time_sub(t, time_now());
}

// time_add_date returns the time corresponding to adding the
// given number of years, months, and days to t.
// For example, time_add_date(-1, 2, 3) applied to January 1, 2011
// returns March 4, 2010.
//
// time_add_date normalizes its result in the same way that Date does,
// so, for example, adding one month to October 31 yields
// December 1, the normalized form for November 31.
Time time_add_date(Time t, int years, int months, int days) {
    int year, day;
    enum Month month;
    time_get_date(t, &year, &month, &day);
    int hour, min, sec;
    time_get_clock(t, &hour, &min, &sec);
    return time_date(year + years, month + months, day + days, hour, min, sec, t.nsec, TIMEX_UTC);
}

#pragma endregion

#pragma region Rounding

// time_truncate returns the result of rounding t down to a multiple of d (since the zero time).
// Only supports d which is a multiple of 1 second. If d <= 0, returns t unchanged.
Time time_truncate(Time t, Duration d) {
    if (d <= 0) {
        return t;
    }
    Duration r = time_div(t, d);
    return time_add(t, -r);
}

// time_round returns the result of rounding t to the nearest multiple of d (since the zero time).
// The rounding behavior for halfway values is to round up.
// If d <= 0, returns t unchanged.
Time time_round(Time t, Duration d) {
    if (d <= 0) {
        return t;
    }
    Duration r = time_div(t, d);
    if (tless_than_half(r, d)) {
        return time_add(t, -r);
    }
    return time_add(t, d - r);
}

#pragma endregion

#pragma region Formatting

// time_fmt_iso returns an ISO 8601 time string for the given time value.
// Converts the time value to the given timezone offset before formatting.
// Chooses the most compact representation:
//  - 2006-01-02T15:04:05.999999999+07:00
//  - 2006-01-02T15:04:05.999999999Z
//  - 2006-01-02T15:04:05+07:00
//  - 2006-01-02T15:04:05Z
size_t time_fmt_iso(char* buf, size_t size, Time t, int offset_sec) {
    int year, day, hour, min, sec;
    enum Month month;
    const char* layout;
    size_t n = 0;

    if (offset_sec == 0) {
        time_get_date(t, &year, &month, &day);
        time_get_clock(t, &hour, &min, &sec);
        if (t.nsec == 0) {
            layout = "%04d-%02d-%02dT%02d:%02d:%02dZ";
            n = snprintf(buf, size, layout, year, month, day, hour, min, sec);
        } else {
            layout = "%04d-%02d-%02dT%02d:%02d:%02d.%09dZ";
            n = snprintf(buf, size, layout, year, month, day, hour, min, sec, t.nsec);
        }
    } else {
        Time loc_t = time_add(t, offset_sec * Second);
        time_get_date(loc_t, &year, &month, &day);
        time_get_clock(loc_t, &hour, &min, &sec);
        int ofhour = offset_sec / 3600;
        int ofmin = (offset_sec % 3600) / 60;
        if (ofmin < 0) {
            ofmin = -ofmin;
        }
        if (loc_t.nsec == 0) {
            layout = "%04d-%02d-%02dT%02d:%02d:%02d%+03d:%02d";
            n = snprintf(buf, size, layout, year, month, day, hour, min, sec, ofhour, ofmin);
        } else {
            layout = "%04d-%02d-%02dT%02d:%02d:%02d.%09d%+03d:%02d";
            n = snprintf(buf, size, layout, year, month, day, hour, min, sec, loc_t.nsec, ofhour,
                         ofmin);
        }
    }
    return n;
}

// time_fmt_datetime returns a datetime string
// (2006-01-02 15:04:05) for the given time value.
// Converts the time value to the given timezone offset before formatting.
size_t time_fmt_datetime(char* buf, size_t size, Time t, int offset_sec) {
    int year, day, hour, min, sec;
    enum Month month;
    if (offset_sec == 0) {
        time_get_date(t, &year, &month, &day);
        time_get_clock(t, &hour, &min, &sec);
    } else {
        Time loc_t = time_add(t, offset_sec * Second);
        time_get_date(loc_t, &year, &month, &day);
        time_get_clock(loc_t, &hour, &min, &sec);
    }
    return snprintf(buf, size, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec);
}

// time_fmt_date returns a date string
// (2006-01-02) for the given time value.
// Converts the time value to the given timezone offset before formatting.
size_t time_fmt_date(char* buf, size_t size, Time t, int offset_sec) {
    int year, day;
    enum Month month;
    if (offset_sec == 0) {
        time_get_date(t, &year, &month, &day);
    } else {
        Time loc_t = time_add(t, offset_sec * Second);
        time_get_date(loc_t, &year, &month, &day);
    }
    return snprintf(buf, size, "%04d-%02d-%02d", year, month, day);
}

// time_fmt_time returns a time string
// (15:04:05) for the given time value.
// Converts the time value to the given timezone offset before formatting.
size_t time_fmt_time(char* buf, size_t size, Time t, int offset_sec) {
    int hour, min, sec;
    if (offset_sec == 0) {
        time_get_clock(t, &hour, &min, &sec);
    } else {
        Time loc_t = time_add(t, offset_sec * Second);
        time_get_clock(loc_t, &hour, &min, &sec);
    }
    return snprintf(buf, size, "%02d:%02d:%02d", hour, min, sec);
}

// time_parse parses a formatted string and returns the time value it represents.
// Supports a limited set of layouts:
// - "2006-01-02T15:04:05.999999999+07:00" (ISO 8601 with nanoseconds and timezone)
// - "2006-01-02T15:04:05.999999999Z" (ISO 8601 with nanoseconds, UTC)
// - "2006-01-02T15:04:05+07:00" (ISO 8601 with timezone)
// - "2006-01-02T15:04:05Z" (ISO 8601, UTC)
// - "2006-01-02 15:04:05" (date and time, UTC)
// - "2006-01-02" (date only, UTC)
// - "15:04:05" (time only, UTC)
Time time_parse(const char* value) {
    Time zero = {0, 0};
    size_t len = strlen(value);
    if (len < 8 || len > 35) {
        return zero;
    }

    int year = 1, month = 1, day = 1, hour = 0, min = 0, sec = 0, nsec = 0, offset_sec = TIMEX_UTC;
    char tz[7] = "";

    if (len == 35) {
        // "2006-01-02T15:04:05.999999999+07:00"
        int n = sscanf(value, "%d-%d-%dT%d:%d:%d.%d%6s", &year, &month, &day, &hour, &min, &sec,
                       &nsec, tz);
        if (n != 8) {
            return zero;
        }
    }

    if (len == 30) {
        // "2006-01-02T15:04:05.999999999Z"
        int n =
            sscanf(value, "%d-%d-%dT%d:%d:%d.%dZ", &year, &month, &day, &hour, &min, &sec, &nsec);
        if (n != 7) {
            return zero;
        }
    }

    if (len == 25) {
        // "2006-01-02T15:04:05+07:00"
        int n = sscanf(value, "%d-%d-%dT%d:%d:%d%6s", &year, &month, &day, &hour, &min, &sec, tz);
        if (n != 7) {
            return zero;
        }
    }

    if (len == 19 || len == 20) {
        // "2006-01-02T15:04:05Z"
        // "2006-01-02 15:04:05"
        int n = sscanf(value, "%d-%d-%d%*c%d:%d:%d", &year, &month, &day, &hour, &min, &sec);
        if (n != 6) {
            return zero;
        }
    }

    if (len == 10) {
        // "2006-01-02"
        int n = sscanf(value, "%d-%d-%d", &year, &month, &day);
        if (n != 3) {
            return zero;
        }
    }

    if (len == 8) {
        // "15:04:05"
        int n = sscanf(value, "%d:%d:%d", &hour, &min, &sec);
        if (n != 3) {
            return zero;
        }
    }

    if (tz[0] != '\0') {
        // Parse timezone offset.
        // + 0 7 : 0 0
        // ⁰ ¹ ² ³ ⁴ ⁵
        // tz[0] is the sign.
        int sign = (tz[0] == '-') ? -1 : 1;
        // tz[1] and tz[2] are hours.
        offset_sec = ((tz[1] - '0') * 10 + (tz[2] - '0')) * 3600 * sign;
        // tz[4] and tz[5] are minutes.
        offset_sec += ((tz[4] - '0') * 10 + (tz[5] - '0')) * 60 * sign;
    }

    return time_date(year, (enum Month)month, day, hour, min, sec, nsec, offset_sec);
}

#pragma endregion

#pragma region Marshaling

// time_blob returns the time instant represented by the binary data.
// The blob must have been created by time_to_blob and be at least 13 bytes long.
Time time_blob(const uint8_t* buf) {
    const uint8_t version = buf[0];
    if (version != 1) {
        return (Time){0, 0};
    }

    int64_t sec = (int64_t)buf[8] | (int64_t)buf[7] << 8 | (int64_t)buf[6] << 16 |
                  (int64_t)buf[5] << 24 | (int64_t)buf[4] << 32 | (int64_t)buf[3] << 40 |
                  (int64_t)buf[2] << 48 | (int64_t)buf[1] << 56;

    int32_t nsec =
        (int32_t)buf[12] | (int32_t)buf[11] << 8 | (int32_t)buf[10] << 16 | (int32_t)buf[9] << 24;

    return (Time){sec, nsec};
}

// time_to_blob returns the binary representation of the time instant t.
// The result is a byte slice with the following layout:
// 0: version (currently 1)
// 1-8: seconds
// 9-12: nanoseconds
void time_to_blob(Time t, uint8_t* buf) {
    const uint8_t version = 1;
    buf[0] = version;
    buf[1] = t.sec >> 56;  // bytes 1-8: seconds
    buf[2] = t.sec >> 48;
    buf[3] = t.sec >> 40;
    buf[4] = t.sec >> 32;
    buf[5] = t.sec >> 24;
    buf[6] = t.sec >> 16;
    buf[7] = t.sec >> 8;
    buf[8] = t.sec;
    buf[9] = t.nsec >> 24;  // bytes 9-12: nanoseconds
    buf[10] = t.nsec >> 16;
    buf[11] = t.nsec >> 8;
    buf[12] = t.nsec;
}

#pragma endregion
