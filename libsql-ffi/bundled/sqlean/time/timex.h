// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Based on Go's time package, BSD 3-Clause License
// https://github.com/golang/go

// Package timex provides functionality for working with time.
// The calendrical calculations always assume a Gregorian calendar, with no leap seconds.

#ifndef TIMEX_H
#define TIMEX_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

// Month is a month of the year.
enum Month {
    January = 1,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
};

// Weekday is a day of the week (Sunday = 0, ...).
enum Weekday {
    Sunday = 0,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
};

// Time represents an instant in time with nanosecond precision.
// The zero value is January 1, year 1, 00:00:00.000000000 UTC.
typedef struct {
    int64_t sec;   // seconds since zero time
    int32_t nsec;  // nanoseconds within the second [0, 999999999]
} Time;

#define TIMEX_BLOB_SIZE 13
#define TIMEX_UTC 0

// Duration represents the elapsed time between two instants
// as an int64 nanosecond count. The representation limits the
// largest representable duration to approximately 290 years.
typedef int64_t Duration;

// --- Time ---

// Constructors.

// time_now returns the current time in UTC.
Time time_now(void);

// time_date returns the Time corresponding to
// yyyy-mm-dd hh:mm:ss + nsec nanoseconds
// with the given timezone offset in seconds.
Time time_date(int year,
               enum Month month,
               int day,
               int hour,
               int min,
               int sec,
               int nsec,
               int offset_sec);

// Time parts.

// time_get_date returns the year, month, and day in which t occurs.
void time_get_date(Time t, int* year, enum Month* month, int* day);

// time_get_year returns the year in which t occurs.
int time_get_year(Time t);

// time_get_month returns the month of the year specified by t.
enum Month time_get_month(Time t);

// time_get_day returns the day of the month specified by t.
int time_get_day(Time t);

// time_get_clock returns the hour, minute, and second within the day specified by t.
void time_get_clock(Time t, int* hour, int* min, int* sec);

// time_get_hour returns the hour within the day specified by t.
int time_get_hour(Time t);

// time_get_minute returns the minute offset within the hour specified by t.
int time_get_minute(Time t);

// time_get_second returns the second offset within the minute specified by t.
int time_get_second(Time t);

// time_get_nano returns the nanosecond offset within the second specified by t.
int time_get_nano(Time t);

// time_get_weekday returns the day of the week specified by t.
enum Weekday time_get_weekday(Time t);

// time_get_yearday returns the day of the year specified by t.
int time_get_yearday(Time t);

// time_get_isoweek returns the ISO 8601 year and week number in which t occurs.
void time_get_isoweek(Time t, int* year, int* week);

// Unix time.

// time_unix returns the Time corresponding to the given Unix time,
// sec seconds and nsec nanoseconds since January 1, 1970 UTC.
Time time_unix(int64_t sec, int64_t nsec);

// time_milli returns the Time corresponding to the given Unix time,
// msec milliseconds since January 1, 1970 UTC.
Time time_milli(int64_t msec);

// time_micro returns the local Time corresponding to the given Unix time,
// usec microseconds since January 1, 1970 UTC.
Time time_micro(int64_t usec);

// time_nano returns the Time corresponding to the given Unix time,
// nsec nanoseconds since January 1, 1970 UTC.
Time time_nano(int64_t nsec);

// time_to_unix returns t as a Unix time, the number of seconds elapsed
// since January 1, 1970 UTC.
int64_t time_to_unix(Time t);

// time_to_milli returns t as a Unix time, the number of milliseconds elapsed since
// January 1, 1970 UTC.
int64_t time_to_milli(Time t);

// time_to_micro returns t as a Unix time, the number of microseconds elapsed since
// January 1, 1970 UTC.
int64_t time_to_micro(Time t);

// time_to_nano returns t as a Unix time, the number of nanoseconds elapsed
// since January 1, 1970 UTC.
int64_t time_to_nano(Time t);

// Calendar time.

// time_tm returns the Time corresponding to the given calendar time at the given timezone offset.
Time time_tm(struct tm tm, int offset_sec);

// time_to_tm returns t in the given timezone offset as a calendar time.
struct tm time_to_tm(Time t, int offset_sec);

// Comparison.

// time_after reports whether the time instant t is after u.
bool time_after(Time t, Time u);

// time_before reports whether the time instant t is before u.
bool time_before(Time t, Time u);

// time_compare compares the time instant t with u.
int time_compare(Time t, Time u);

// time_equal reports whether t and u represent the same time instant.
bool time_equal(Time t, Time u);

// time_is_zero reports whether t represents the zero time instant,
// January 1, year 1, 00:00:00 UTC.
bool time_is_zero(Time t);

// Arithmetic.

// time_add returns the time t+d.
Time time_add(Time t, Duration d);

// time_sub returns the duration t-u.
Duration time_sub(Time t, Time u);

// time_since returns the time elapsed since t.
Duration time_since(Time t);

// time_until returns the duration until t.
Duration time_until(Time t);

// time_add_date returns the time corresponding to adding the
// given number of years, months, and days to t.
Time time_add_date(Time t, int years, int months, int days);

// Rounding.

// time_truncate returns the result of rounding t down to a multiple of d.
Time time_truncate(Time t, Duration d);

// time_round returns the result of rounding t to the nearest multiple of d.
Time time_round(Time t, Duration d);

// Formatting.

// time_fmt_iso returns an ISO 8601 time string for the given time value.
size_t time_fmt_iso(char* buf, size_t size, Time t, int offset_sec);

// time_fmt_datetime returns a datetime string for the given time value.
size_t time_fmt_datetime(char* buf, size_t size, Time t, int offset_sec);

// time_fmt_date returns a date string for the given time value.
size_t time_fmt_date(char* buf, size_t size, Time t, int offset_sec);

// time_fmt_time returns a time string for the given time value.
size_t time_fmt_time(char* buf, size_t size, Time t, int offset_sec);

// time_parse parses a formatted string and returns the time value it represents.
Time time_parse(const char* value);

// Marshaling.

// time_blob returns the time instant represented by the binary data.
Time time_blob(const uint8_t* buf);

// time_to_blob returns the binary representation of the time instant t.
void time_to_blob(Time t, uint8_t* buf);

// --- Duration ---

// Min/Max durations.
#define MIN_DURATION INT64_MIN
#define MAX_DURATION INT64_MAX

// Common durations. There is no definition for units of Day or larger
// to avoid confusion across daylight savings time zone transitions.
extern const Duration Nanosecond;
extern const Duration Microsecond;
extern const Duration Millisecond;
extern const Duration Second;
extern const Duration Minute;
extern const Duration Hour;

// Conversion.

// dur_to_micro returns the duration as an integer microsecond count.
int64_t dur_to_micro(Duration d);

// dur_to_milli returns the duration as an integer millisecond count.
int64_t dur_to_milli(Duration d);

// dur_to_seconds returns the duration as a floating point number of seconds.
double dur_to_seconds(Duration d);

// dur_to_minutes returns the duration as a floating point number of minutes.
double dur_to_minutes(Duration d);

// dur_to_hours returns the duration as a floating point number of hours.
double dur_to_hours(Duration d);

// Rounding.

// dur_truncate returns the result of rounding d toward zero to a multiple of m.
Duration dur_truncate(Duration d, Duration m);

// dur_round returns the result of rounding d to the nearest multiple of m.
Duration dur_round(Duration d, Duration m);

// dur_abs returns the absolute value of d.
Duration dur_abs(Duration d);

#endif /* TIMEX_H */
