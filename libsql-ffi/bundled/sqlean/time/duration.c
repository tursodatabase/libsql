// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Based on Go's time package, BSD 3-Clause License
// https://github.com/golang/go

// Duration methods.

#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include "time/timex.h"

// Common durations.
const Duration Nanosecond = 1;
const Duration Microsecond = 1000 * Nanosecond;
const Duration Millisecond = 1000 * Microsecond;
const Duration Second = 1000 * Millisecond;
const Duration Minute = 60 * Second;
const Duration Hour = 60 * Minute;

#pragma region Conversion

// dur_to_micro returns the duration as an integer microsecond count.
int64_t dur_to_micro(Duration d) {
    return d / Microsecond;
}

// dur_to_milli returns the duration as an integer millisecond count.
int64_t dur_to_milli(Duration d) {
    return d / Millisecond;
}

// dur_to_seconds returns the duration as a floating point number of seconds.
double dur_to_seconds(Duration d) {
    int64_t sec = d / Second;
    int64_t nsec = d % Second;
    return (double)sec + (double)nsec / 1e9;
}

// dur_to_minutes returns the duration as a floating point number of minutes.
double dur_to_minutes(Duration d) {
    int64_t min = d / Minute;
    int64_t nsec = d % Minute;
    return (double)min + (double)nsec / (60 * 1e9);
}

// dur_to_hours returns the duration as a floating point number of hours.
double dur_to_hours(Duration d) {
    int64_t hour = d / Hour;
    int64_t nsec = d % Hour;
    return (double)hour + (double)nsec / (60 * 60 * 1e9);
}

#pragma endregion

#pragma region Rounding

// dless_than_half reports whether x+x < y but avoids overflow,
// assuming x and y are both positive (Duration is signed).
static bool dless_than_half(Duration x, Duration y) {
    return (uint64_t)x + (uint64_t)x < (uint64_t)y;
}

// dur_truncate returns the result of rounding d toward zero to a multiple of m.
// If m <= 0, Truncate returns d unchanged.
Duration dur_truncate(Duration d, Duration m) {
    if (m <= 0) {
        return d;
    }
    return d - d % m;
}

// dur_round returns the result of rounding d to the nearest multiple of m.
// The rounding behavior for halfway values is to round away from zero.
// If the result exceeds the maximum (or minimum)
// value that can be stored in a Duration,
// Round returns the maximum (or minimum) duration.
// If m <= 0, Round returns d unchanged.
Duration dur_round(Duration d, Duration m) {
    if (m <= 0) {
        return d;
    }
    int64_t r = d % m;

    if (d < 0) {
        r = -r;
        if (dless_than_half(r, m)) {
            return d + r;
        }
        int64_t d1 = d - m + r;
        if (d1 < d) {
            return d1;
        }
        return MIN_DURATION;  // overflow
    }

    if (dless_than_half(r, m)) {
        return d - r;
    }
    int64_t d1 = d + m - r;
    if (d1 > d) {
        return d1;
    }
    return MAX_DURATION;  // overflow
}

// dur_abs returns the absolute value of d.
// As a special case, MIN_DURATION is converted to MAX_DURATION.
Duration dur_abs(Duration d) {
    if (d == MIN_DURATION) {
        return MAX_DURATION;
    }
    return d < 0 ? -d : d;
}

#pragma endregion
