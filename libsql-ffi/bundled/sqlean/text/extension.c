// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite extension for working with text.

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "text/bstring.h"
#include "text/rstring.h"
#include "text/utf8/utf8.h"

#pragma region Substrings

// Extracts a substring starting at the `start` position (1-based).
// text_substring(str, start)
// [pg-compatible] substr(string, start)
static void text_substring2(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "start parameter should be integer", -1);
        return;
    }
    int start = sqlite3_value_int(argv[1]);

    // convert to 0-based index
    // postgres-compatible: treat negative index as zero
    start = start > 0 ? start - 1 : 0;

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_res = rstring_slice(s_src, start, s_src.length);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

// Extracts a substring of `length` characters starting at the `start` position (1-based).
// text_substring(str, start, length)
// [pg-compatible] substr(string, start, count)
static void text_substring3(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "start parameter should be integer", -1);
        return;
    }
    int start = sqlite3_value_int(argv[1]);

    if (sqlite3_value_type(argv[2]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "length parameter should be integer", -1);
        return;
    }
    int length = sqlite3_value_int(argv[2]);
    if (length < 0) {
        sqlite3_result_error(context, "length parameter should >= 0", -1);
        return;
    }

    // convert to 0-based index
    start -= 1;
    // postgres-compatible: treat negative start as 0, but shorten the length accordingly
    if (start < 0) {
        length += start;
        start = 0;
    }

    // zero-length substring
    if (length <= 0) {
        sqlite3_result_text(context, "", -1, SQLITE_TRANSIENT);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);

    // postgres-compatible: the substring cannot be longer the the original string
    if ((size_t)length > s_src.length) {
        length = s_src.length;
    }

    RuneString s_res = rstring_substring(s_src, start, length);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

// Extracts a substring starting at the `start` position (1-based).
// text_slice(str, start)
static void text_slice2(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "start parameter should be integer", -1);
        return;
    }
    int start = sqlite3_value_int(argv[1]);

    // convert to 0-based index
    start = start > 0 ? start - 1 : start;

    RuneString s_src = rstring_from_cstring(src);

    // python-compatible: treat negative index larger than the length of the string as zero
    // and return the original string
    if (start < -(int)s_src.length) {
        sqlite3_result_text(context, src, -1, SQLITE_TRANSIENT);
        rstring_free(s_src);
        return;
    }

    RuneString s_res = rstring_slice(s_src, start, s_src.length);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

// Extracts a substring from `start` position inclusive to `end` position non-inclusive (1-based).
// text_slice(str, start, end)
static void text_slice3(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "start parameter should be integer", -1);
        return;
    }
    int start = sqlite3_value_int(argv[1]);
    // convert to 0-based index
    start = start > 0 ? start - 1 : start;

    if (sqlite3_value_type(argv[2]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "end parameter should be integer", -1);
        return;
    }
    int end = sqlite3_value_int(argv[2]);
    // convert to 0-based index
    end = end > 0 ? end - 1 : end;

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_res = rstring_slice(s_src, start, end);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

// Extracts a substring of `length` characters from the beginning of the string.
// For `length < 0`, extracts all but the last `|length|` characters.
// text_left(str, length)
// [pg-compatible] left(string, n)
static void text_left(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "length parameter should be integer", -1);
        return;
    }
    int length = sqlite3_value_int(argv[1]);

    RuneString s_src = rstring_from_cstring(src);
    if (length < 0) {
        length = s_src.length + length;
        length = length >= 0 ? length : 0;
    }
    RuneString s_res = rstring_substring(s_src, 0, length);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

// Extracts a substring of `length` characters from the end of the string.
// For `length < 0`, extracts all but the first `|length|` characters.
// text_right(str, length)
// [pg-compatible] right(string, n)
static void text_right(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "length parameter should be integer", -1);
        return;
    }
    int length = sqlite3_value_int(argv[1]);

    RuneString s_src = rstring_from_cstring(src);

    length = (length < 0) ? (int)s_src.length + length : length;
    int start = (int)s_src.length - length;
    start = start < 0 ? 0 : start;

    RuneString s_res = rstring_substring(s_src, start, length);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

#pragma endregion

#pragma region Search and match

// Returns the first index of the substring in the original string.
// text_index(str, other)
// [pg-compatible] strpos(string, substring)
static void text_index(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_other = rstring_from_cstring(other);
    int idx = rstring_index(s_src, s_other);
    sqlite3_result_int64(context, idx + 1);
    rstring_free(s_src);
    rstring_free(s_other);
}

// Returns the last index of the substring in the original string.
// text_last_index(str, other)
static void text_last_index(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_other = rstring_from_cstring(other);
    int idx = rstring_last_index(s_src, s_other);
    sqlite3_result_int64(context, idx + 1);
    rstring_free(s_src);
    rstring_free(s_other);
}

// Checks if the string contains the substring_
// text_contains(str, other)
static void text_contains(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_other = bstring_from_cstring(other, sqlite3_value_bytes(argv[1]));
    bool found = bstring_contains(s_src, s_other);
    sqlite3_result_int(context, found);
    bstring_free(s_src);
    bstring_free(s_other);
}

// Checks if the string starts with the substring_
// text_has_prefix(str, other)
// [pg-compatible] starts_with(string, prefix)
static void text_has_prefix(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_other = bstring_from_cstring(other, sqlite3_value_bytes(argv[1]));
    bool found = bstring_has_prefix(s_src, s_other);
    sqlite3_result_int(context, found);
    bstring_free(s_src);
    bstring_free(s_other);
}

// Checks if the string ends with the substring_
// text_has_suffix(str, other)
static void text_has_suffix(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_other = bstring_from_cstring(other, sqlite3_value_bytes(argv[1]));
    bool found = bstring_has_suffix(s_src, s_other);
    sqlite3_result_int(context, found);
    bstring_free(s_src);
    bstring_free(s_other);
}

// Counts how many times the substring is contained in the original string.
// text_count(str, other)
static void text_count(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* other = (char*)sqlite3_value_text(argv[1]);
    if (other == NULL) {
        sqlite3_result_null(context);
        return;
    }

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_other = bstring_from_cstring(other, sqlite3_value_bytes(argv[1]));
    size_t count = bstring_count(s_src, s_other);
    sqlite3_result_int(context, count);
    bstring_free(s_src);
    bstring_free(s_other);
}

// Checks if the string matches the pattern using the SQL LIKE syntax.
// text_like(pattern, str)
// like(pattern, str)
// str LIKE pattern
static void text_like(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* pattern = (char*)sqlite3_value_text(argv[0]);
    if (pattern == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* str = (char*)sqlite3_value_text(argv[1]);
    if (str == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_pattern = rstring_from_cstring(pattern);
    RuneString s_str = rstring_from_cstring(str);
    bool match = rstring_like(s_pattern, s_str);
    sqlite3_result_int(context, match);
    rstring_free(s_pattern);
    rstring_free(s_str);
}

#pragma endregion

#pragma region Split and join

// Splits a string by a separator and returns the n-th part (counting from one).
// When n is negative, returns the |n|'th-from-last part.
// text_split(str, sep, n)
// [pg-compatible] split_part(string, delimiter, n)
static void text_split(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* sep = (const char*)sqlite3_value_text(argv[1]);
    if (sep == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[2]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "part parameter should be integer", -1);
        return;
    }
    int part = sqlite3_value_int(argv[2]);
    // pg-compatible
    if (part == 0) {
        sqlite3_result_error(context, "part parameter should not be 0", -1);
        return;
    }
    // convert to 0-based index
    part = part > 0 ? part - 1 : part;

    ByteString s_src = bstring_from_cstring(src, strlen(src));
    ByteString s_sep = bstring_from_cstring(sep, strlen(sep));

    // count from the last part backwards
    if (part < 0) {
        int n_parts = bstring_count(s_src, s_sep) + 1;
        part = n_parts + part;
    }

    ByteString s_part = bstring_split_part(s_src, s_sep, part);
    sqlite3_result_text(context, s_part.bytes, -1, SQLITE_TRANSIENT);
    bstring_free(s_src);
    bstring_free(s_sep);
    bstring_free(s_part);
}

// Joins strings using the separator and returns the resulting string. Ignores nulls.
// text_join(sep, str, ...)
// [pg-compatible] concat_ws(sep, val1[, val2 [, ...]])
static void text_join(sqlite3_context* context, int argc, sqlite3_value** argv) {
    if (argc < 2) {
        sqlite3_result_error(context, "expected at least 2 parameters", -1);
        return;
    }

    // separator
    const char* sep = (char*)sqlite3_value_text(argv[0]);
    if (sep == NULL) {
        sqlite3_result_null(context);
        return;
    }
    ByteString s_sep = bstring_from_cstring(sep, sqlite3_value_bytes(argv[0]));

    // parts
    size_t n_parts = argc - 1;
    ByteString* s_parts = malloc(n_parts * sizeof(ByteString));
    if (s_parts == NULL) {
        sqlite3_result_null(context);
        return;
    }
    for (size_t i = 1, part_idx = 0; i < (size_t)argc; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            // ignore nulls
            n_parts--;
            continue;
        }
        const char* part = (char*)sqlite3_value_text(argv[i]);
        int part_len = sqlite3_value_bytes(argv[i]);
        s_parts[part_idx] = bstring_from_cstring(part, part_len);
        part_idx++;
    }

    // join parts with separator
    ByteString s_res = bstring_join(s_parts, n_parts, s_sep);
    const char* res = bstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, SQLITE_TRANSIENT);
    bstring_free(s_sep);
    bstring_free(s_res);
    free(s_parts);
}

// Concatenates strings and returns the resulting string. Ignores nulls.
// text_concat(str, ...)
// [pg-compatible] concat(val1[, val2 [, ...]])
static void text_concat(sqlite3_context* context, int argc, sqlite3_value** argv) {
    if (argc < 1) {
        sqlite3_result_error(context, "expected at least 1 parameter", -1);
        return;
    }

    // parts
    size_t n_parts = argc;
    ByteString* s_parts = malloc(n_parts * sizeof(ByteString));
    if (s_parts == NULL) {
        sqlite3_result_null(context);
        return;
    }
    for (size_t i = 0, part_idx = 0; i < (size_t)argc; i++) {
        if (sqlite3_value_type(argv[i]) == SQLITE_NULL) {
            // ignore nulls
            n_parts--;
            continue;
        }
        const char* part = (char*)sqlite3_value_text(argv[i]);
        int part_len = sqlite3_value_bytes(argv[i]);
        s_parts[part_idx] = bstring_from_cstring(part, part_len);
        part_idx++;
    }

    // join parts
    ByteString s_res = bstring_concat(s_parts, n_parts);
    const char* res = bstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, SQLITE_TRANSIENT);
    bstring_free(s_res);
    free(s_parts);
}

// Concatenates the string to itself a given number of times and returns the resulting string.
// text_repeat(str, count)
// [pg-compatible] repeat(string, number)
static void text_repeat(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "count parameter should be integer", -1);
        return;
    }
    int count = sqlite3_value_int(argv[1]);
    // pg-compatible: treat negative count as zero
    count = count >= 0 ? count : 0;

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_res = bstring_repeat(s_src, count);
    const char* res = bstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, SQLITE_TRANSIENT);
    bstring_free(s_src);
    bstring_free(s_res);
}

#pragma endregion

#pragma region Trim and pad

// Trims certain characters (spaces by default) from the beginning/end of the string.
// text_ltrim(str [,chars])
// text_rtrim(str [,chars])
// text_trim(str [,chars])
// [pg-compatible] ltrim(string [, characters])
// [pg-compatible] rtrim(string [, characters])
// [pg-compatible] btrim(string [, characters])
static void text_trim(sqlite3_context* context, int argc, sqlite3_value** argv) {
    if (argc != 1 && argc != 2) {
        sqlite3_result_error(context, "expected 1 or 2 parameters", -1);
        return;
    }

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* chars = argc == 2 ? (char*)sqlite3_value_text(argv[1]) : " ";
    if (chars == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString (*trim_func)(RuneString, RuneString) = (void*)sqlite3_user_data(context);

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_chars = rstring_from_cstring(chars);
    RuneString s_res = trim_func(s_src, s_chars);
    const char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_chars);
    rstring_free(s_res);
}

// Pads the string to the specified length by prepending/appending certain characters
// (spaces by default).
// text_lpad(str, length [,fill])
// text_rpad(str, length [,fill])
// [pg-compatible] lpad(string, length [, fill])
// [pg-compatible] rpad(string, length [, fill])
// (!) postgres does not support unicode strings in lpad/rpad, while this function does.
static void text_pad(sqlite3_context* context, int argc, sqlite3_value** argv) {
    if (argc != 2 && argc != 3) {
        sqlite3_result_error(context, "expected 2 or 3 parameters", -1);
        return;
    }

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "length parameter should be integer", -1);
        return;
    }
    int length = sqlite3_value_int(argv[1]);
    // postgres-compatible: treat negative length as zero
    length = length < 0 ? 0 : length;

    const char* fill = argc == 3 ? (char*)sqlite3_value_text(argv[2]) : " ";
    if (fill == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString (*pad_func)(RuneString, size_t, RuneString) = (void*)sqlite3_user_data(context);

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_fill = rstring_from_cstring(fill);
    RuneString s_res = pad_func(s_src, length, s_fill);
    const char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_fill);
    rstring_free(s_res);
}

#pragma endregion

#pragma region Change case

// Changes the case of the string.
// text_upper(str)
// text_lower(str)
// text_title(str)
// text_casefold(str)
static void text_change_case(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);

    const char* src = (const char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }
    size_t n = sqlite3_value_bytes(argv[0]);

    char* res = malloc(n + 1);
    if (res == NULL) {
        sqlite3_result_error_nomem(context);
        return;
    }
    memcpy(res, src, n);
    res[n] = '\0';

    bool (*fn)(char*, size_t) = sqlite3_user_data(context);
    fn(res, n);

    sqlite3_result_text(context, res, n, free);
}

#pragma endregion

#pragma region Other modifications

// Replaces all old substrings with new substrings in the original string.
// text_replace(str, old, new)
// [pg-compatible] replace(string, from, to)
static void text_replace_all(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* old = (char*)sqlite3_value_text(argv[1]);
    if (old == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* new = (char*)sqlite3_value_text(argv[2]);
    if (new == NULL) {
        sqlite3_result_null(context);
        return;
    }

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_old = bstring_from_cstring(old, sqlite3_value_bytes(argv[1]));
    ByteString s_new = bstring_from_cstring(new, sqlite3_value_bytes(argv[2]));
    ByteString s_res = bstring_replace_all(s_src, s_old, s_new);
    const char* res = bstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, SQLITE_TRANSIENT);
    bstring_free(s_src);
    bstring_free(s_old);
    bstring_free(s_new);
    bstring_free(s_res);
}

// Replaces old substrings with new substrings in the original string,
// but not more than `count` times.
// text_replace(str, old, new, count)
static void text_replace(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 4);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* old = (char*)sqlite3_value_text(argv[1]);
    if (old == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* new = (char*)sqlite3_value_text(argv[2]);
    if (new == NULL) {
        sqlite3_result_null(context);
        return;
    }

    if (sqlite3_value_type(argv[3]) != SQLITE_INTEGER) {
        sqlite3_result_error(context, "count parameter should be integer", -1);
        return;
    }
    int count = sqlite3_value_int(argv[3]);
    // treat negative count as zero
    count = count < 0 ? 0 : count;

    ByteString s_src = bstring_from_cstring(src, sqlite3_value_bytes(argv[0]));
    ByteString s_old = bstring_from_cstring(old, sqlite3_value_bytes(argv[1]));
    ByteString s_new = bstring_from_cstring(new, sqlite3_value_bytes(argv[2]));
    ByteString s_res = bstring_replace(s_src, s_old, s_new, count);
    const char* res = bstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, SQLITE_TRANSIENT);
    bstring_free(s_src);
    bstring_free(s_old);
    bstring_free(s_new);
    bstring_free(s_res);
}

// Replaces each string character that matches a character in the `from` set
// with the corresponding character in the `to` set. If `from` is longer than `to`,
// occurrences of the extra characters in `from` are deleted.
// text_translate(str, from, to)
// [pg-compatible] translate(string, from, to)
// (!) postgres does not support unicode strings in translate, while this function does.
static void text_translate(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 3);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* from = (char*)sqlite3_value_text(argv[1]);
    if (from == NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* to = (char*)sqlite3_value_text(argv[2]);
    if (to == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_from = rstring_from_cstring(from);
    RuneString s_to = rstring_from_cstring(to);
    RuneString s_res = rstring_translate(s_src, s_from, s_to);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_from);
    rstring_free(s_to);
    rstring_free(s_res);
}

// Reverses the order of the characters in the string.
// text_reverse(str)
// [pg-compatible] reverse(text)
// (!) postgres does not support unicode strings in reverse, while this function does.
static void text_reverse(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);
    RuneString s_res = rstring_reverse(s_src);
    char* res = rstring_to_cstring(s_res);
    sqlite3_result_text(context, res, -1, free);
    rstring_free(s_src);
    rstring_free(s_res);
}

#pragma endregion

#pragma region Properties

// Returns the number of characters in the string.
// text_length(str)
// [pg-compatible] length(text)
// [pg-compatible] char_length(text)
// [pg-compatible] character_length(text)
static void text_length(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    RuneString s_src = rstring_from_cstring(src);
    sqlite3_result_int64(context, s_src.length);
    rstring_free(s_src);
}

// Returns the number of bytes in the string.
// text_size(str)
// [pg-compatible] octet_length(text)
static void text_size(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    sqlite3_result_int64(context, sqlite3_value_bytes(argv[0]));
}

// Returns the number of bits in the string.
// text_bitsize(str)
// [pg-compatible] bit_length(text)
static void text_bit_size(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);

    const char* src = (char*)sqlite3_value_text(argv[0]);
    if (src == NULL) {
        sqlite3_result_null(context);
        return;
    }

    int size = sqlite3_value_bytes(argv[0]);
    sqlite3_result_int64(context, 8 * size);
}

#pragma endregion

#pragma region Collation

static int collate_nocase(void* unused, int n1, const void* s1, int n2, const void* s2) {
    (void)unused;
    return utf8_icmp((const char*)s1, (size_t)n1, (const char*)s2, (size_t)n2);
}

#pragma endregion

int text_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;

    // substrings
    sqlite3_create_function(db, "text_substring", 2, flags, 0, text_substring2, 0, 0);
    sqlite3_create_function(db, "text_substring", 3, flags, 0, text_substring3, 0, 0);
    sqlite3_create_function(db, "text_slice", 2, flags, 0, text_slice2, 0, 0);
    sqlite3_create_function(db, "text_slice", 3, flags, 0, text_slice3, 0, 0);
    sqlite3_create_function(db, "text_left", 2, flags, 0, text_left, 0, 0);
    sqlite3_create_function(db, "left", 2, flags, 0, text_left, 0, 0);
    sqlite3_create_function(db, "text_right", 2, flags, 0, text_right, 0, 0);
    sqlite3_create_function(db, "right", 2, flags, 0, text_right, 0, 0);

    // search and match
    sqlite3_create_function(db, "text_index", 2, flags, 0, text_index, 0, 0);
    sqlite3_create_function(db, "strpos", 2, flags, 0, text_index, 0, 0);
    sqlite3_create_function(db, "text_last_index", 2, flags, 0, text_last_index, 0, 0);
    sqlite3_create_function(db, "text_contains", 2, flags, 0, text_contains, 0, 0);
    sqlite3_create_function(db, "text_has_prefix", 2, flags, 0, text_has_prefix, 0, 0);
    sqlite3_create_function(db, "starts_with", 2, flags, 0, text_has_prefix, 0, 0);
    sqlite3_create_function(db, "text_has_suffix", 2, flags, 0, text_has_suffix, 0, 0);
    sqlite3_create_function(db, "text_count", 2, flags, 0, text_count, 0, 0);
    sqlite3_create_function(db, "text_like", 2, flags, 0, text_like, 0, 0);

    // split and join
    sqlite3_create_function(db, "text_split", 3, flags, 0, text_split, 0, 0);
    sqlite3_create_function(db, "split_part", 3, flags, 0, text_split, 0, 0);
    sqlite3_create_function(db, "text_join", -1, flags, 0, text_join, 0, 0);
    sqlite3_create_function(db, "concat_ws", -1, flags, 0, text_join, 0, 0);
    sqlite3_create_function(db, "text_concat", -1, flags, 0, text_concat, 0, 0);
    sqlite3_create_function(db, "concat", -1, flags, 0, text_concat, 0, 0);
    sqlite3_create_function(db, "text_repeat", 2, flags, 0, text_repeat, 0, 0);
    sqlite3_create_function(db, "repeat", 2, flags, 0, text_repeat, 0, 0);

    // trim and pad
    sqlite3_create_function(db, "text_ltrim", -1, flags, rstring_trim_left, text_trim, 0, 0);
    sqlite3_create_function(db, "ltrim", -1, flags, rstring_trim_left, text_trim, 0, 0);
    sqlite3_create_function(db, "text_rtrim", -1, flags, rstring_trim_right, text_trim, 0, 0);
    sqlite3_create_function(db, "rtrim", -1, flags, rstring_trim_right, text_trim, 0, 0);
    sqlite3_create_function(db, "text_trim", -1, flags, rstring_trim, text_trim, 0, 0);
    sqlite3_create_function(db, "btrim", -1, flags, rstring_trim, text_trim, 0, 0);
    sqlite3_create_function(db, "text_lpad", -1, flags, rstring_pad_left, text_pad, 0, 0);
    sqlite3_create_function(db, "lpad", -1, flags, rstring_pad_left, text_pad, 0, 0);
    sqlite3_create_function(db, "text_rpad", -1, flags, rstring_pad_right, text_pad, 0, 0);
    sqlite3_create_function(db, "rpad", -1, flags, rstring_pad_right, text_pad, 0, 0);

    // change case
    sqlite3_create_function(db, "text_upper", 1, flags, utf8_toupper, text_change_case, 0, 0);
    sqlite3_create_function(db, "text_lower", 1, flags, utf8_tolower, text_change_case, 0, 0);
    sqlite3_create_function(db, "text_title", 1, flags, utf8_totitle, text_change_case, 0, 0);
    sqlite3_create_function(db, "text_casefold", 1, flags, utf8_casefold, text_change_case, 0, 0);

    // other modifications
    sqlite3_create_function(db, "text_replace", 3, flags, 0, text_replace_all, 0, 0);
    sqlite3_create_function(db, "text_replace", 4, flags, 0, text_replace, 0, 0);
    sqlite3_create_function(db, "text_translate", 3, flags, 0, text_translate, 0, 0);
    sqlite3_create_function(db, "translate", 3, flags, 0, text_translate, 0, 0);
    sqlite3_create_function(db, "text_reverse", 1, flags, 0, text_reverse, 0, 0);
    sqlite3_create_function(db, "reverse", 1, flags, 0, text_reverse, 0, 0);

    // properties
    sqlite3_create_function(db, "text_length", 1, flags, 0, text_length, 0, 0);
    sqlite3_create_function(db, "char_length", 1, flags, 0, text_length, 0, 0);
    sqlite3_create_function(db, "character_length", 1, flags, 0, text_length, 0, 0);
    sqlite3_create_function(db, "text_size", 1, flags, 0, text_size, 0, 0);
    sqlite3_create_function(db, "octet_length", 1, flags, 0, text_size, 0, 0);
    sqlite3_create_function(db, "text_bitsize", 1, flags, 0, text_bit_size, 0, 0);
    sqlite3_create_function(db, "bit_length", 1, flags, 0, text_bit_size, 0, 0);

    // collation
    sqlite3_create_collation(db, "text_nocase", SQLITE_UTF8, NULL, collate_nocase);

    return SQLITE_OK;
}
