// Originally from the uuid SQLite exension, Public Domain
// https://www.sqlite.org/src/file/ext/misc/uuid.c

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// Universally Unique IDentifiers (UUIDs) in SQLite

/*
 * This SQLite extension implements functions that handling RFC-4122 for UUIDv4
 * and RFC-9562 for UUIDv7
 *
 * Five SQL functions are implemented:
 *
 *     uuid4()                  - generate a version 4 UUID as a string
 *     uuid7()                  - generate a version 7 UUID as a string
 *     uuid_str(X)              - convert a UUID X into a well-formed UUID string
 *     uuid_blob(X)             - convert a UUID X into a 16-byte blob
 *     uuid7_timestamp_ms(X)    - extract unix timestamp in miliseconds
 *                                from version 7 UUID X.
 *
 * The output from uuid4(), uuid7() and uuid_str(X) are always well-formed RFC-4122
 * UUID strings in this format:
 *
 *        xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
 *
 * All of the 'x', 'M', and 'N' values are lower-case hexadecimal digits.
 * The M digit indicates the "version".  For uuid4()-generated UUIDs, the
 * version is always "4" (a random UUID).  The upper three bits of N digit
 * are the "variant".  This library only supports variant 1 (indicated
 * by values of N between '8' and 'b') as those are overwhelming the most
 * common.  Other variants are for legacy compatibility only.
 *
 * The output of uuid_blob(X) is always a 16-byte blob. The UUID input
 * string is converted in network byte order (big-endian) in accordance
 * with RFC-4122 specifications for variant-1 UUIDs.  Note that network
 * byte order is *always* used, even if the input self-identifies as a
 * variant-2 UUID.
 *
 * The input X to the uuid_str() and uuid_blob() functions can be either
 * a string or a BLOB. If it is a BLOB it must be exactly 16 bytes in
 * length or else a NULL is returned.  If the input is a string it must
 * consist of 32 hexadecimal digits, upper or lower case, optionally
 * surrounded by {...} and with optional "-" characters interposed in the
 * middle.  The flexibility of input is inspired by the PostgreSQL
 * implementation of UUID functions that accept in all of the following
 * formats:
 *
 *     A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11
 *     {a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}
 *     a0eebc999c0b4ef8bb6d6bb9bd380a11
 *     a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11
 *     {a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}
 *
 * If any of the above inputs are passed into uuid_str(), the output will
 * always be in the canonical RFC-4122 format:
 *
 *     a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
 *
 * If the X input string has too few or too many digits or contains
 * stray characters other than {, }, or -, then NULL is returned.
 */
#include <assert.h>
#include <ctype.h>
#include <string.h>
#include <time.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#if !defined(SQLITE_ASCII) && !defined(SQLITE_EBCDIC)
#define SQLITE_ASCII 1
#endif

/*
 * Translate a single byte of Hex into an integer.
 * This routine only works if h really is a valid hexadecimal
 * character:  0..9a..fA..F
 */
static unsigned char sqlite3UuidHexToInt(int h) {
    assert((h >= '0' && h <= '9') || (h >= 'a' && h <= 'f') || (h >= 'A' && h <= 'F'));
#ifdef SQLITE_ASCII
    h += 9 * (1 & (h >> 6));
#endif
#ifdef SQLITE_EBCDIC
    h += 9 * (1 & ~(h >> 4));
#endif
    return (unsigned char)(h & 0xf);
}

/*
 * Convert a 16-byte BLOB into a well-formed RFC-4122 UUID.  The output
 * buffer zStr should be at least 37 bytes in length.   The output will
 * be zero-terminated.
 */
static void sqlite3_uuid_blob_to_str(const unsigned char* aBlob, /* Input blob */
                                     unsigned char* zStr         /* Write the answer here */
) {
    static const char zDigits[] = "0123456789abcdef";
    int i, k;
    unsigned char x;
    k = 0;
    for (i = 0, k = 0x550; i < 16; i++, k = k >> 1) {
        if (k & 1) {
            zStr[0] = '-';
            zStr++;
        }
        x = aBlob[i];
        zStr[0] = zDigits[x >> 4];
        zStr[1] = zDigits[x & 0xf];
        zStr += 2;
    }
    *zStr = 0;
}

/*
 * Attempt to parse a zero-terminated input string zStr into a binary
 * UUID.  Return 0 on success, or non-zero if the input string is not
 * parsable.
 */
static int sqlite3_uuid_str_to_blob(const unsigned char* zStr, /* Input string */
                                    unsigned char* aBlob       /* Write results here */
) {
    int i;
    if (zStr[0] == '{')
        zStr++;
    for (i = 0; i < 16; i++) {
        if (zStr[0] == '-')
            zStr++;
        if (isxdigit(zStr[0]) && isxdigit(zStr[1])) {
            aBlob[i] = (sqlite3UuidHexToInt(zStr[0]) << 4) + sqlite3UuidHexToInt(zStr[1]);
            zStr += 2;
        } else {
            return 1;
        }
    }
    if (zStr[0] == '}')
        zStr++;
    return zStr[0] != 0;
}

/*
 * Render sqlite3_value pIn as a 16-byte UUID blob.  Return a pointer
 * to the blob, or NULL if the input is not well-formed.
 */
static const unsigned char* sqlite3_uuid_input_to_blob(sqlite3_value* pIn, /* Input text */
                                                       unsigned char* pBuf /* output buffer */
) {
    switch (sqlite3_value_type(pIn)) {
        case SQLITE_TEXT: {
            const unsigned char* z = sqlite3_value_text(pIn);
            if (sqlite3_uuid_str_to_blob(z, pBuf))
                return 0;
            return pBuf;
        }
        case SQLITE_BLOB: {
            int n = sqlite3_value_bytes(pIn);
            return n == 16 ? sqlite3_value_blob(pIn) : 0;
        }
        default: {
            return 0;
        }
    }
}

/*
 * uuid_v4_generate generates a version 4 UUID as a string
 */
static void uuid_v4_generate(sqlite3_context* context, int argc, sqlite3_value** argv) {
    unsigned char aBlob[16];
    unsigned char zStr[37];
    (void)argc;
    (void)argv;
    sqlite3_randomness(16, aBlob);
    aBlob[6] = (aBlob[6] & 0x0f) + 0x40;
    aBlob[8] = (aBlob[8] & 0x3f) + 0x80;
    sqlite3_uuid_blob_to_str(aBlob, zStr);
    sqlite3_result_text(context, (char*)zStr, 36, SQLITE_TRANSIENT);
}

// Time functions are not available of some older systems like 32-bit Windows.
#ifndef SQLEAN_OMIT_UUID7
/*
 * uuid_v7_generate generates a version 7 UUID as a string
 */
static void uuid_v7_generate(sqlite3_context* context, int argc, sqlite3_value** argv) {
    unsigned char aBlob[16];
    unsigned char zStr[37];
    (void)argc;
    (void)argv;

    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    unsigned long long timestampMs = ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;

    sqlite3_randomness(16, aBlob);
    aBlob[0] = timestampMs >> 40;
    aBlob[1] = timestampMs >> 32;
    aBlob[2] = timestampMs >> 24;
    aBlob[3] = timestampMs >> 16;
    aBlob[4] = timestampMs >> 8;
    aBlob[5] = timestampMs;
    aBlob[6] = (aBlob[6] & 0x0f) + 0x70;
    aBlob[8] = (aBlob[8] & 0x3f) + 0x80;
    sqlite3_uuid_blob_to_str(aBlob, zStr);
    sqlite3_result_text(context, (char*)zStr, 36, SQLITE_TRANSIENT);
}

/*
 * uuid_v7_extract_timestamp_ms extract unix timestamp in miliseconds
 * from a version 7 UUID.
 * X can be either a string or a blob.
 * If X is not a version 7 UUID, return NULL.
 */
static void uuid_v7_extract_timestamp_ms(sqlite3_context* context, int argc, sqlite3_value** argv) {
    unsigned char aBlob[16];
    const unsigned char* pBlob;
    (void)argc;
    pBlob = sqlite3_uuid_input_to_blob(argv[0], aBlob);
    if (pBlob == 0 || (pBlob[6] >> 4) != 7)
        return;

    unsigned long long timestampMs = 0;
    for (size_t i = 0; i < 6; ++i) {
        timestampMs = (timestampMs << 8) + pBlob[i];
    }

    sqlite3_result_int64(context, timestampMs);
}

#endif

/*
 * uuid_str converts a UUID X into a well-formed UUID string.
 * X can be either a string or a blob.
 */
static void uuid_str(sqlite3_context* context, int argc, sqlite3_value** argv) {
    unsigned char aBlob[16];
    unsigned char zStr[37];
    const unsigned char* pBlob;
    (void)argc;
    pBlob = sqlite3_uuid_input_to_blob(argv[0], aBlob);
    if (pBlob == 0)
        return;
    sqlite3_uuid_blob_to_str(pBlob, zStr);
    sqlite3_result_text(context, (char*)zStr, 36, SQLITE_TRANSIENT);
}

/*
 * uuid_blob converts a UUID X into a 16-byte blob.
 * X can be either a string or a blob.
 */
static void uuid_blob(sqlite3_context* context, int argc, sqlite3_value** argv) {
    unsigned char aBlob[16];
    const unsigned char* pBlob;
    (void)argc;
    pBlob = sqlite3_uuid_input_to_blob(argv[0], aBlob);
    if (pBlob == 0)
        return;
    sqlite3_result_blob(context, pBlob, 16, SQLITE_TRANSIENT);
}

int uuid_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS;
    static const int det_flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "uuid4", 0, flags, 0, uuid_v4_generate, 0, 0);
    sqlite3_create_function(db, "gen_random_uuid", 0, flags, 0, uuid_v4_generate, 0, 0);
#ifndef SQLEAN_OMIT_UUID7
    sqlite3_create_function(db, "uuid7", 0, flags, 0, uuid_v7_generate, 0, 0);
    sqlite3_create_function(db, "uuid7_timestamp_ms", 1, det_flags, 0, uuid_v7_extract_timestamp_ms,
                            0, 0);
#endif
    sqlite3_create_function(db, "uuid_str", 1, det_flags, 0, uuid_str, 0, 0);
    sqlite3_create_function(db, "uuid_blob", 1, det_flags, 0, uuid_blob, 0, 0);
    return SQLITE_OK;
}
