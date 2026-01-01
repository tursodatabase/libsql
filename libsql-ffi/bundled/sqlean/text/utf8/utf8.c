/* MIT License
 *
 * Copyright (c) 2023 Tyge Løvset
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// UTF-8 string handling.

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "text/utf8/rune.h"
#include "text/utf8/utf8.h"

const uint8_t utf8_dtab[] = {
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  1,  1,  1,  1,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  10, 3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  3,  3,  4,  3,  3,  11, 6,  6,  6,  5,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  0,  12, 24, 36, 60, 96, 84, 12, 12, 12, 48, 72, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 0,  12, 12, 12, 12, 12, 0,  12, 0,  12, 12, 12, 24, 12, 12, 12, 12, 12,
    24, 12, 24, 12, 12, 12, 12, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12,
    12, 12, 12, 24, 12, 12, 12, 12, 12, 12, 12, 12, 12, 36, 12, 36, 12, 12, 12, 36, 12, 12, 12,
    12, 12, 36, 12, 36, 12, 12, 12, 36, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
};

// Encode/decode functions.

// utf8_decode decodes a byte as part of a utf8 codepoint.
uint32_t utf8_decode(utf8_decode_t* d, const uint32_t byte) {
    const uint32_t type = utf8_dtab[byte];
    d->codep = d->state ? (byte & 0x3fu) | (d->codep << 6) : (0xffU >> type) & byte;
    return d->state = utf8_dtab[256 + d->state + type];
}

// utf8_encode encodes the utf8 codepoint c to s
// and returns the number of bytes written.
int utf8_encode(char* out, uint32_t c) {
    if (c < 0x80U) {
        out[0] = (char)c;
        return 1;
    } else if (c < 0x0800U) {
        out[0] = (char)((c >> 6 & 0x1F) | 0xC0);
        out[1] = (char)((c & 0x3F) | 0x80);
        return 2;
    } else if (c < 0x010000U) {
        if ((c < 0xD800U) | (c >= 0xE000U)) {
            out[0] = (char)((c >> 12 & 0x0F) | 0xE0);
            out[1] = (char)((c >> 6 & 0x3F) | 0x80);
            out[2] = (char)((c & 0x3F) | 0x80);
            return 3;
        }
    } else if (c < 0x110000U) {
        out[0] = (char)((c >> 18 & 0x07) | 0xF0);
        out[1] = (char)((c >> 12 & 0x3F) | 0x80);
        out[2] = (char)((c >> 6 & 0x3F) | 0x80);
        out[3] = (char)((c & 0x3F) | 0x80);
        return 4;
    }
    return 0;
}

// String functions.

// utf8_at returns a pointer to the utf8 codepoint at index in s.
const char* utf8_at(const char* s, size_t n, size_t index) {
    while ((index > 0) & (*s != 0) & (n-- != 0)) {
        index -= (*++s & 0xC0) != 0x80;
    }
    return s;
}

// utf8_pos returns the byte position of the utf8 codepoint at index in s.
size_t utf8_pos(const char* s, size_t n, size_t index) {
    return (size_t)(utf8_at(s, n, index) - s);
}

// utf8_len returns the number of utf8 codepoints in s.
size_t utf8_len(const char* s, size_t n) {
    size_t size = 0;
    while ((n-- != 0) & (*s != 0)) {
        size += (*++s & 0xC0) != 0x80;
    }
    return size;
}

// utf8_peek returns the utf8 codepoint at the start of s.
uint32_t utf8_peek(const char* s) {
    utf8_decode_t d = {.state = 0};
    do {
        utf8_decode(&d, (uint8_t)*s++);
    } while (d.state);
    return d.codep;
}

// utf8_peek_at returns the utf8 codepoint at the index pos from s.
uint32_t utf8_peek_at(const char* s, size_t n, size_t pos) {
    return utf8_peek(utf8_at(s, n, pos));
}

// utf8_icmp compares the utf8 strings s1 and s2 case-insensitively.
int utf8_icmp(const char* s1, size_t n1, const char* s2, size_t n2) {
    utf8_decode_t d1 = {.state = 0}, d2 = {.state = 0};
    size_t j1 = 0, j2 = 0;
    while ((j1 < n1) & (j2 < n2)) {
        do {
            utf8_decode(&d1, (uint8_t)s1[j1++]);
        } while (d1.state);
        do {
            utf8_decode(&d2, (uint8_t)s2[j2++]);
        } while (d2.state);
        int32_t c = (int32_t)rune_casefold(d1.codep) - (int32_t)rune_casefold(d2.codep);
        if (c || !s2[j2 - 1])  // OK if n1 and n2 are npos
            return (int)c;
    }
    return (int)(n1 - n2);
}

// utf8_valid returns true if s is a valid utf8 string.
bool utf8_valid(const char* s, size_t n) {
    utf8_decode_t d = {.state = 0};
    while ((n-- != 0) & (*s != 0)) {
        utf8_decode(&d, (uint8_t)*s++);
    }
    return d.state == 0;
}
