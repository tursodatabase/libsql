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

// Rune (utf8 codepoint) handling.

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "text/utf8/rune.h"

#include "text/utf8/groups.h"
#include "text/utf8/tables.h"

#define c_arraylen(a) (size_t)(sizeof(a) / sizeof 0 [a])

// rune_isgroup returns true if c is in the unicode group.
static bool rune_isgroup(int group, uint32_t c) {
    for (int j = 0; j < _utf8_unicode_groups[group].nr16; ++j) {
        if (c < _utf8_unicode_groups[group].r16[j].lo) {
            return false;
        }
        if (c <= _utf8_unicode_groups[group].r16[j].hi) {
            return true;
        }
    }
    return false;
}

// rune_isupper returns true if c is an uppercase letter.
bool rune_isupper(uint32_t c) {
    return rune_tolower(c) != c;
}

// rune_islower returns true if c is a lowercase letter.
bool rune_islower(uint32_t c) {
    return rune_toupper(c) != c;
}

// rune_isdigit returns true if c is a digit character.
bool rune_isdigit(uint32_t c) {
    if (c < 128) {
        return isdigit((int)c) != 0;
    }
    return rune_isgroup(U8G_Nd, c);
}

// rune_isalpha returns true if c is an alphabetic character.
bool rune_isalpha(uint32_t c) {
    static int16_t groups[] = {U8G_Latin, U8G_Nl,         U8G_Greek, U8G_Cyrillic,
                               U8G_Han,   U8G_Devanagari, U8G_Arabic};
    if (c < 128) {
        return isalpha((int)c) != 0;
    }
    for (size_t j = 0; j < c_arraylen(groups); ++j) {
        if (rune_isgroup(groups[j], c)) {
            return true;
        }
    }
    return false;
}

// rune_isalnum returns true if c is an alphanumeric character.
bool rune_isalnum(uint32_t c) {
    if (c < 128) {
        return isalnum((int)c) != 0;
    }
    return rune_isalpha(c) || rune_isgroup(U8G_Nd, c);
}

// rune_isblank returns true if c is a blank character.
bool rune_isblank(uint32_t c) {
    if (c < 128) {
        return (c == ' ') | (c == '\t');
    }
    return rune_isgroup(U8G_Zs, c);
}

// rune_isspace returns true if c is a whitespace character.
bool rune_isspace(uint32_t c) {
    if (c < 128) {
        return isspace((int)c) != 0;
    }
    return ((c == 8232) | (c == 8233)) || rune_isgroup(U8G_Zs, c);
}

// rune_iscased returns true if c is a cased character.
bool rune_iscased(uint32_t c) {
    if (c < 128) {
        return isalpha((int)c) != 0;
    }
    return rune_islower(c) || rune_isupper(c) || rune_isgroup(U8G_Lt, c);
}

// rune_isword returns true if c is a word character.
bool rune_isword(uint32_t c) {
    if (c < 128) {
        return (isalnum((int)c) != 0) | (c == '_');
    }
    return rune_isalpha(c) || rune_isgroup(U8G_Nd, c) || rune_isgroup(U8G_Pc, c);
}

// Character transformation functions.

// rune_casefold returns the unicode casefold of c.
uint32_t rune_casefold(uint32_t c) {
    for (int i = 0; i < casefold_len; ++i) {
        const struct CaseMapping entry = casemappings[i];
        if (c <= entry.c2) {
            if (c < entry.c1) {
                return c;
            }
            int d = entry.m2 - entry.c2;
            if (d == 1) {
                return c + ((entry.c2 & 1) == (c & 1));
            }
            return (uint32_t)((int)c + d);
        }
    }
    return c;
}

// rune_tolower returns the lowercase version of c.
uint32_t rune_tolower(uint32_t c) {
    for (int i = 0; i < (int)(sizeof upcase_ind / sizeof *upcase_ind); ++i) {
        const struct CaseMapping entry = casemappings[upcase_ind[i]];
        if (c <= entry.c2) {
            if (c < entry.c1) {
                return c;
            }
            int d = entry.m2 - entry.c2;
            if (d == 1) {
                return c + ((entry.c2 & 1) == (c & 1));
            }
            return (uint32_t)((int)c + d);
        }
    }
    return c;
}

// rune_toupper returns the uppercase version of c.
uint32_t rune_toupper(uint32_t c) {
    for (int i = 0; i < (int)(sizeof lowcase_ind / sizeof *lowcase_ind); ++i) {
        const struct CaseMapping entry = casemappings[lowcase_ind[i]];
        if (c <= entry.m2) {
            int d = entry.m2 - entry.c2;
            if (c < (uint32_t)(entry.c1 + d)) {
                return c;
            }
            if (d == 1) {
                return c - ((entry.m2 & 1) == (c & 1));
            }
            return (uint32_t)((int)c - d);
        }
    }
    return c;
}
