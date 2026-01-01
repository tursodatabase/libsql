// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Rune (utf8 codepoint) handling.

#ifndef UTF8_RUNE_H
#define UTF8_RUNE_H

#include <stdbool.h>
#include <stdint.h>

enum {
    U8G_Cc,
    U8G_Lt,
    U8G_Nd,
    U8G_Nl,
    U8G_Pc,
    U8G_Pd,
    U8G_Pf,
    U8G_Pi,
    U8G_Sc,
    U8G_Zl,
    U8G_Zp,
    U8G_Zs,
    U8G_Arabic,
    U8G_Cyrillic,
    U8G_Devanagari,
    U8G_Greek,
    U8G_Han,
    U8G_Latin,
    U8G_SIZE
};

bool rune_isupper(uint32_t c);
bool rune_islower(uint32_t c);
bool rune_isdigit(uint32_t c);
bool rune_isalpha(uint32_t c);
bool rune_isalnum(uint32_t c);
bool rune_isblank(uint32_t c);
bool rune_isspace(uint32_t c);
bool rune_iscased(uint32_t c);
bool rune_isword(uint32_t c);

uint32_t rune_casefold(uint32_t c);
uint32_t rune_tolower(uint32_t c);
uint32_t rune_toupper(uint32_t c);

#endif  // UTF8_RUNE_H