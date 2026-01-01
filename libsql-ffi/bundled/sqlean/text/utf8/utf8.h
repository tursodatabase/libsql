// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// UTF-8 string handling.

#ifndef UTF8_H
#define UTF8_H

#include <stdint.h>

// decode next utf8 codepoint.
// See https://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.
typedef struct {
    uint32_t state, codep;
} utf8_decode_t;

// utf8_decode decodes a byte as part of a utf8 codepoint.
uint32_t utf8_decode(utf8_decode_t* d, const uint32_t byte);
// utf8_encode encodes the utf8 codepoint c to s
// and returns the number of bytes written.
int utf8_encode(char* out, uint32_t c);

// utf8_len returns the number of utf8 codepoints in s.
size_t utf8_len(const char* s, size_t n);

// utf8_at returns a pointer to the utf8 codepoint at index in s.
const char* utf8_at(const char* s, size_t n, size_t index);
// utf8_pos returns the byte position of the utf8 codepoint at index in s.
size_t utf8_pos(const char* s, size_t n, size_t index);

// utf8_peek returns the utf8 codepoint at the start of s.
uint32_t utf8_peek(const char* s);
// utf8_peek_at returns the utf8 codepoint at the index pos from s.
uint32_t utf8_peek_at(const char* s, size_t n, size_t pos);

// utf8_icmp compares the utf8 strings s1 and s2 case-insensitively.
int utf8_icmp(const char* s1, size_t n1, const char* s2, size_t n2);

// utf8_valid returns true if s is a valid utf8 string.
bool utf8_valid(const char* s, size_t n);

// utf8_tolower converts the utf8 string s to lowercase.
bool utf8_tolower(char* s, size_t n);
// utf8_toupper converts the utf8 string s to uppercase.
bool utf8_toupper(char* s, size_t n);
// utf8_totitle converts the utf8 string s to title-case.
bool utf8_totitle(char* s, size_t n);
// utf8_casefold converts the utf8 string s to folded-case.
bool utf8_casefold(char* s, size_t n);

#endif  // UTF8_H
