// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

#ifndef REGEXP_H
#define REGEXP_H

#include "regexp/pcre2/pcre2.h"

pcre2_code* regexp_compile(const char* pattern);
void regexp_free(pcre2_code* re);
char* regexp_get_error(const char* pattern);
int regexp_like(pcre2_code* re, const char* source);
int regexp_extract(pcre2_code* re, const char* source, size_t group_idx, char** substr);
int regexp_replace(pcre2_code* re, const char* source, const char* repl, char** dest);

#endif /* REGEXP_H */
