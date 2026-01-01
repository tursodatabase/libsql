// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// PCRE2 build constants.

#ifndef REGEXP_CONSTANTS_H
#define REGEXP_CONSTANTS_H

#define PCRE2_CODE_UNIT_WIDTH 8
#define LINK_SIZE 2
#define HAVE_CONFIG_H
#define SUPPORT_UNICODE

#if defined(_WIN32)
#define PCRE2_STATIC
#endif

#endif /* REGEXP_CONSTANTS_H */
