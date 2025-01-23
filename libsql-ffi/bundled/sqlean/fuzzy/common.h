// Adapted from the spellfix SQLite exension, Public Domain
// https://www.sqlite.org/src/file/ext/misc/spellfix.c

#ifndef COMMON_H
#define COMMON_H

/*
** Character classes for ASCII characters:
**
**   0   ''        Silent letters:   H W
**   1   'A'       Any vowel:   A E I O U (Y)
**   2   'B'       A bilabeal stop or fricative:  B F P V W
**   3   'C'       Other fricatives or back stops:  C G J K Q S X Z
**   4   'D'       Alveolar stops:  D T
**   5   'H'       Letter H at the beginning of a word
**   6   'L'       Glide:  L
**   7   'R'       Semivowel:  R
**   8   'M'       Nasals:  M N
**   9   'Y'       Letter Y at the beginning of a word.
**   10  '9'       Digits: 0 1 2 3 4 5 6 7 8 9
**   11  ' '       White space
**   12  '?'       Other.
*/
#define CCLASS_SILENT 0
#define CCLASS_VOWEL 1
#define CCLASS_B 2
#define CCLASS_C 3
#define CCLASS_D 4
#define CCLASS_H 5
#define CCLASS_L 6
#define CCLASS_R 7
#define CCLASS_M 8
#define CCLASS_Y 9
#define CCLASS_DIGIT 10
#define CCLASS_SPACE 11
#define CCLASS_OTHER 12

#define SCRIPT_LATIN 0x0001
#define SCRIPT_CYRILLIC 0x0002
#define SCRIPT_GREEK 0x0004
#define SCRIPT_HEBREW 0x0008
#define SCRIPT_ARABIC 0x0010

#define ALWAYS(X) 1
#define NEVER(X) 0

// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#define EQ(a, b) ((a) == (b))
#define NOT_EQ(a, b) !EQ(a, b)

#define MIN(a, b) ((a) < (b)) ? (a) : (b)
#define MIN3(a, b, c) MIN(MIN(a, b), c)
#define MIN4(a, b, c, d) MIN(MIN(a, b), MIN(c, d))

#define MAX(a, b) ((a) > (b)) ? (a) : (b)
#define MAX3(a, b, c) MAX(MAX(a, b), c)
#define MAX4(a, b, c, d) MAX(MAX(a, b), MAX(b, c))

#endif
