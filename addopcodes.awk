#!/usr/bin/awk
#
# This script appends additional token codes to the end of the
# parse.h file that lemon generates.  These extra token codes are
# not used by the parser.  But they are used by the tokenizer and/or
# the code generator.
#
#
BEGIN {
  max = 0
}
/^#define TK_/ {
  print $0
  if( max<$3 ) max = $3
}
END {
  printf "#define TK_%-29s %4d\n", "TO_TEXT",         ++max
  printf "#define TK_%-29s %4d\n", "TO_BLOB",         ++max
  printf "#define TK_%-29s %4d\n", "TO_NUMERIC",      ++max
  printf "#define TK_%-29s %4d\n", "TO_INT",          ++max
  printf "#define TK_%-29s %4d\n", "TO_REAL",         ++max
  printf "#define TK_%-29s %4d\n", "ISNOT",           ++max
  printf "#define TK_%-29s %4d\n", "END_OF_FILE",     ++max
  printf "#define TK_%-29s %4d\n", "ILLEGAL",         ++max
  printf "#define TK_%-29s %4d\n", "SPACE",           ++max
  printf "#define TK_%-29s %4d\n", "UNCLOSED_STRING", ++max
  printf "#define TK_%-29s %4d\n", "FUNCTION",        ++max
  printf "#define TK_%-29s %4d\n", "COLUMN",          ++max
  printf "#define TK_%-29s %4d\n", "AGG_FUNCTION",    ++max
  printf "#define TK_%-29s %4d\n", "AGG_COLUMN",      ++max
  printf "#define TK_%-29s %4d\n", "UMINUS",          ++max
  printf "#define TK_%-29s %4d\n", "UPLUS",           ++max
  printf "#define TK_%-29s %4d\n", "REGISTER",        ++max
}
