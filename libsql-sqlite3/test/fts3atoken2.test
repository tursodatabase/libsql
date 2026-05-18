# 2025 September 5
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
# This file implements regression tests for SQLite library. The focus 
# of this script is testing the pluggable tokeniser feature of the 
# FTS3 module.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# If SQLITE_ENABLE_FTS3 is defined, omit this file.
ifcapable !fts3 {
  finish_test
  return
}

set ::testprefix fts3atoken2


reset_db
sqlite3_db_config db SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER 0

# With ENABLE_FTS3_TOKENIZER set to 0:
#
#   * It is not possible to get a pointer to a token implementation
#     using single arg fts3_tokenize() unless the name of the tokenizer
#     is a bound paramter - function should return NULL.
#
#   * But it is possible with a bound parameter.
#
do_execsql_test 1.1.1 {
  SELECT typeof( fts3_tokenizer('simple') );
} {null}
set bound "simple"
do_execsql_test 1.1.2 {
  SELECT typeof( fts3_tokenizer($bound) );
} {blob}

# With ENABLE_FTS3_TOKENIZER set to 0:
#
#   * It is not possible to create a token implementation using anything
#     other than a bound parameter.
#
#   * But it is possible with a bound parameter.
#
set literal [db one {SELECT quote( fts3_tokenizer($bound) )}]
set blob [db one {SELECT fts3_tokenizer($bound) }]

do_catchsql_test 1.2.1 "
  SELECT fts3_tokenizer('mytok', $literal)
" {1 {fts3tokenize disabled}}
do_catchsql_test 1.2.2 {
  CREATE VIRTUAL TABLE x1 USING fts3(col, tokenize=mytok);
} {1 {unknown tokenizer: mytok}}
do_catchsql_test 1.2.3 {
  SELECT fts3_tokenizer('mytok', $blob)
} {0 {{}}}
do_execsql_test 1.2.4 {
  CREATE VIRTUAL TABLE x1 USING fts3(col, tokenize=mytok);
}

# With ENABLE_FTS3_TOKENIZER set to 1:
#
#   * It is possible to get a pointer to a token implementation with either
#     a bound parameter or a literal.
#
sqlite3_db_config db SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER 1
set bound "simple"
do_execsql_test 1.3.1 {
  SELECT typeof( fts3_tokenizer('simple') );
} {blob}
do_execsql_test 1.3.2 {
  SELECT typeof( fts3_tokenizer($bound) );
} {blob}

# With ENABLE_FTS3_TOKENIZER set to 1:
#
#   * It is not possible to create a token implementation using either
#     a bound parameter or a literal.
#
set literal [db one {SELECT quote( fts3_tokenizer($bound) )}]
set blob    [db one {SELECT fts3_tokenizer($bound) }]

do_execsql_test 1.4.1 "
  SELECT typeof( fts3_tokenizer('mytok2', $literal) );
" {blob}
do_execsql_test 1.4.2 {
  CREATE VIRTUAL TABLE x2 USING fts3(col, tokenize=mytok2);
}
do_execsql_test 1.4.3 {
  SELECT typeof( fts3_tokenizer('mytok3', $blob) );
} {blob}
do_execsql_test 1.4.4 {
  CREATE VIRTUAL TABLE x3 USING fts3(col, tokenize=mytok3);
}

finish_test

