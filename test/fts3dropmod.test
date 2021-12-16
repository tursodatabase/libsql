# 2021 December 16
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this script is testing the FTS3 module.
#
# $Id: fts3aa.test,v 1.1 2007/08/20 17:38:42 shess Exp $
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix fts3dropmod

# If SQLITE_ENABLE_FTS3 is defined, omit this file.
ifcapable !fts3 {
  finish_test
  return
}

sqlite3_drop_modules db fts3
do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE t1 USING fts3(x);
} 
do_catchsql_test 1.1 {
  CREATE VIRTUAL TABLE t2 USING fts4(x);
} {1 {no such module: fts4}}

reset_db
sqlite3_drop_modules db fts4
do_execsql_test 2.0 {
  CREATE VIRTUAL TABLE t1 USING fts4(x);
} 
do_catchsql_test 2.1 {
  CREATE VIRTUAL TABLE t2 USING fts3(x);
} {1 {no such module: fts3}}

finish_test
