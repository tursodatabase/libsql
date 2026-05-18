# 2024 Jan 8
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# TESTRUNNER: shell
#
# The focus of this file is testing the CLI shell tool. Specifically, 
# testing that it is possible to run a ".dump" script that creates
# virtual tables without explicitly disabling defensive mode.
#
# And, that it can process a ".dump" script that contains strings
# delimited using double-quotes in the schema (DQS_DDL setting).
#

# Test plan:
#
#   shell1-1.*: Basic command line option handling.
#   shell1-2.*: Basic "dot" command token parsing.
#   shell1-3.*: Basic test that "dot" command can be called.
#   shell1-{4-8}.*: Test various "dot" commands's functionality.
#   shell1-9.*: Basic test that "dot" commands and SQL intermix ok.
#
set testdir [file dirname $argv0]
source $testdir/tester.tcl
set CLI [test_cli_invocation]

set ::testprefix shell9

ifcapable !fts5 {
  finish_test
  return
}

#----------------------------------------------------------------------------
# Test cases shell9-1.* verify that scripts output by .dump may be parsed
# by the shell tool without explicitly disabling DEFENSIVE mode, unless
# the shell is in safe mode.
#
do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE t1 USING fts5(a, b, c);
  INSERT INTO t1 VALUES('one', 'two', 'three');
}
db close

# Create .dump file in "testdump.txt".
#
set out [open testdump.txt w]
puts $out [lindex [catchcmd test.db .dump] 1]
close $out

# Check testdump.txt can be processed if the initial db is empty.
#
do_test 1.1.1 {
  forcedelete test.db
  catchcmd test.db ".read testdump.txt"
} {0 {}}
sqlite3 db test.db
do_execsql_test 1.1.2 {
  SELECT * FROM t1;
} {one two three}

# Check testdump.txt cannot be processed if the initial db is not empty.
#
reset_db
do_execsql_test 1.2.1 {
  CREATE TABLE t4(hello);
}
db close
do_test 1.2.2 {
  catchcmd test.db ".read testdump.txt"
} {1 {Parse error near line 5: table sqlite_master may not be modified}}

# Check testdump.txt cannot be processed if the db is in safe mode
#
do_test 1.3.1 {
  forcedelete test.db
  catchsafecmd test.db ".read testdump.txt"
} {1 {line 1: cannot run .read in safe mode}}
do_test 1.3.2 {
  set fd [open testdump.txt]
  set script [read $fd]
  close $fd
  forcedelete test.db
  catchsafecmd test.db $script
} {1 {Parse error near line 5: table sqlite_master may not be modified}}
do_test 1.3.3 {
  # Quick check that the above would have worked but for safe mode.
  forcedelete test.db
  catchcmd test.db $script
} {0 {}}

#----------------------------------------------------------------------------
# Test cases shell9-2.* verify that a warning is printed at the top of
# .dump scripts that contain virtual tables.
#
proc contains_warning {text} {
  return [string match "*WARNING: Script requires that*" $text]
}

reset_db
do_execsql_test 2.0.1 {
  CREATE TABLE t1(x);
  CREATE TABLE t2(y);
  INSERT INTO t1 VALUES('one');
  INSERT INTO t2 VALUES('two');
}
do_test 2.0.2 {
  contains_warning [catchcmd test.db .dump]
} 0

do_execsql_test 2.1.1 {
  CREATE virtual TABLE r1 USING fts5(x);
}
do_test 2.1.2 {
  contains_warning [catchcmd test.db .dump]
} 1

do_test 2.2.1 {
  contains_warning [catchcmd test.db ".dump t1"]
} 0
do_test 2.2.2 {
  contains_warning [catchcmd test.db ".dump r1"]
} 1

#-------------------------------------------------------------------------
reset_db
sqlite3_db_config db DQS_DDL 1
do_execsql_test 3.1.0 {
  CREATE TABLE t4(hello, check( hello IS NOT "xyz") );
}
db close

# Create .dump file in "testdump.txt".
#
set out [open testdump.txt w]
puts $out [lindex [catchcmd test.db .dump] 1]
close $out
do_test 3.1.1 {
  forcedelete test.db
  catchcmd test.db ".read testdump.txt"
} {0 {}}

finish_test
