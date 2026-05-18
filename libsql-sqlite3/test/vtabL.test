# 2024-03-26
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# 

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix vtabL

ifcapable !vtab {
  finish_test
  return
}

register_tcl_module db

proc vtab_command {method args} {
  switch -- $method {
    xConnect {
      return $::create_table_sql
    }
  }

  return {}
}

foreach {tn cts} {
  1 {SELECT 123}
  2 {SELECT 123, 456}
  3 {INSERT INTO t1 VALUES(5, 6)}
  4 {CREATE INDEX i1 ON t1(a)}
  5 {CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT 1; END;}
  6 {DROP TABLE nosuchtable}
  7 {DROP TABLE x1}
  8 {DROP TABLE t1}
} {
  set ::create_table_sql $cts
  do_catchsql_test 1.$tn {
    CREATE VIRTUAL TABLE x1 USING tcl(vtab_command);
  } {1 {declare_vtab: syntax error}}
}

foreach {tn cts} {
  9 {CREATE TABLE xyz AS SELECT * FROM sqlite_schema}
  10 {CREATE TABLE xyz AS SELECT 1 AS 'col'}
} {
  set ::create_table_sql $cts
  do_catchsql_test 1.$tn {
    CREATE VIRTUAL TABLE x1 USING tcl(vtab_command);
  } {1 {declare_vtab: SQL logic error}}
}

foreach {tn cts} {
  1 {CREATE TABLE IF NOT EXISTS t1(a, b)}
  2 {CREATE TABLE ""(a, b PRIMARY KEY) WITHOUT ROWID}
} {
  set ::create_table_sql $cts
  execsql { DROP TABLE IF EXISTS x1 }
  do_execsql_test 2.$tn.1 {
    CREATE VIRTUAL TABLE x1 USING tcl(vtab_command);
  }
  do_execsql_test 2.$tn.2 {
    SELECT a, b FROM x1
  }
}

finish_test

