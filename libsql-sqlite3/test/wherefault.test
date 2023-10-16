# 2008 December 23
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library. The focus
# is testing of where.c. More specifically, the focus is on handling OOM
# errors within the code that optimizes WHERE clauses that feature the 
# OR operator.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

source $testdir/malloc_common.tcl

set testprefix wherefault

do_malloc_test 1 -sqlprep {
  CREATE TABLE t1(a, b, c);
  CREATE INDEX i1 ON t1(a);
  CREATE INDEX i2 ON t1(b);
} -sqlbody {
  SELECT c FROM t1
  WHERE 
    a = 2 OR b = 'three' OR a = 4 OR b = 'five' OR a = 6 OR
    b = 'seven' OR a = 8 OR b = 'nine' OR a = 10
  ORDER BY rowid;

  SELECT c FROM t1 WHERE
    a = 1 OR a = 2 OR a = 3 OR a = 4 OR a = 5 OR a = 6;

  SELECT c FROM t1 WHERE
    a BETWEEN 1 AND 3  AND b < 5 AND b > 2 AND c = 4;
}

do_malloc_test 2 -tclprep {
  db eval {
    BEGIN;
    CREATE TABLE t1(a, b, c);
    CREATE INDEX i1 ON t1(a);
    CREATE INDEX i2 ON t1(b);
  }
  for {set i 0} {$i < 1000} {incr i} {
    set ii [expr $i*$i]
    set iii [expr $i*$i]
    db eval { INSERT INTO t1 VALUES($i, $ii, $iii) }
  }
  db eval COMMIT
} -sqlbody {
  SELECT count(*) FROM t1 WHERE a BETWEEN 5 AND 995 OR b BETWEEN 5 AND 900000;
}

reset_db
do_execsql_test 3.0 {
  PRAGMA writable_schema = 1;
  BEGIN TRANSACTION;    
    CREATE TABLE t1(
      a INT AS (c*11),
      b TEXT AS (substr(d,1,3)) STORED, 
      c INTEGEB PRIMARI KEY, d TEXT
    );
    CREATE INDEX t1a ON t1(a);
  COMMIT;
}
faultsim_save_and_close

do_faultsim_test 3.1 -faults oom* -prep {
  faultsim_restore_and_reopen
} -body {
  execsql {
    SELECT * FROM (SELECT a FROM t1 NATURAL JOIN t1 WHERE a IN (SELECT b FROM t1 ORDER BY b)) WHERE (SELECT a FROM t1 NATURAL JOIN (SELECT * FROM (SELECT a FROM t1 NATURAL JOIN t1 WHERE a IN (SELECT CASE b WHEN 82 THEN 207 WHEN 869 THEN 406 WHEN 85 THEN 83 WHEN 705 THEN 698 ELSE 1992229051 END%5 FROM t1 ORDER BY b)) WHERE (SELECT a FROM t1 NATURAL JOIN (SELECT b FROM t1 ORDER BY b) WHERE a IN (SELECT b FROM t1 ORDER BY b))) WHERE a );
  }
} -test {
  faultsim_test_result {0 {}}
}

finish_test
