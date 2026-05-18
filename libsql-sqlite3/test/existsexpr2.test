# 2024 June 14
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
source $testdir/lock_common.tcl
set testprefix existsexpr2


do_execsql_test 1.0 {
  CREATE TABLE x1(a, b, PRIMARY KEY(a)) WITHOUT ROWID;
  INSERT INTO x1 VALUES(1, 2), (3, 4), (5, 6);
  CREATE INDEX x1b ON x1(b);

  CREATE TABLE x2(x, y);
  INSERT INTO x2 VALUES(1, 2), (3, 4), (5, 6);
}

do_execsql_test 1.1 {
  SELECT * FROM x1 WHERE EXISTS (SELECT 1 FROM x2 WHERE a!=123)
} {1 2   3 4   5 6}

do_execsql_test 1.2 {
  CREATE TABLE x3(u, v);
  CREATE INDEX x3u ON x3(u);
  INSERT INTO x3 VALUES
    (1, 1), (1, 2), (1, 3),
    (2, 1), (2, 2), (2, 3);
}

do_execsql_test 1.3 {
  SELECT * FROM x1 WHERE EXISTS (
    SELECT 1 FROM x3 WHERE u IN (1, 2, 3, 4) AND v=b
  );
} {
  1 2
}

#-------------------------------------------------------------------------
#
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a, b, c);
  CREATE INDEX t1ab ON t1(a,b);

  INSERT INTO t1 VALUES
      ('abc', 1, 1),
      ('abc', 2, 2),
      ('abc', 2, 3),

      ('def', 1, 1),
      ('def', 2, 2),
      ('def', 2, 3);

  CREATE TABLE t2(x, y);
  INSERT INTO t2 VALUES(1, 1), (2, 2), (3, 3);

  ANALYZE;
  DELETE FROM sqlite_stat1;
  INSERT INTO sqlite_stat1 VALUES('t1','t1ab','10000 5000 2');
  ANALYZE sqlite_master;
}


do_execsql_test 2.1 {
  SELECT a,b,c FROM t1 WHERE b=2 ORDER BY a
} {
  abc 2 2
  abc 2 3
  def 2 2
  def 2 3
}

do_execsql_test 2.2 {
  SELECT x, y FROM t2 WHERE EXISTS (
    SELECT 1 FROM t1 WHERE b=x
  )
} {
  1 1
  2 2
}



finish_test


