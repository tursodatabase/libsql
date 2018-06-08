# 2018 May 19
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

source [file join [file dirname $argv0] pg_common.tcl]

#=========================================================================

start_test window4 "2018 June 04"

execsql_test 1.0 {
  DROP TABLE IF EXISTS t3;
  CREATE TABLE t3(a TEXT PRIMARY KEY);
  INSERT INTO t3 VALUES('a'), ('b'), ('c'), ('d'), ('e');
  INSERT INTO t3 VALUES('f'), ('g'), ('h'), ('i'), ('j');
}

for {set i 1} {$i < 20} {incr i} {
  execsql_test 1.$i "SELECT a, ntile($i) OVER (ORDER BY a) FROM t3"
}

execsql_test 2.0 {
  DROP TABLE IF EXISTS t4;
  CREATE TABLE t4(a INTEGER PRIMARY KEY, b TEXT, c INTEGER);
  INSERT INTO t4 VALUES(1, 'A', 9);
  INSERT INTO t4 VALUES(2, 'B', 3);
  INSERT INTO t4 VALUES(3, 'C', 2);
  INSERT INTO t4 VALUES(4, 'D', 10);
  INSERT INTO t4 VALUES(5, 'E', 5);
  INSERT INTO t4 VALUES(6, 'F', 1);
  INSERT INTO t4 VALUES(7, 'G', 1);
  INSERT INTO t4 VALUES(8, 'H', 2);
  INSERT INTO t4 VALUES(9, 'I', 10);
  INSERT INTO t4 VALUES(10, 'J', 4);
}

execsql_test 2.1 {
  SELECT a, nth_value(b, c) OVER (ORDER BY a) FROM t4
}

execsql_test 2.2.1 {
  SELECT a, lead(b) OVER (ORDER BY a) FROM t4
}
execsql_test 2.2.2 {
  SELECT a, lead(b, 2) OVER (ORDER BY a) FROM t4
}
execsql_test 2.2.3 {
  SELECT a, lead(b, 3, 'abc') OVER (ORDER BY a) FROM t4
}

execsql_test 2.3.1 {
  SELECT a, lag(b) OVER (ORDER BY a) FROM t4
}
execsql_test 2.3.2 {
  SELECT a, lag(b, 2) OVER (ORDER BY a) FROM t4
}
execsql_test 2.3.3 {
  SELECT a, lag(b, 3, 'abc') OVER (ORDER BY a) FROM t4
}

execsql_test 2.4.1 {
  SELECT string_agg(b, '.') OVER (
    ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) FROM t4
}

execsql_test 3.0 {
  DROP TABLE IF EXISTS t5;
  CREATE TABLE t5(a INTEGER PRIMARY KEY, b TEXT, c TEXT, d INTEGER);
  INSERT INTO t5 VALUES(1, 'A', 'one',   5);
  INSERT INTO t5 VALUES(2, 'B', 'two',   4);
  INSERT INTO t5 VALUES(3, 'A', 'three', 3);
  INSERT INTO t5 VALUES(4, 'B', 'four',  2);
  INSERT INTO t5 VALUES(5, 'A', 'five',  1);
}

execsql_test 3.1 {
  SELECT a, nth_value(c, d) OVER (ORDER BY b) FROM t5
}

execsql_test 3.2 {
  SELECT a, nth_value(c, d) OVER (PARTITION BY b ORDER BY a) FROM t5
}

execsql_test 3.3 {
  SELECT a, count(*) OVER abc, count(*) OVER def FROM t5
  WINDOW abc AS (ORDER BY a), 
         def AS (ORDER BY a DESC)
  ORDER BY a;
}

finish_test

