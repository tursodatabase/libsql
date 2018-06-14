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

execsql_test 3.4 {
  SELECT a, max(a) FILTER (WHERE (a%2)=0) OVER w FROM t5 
  WINDOW w AS (ORDER BY a)
}

execsql_test 3.5.1 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING)
  FROM t5
}
execsql_test 3.5.2 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
  FROM t5
}
execsql_test 3.5.3 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 0 PRECEDING AND 0 PRECEDING)
  FROM t5
}

execsql_test 3.6.1 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING)
  FROM t5
}
execsql_test 3.6.2 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
  FROM t5
}
execsql_test 3.6.3 {
  SELECT a, max(c) OVER (ORDER BY a ROWS BETWEEN 0 FOLLOWING AND 0 FOLLOWING)
  FROM t5
}

==========

execsql_test 4.0 {
  DROP TABLE IF EXISTS ttt;
  CREATE TABLE ttt(a INTEGER PRIMARY KEY, b INTEGER, c INTEGER);
  INSERT INTO ttt VALUES(1, 1, 1);
  INSERT INTO ttt VALUES(2, 2, 2);
  INSERT INTO ttt VALUES(3, 3, 3);

  INSERT INTO ttt VALUES(4, 1, 2);
  INSERT INTO ttt VALUES(5, 2, 3);
  INSERT INTO ttt VALUES(6, 3, 4);

  INSERT INTO ttt VALUES(7, 1, 3);
  INSERT INTO ttt VALUES(8, 2, 4);
  INSERT INTO ttt VALUES(9, 3, 5);
}

execsql_test 4.1 {
  SELECT max(c), max(b) OVER (ORDER BY b) FROM ttt GROUP BY b;
}

execsql_test 4.2 {
  SELECT max(b) OVER (ORDER BY max(c)) FROM ttt GROUP BY b;
}

execsql_test 4.3 {
  SELECT abs(max(b) OVER (ORDER BY b)) FROM ttt GROUP BY b;
}

execsql_test 4.4 {
  SELECT sum(b) OVER (
    ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  ) FROM ttt;
}

set lPart  [list "PARTITION BY b" "PARTITION BY b, a" "" "PARTITION BY a"]
set lOrder [list "ORDER BY a" "ORDER BY a DESC" "" "ORDER BY b, a"]
set lRange {
    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    "RANGE BETWEEN CURRENT ROW AND CURRENT ROW"
    "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"
}

set lRows {
    "ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING"
    "ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING"
    "ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING"
    "ROWS BETWEEN 0 PRECEDING AND 1 PRECEDING"
    "ROWS BETWEEN 1 FOLLOWING AND 500 FOLLOWING"
}

set tn 1
set SQL {
  SELECT max(c) OVER ($p1 $o1 $r1), 
  min(c) OVER ($p2 $o2 $r2)
  FROM ttt ORDER BY a
}
set SQL2 {
  SELECT sum(c) OVER ($p1 $o1 $r1), 
         sum(c) OVER ($p2 $o2 $r2)
  FROM ttt ORDER BY a
}

set o1 [lindex $lOrder 0]
set o2 [lindex $lOrder 0]
set r1 [lindex $lRange 0]
set r2 [lindex $lRange 0]
foreach p1 $lPart { foreach p2 $lPart { 
  execsql_test 4.5.$tn.1 [subst $SQL]
  execsql_test 4.5.$tn.2 [subst $SQL2]
  incr tn
}}

set o1 [lindex $lOrder 0]
set o2 [lindex $lOrder 0]
set p1 [lindex $lPart 0]
set p2 [lindex $lPart 0]
foreach r1 $lRange { foreach r2 $lRange { 
  execsql_test 4.5.$tn.1 [subst $SQL]
  execsql_test 4.5.$tn.2 [subst $SQL2]
  incr tn
}}
foreach r1 $lRows { foreach r2 $lRows { 
  execsql_test 4.5.$tn.1 [subst $SQL]
  execsql_test 4.5.$tn.2 [subst $SQL2]
  incr tn
}}

set r1 [lindex $lRange 0]
set r2 [lindex $lRange 0]
set p1 [lindex $lPart 0]
set p2 [lindex $lPart 0]
foreach o1 $lOrder { foreach o2 $lOrder { 
  execsql_test 4.5.$tn.1 [subst $SQL]
  execsql_test 4.5.$tn.2 [subst $SQL2]
  incr tn
}}


finish_test

