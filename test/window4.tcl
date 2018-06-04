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

finish_test
