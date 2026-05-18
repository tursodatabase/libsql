# 2025-10-07
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
# This file implements tests for CARRAY extension
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix carray02

ifcapable !vtab {
  finish_test
  return
}

proc run_stmt {stmt} {
  set r {}
  while {[sqlite3_step $stmt]=="SQLITE_ROW"} {
    for {set i 0} {$i<[sqlite3_data_count $stmt]} {incr i} {
      lappend r [sqlite3_column_text $stmt $i]
    }
  }
  sqlite3_reset $stmt
  return $r
}

# Bind some text values using sqlite3_carray_bind(). Including a NULL pointer.
#
set STMT [
  sqlite3_prepare_v2 db "SELECT value, value IS NULL FROM carray(?)" -1 TAIL
]
do_test 1.0 {
  sqlite3_carray_bind -transient -text $STMT 1 "abc" NULL "def"
  run_stmt $STMT
} {abc 0 {} 1 def 0}
sqlite3_finalize $STMT

# Pass bad values as the "flags" parameter to sqlite3_carray_bind().
#
set STMT [sqlite3_prepare_v2 db "SELECT value FROM carray(?)" -1 TAIL]
do_test 2.0 {
  list [catch {sqlite3_carray_bind -flags 5 -int32 $STMT 1 1 2 3 4} msg] $msg
} {1 {SQL logic error}}
do_test 2.1 {
  list [catch {sqlite3_carray_bind -flags -1 -int32 $STMT 1 1 2 3 4} msg] $msg
} {1 {SQL logic error}}
sqlite3_finalize $STMT

# In each of the following, carray(?) contains integer values 1 to 5, bound
# using sqlite3_carray_bind().
#
foreach {tn sql res} {
  1 { SELECT value FROM carray(?) WHERE value>2 } {3 4 5}
  2 { 
    WITH s(i) AS ( VALUES(1) UNION ALL VALUES(2) )
    SELECT i, value FROM s, carray(?) WHERE i=value;
  } {1 1 2 2}
} {
  do_test 2.2.$tn {
    set STMT [sqlite3_prepare_v2 db $sql -1 TAIL]
    sqlite3_carray_bind -int32 $STMT 1 1 2 3 4 5
    set r [run_stmt $STMT]
    sqlite3_finalize $STMT
    set r
  } $res
}

# In each of the following, ? is bound to an int array containing 1 to 5.
# Bound using C code like:
#
#     sqlite3_bind_pointer(pStmt, 1, aInt, "carray", SQLITE_TRANSIENT)
#
foreach {tn sql res} {
  1 { SELECT value FROM carray(?, 5) } {1 2 3 4 5}
  2 { SELECT value FROM carray(?, 3, 'int32') } {1 2 3}
  3 { SELECT value, pointer, count, ctype FROM carray(?, 5, 'int32') } 
    {1 {} 5 int32 2 {} 5 int32 3 {} 5 int32 4 {} 5 int32 5 {} 5 int32}
  4 { SELECT rowid, value FROM carray(?, 5, 'int32') } 
    {1 1 2 2 3 3 4 4 5 5}
} {
  do_test 2.3.$tn {
    set STMT [sqlite3_prepare_v2 db $sql -1 TAIL]
    bind_carray_intptr $STMT 1   1 2 3 4 5
    set r [run_stmt $STMT]
    sqlite3_finalize $STMT
    set r
  } $res
}

# In each of the following. Both carray(?1) and carray(?2) contain integer
# values 1 to 5. Bound by sqlite3_carray_bind().
#
foreach {tn sql res} {
  1 { 
    SELECT * FROM carray(?1) AS a, carray(?2) AS b 
    WHERE a.value=b.value
  } {1 1 2 2 3 3 4 4 5 5}

  2 { 
    SELECT * FROM carray(?1) AS a, carray(?2) AS b 
    WHERE a.value=b.value AND a.value<3 AND b.value<3
  } {1 1 2 2 3 3}

  3 { 
    SELECT * FROM carray(?1) AS a, carray(?2) AS b 
    WHERE a.value<3 AND b.value<3 AND a.value=b.value
  } {1 1 2 2 3 3}

  4 { 
    SELECT * FROM carray(?1) AS a, carray(?2, a.value) AS b 
    WHERE a.value=b.value
  } {1 1 2 2 3 3}

} {
  do_test 2.4.$tn {
    set STMT [sqlite3_prepare_v2 db {
      SELECT * FROM carray(?1) AS a, carray(?2) AS b WHERE a.value=b.value
    } -1 TAIL]
    sqlite3_carray_bind $STMT 1   1 2 3 4 5
    sqlite3_carray_bind $STMT 2   1 2 3 4 5
    set r [run_stmt $STMT]
    sqlite3_finalize $STMT
    set r
  } {1 1 2 2 3 3 4 4 5 5}
}

# Test that not binding any pointer, or passing a value that is not a bound
# pointer to carray() produces no rows of output.
#
do_execsql_test 3.0.0 {
  SELECT * FROM carray
} {}
do_execsql_test 3.0.1 {
  SELECT * FROM carray('0xFFFF', 5)
} {}
do_execsql_test 3.0.2 {
  SELECT * FROM carray('0xFFFF')
} {}
do_execsql_test 3.0.3 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES('0xFFFF');
  SELECT * FROM t1, carray WHERE carray.pointer = t1.x;
} {}

# Test passing an unknown datatype.
#
do_test 3.1 {
  set STMT [sqlite3_prepare_v2 db {SELECT * FROM carray(?, 5, 'apples')} -1 T]
  sqlite3_carray_bind $STMT 1   1 2 3 4 5
  sqlite3_step $STMT
  list [sqlite3_finalize $STMT] [sqlite3_errmsg db]
} {SQLITE_ERROR {unknown datatype: 'apples'}}

finish_test
