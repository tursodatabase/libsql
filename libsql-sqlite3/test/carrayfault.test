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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix carrayfault

ifcapable {!carray||!vtab} {
  finish_test; return
}

sqlite3 db test.db

do_execsql_test 2.0 {
  CREATE TABLE t1(a);
}

set STMT [sqlite3_prepare_v2 db "SELECT value FROM carray(?)" -1 TAIL]

# Test OOM faults in sqlite3_carray_bind() when binding an integer
# array.
#
foreach {tn mem} {
  1 -static
  2 -transient
  3 -malloc
} {
  do_faultsim_test 2.$tn -faults oom* -prep {
  } -body {
    sqlite3_carray_bind $::mem -int64 $::STMT 1   100 200 300 400
  } -test {
    faultsim_test_result {0 {}} {1 {out of memory}}
  }
}

# Test OOM faults in sqlite3_carray_bind() when binding a text array.
#
do_faultsim_test 3 -faults oom* -prep {
} -body {
    sqlite3_carray_bind -transient -text $::STMT 1 "hello" "world"
} -test {
  faultsim_test_result {0 {}} {1 {initialization of carray failed: }}
}

sqlite3_finalize $STMT


# Test OOM faults when running queries against carray.
#
do_faultsim_test 4 -faults oom* -prep {
  set ::STMT [sqlite3_prepare_v2 db "SELECT value FROM carray(?)" -1 TAIL]
  sqlite3_carray_bind -transient -text $::STMT 1 "hello" "world"
} -body {
    set myres [list]
    while { "SQLITE_ROW"==[sqlite3_step $::STMT] } {
      lappend myres [sqlite3_column_text $::STMT 1]
    }
    sqlite3_reset $::STMT
} -test {
  faultsim_test_result {0 SQLITE_OK} {0 SQLITE_NOMEM}
  sqlite3_finalize $::STMT
}

# Test OOM faults when preparing queries against carray.
#
do_faultsim_test 5 -faults oom* -prep {
  sqlite3 db test.db
} -body {
  execsql "SELECT value FROM carray(?)"
} -test {
  faultsim_test_result {0 {}}
}



sqlite3_carray_bind

finish_test
