# 2022 January 5
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
source $testdir/malloc_common.tcl

set ::testprefix returningfault


do_execsql_test 1.0 {
  CREATE TABLE t1 (b);
} {}
faultsim_save_and_close

do_faultsim_test 1 -faults oom-t* -prep {
  faultsim_restore_and_reopen
} -body {
  execsql { 
    INSERT INTO t1(b) VALUES(65) RETURNING (
      SELECT * FROM sqlite_temp_schema
    ) AS aaa;
  }
} -test {
  faultsim_test_result {1 {sub-select returns 5 columns - expected 1}}
}

ifcapable vtab {
  reset_db
  do_execsql_test 2.0 {
    CREATE TABLE t1(x);
  }
 
  proc eponymous_cmd {method args} {
    switch -- $method {
      xConnect {
        db eval { SELECT * FROM sqlite_schema }
        return "CREATE TABLE t1 (a, b)"
      }
  
      xBestIndex {
        return "idxnum 555"
      }
  
      xFilter {
        return [list sql {SELECT 123, 'A', 'B'}]
      }
  
      xUpdate {
        return 123
      }
  
    }
  
    return {}
  }

  faultsim_save_and_close

  do_faultsim_test 2 -faults oom* -prep {
    faultsim_restore_and_reopen
    register_tcl_module db eponymous_cmd
    db eval { SELECT * FROM t1 }
    sqlite3 db2 test.db
    db2 eval { CREATE TABLE t2(y) }
    db2 close
  } -body {
    db eval {
      INSERT INTO tcl VALUES('hello', 'world') RETURNING *
    }
  } -test {
    faultsim_test_result {0 {hello world}} {1 {vtable constructor failed: tcl}}
  }
}

finish_test
