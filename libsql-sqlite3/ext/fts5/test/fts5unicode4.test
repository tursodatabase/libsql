# 2018 July 25
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
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5unicode4

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE sss USING fts5(a, prefix=3); 
}

do_execsql_test 1.1 {
  INSERT INTO sss VALUES('まりや');
}

foreach {tn enc tok} {
  1   utf-8    ascii
  2   utf-16   ascii
  3   utf-8    unicode61
  4   utf-16   unicode61
} {
  reset_db

  do_execsql_test 1.$tn.0 " 
    PRAGMA encoding = '$enc'; 
    CREATE VIRTUAL TABLE vt2 USING fts5(c0, c1, tokenize=$tok);
  "

  do_execsql_test 1.$tn.1 {
    INSERT INTO vt2(c0, c1) VALUES ('bhal', x'17db');
  }

  do_execsql_test 1.$tn.2 {
    UPDATE vt2 SET c0='bhal';
  }

  do_execsql_test 1.$tn.3 {
    INSERT INTO vt2(vt2) VALUES('integrity-check')
  }

  do_execsql_test 1.$tn.4 {
    SELECT quote(c1) FROM vt2
  } {X'17DB'}
}

finish_test
