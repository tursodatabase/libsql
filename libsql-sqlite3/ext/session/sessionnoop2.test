# 2011 March 07
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix sessionnoop2

foreach {tn wo} {
  1 ""
  2 " WITHOUT ROWID "
} {
  reset_db
  eval [string map [list %WO% $wo] {
do_execsql_test $tn.1.0 {
  CREATE TABLE t1(a PRIMARY KEY, b, c) %WO%;
  INSERT INTO t1 VALUES('a', 'A', 'AAA');
  INSERT INTO t1 VALUES('b', 'B', 'BBB');
  INSERT INTO t1 VALUES('c', 'C', 'CCC');
  INSERT INTO t1 VALUES('d', 'D', 'DDD');
  INSERT INTO t1 VALUES('e', 'E', 'EEE');
}

forcedelete test.db2
sqlite3 db2 test.db2

do_execsql_test -db db2 $tn.1.1 {
  CREATE TABLE t1(a PRIMARY KEY, b, c) %WO%;
  INSERT INTO t1 VALUES('a', 'A', 'AAA');
  INSERT INTO t1 VALUES('b', 'B', '123');
  INSERT INTO t1 VALUES('c', 'C', 'CCC');
  INSERT INTO t1 VALUES('e', 'E', 'EEE');
  INSERT INTO t1 VALUES('f', 'F', 'FFF');
}

set C [changeset_from_sql {
  UPDATE t1 SET c='123' WHERE a='b';
  DELETE FROM t1 WHERE a='d';
  INSERT INTO t1 VALUES('f', 'F', 'FFF');
}]


set ::conflict_list [list]
proc xConflict {args} {
  lappend ::conflict_list $args
  return "OMIT"
}
do_test $tn.1.2 {
  sqlite3changeset_apply_v2 db2 $C xConflict
  set ::conflict_list
} [list {*}{
  {UPDATE t1 DATA {t b {} {} t BBB} {{} {} {} {} t 123} {t b t B t 123}}
  {INSERT t1 CONFLICT {t f t F t FFF} {t f t F t FFF}}
  {DELETE t1 NOTFOUND {t d t D t DDD}}
}]
do_test $tn.1.3 {
  set ::conflict_list [list]
  sqlite3changeset_apply_v2 db2 $C xConflict
  set ::conflict_list
} [list {*}{
  {UPDATE t1 DATA {t b {} {} t BBB} {{} {} {} {} t 123} {t b t B t 123}}
  {INSERT t1 CONFLICT {t f t F t FFF} {t f t F t FFF}}
  {DELETE t1 NOTFOUND {t d t D t DDD}}
}]

do_test $tn.1.4 {
  set ::conflict_list [list]
  sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
  set ::conflict_list
} {}

do_execsql_test -db db2 1.5 {
  UPDATE t1 SET b='G' WHERE a='f';
  UPDATE t1 SET c='456' WHERE a='b';
}

do_test $tn.1.6 {
  set ::conflict_list [list]
  sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
  set ::conflict_list
} [list {*}{
  {UPDATE t1 DATA {t b {} {} t BBB} {{} {} {} {} t 123} {t b t B t 456}}
  {INSERT t1 CONFLICT {t f t F t FFF} {t f t G t FFF}}
}]

db2 close

#--------------------------------------------------------------------------

reset_db
forcedelete test.db2
sqlite3 db2 test.db2
do_execsql_test $tn.2.0 {
  CREATE TABLE t1(a PRIMARY KEY, b) %WO%;
}
do_execsql_test -db db2 $tn.2.1 {
  CREATE TABLE t1(a PRIMARY KEY, b, c DEFAULT 'val') %WO%;
}

do_test $tn.2.2 {
  do_then_apply_sql -ignorenoop {
    INSERT INTO t1 VALUES(1, 2);
  }
  do_then_apply_sql -ignorenoop {
    UPDATE t1 SET b=2 WHERE a=1
  }
} {}

db2 close

}]
}


#-------------------------------------------------------------------------
reset_db
forcedelete test.db2
do_execsql_test 3.0 {
  CREATE TABLE xyz(a, b, c, PRIMARY KEY(a, b), UNIQUE(c));
  ANALYZE;
  WITH s(i) AS (
    VALUES(1) UNION ALL SELECT i+1 FROM s WHERE i<100
  )
  INSERT INTO xyz SELECT i, i, i FROM s;
  VACUUM INTO 'test.db2';
}

set C [changeset_from_sql { ANALYZE }]
sqlite3 db2 test.db2

set ::conflict_list [list]
proc xConflict {args} { lappend ::conflict_list $args ; return "OMIT" }
do_test 3.1 {
  sqlite3changeset_apply_v2 db2 $C xConflict
  set ::conflict_list
} {}

do_test 3.2 {
  sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
  set ::conflict_list
} {}

do_test 3.3 {
  sqlite3changeset_apply_v2 db2 $C xConflict
  set ::conflict_list
} [list {*}{
  {INSERT sqlite_stat1 CONFLICT {t xyz t sqlite_autoindex_xyz_1 t {100 1 1}} {t xyz t sqlite_autoindex_xyz_1 t {100 1 1}}} 
  {INSERT sqlite_stat1 CONFLICT {t xyz t sqlite_autoindex_xyz_2 t {100 1}} {t xyz t sqlite_autoindex_xyz_2 t {100 1}}}
}]

do_execsql_test -db db2 3.4 {
  UPDATE sqlite_stat1 SET stat='200 1 1' WHERE idx='sqlite_autoindex_xyz_1';
}

do_test 3.5 {
  set ::conflict_list [list]
  sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
  set ::conflict_list
} [list {*}{
  {INSERT sqlite_stat1 CONFLICT {t xyz t sqlite_autoindex_xyz_1 t {100 1 1}} {t xyz t sqlite_autoindex_xyz_1 t {200 1 1}}} 
}]

#-------------------------------------------------------------------------
# Test that the conflict-handler is invoked for conflicts caused by
# DELETE ops even if SQLITE_CHANGESETAPPLY_IGNORENOOP is specified.
#
set ::conflict_list [list]
proc xConflict {args} {
  lappend ::conflict_list $args
  return "OMIT"
}

foreach {tn wo} {
  1 ""
  2 " WITHOUT ROWID "
} {
  reset_db
  catch { db2 close }
  forcedelete test.db2
  do_execsql_test 4.$tn.0 "
    CREATE TABLE x1(a INTEGER PRIMARY KEY, b, c) $wo;
    INSERT INTO x1 VALUES(1, 'one', 'i');
    INSERT INTO x1 VALUES(2, 'two', 'ii');
    INSERT INTO x1 VALUES(3, 'three', 'iii');
    VACUUM INTO 'test.db2';
  "
  
  set C [changeset_from_sql {
    DELETE FROM x1 WHERE a=2;
  }]
  
  sqlite3 db2 test.db2
  do_execsql_test -db db2 4.$tn.1 {
    UPDATE x1 SET b='four hundred' WHERE a=2;
  }
  
  do_test 4.$tn.2.1 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {DELETE x1 DATA {i 2 t two t ii} {i 2 t {four hundred} t ii}}
  }]
  
  do_test 4.$tn.2.2 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {DELETE x1 DATA {i 2 t two t ii} {i 2 t {four hundred} t ii}}
  }]
}

foreach {tn wo} {
  1 ""
  2 " WITHOUT ROWID "
} {
  reset_db
  catch { db2 close }
  forcedelete test.db2
  do_execsql_test 5.$tn.0 "
    PRAGMA foreign_keys = ON;
    CREATE TABLE p1(a INTEGER PRIMARY KEY, b, c) $wo;
    CREATE TABLE c1(a INTEGER PRIMARY KEY REFERENCES p1, b);
    INSERT INTO p1 VALUES(1, 'one', 'i');
    INSERT INTO p1 VALUES(2, 'two', 'ii');
    INSERT INTO p1 VALUES(3, 'three', 'iii');
    VACUUM INTO 'test.db2';
  "
  
  set C [changeset_from_sql {
    DELETE FROM p1 WHERE a=2;
  }]
  
  sqlite3 db2 test.db2
  do_execsql_test -db db2 5.$tn.1 {
    PRAGMA foreign_keys = ON;
    INSERT INTO c1 VALUES(2, 'xyz');
  }

  do_test 5.$tn.2.1 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {FOREIGN_KEY 1}
  }]

  # Reinsert the row deleted by test case 5.$tn.2.1
  execsql { INSERT INTO p1 VALUES(2, 'two', 'ii') } db2

  do_test 5.$tn.2.2 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {FOREIGN_KEY 1}
  }]
}

foreach {tn wo} {
  1 ""
  2 " WITHOUT ROWID "
} {
  reset_db
  catch { db2 close }
  forcedelete test.db2
  do_execsql_test 6.$tn.0 "
    CREATE TABLE x1(a INTEGER PRIMARY KEY, b, c) $wo;
    CREATE TABLE x2(x UNIQUE);

    INSERT INTO x1 VALUES(1, 'one', 'i');
    INSERT INTO x1 VALUES(2, 'two', 'ii');
    INSERT INTO x1 VALUES(3, 'three', 'iii');
    VACUUM INTO 'test.db2';
  "
  
  set C [changeset_from_sql {
    DELETE FROM x1 WHERE a=2;
  }]
  
  sqlite3 db2 test.db2
  do_execsql_test -db db2 6.$tn.1 {
    INSERT INTO x2 VALUES(0);
    CREATE TRIGGER tr1 AFTER DELETE ON x1 BEGIN
      INSERT INTO x2 VALUES(0);
    END;
  }

  do_test 6.$tn.2.1 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {DELETE x1 CONSTRAINT {i 2 t two t ii}}
  }]

breakpoint
  do_test 6.$tn.2.2 {
    set ::conflict_list [list]
    sqlite3changeset_apply_v2 -ignorenoop db2 $C xConflict
    set ::conflict_list
  } [list {*}{
    {DELETE x1 CONSTRAINT {i 2 t two t ii}}
  }]
}


finish_test

