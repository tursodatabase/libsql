# 2011 September 20
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
# Tests for the sqlite3_db_status() function
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

set ::testprefix dbstatus2

do_execsql_test 1.0 {
  PRAGMA page_size = 1024;
  PRAGMA auto_vacuum = 0;

  CREATE TABLE t1(a PRIMARY KEY, b);
  INSERT INTO t1 VALUES(1, randomblob(600));
  INSERT INTO t1 VALUES(2, randomblob(600));
  INSERT INTO t1 VALUES(3, randomblob(600));
}

proc db_hit_miss {db {reset 0}} {
  set nHit  [sqlite3_db_status $db CACHE_HIT $reset]
  set nMiss [sqlite3_db_status $db CACHE_MISS $reset]
  list $nHit $nMiss
}

proc db_write {db {reset 0}} {
  sqlite3_db_status $db CACHE_WRITE $reset
}

proc db_spill {db {reset 0}} {
  sqlite3_db_status $db CACHE_SPILL $reset
}

proc db_temp_spill {db {reset 0}} {
  sqlite3_db_status $db TEMPBUF_SPILL $reset
}

do_test 1.1 {
  db close
  sqlite3 db test.db
  execsql { PRAGMA mmap_size = 0 }
  expr {[file size test.db] / 1024}
} 6

do_test 1.2 {
  execsql { SELECT b FROM t1 WHERE a=2 }
  db_hit_miss db
} {{0 2 0} {0 4 0}}

do_test 1.3 { 
  execsql { SELECT b FROM t1 WHERE a=2 }
  db_hit_miss db
} {{0 6 0} {0 4 0}}

do_test 1.4 { 
  execsql { SELECT b FROM t1 WHERE a=2 }
  db_hit_miss db
} {{0 10 0} {0 4 0}}

do_test 1.5 { 
  db_hit_miss db 1
} {{0 10 0} {0 4 0}}

do_test 1.6 { 
  db_hit_miss db 0
} {{0 0 0} {0 0 0}}

do_test 1.7 {
  set fd [db incrblob main t1 b 1]
  fconfigure $fd -translation binary
  set len [string length [read $fd]]
  close $fd
  set len
} 600
do_test 1.8 { sqlite3_db_status db CACHE_HIT  0 } {0 2 0}
do_test 1.9 { sqlite3_db_status db CACHE_MISS 0 } {0 1 0}

do_test 2.1 { db_write db } {0 0 0}
do_test 2.2 { 
  execsql { INSERT INTO t1 VALUES(4, randomblob(600)) }
  db_write db
} {0 4 0}
do_test 2.3 { db_write db 1 } {0 4 0}
do_test 2.4 { db_write db 0 } {0 0 0}
do_test 2.5 { db_write db 1 } {0 0 0}

if {[wal_is_capable]} {
  do_test 2.6 { 
    execsql { PRAGMA journal_mode = WAL }
    db_write db 1
  } {0 1 0}
}
do_test 2.7 { 
  execsql { INSERT INTO t1 VALUES(5, randomblob(600)) }
  db_write db
} {0 4 0}
do_test 2.8 { db_write db 1 } {0 4 0}
do_test 2.9 { db_write db 0 } {0 0 0}

do_test 3.0 { db_spill db 1 } {0 0 0}
do_test 3.1 { db_spill db 0 } {0 0 0}
do_execsql_test 3.2 {
  PRAGMA journal_mode=DELETE;
  PRAGMA cache_size=3;
  UPDATE t1 SET b=randomblob(1000);
} {delete}
do_test 3.3 { db_spill db 0 } {0 8 0}


if {$::TEMP_STORE<3} {
  do_execsql_test 4.0 {
    PRAGMA temp_store = file;
    PRAGMA cache_size = -1024;
  } {}

  do_test 4.1 { db_temp_spill db 0 } {0 0 0}

  do_execsql_test 4.2 {
    CREATE TABLE data(a INTEGER, b BLOB);

    -- Insert 5-6 MB of data.
    WITH s(i) AS ( SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<75000 )
    INSERT INTO data SELECT i, hex(randomblob(50)) FROM s;
  }

  do_test 4.3 { db_temp_spill db 0 } {0 0 0}

  do_test 4.4 {
    execsql { SELECT a, b FROM data ORDER BY a }
    set nTmpSpill [lindex [db_temp_spill db 1] 1]
    expr {($nTmpSpill>7*1000*1000) && ($nTmpSpill<10*1000*1000)?"ok":$nTmpSpill}
  } ok

  # The previous test case reset the status value.
  do_test 4.5 { db_temp_spill db 0 } {0 0 0}

  do_test 4.6 {
    execsql { CREATE INDEX i1 ON data(a) }
    set nTmpSpill [lindex [db_temp_spill db 1] 1]
    expr {($nTmpSpill>384*1000) && ($nTmpSpill<768*1000)?"ok":$nTmpSpill}
  } ok

  # The previous test case reset the status value.
  do_test 4.7 { db_temp_spill db 0 } {0 0 0}

  # Same query as in (4.4). Now does not require temp space.
  do_test 4.8 {
    execsql { SELECT a, b FROM data ORDER BY a }
    db_temp_spill db 0
  } {0 0 0}
}

 
finish_test
