# 2022 August 28
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

source [file join [file dirname [info script]] recover_common.tcl]
set testprefix recover1

proc compare_result {db1 db2 sql} {
  set r1 [$db1 eval $sql]
  set r2 [$db2 eval $sql]
  if {$r1 != $r2} {
    puts "r1: $r1"
    puts "r2: $r2"
    error "mismatch for $sql"
  }
  return ""
}

proc compare_dbs {db1 db2} {
  compare_result $db1 $db2 "SELECT sql FROM sqlite_master ORDER BY 1"
  foreach tbl [$db1 eval {SELECT name FROM sqlite_master WHERE type='table'}] {
    compare_result $db1 $db2 "SELECT * FROM $tbl"
  }

  compare_result $db1 $db2 "PRAGMA page_size"
  compare_result $db1 $db2 "PRAGMA auto_vacuum"
  compare_result $db1 $db2 "PRAGMA encoding"
  compare_result $db1 $db2 "PRAGMA user_version"
  compare_result $db1 $db2 "PRAGMA application_id"
}

proc do_recover_test {tn} {
  forcedelete test.db2
  forcedelete rstate.db

  uplevel [list do_test $tn.1 {
    set R [sqlite3_recover_init db main test.db2]
    $R config testdb rstate.db
    $R run
    $R finish
  } {}]

  sqlite3 db2 test.db2
  uplevel [list do_test $tn.2 [list compare_dbs db db2] {}]
  db2 close

  forcedelete test.db2
  forcedelete rstate.db

  uplevel [list do_test $tn.3 {
    set ::sqlhook [list]
    set R [sqlite3_recover_init_sql db main my_sql_hook]
    $R config testdb rstate.db
    $R config rowids 1
    $R run
    $R finish
  } {}]

  sqlite3 db2 test.db2
  execsql [join $::sqlhook ";"] db2
  db2 close
  sqlite3 db2 test.db2
  uplevel [list do_test $tn.4 [list compare_dbs db db2] {}]
  db2 close
}

proc my_sql_hook {sql} {
  lappend ::sqlhook $sql
  return 0
}

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  CREATE TABLE t2(a INTEGER PRIMARY KEY, b) WITHOUT ROWID;
  WITH s(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<10
  )
  INSERT INTO t1 SELECT i*2, hex(randomblob(250)) FROM s;
  INSERT INTO t2 SELECT * FROM t1;
}

do_recover_test 1

do_execsql_test 2.0 {
  ALTER TABLE t1 ADD COLUMN c DEFAULT 'xyz'
}
do_recover_test 2

do_execsql_test 3.0 {
  CREATE INDEX i1 ON t1(c);
}
do_recover_test 3

do_execsql_test 4.0 {
  CREATE VIEW v1 AS SELECT * FROM t2;
}
do_recover_test 4

do_execsql_test 5.0 {
  CREATE UNIQUE INDEX i2 ON t1(c, b);
}
do_recover_test 5

#--------------------------------------------------------------------------
#
reset_db
do_execsql_test 6.0 {
  CREATE TABLE t1(
      a INTEGER PRIMARY KEY,
      b INT,
      c TEXT,
      d INT GENERATED ALWAYS AS (a*abs(b)) VIRTUAL,
      e TEXT GENERATED ALWAYS AS (substr(c,b,b+1)) STORED,
      f TEXT GENERATED ALWAYS AS (substr(c,b,b+1)) STORED
  );

  INSERT INTO t1 VALUES(1, 2, 'hello world');
}
do_recover_test 6

do_execsql_test 7.0 {
  CREATE TABLE t2(i, j GENERATED ALWAYS AS (i+1) STORED, k);
  INSERT INTO t2 VALUES(10, 'ten');
}
do_execsql_test 7.1 {
  SELECT * FROM t2
} {10 11 ten}

do_recover_test 7.2

#--------------------------------------------------------------------------
#
reset_db
do_execsql_test 8.0 {
  CREATE TABLE x1(a INTEGER PRIMARY KEY AUTOINCREMENT, b, c);
  WITH s(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<2
  )
  INSERT INTO x1(b, c) SELECT hex(randomblob(100)), hex(randomblob(100)) FROM s;
  
  CREATE INDEX x1b ON x1(b);
  CREATE INDEX x1cb ON x1(c, b);
  DELETE FROM x1 WHERE a>50;

  ANALYZE;
}

do_recover_test 8

#-------------------------------------------------------------------------
reset_db
ifcapable fts5 {
  do_execsql_test 9.1 {
    CREATE VIRTUAL TABLE ft5 USING fts5(a, b);
    INSERT INTO ft5 VALUES('hello', 'world');
  }
  do_recover_test 9 
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 10.1 {
  CREATE TABLE x1(a PRIMARY KEY, str TEXT) WITHOUT ROWID;
  INSERT INTO x1 VALUES(1, '
    \nhello\012world(\n0)(\n1)
  ');
  INSERT INTO x1 VALUES(2, '
    \nhello
  ');
}
do_execsql_test 10.2 "
  INSERT INTO x1 VALUES(3, '\012hello there\015world');
  INSERT INTO x1 VALUES(4, '\015hello there\015world');
"
do_recover_test 10 

#-------------------------------------------------------------------------
reset_db
do_execsql_test 11.1 {
  PRAGMA page_size = 4096;
  PRAGMA encoding='utf16';
  PRAGMA auto_vacuum = 2;
  PRAGMA user_version = 45;
  PRAGMA application_id = 22;

  CREATE TABLE u1(u, v);
  INSERT INTO u1 VALUES('edvin marton', 'bond');
  INSERT INTO u1 VALUES(1, 4.0);
}
do_execsql_test 11.1a {
  PRAGMA auto_vacuum;
} {2}

do_recover_test 11 

do_test 12.1 {
  set R [sqlite3_recover_init db "" test.db2]
  $R config lostandfound ""
  $R config invalid xyz
} {12}
do_test 12.2 {
  $R run
  $R run
} {0}

do_test 12.3 {
  $R finish
} {}



#-------------------------------------------------------------------------
reset_db
file_control_reservebytes db 16
do_execsql_test 12.1 {
  PRAGMA auto_vacuum = 2;
  PRAGMA user_version = 45;
  PRAGMA application_id = 22;

  CREATE TABLE u1(u, v);
  CREATE UNIQUE INDEX i1 ON u1(u, v);
  INSERT INTO u1 VALUES(1, 2), (3, 4);

  CREATE TABLE u2(u, v);
  CREATE UNIQUE INDEX i2 ON u1(u, v);
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(50000)), hex(randomblob(20000)));
}

do_recover_test 12 

#-------------------------------------------------------------------------
reset_db
sqlite3 db "" 
do_recover_test 13

do_execsql_test 14.1 {
  PRAGMA auto_vacuum = 2;
  PRAGMA user_version = 45;
  PRAGMA application_id = 22;

  CREATE TABLE u1(u, v);
  CREATE UNIQUE INDEX i1 ON u1(u, v);
  INSERT INTO u1 VALUES(1, 2), (3, 4);

  CREATE TABLE u2(u, v);
  CREATE UNIQUE INDEX i2 ON u1(u, v);
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(500)), hex(randomblob(1000)));
  INSERT INTO u2 VALUES(hex(randomblob(50000)), hex(randomblob(20000)));
}
do_recover_test 14 

#-------------------------------------------------------------------------
reset_db
execsql {
  PRAGMA journal_mode=OFF;
  PRAGMA mmap_size=10;
}
do_execsql_test 15.1 {
  CREATE TABLE t1(x);
} {}
do_recover_test 15 

#-------------------------------------------------------------------------
reset_db
do_test 16.1 {
  execsql { PRAGMA journal_mode = wal }
  execsql {
    CREATE TABLE t1(x);
    INSERT INTO t1 VALUES(1), (2), (3);
  }
} {}
do_test 16.2 {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  $R finish
} {}
do_execsql_test 16.3 {
  SELECT * FROM t1;
} {1 2 3}

do_execsql_test 16.4 {
  BEGIN;
    SELECT * FROM t1;
} {1 2 3}
do_test 16.5 {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  list [catch { $R finish } msg] $msg
} {1 {cannot start a transaction within a transaction}}
do_execsql_test 16.6 {
  SELECT * FROM t1;
} {1 2 3}
do_execsql_test 16.7 {
  INSERT INTO t1 VALUES(4);
}
do_test 16.8 {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  list [catch { $R finish } msg] $msg
} {1 {cannot start a transaction within a transaction}}
do_execsql_test 16.9 {
  SELECT * FROM t1;
  COMMIT;
} {1 2 3 4}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 17.1 {
  CREATE TABLE t(a, PRIMARY KEY(a, a COLLATE NOCASE)) WITHOUT ROWID;
  INSERT INTO t VALUES('abc');
  INSERT INTO t VALUES('def');
}
do_test 17.2 {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  list [catch { $R finish } msg] $msg
} {0 {}}

#-------------------------------------------------------------------------
foreach enc {utf8 utf16 utf16le utf16be} {
  reset_db
  do_execsql_test 18.$enc.1 {
    PRAGMA auto_vacuum = 0;
    PRAGMA encoding='utf16';
    CREATE TABLE t1(a, b);
    CREATE TABLE t2(a, b);
    INSERT INTO t1 VALUES('abc', 'def');
    PRAGMA writable_schema = 1;
    DELETE FROM sqlite_schema WHERE name='t1';
  }

  proc my_sql_hook2 {sql} {
    if {[string match "INSERT INTO lostandfound*" $sql]} {
      lappend ::script $sql
    }
    return 0
  }
  do_test 18.$enc.2 {
    set ::script [list]
    set R [sqlite3_recover_init_sql db main my_sql_hook2]
    $R config lostandfound lostandfound
    $R run
    $R finish
    set ::script
  } {{INSERT INTO lostandfound VALUES(2, 2, 2, 1, 'abc', 'def')}}
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 19.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1 VALUES(1, 'one');
  INSERT INTO t1 VALUES(2, 'two');

  ALTER TABLE t1 ADD COLUMN c NOT NULL DEFAULT 13;
  INSERT INTO t1 VALUES(3, 'three', 'hello world');
}

do_recover_test 19.1



finish_test

