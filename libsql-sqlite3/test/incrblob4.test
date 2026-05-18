# 2012 March 23
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
set testdir [file dirname $argv0]
source $testdir/tester.tcl
ifcapable {!incrblob} { finish_test ; return }
set testprefix incrblob4

proc create_t1 {} {
  execsql {
    PRAGMA page_size = 1024;
    CREATE TABLE t1(k INTEGER PRIMARY KEY, v);
  }
}

proc populate_t1 {} {
  set data [list a b c d e f g h i j k l m n o p q r s t u v w x y z]
  foreach d $data {
    set blob [string repeat $d 900]
    execsql { INSERT INTO t1(v) VALUES($blob) }
  }
}


do_test 1.1 { 
  create_t1
  populate_t1 
} {}

do_test 1.2 {
  set blob [db incrblob t1 v 5]
  read $blob 10
} {eeeeeeeeee}

do_test 1.3 {
  execsql { DELETE FROM t1 }
  populate_t1
} {}



do_test 2.1 { 
  reset_db
  create_t1
  populate_t1 
} {}

do_test 2.2 {
  set blob [db incrblob t1 v 10]
  read $blob 10
} {jjjjjjjjjj}

do_test 2.3 {
  set new [string repeat % 900]
  execsql { DELETE FROM t1 WHERE k=10 }
  execsql { DELETE FROM t1 WHERE k=9 }
  execsql { INSERT INTO t1(v) VALUES($new) }
} {}



do_test 3.1 {
  reset_db
  create_t1
  populate_t1 
} {}

do_test 3.2 {
  set blob [db incrblob t1 v 20]
  read $blob 10
} {tttttttttt}

do_test 3.3 {
  set new [string repeat % 900]
  execsql { UPDATE t1 SET v = $new WHERE k = 20 }
  execsql { DELETE FROM t1 WHERE k=19 }
  execsql { INSERT INTO t1(v) VALUES($new) }
} {}

#-------------------------------------------------------------------------
# Test that it is not possible to DROP a table with an incremental blob
# cursor open on it.
#
do_execsql_test 4.1 {
  CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
  INSERT INTO t2 VALUES(456, '0123456789');
}
do_test 4.2 {
  set blob [db incrblob -readonly t2 b 456]
  read $blob 5
} {01234}
do_catchsql_test 4.3 {
  DROP TABLE t2
} {1 {database table is locked}}
do_test 4.4 {
  sqlite3_extended_errcode db
} {SQLITE_LOCKED}
close $blob

#-------------------------------------------------------------------------

ifcapable preupdate {

reset_db
do_execsql_test 5.1 {
  CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
  INSERT INTO t2 VALUES(1000, 'abcdefghijklmnopqrstuvwxyz');
  INSERT INTO t2 VALUES(2000, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
  INSERT INTO t2 VALUES(3000, 'abcdefghijklmnopqrstuvwxyz');
}

do_test 5.2.1 {
  execsql BEGIN
  set blob [db incrblob t2 b 2000]
  seek $blob 0
  puts -nonewline $blob "hello "
  flush $blob
  execsql ROLLBACK
} {}

do_test 5.2.2 {
  puts -nonewline $blob "world"
  set rc [catch { flush $blob } msg]
  list $rc [regsub {input/output} $msg {I/O}]
} "1 {error flushing \"$blob\": I/O error}"
catch { close $blob }

set preupdate_count 0
proc preupdate {args} { incr ::preupdate_count ; return {} }
db preupdate hook preupdate

set preupdate_count 0
do_test 5.3.1 {
  execsql BEGIN
  set blob [db incrblob t2 b 1000]
  seek $blob 0
  puts -nonewline $blob "hello "
  flush $blob
  execsql ROLLBACK
} {}

do_test 5.3.2 {
  puts -nonewline $blob "world"
  set rc [catch { flush $blob } msg]
  list $rc [regsub {input/output} $msg {I/O}]
} "1 {error flushing \"$blob\": I/O error}"
catch { close $blob }

do_test 5.3.3 {
  set ::preupdate_count
} {1}

set preupdate_count 0
do_test 5.4.1 {
  execsql BEGIN
  set blob [db incrblob t2 b 1000]
  seek $blob 0
  puts -nonewline $blob "hello "
  flush $blob
  execsql { DELETE FROM t2 WHERE a=3000; }
} {}

do_test 5.4.2 {
  puts -nonewline $blob "world"
  list [catch { flush $blob } msg] $msg
} "0 {}"
catch { close $blob }
catchsql { ROLLBACK }

do_test 5.3.3 {
  set ::preupdate_count
} {3}

set preupdate_count 0
do_test 5.4.3 {
  execsql BEGIN
  set blob [db incrblob t2 b 2000]
  seek $blob 0
  puts -nonewline $blob "hello "
  flush $blob
  execsql { UPDATE t2 SET b='abcdefghijklmnopqrstuvwxyz' WHERE a=2000 }
} {}

do_test 5.4.4 {
  puts -nonewline $blob "world"
  set rc [catch { flush $blob } msg]
  list $rc [regsub {input/output} $msg {I/O}]
} "1 {error flushing \"$blob\": I/O error}"
catch { close $blob }
catchsql { ROLLBACK }

do_test 5.4.5 {
  set ::preupdate_count
} {2}

}

finish_test
