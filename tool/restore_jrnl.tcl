# 2010 January 7
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements utility functions for SQLite library.
#
# This file attempts to restore the header of a journal.
# This may be useful for rolling-back the last committed 
# transaction from a recovered journal.
#
# $Id: restore_jrnl.tcl,v 1.14 2009/07/11 06:55:34 danielk1977 Exp $

package require sqlite3

if { $argc != 2 } {
  puts "USAGE: restore_jrnl.tcl db_name jrnl_name"
  puts "Example: restore_jrnl.tcl foo.sqlite foo.sqlite-journal"
  return
} else {
  set db_name [lindex $argv 0]
  set jrnl_name [lindex $argv 1]
}

# is there a way to determine this?
set sectsz 512

# Copy file $from into $to
#
proc copy_file {from to} {
  file copy -force $from $to
}

# Execute some SQL
#
proc catchsql {sql} {
  set rc [catch {uplevel [list db eval $sql]} msg]
  list $rc $msg
}

# Perform a test
#
proc do_test {name cmd expected} {
  puts -nonewline "$name ..."
  set res [uplevel $cmd]
  if {$res eq $expected} {
    puts Ok
  } else {
    puts Error
    puts "  Got: $res"
    puts "  Expected: $expected"
  }
}

# Setup for the tests.  Make a backup copy of the files.
#
if [file exist $db_name.org] {
  puts "ERROR: during back-up: $db_name.org exists already."
  return;
}
if [file exist $jrnl_name.org] {
  puts "ERROR: during back-up: $jrnl_name.org exists already."
  return
}
copy_file $db_name $db_name.org
copy_file $jrnl_name $jrnl_name.org

set db_fsize [file size $db_name]
sqlite3 db $db_name
set db_pgsz [db eval {PRAGMA page_size}]
db close
set db_npage [expr {$db_fsize / $db_pgsz}]

# restore in case get the page_size above changed things
copy_file $db_name.org $db_name
copy_file $jrnl_name.org $jrnl_name

# calculate checksum nonce
set pgno 0
set pg_offset [expr $sectsz+((4+$db_pgsz+4)*$pgno)]
set nonce [hexio_get_int [hexio_read $jrnl_name [expr $pg_offset+4+$db_pgsz] 4]]
for {set i [expr $db_pgsz-200]} {$i>0} {set i [expr $i-200]} {
  set byte [hexio_get_int [hexio_read $jrnl_name [expr $pg_offset+4+$i] 1]]
  set nonce [expr $nonce-$byte]
}

# write the 8 byte magic string
hexio_write $jrnl_name 0 d9d505f920a163d7

# write -1 for number of records
hexio_write $jrnl_name 8 ffffffff

# write 00 for checksum nonce
hexio_write $jrnl_name 12 [format %08x $nonce]

# write page count
hexio_write $jrnl_name 16 [format %08x $db_npage]

# write sector size
hexio_write $jrnl_name 20 [format %08x $sectsz]

# write page size
hexio_write $jrnl_name 24 [format %08x $db_pgsz]

# check the integrity of the database.
sqlite3 db $db_name
do_test restore_jrnl-1.0 {
  catchsql {PRAGMA integrity_check}
} {0 ok}

db close
