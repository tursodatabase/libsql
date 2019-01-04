#!/usr/bin/tclsh
#
# This script is used to run the performance test cases described in
# README-server-edition.html.
#


package require sqlite3

# Default values for command line switches:
set O(-database)  ""
set O(-rows)      [expr 5000000]
set O(-mode)      wal2
set O(-tserver)   "./tserver"
set O(-seconds)   20
set O(-writers)   1
set O(-readers)   0
set O(-integrity) 0
set O(-verbose)   0
set O(-external)   0


proc error_out {err} {
  puts stderr $err
  exit -1
}

proc usage {} {
  puts stderr "Usage: $::argv0 ?OPTIONS?"
  puts stderr ""
  puts stderr "Where OPTIONS are:"
  puts stderr "  -database <database file>             (default: test.db)"
  puts stderr "  -mode <mode>                          (default: wal2)"
  puts stderr "  -rows <number of rows>                (default: 5000000)"
  puts stderr "  -tserver <path to tserver executable> (default: ./tserver)"
  puts stderr "  -seconds <time to run for in seconds> (default: 20)"
  puts stderr "  -writers <number of writer clients>   (default: 1)"
  puts stderr "  -readers <number of reader clients>   (default: 0)"
  puts stderr "  -integrity <number of IC clients>     (default: 0)"
  puts stderr "  -verbose 0|1                          (default: 0)"
  puts stderr "  -external 0|1                         (default: 0)"
  exit -1
}

for {set i 0} {$i < [llength $argv]} {incr i} {
  set opt ""
  set arg [lindex $argv $i]
  set n [expr [string length $arg]-1]
  foreach k [array names ::O] {
    if {[string range $k 0 $n]==$arg} {
      if {$opt==""} {
        set opt $k
      } else {
        error_out "ambiguous option: $arg ($k or $opt)"
      }
    }
  }
  if {$opt==""} { usage }
  if {$i==[llength $argv]-1} {
    error_out "option requires an argument: $opt"
  }
  incr i
  set val [lindex $argv $i]
  switch -- $opt {
    -mode {
      if {$val != "wal" && $val != "wal2"} {
        set xyz "\"wal\" or \"wal2\""
        error_out "Found \"$val\" - expected $xyz"
      }
    }
  }
  set O($opt) [lindex $argv $i]
}
if {$O(-database)==""} {
  set O(-database) "test.db"
}

set O(-rows) [expr $O(-rows)]

#--------------------------------------------------------------------------
# Create and populate the required test database, if it is not already 
# present in the file-system.
#
proc create_test_database {} {
  global O

  if {$O(-external)} return

  if {[file exists $O(-database)]} {
    sqlite3 db $O(-database)

    # Check the schema looks Ok.
    set s [db one {
      SELECT group_concat(name||pk, '.') FROM pragma_table_info('t1');
    }]
    if {$s != "a1.b0.c0.d0"} {
      error_out "Database $O(-database) exists but is not usable (schema)"
    }

    # Check that the row count matches.
    set n [db one { SELECT count(*) FROM t1 }]
    if {$n != $O(-rows)} {
      error_out "Database $O(-database) exists but is not usable (row-count)"
    }
    db close
  } else {
    catch { file delete -force $O(-database)-journal }
    catch { file delete -force $O(-database)-wal }

    if {$O(-verbose)} {
      puts "Building database $O(-database)..."
    }

    sqlite3 db $O(-database)
    db eval {
      CREATE TABLE t1(
        a INTEGER PRIMARY KEY, 
        b BLOB(16), 
        c BLOB(16), 
        d BLOB(400)
      );
      CREATE INDEX i1 ON t1(b);
      CREATE INDEX i2 ON t1(c);

      WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<$O(-rows))
      INSERT INTO t1 
      SELECT i-1, randomblob(16), randomblob(16), randomblob(400) FROM s;
    }
    db close
  }

  switch -- $O(-mode) {
    wal {
      sqlite3 db $O(-database)
      db eval {PRAGMA journal_mode = delete}
      db eval {PRAGMA journal_mode = wal}
      db close
    }

    wal2 {
      sqlite3 db $O(-database)
      db eval {PRAGMA journal_mode = delete}
      db eval {PRAGMA journal_mode = wal2}
      db close
    }
  }
}

#-------------------------------------------------------------------------
# Functions to start and stop the tserver process:
#
#   tserver_start
#   tserver_stop
#
set ::tserver {}
proc tserver_start {} {
  global O
  if {$O(-external)} return
  set cmd "|$O(-tserver) -vfs unix-excl "
  if {$O(-mode)=="wal2"} {
    append cmd " -walautocheckpoint 0 "
  }
  append cmd "$O(-database)"
  set ::tserver [open $cmd]
  fconfigure $::tserver -blocking 0
  fileevent $::tserver readable tserver_data
}

proc tserver_data {} {
  global O
  if {[eof $::tserver]} {
    error_out "tserver has exited"
  }
  set line [gets $::tserver]
  if {$line != "" && $O(-verbose)} {
    puts "tserver: $line"
  }
}

proc tserver_stop {} {
  global O
  if {$O(-external)} return
  close $::tserver
  set fd [socket localhost 9999]
  puts $fd ".stop"
  close $fd
}
#-------------------------------------------------------------------------

set ::nClient 0
set ::client_output [list]

proc client_data {name fd} {
  global O
  if {[eof $fd]} {
    incr ::nClient -1
    close $fd
    return
  }
  set str [gets $fd]
  if {[string trim $str]!=""} {
    if {[string range $str 0 3]=="### "} {
      lappend ::client_output [concat [list name $name] [lrange $str 1 end]]
    } 
    if {$O(-verbose)} {
      puts "$name: $str"
    }
  }
}

proc client_launch {name script} {
  global O
  set fd [socket localhost 9999]
  fconfigure $fd -blocking 0
  puts $fd "PRAGMA synchronous = OFF;"
  puts $fd ".repeat 1"
  puts $fd ".run"
  puts $fd $script
  puts $fd ".seconds $O(-seconds)"
  puts $fd ".run"
  puts $fd ".quit"
  flush $fd
  incr ::nClient
  fileevent $fd readable [list client_data $name $fd]
}

proc client_wait {} {
  while {$::nClient>0} {vwait ::nClient}
}

proc script_writer {bCkpt} {
  global O

  set commit ".mutex_commit"
  set begin "BEGIN CONCURRENT;"
  set ckpt ""
  if {$bCkpt} {
    set ckpt ".checkpoint 2000"
  }

  set tail "randomblob(16), randomblob(16), randomblob(400));"
  return [subst -nocommands {
    $ckpt
    $begin
      REPLACE INTO t1 VALUES(abs(random() % $O(-rows)), $tail
    $commit
  }]
}

proc script_reader {} {
  global O

  return [subst -nocommands {
    BEGIN;
      SELECT * FROM t1 WHERE a>abs((random()%$O(-rows))) LIMIT 10;
    END;
  }]
}

proc script_integrity {} {
  global O

  return {
    .integrity_check
  }
}


create_test_database
tserver_start

for {set i 0} {$i < $O(-writers)} {incr i} {
  client_launch w.$i [script_writer [expr {$i==0 && $O(-mode)=="wal2"}]]
}
for {set i 0} {$i < $O(-readers)} {incr i} {
  client_launch r.$i [script_reader]
}
for {set i 0} {$i < $O(-integrity)} {incr i} {
  client_launch i.$i [script_integrity]
}
client_wait

set name(w) "Writers"
set name(r) "Readers"
set name(i) "Integrity"
foreach r $::client_output {
  array set a $r
  set type [string range $a(name) 0 0]
  incr x($type.ok) $a(ok);
  incr x($type.busy) $a(busy);
  incr x($type.n) 1
  set t($type) 1
}

foreach type [array names t] {
  set nTPS [expr $x($type.ok) / $O(-seconds)]
  set nC [expr $nTPS / $x($type.n)]
  set nTotal [expr $x($type.ok) + $x($type.busy)]
  set bp [format %.2f [expr $x($type.busy) * 100.0 / $nTotal]]
  puts "$name($type): $nTPS transactions/second ($nC per client) ($bp% busy)"
}

tserver_stop

