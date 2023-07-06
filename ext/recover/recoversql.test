# 2022 September 13
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

source [file join [file dirname [info script]] recover_common.tcl]
set testprefix recoversql

do_execsql_test 1.0 {
  CREATE TABLE "x.1" (x, y);
  INSERT INTO "x.1" VALUES(1, 1), (2, 2), (3, 3);
  CREATE INDEX "i.1" ON "x.1"(y, x);
}

proc sql_hook {sql} {
  incr ::iSqlHook
  if {$::iSqlHook==$::sql_hook_cnt} { return 4 }
  return 0
}

do_test 1.1 {
  set ::sql_hook_cnt -1
  set ::iSqlHook 0
  set R [sqlite3_recover_init_sql db main sql_hook]
  $R run
  $R finish
} {}

set nSqlCall $iSqlHook

for {set ii 1} {$ii<$nSqlCall} {incr ii} {
  set iSqlHook 0
  set sql_hook_cnt $ii
  do_test 1.$ii.a {
    set R [sqlite3_recover_init_sql db main sql_hook]
    $R run
  } {1}
  do_test 1.$ii.b {
    list [catch { $R finish } msg] $msg
  } {1 {callback returned an error - 4}}
}


finish_test
