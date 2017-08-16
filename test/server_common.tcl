# 2017 July 25
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

ifcapable !server {
  proc return_if_no_server {} {
    finish_test
    return -code return
  }
  return
} else {
  proc return_if_no_server {} {}
}

proc server_sqlite3 {cmd file} {
  sqlite3 $cmd $file -vfs $::server_vfs
}

proc server_reset_db {} {
  catch {db close}
  forcedelete test.db test.db-journal test.db-wal
  file mkdir test.db-journal
  server_sqlite3 db test.db 
}

set ::server_vfs unix-excl
proc server_set_vfs {vfs} {
  set ::server_vfs $vfs
}

