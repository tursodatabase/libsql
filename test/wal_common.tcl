# 2010 June 03
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
# This file contains common code used by many different malloc tests
# within the test suite.
#

proc wal_file_size {nFrame pgsz} {
  expr {24 + ($pgsz+24)*$nFrame}
}

proc wal_frame_count {zFile pgsz} {
  set f [file size $zFile]
  expr {($f - 24) / ($pgsz+24)}
}



